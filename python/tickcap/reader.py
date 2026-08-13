"""Independent reader for the TCAP columnar tick capture format.

This is written from docs/FORMAT.md, not from the C++ headers, and shares no
code with them. That independence is the whole point: if the reader were
generated from the writer, agreement between them would prove nothing except
that the code is self-consistent. Two implementations built from a written
specification agreeing byte for byte is evidence that the specification is
complete and that the format is genuinely portable.

Decoding is vectorized with numpy rather than looped in Python. A capture of
a few million ticks holds tens of millions of encoded values, and a
byte-at-a-time varint loop in CPython turns a sub-second read into minutes.
The varint decoder below computes each value's byte-group and position with
array operations and sums the 7-bit payloads with np.add.reduceat, which
keeps the whole thing in compiled code.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence

import numpy as np

FILE_MAGIC = b"TCAPv001"
BLOCK_MAGIC = 0x004B4C42
FILE_HEADER_SIZE = 56
COLUMN_SCHEMA_SIZE = 40
BLOCK_PREFIX_SIZE = 16
BLOCK_DIR_ENTRY = 8
BLOCK_INDEX_ENTRY = 16

# Encoding ids, per the spec.
ENC_RAW, ENC_DELTA, ENC_DOD, ENC_FOR, ENC_RLE = 0, 1, 2, 3, 4
ENCODING_NAMES = {
    ENC_RAW: "RAW",
    ENC_DELTA: "DELTA",
    ENC_DOD: "DOD",
    ENC_FOR: "FOR",
    ENC_RLE: "RLE",
}

# Physical type ids -> (numpy dtype for RAW storage, is_signed).
PHYS_TYPES = {
    0: ("<u1", False),
    1: ("<u2", False),
    2: ("<u4", False),
    3: ("<u8", False),
    4: ("<i8", True),
}

U64 = np.uint64


def _u64(x) -> np.uint64:
    return np.uint64(x)


# ----------------------------------------------------------------- varints


def _read_uvarint(buf: bytes, off: int):
    """Reads one LEB128 varint. Used for the handful of per-chunk scalars."""
    result = 0
    shift = 0
    while True:
        if off >= len(buf):
            raise ValueError("truncated varint")
        b = buf[off]
        off += 1
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            return result & 0xFFFFFFFFFFFFFFFF, off
        shift += 7
        if shift > 63:
            raise ValueError("varint too long")


def _zigzag_decode_scalar(v: int) -> int:
    return (v >> 1) ^ -(v & 1)


def _read_svarint(buf: bytes, off: int):
    v, off = _read_uvarint(buf, off)
    return _zigzag_decode_scalar(v), off


def _decode_uvarints(payload: bytes, off: int, count: int):
    """Vectorized LEB128 decode of `count` varints starting at `off`.

    Returns (values as uint64 array, offset just past the last value).
    """
    if count == 0:
        return np.zeros(0, dtype=U64), off

    buf = np.frombuffer(payload, dtype=np.uint8, offset=off)
    term = (buf & 0x80) == 0                      # bytes that end a value
    term_idx = np.flatnonzero(term)
    if term_idx.size < count:
        raise ValueError(
            f"expected {count} varints, found {term_idx.size} terminators"
        )

    end = int(term_idx[count - 1])
    buf = buf[: end + 1]
    term = term[: end + 1]

    term_i = term.astype(np.int64)
    # Group id for every byte: how many values have already terminated.
    grp = np.cumsum(term_i) - term_i
    starts = np.empty(count, dtype=np.int64)
    starts[0] = 0
    starts[1:] = term_idx[: count - 1] + 1

    pos = np.arange(buf.size, dtype=np.int64) - starts[grp]
    contrib = (buf & 0x7F).astype(U64) << (_u64(7) * pos.astype(U64))
    # The 7-bit windows are disjoint, so summing is equivalent to OR-ing and
    # reduceat keeps it in one compiled pass.
    vals = np.add.reduceat(contrib, starts).astype(U64)
    return vals, off + end + 1


def _zigzag_decode(v: np.ndarray) -> np.ndarray:
    """Vectorized zigzag, staying in uint64 throughout.

    ~(v >> 1) is the two's-complement negation path; doing it with np.where
    rather than arithmetic avoids numpy promoting uint64 to float64, which is
    the classic way to silently lose the top bits of a 64-bit value.
    """
    odd = (v & _u64(1)).astype(bool)
    half = v >> _u64(1)
    return np.where(odd, ~half, half).astype(U64)


def _decode_svarints(payload: bytes, off: int, count: int):
    vals, off = _decode_uvarints(payload, off, count)
    return _zigzag_decode(vals), off


# ----------------------------------------------------------------- codecs


def _decode_chunk(payload: bytes, encoding: int, phys: int, count: int) -> np.ndarray:
    """Decodes one column chunk into a uint64 array of raw bit patterns."""
    dtype, signed = PHYS_TYPES[phys]

    if encoding == ENC_RAW:
        arr = np.frombuffer(payload, dtype=dtype, count=count)
        # Reinterpret rather than convert: for signed columns the uint64 bit
        # pattern is what everything downstream compares on.
        if signed:
            return arr.astype(np.int64).view(U64)
        return arr.astype(U64)

    if encoding == ENC_DELTA:
        first, off = (_read_svarint(payload, 0) if signed else _read_uvarint(payload, 0))
        out = np.empty(count, dtype=U64)
        out[0] = _u64(first & 0xFFFFFFFFFFFFFFFF)
        if count > 1:
            deltas, _ = _decode_svarints(payload, off, count - 1)
            out[1:] = out[0] + np.cumsum(deltas, dtype=U64)
        return out

    if encoding == ENC_DOD:
        first, off = (_read_svarint(payload, 0) if signed else _read_uvarint(payload, 0))
        out = np.empty(count, dtype=U64)
        out[0] = _u64(first & 0xFFFFFFFFFFFFFFFF)
        if count > 1:
            # First entry is the initial delta; the rest are second
            # differences. Cumsum once to recover deltas, again for values.
            raw, _ = _decode_svarints(payload, off, count - 1)
            deltas = np.cumsum(raw, dtype=U64)
            out[1:] = out[0] + np.cumsum(deltas, dtype=U64)
        return out

    if encoding == ENC_FOR:
        if signed:
            base, off = _read_svarint(payload, 0)
        else:
            base, off = _read_uvarint(payload, 0)
        residuals, _ = _decode_uvarints(payload, off, count)
        return (_u64(base & 0xFFFFFFFFFFFFFFFF) + residuals).astype(U64)

    if encoding == ENC_RLE:
        runs, off = _read_uvarint(payload, 0)
        lengths = np.empty(runs, dtype=np.int64)
        values = np.empty(runs, dtype=U64)
        for i in range(runs):
            ln, off = _read_uvarint(payload, off)
            if signed:
                val, off = _read_svarint(payload, off)
            else:
                val, off = _read_uvarint(payload, off)
            lengths[i] = ln
            values[i] = _u64(val & 0xFFFFFFFFFFFFFFFF)
        out = np.repeat(values, lengths)
        if out.size != count:
            raise ValueError(f"RLE produced {out.size} values, expected {count}")
        return out

    raise ValueError(f"unknown encoding id {encoding}")


# ------------------------------------------------------------------ reader


@dataclass
class ColumnSchema:
    name: str
    phys: int

    @property
    def signed(self) -> bool:
        return PHYS_TYPES[self.phys][1]


@dataclass
class BlockInfo:
    offset: int
    count: int


class TickCapture:
    """Reads a TCAP capture file.

    Columns are read individually and on demand, so asking for one field does
    not decode the other ten -- the same selectivity the format exists for.
    """

    def __init__(self, path: str):
        self.path = path
        self._f = open(path, "rb")

        head = self._f.read(FILE_HEADER_SIZE)
        if len(head) != FILE_HEADER_SIZE or head[:8] != FILE_MAGIC:
            raise ValueError(f"{path}: not a TCAP file")
        self.version = int.from_bytes(head[8:12], "little")
        self.num_columns = int.from_bytes(head[12:16], "little")
        self.num_records = int.from_bytes(head[16:24], "little")
        self.num_blocks = int.from_bytes(head[24:32], "little")
        self.block_index_offset = int.from_bytes(head[32:40], "little")
        self.session = head[40:50]
        self.records_per_block = int.from_bytes(head[50:54], "little")

        raw_schema = self._f.read(COLUMN_SCHEMA_SIZE * self.num_columns)
        self.columns: List[ColumnSchema] = []
        for i in range(self.num_columns):
            rec = raw_schema[i * COLUMN_SCHEMA_SIZE : (i + 1) * COLUMN_SCHEMA_SIZE]
            name = rec[:32].split(b"\x00", 1)[0].decode("ascii")
            self.columns.append(ColumnSchema(name, rec[32]))
        self._by_name = {c.name: i for i, c in enumerate(self.columns)}

        self._f.seek(self.block_index_offset)
        raw_index = self._f.read(BLOCK_INDEX_ENTRY * self.num_blocks)
        self.blocks: List[BlockInfo] = []
        for i in range(self.num_blocks):
            rec = raw_index[i * BLOCK_INDEX_ENTRY : (i + 1) * BLOCK_INDEX_ENTRY]
            self.blocks.append(
                BlockInfo(
                    int.from_bytes(rec[0:8], "little"),
                    int.from_bytes(rec[8:12], "little"),
                )
            )

        self.bytes_read = 0

    def close(self):
        self._f.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    # -- internals -------------------------------------------------------

    def _pread(self, offset: int, length: int) -> bytes:
        self._f.seek(offset)
        data = self._f.read(length)
        if len(data) != length:
            raise ValueError("short read")
        self.bytes_read += length
        return data

    def _block_directory(self, bi: int):
        b = self.blocks[bi]
        prefix = self._pread(b.offset, BLOCK_PREFIX_SIZE)
        if int.from_bytes(prefix[0:4], "little") != BLOCK_MAGIC:
            raise ValueError(f"block {bi}: bad magic")
        count = int.from_bytes(prefix[4:8], "little")
        ncols = int.from_bytes(prefix[8:12], "little")
        dir_bytes = int.from_bytes(prefix[12:16], "little")
        raw = self._pread(b.offset + BLOCK_PREFIX_SIZE, dir_bytes)
        entries = []
        for c in range(ncols):
            e = raw[c * BLOCK_DIR_ENTRY : (c + 1) * BLOCK_DIR_ENTRY]
            entries.append((e[0], int.from_bytes(e[4:8], "little")))
        payload_start = b.offset + BLOCK_PREFIX_SIZE + dir_bytes
        return count, entries, payload_start

    def _read_block_column(self, bi: int, col: int) -> np.ndarray:
        count, entries, payload_start = self._block_directory(bi)
        off = payload_start
        for c, (enc, length) in enumerate(entries):
            if c == col:
                payload = self._pread(off, length)
                return _decode_chunk(payload, enc, self.columns[col].phys, count)
            off += length
        raise IndexError(col)

    # -- public API ------------------------------------------------------

    def column_index(self, name: str) -> int:
        if name not in self._by_name:
            raise KeyError(f"no column named {name!r}; have {list(self._by_name)}")
        return self._by_name[name]

    def read_column(self, name: str, signed: Optional[bool] = None) -> np.ndarray:
        """Reads one column across every block.

        Returns int64 for signed columns and uint64 otherwise.
        """
        col = self.column_index(name)
        parts = [self._read_block_column(bi, col) for bi in range(self.num_blocks)]
        out = np.concatenate(parts) if parts else np.zeros(0, dtype=U64)
        want_signed = self.columns[col].signed if signed is None else signed
        return out.view(np.int64) if want_signed else out

    def encodings(self) -> Dict[str, Dict[str, int]]:
        """Per-column tally of which codec won, across blocks."""
        tally: Dict[str, Dict[str, int]] = {c.name: {} for c in self.columns}
        for bi in range(self.num_blocks):
            _, entries, _ = self._block_directory(bi)
            for c, (enc, _len) in enumerate(entries):
                name = ENCODING_NAMES.get(enc, str(enc))
                d = tally[self.columns[c].name]
                d[name] = d.get(name, 0) + 1
        return tally

    def column_bytes(self) -> Dict[str, int]:
        """Compressed payload bytes per column, summed over blocks."""
        totals = {c.name: 0 for c in self.columns}
        for bi in range(self.num_blocks):
            _, entries, _ = self._block_directory(bi)
            for c, (_enc, length) in enumerate(entries):
                totals[self.columns[c].name] += length
        return totals

    def to_dataframe(self, columns: Optional[Sequence[str]] = None):
        """Loads columns into a pandas DataFrame (all of them by default)."""
        import pandas as pd

        names = list(columns) if columns else [c.name for c in self.columns]
        return pd.DataFrame({n: self.read_column(n) for n in names})

    def file_size(self) -> int:
        return os.path.getsize(self.path)

    def __repr__(self) -> str:
        return (
            f"TickCapture({self.path!r}, records={self.num_records}, "
            f"blocks={self.num_blocks}, session={self.session!r})"
        )
