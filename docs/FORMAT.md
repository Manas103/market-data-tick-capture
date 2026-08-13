# TCAP v1 — column-oriented tick capture format

This is the authoritative specification. The C++ writer/reader
(`include/tick_capture.hpp`) and the Python reader (`python/tickcap/reader.py`)
are two independent implementations of *this document*, not of each other. If
an implementation and this file disagree, this file is right and the
implementation has a bug.

All integers are **little-endian**. All offsets are byte offsets from the
start of the file. There is no alignment padding anywhere; every structure is
packed.

---

## 1. File layout

```
+--------------------------------------+  offset 0
| FileHeader                  56 bytes |
+--------------------------------------+  offset 56
| ColumnSchema[num_columns]   40 each  |
+--------------------------------------+
| Block 0                              |
| Block 1                              |
| ...                                  |
| Block num_blocks-1                   |
+--------------------------------------+  offset block_index_offset
| BlockIndex[num_blocks]      16 each  |
+--------------------------------------+  EOF
```

Blocks begin immediately after the schema, at offset `56 + 40 * num_columns`,
and are stored back to back. Their exact offsets are given by the block index
rather than being derived, so a reader never has to walk the blocks to find
one.

---

## 2. FileHeader (56 bytes)

| Offset | Size | Type   | Field                | Notes |
|-------:|-----:|--------|----------------------|-------|
| 0      | 8    | ASCII  | `magic`              | Exactly `TCAPv001`, no NUL terminator |
| 8      | 4    | u32    | `version`            | 1 for this specification |
| 12     | 4    | u32    | `num_columns`        | 11 for the schema in §4 |
| 16     | 8    | u64    | `num_records`        | Total records across all blocks |
| 24     | 8    | u64    | `num_blocks`         | Number of blocks |
| 32     | 8    | u64    | `block_index_offset` | Absolute offset of the block index |
| 40     | 10   | bytes  | `session_id`         | Raw feed session id, space-padded, not NUL-terminated |
| 50     | 4    | u32    | `records_per_block`  | Writer's block size; the final block may hold fewer |
| 54     | 2    | u16    | `reserved`           | Must be written as 0 |

`num_records`, `num_blocks` and `block_index_offset` are not known until the
capture is closed. A writer must write zeros for them at open and seek back to
patch them at close. A file whose header still reads zero for `num_records` was
not closed cleanly.

---

## 3. ColumnSchema (40 bytes each, `num_columns` of them)

| Offset | Size | Type  | Field       | Notes |
|-------:|-----:|-------|-------------|-------|
| 0      | 32   | ASCII | `name`      | NUL-padded; the name ends at the first NUL |
| 32     | 1    | u8    | `phys_type` | See below |
| 33     | 7    | bytes | `reserved`  | Must be written as 0 |

### Physical types

| Id | Type  | Width | Signed |
|---:|-------|------:|--------|
| 0  | `u8`  | 1     | no  |
| 1  | `u16` | 2     | no  |
| 2  | `u32` | 4     | no  |
| 3  | `u64` | 8     | no  |
| 4  | `i64` | 8     | yes |

Signedness affects how the *scalars* inside an encoded chunk are written (see
§6): signed columns use zigzag varints for them, unsigned columns use plain
varints. Column values themselves are handled throughout as raw 64-bit
patterns; for `i64` that is the two's-complement representation.

---

## 4. Standard schema (v1)

Columns appear in this order. Every field of every message type has a column,
and fields that do not apply to a given record's message type are **always
exactly zero**. That rule is what makes a capture losslessly re-encodable back
to the original wire bytes.

| # | Name             | Type  | Meaning |
|--:|------------------|-------|---------|
| 0 | `capture_ts_ns`  | `u64` | Receiver arrival time, nanoseconds. **Recorded, never regenerated** |
| 1 | `exchange_ts_ns` | `u64` | Timestamp carried in the message |
| 2 | `seq_num`        | `u64` | Feed sequence number of this message |
| 3 | `msg_type`       | `u8`  | ASCII `A`/`E`/`X`/`D`/`U` |
| 4 | `instrument_id`  | `u16` | Instrument |
| 5 | `order_id`       | `u64` | Order id; for Replace, the *old* id |
| 6 | `new_order_id`   | `u64` | Replace only, else 0 |
| 7 | `side`           | `u8`  | ASCII `B`/`S`; Add only, else 0 |
| 8 | `price_ticks`    | `i64` | Add/Replace, else 0 |
| 9 | `qty`            | `u32` | Add/Replace quantity, Execute executed qty, Cancel cancelled qty |
| 10| `pkt_first`      | `u8`  | 1 if this message was first in its datagram, else 0 |

`pkt_first` is what lets a reader rebuild the original datagram boundaries and
therefore the exact transport byte stream, not merely the message contents.

---

## 5. Block

```
+---------------------------------------------+
| magic          u32   = 0x004B4C42 ("BLK\0") |
| record_count   u32                          |
| num_columns    u32                          |
| dir_bytes      u32                          |
+---------------------------------------------+  16 bytes
| directory[num_columns], 8 bytes each        |
+---------------------------------------------+  16 + dir_bytes
| payload of column 0                         |
| payload of column 1                         |
| ...                                         |
+---------------------------------------------+
```

`magic` is the four bytes `42 4C 4B 00`, i.e. `'B','L','K',0` read as a
little-endian u32.

### Directory entry (8 bytes)

| Offset | Size | Type | Field           |
|-------:|-----:|------|-----------------|
| 0      | 1    | u8   | `encoding`      |
| 1      | 3    | —    | `reserved` (0)  |
| 4      | 4    | u32  | `payload_bytes` |

Directory entries are in schema column order, and payloads follow in the same
order. Column *j*'s payload therefore starts at

```
block_offset + 16 + dir_bytes + sum(payload_bytes[0..j-1])
```

which is computable from the directory alone. **This is the point of the
directory**: a reader wanting one column reads 16 bytes, then `dir_bytes`,
then only that column's payload, and never touches the others.

Every column in a block holds exactly `record_count` values.

---

## 6. Encodings

| Id | Name    |
|---:|---------|
| 0  | `RAW`   |
| 1  | `DELTA` |
| 2  | `DOD`   |
| 3  | `FOR`   |
| 4  | `RLE`   |

A writer may choose any encoding that round-trips; the reference writer
encodes each column-block with all five and keeps the smallest. A reader must
support all five.

### Primitives

**Unsigned varint (LEB128).** Seven payload bits per byte, little-endian
groups, high bit set on every byte except the last. Maximum 10 bytes for a
64-bit value.

**Zigzag.** Signed values map to unsigned as `(n << 1) ^ (n >> 63)`
(arithmetic shift), decoded as `(v >> 1) ^ -(v & 1)`. This keeps small
negative numbers cheap: without it, `-1` would encode as ten bytes.

**Signed varint** = zigzag, then unsigned varint.

**Scalar.** Several encodings begin with a single value, written as a *signed*
varint for signed columns and an *unsigned* varint for unsigned columns. This
document calls that a "scalar".

All arithmetic below is performed on 64-bit values with defined wraparound, so
differences between arbitrary `u64` values round-trip exactly even when they
overflow a signed range.

### 6.1 RAW (0)

`record_count` values at the column's natural width, little-endian. Payload is
exactly `record_count * width` bytes.

### 6.2 DELTA (1)

```
scalar             v[0]
signed varint      v[1] - v[0]
signed varint      v[2] - v[1]
...                (record_count - 1 differences)
```

Decode by running addition from `v[0]`.

### 6.3 DOD (2) — delta of delta

```
scalar             v[0]
signed varint      d1 = v[1] - v[0]
signed varint      (v[2]-v[1]) - d1
signed varint      (v[3]-v[2]) - (v[2]-v[1])
...                (record_count - 1 values in total after the scalar)
```

Decode with two running sums: accumulate the signed varints to recover each
successive difference, then accumulate those to recover values. Empty and
single-value chunks write only what exists (nothing, or just the scalar).

### 6.4 FOR (3) — frame of reference

```
scalar             base   (minimum value in the chunk)
unsigned varint    v[0] - base
unsigned varint    v[1] - base
...                (record_count residuals)
```

For signed columns `base` is the signed minimum and residuals are still
unsigned, since `v - base >= 0` by construction.

### 6.5 RLE (4)

```
unsigned varint    run_count
  per run:
    unsigned varint  run_length
    scalar           value
```

The sum of `run_length` over all runs must equal `record_count`.

---

## 7. BlockIndex (16 bytes per block)

| Offset | Size | Type | Field          |
|-------:|-----:|------|----------------|
| 0      | 8    | u64  | `file_offset`  |
| 8      | 4    | u32  | `record_count` |
| 12     | 4    | u32  | `reserved` (0) |

Entries are in block order. `file_offset` points at the block's `magic`.

---

## 8. Column digest (for cross-implementation checks)

To let independent implementations be compared without exchanging files, a
column's **digest** is defined as:

> SHA-256 over the concatenation of every value in the column, in record
> order, each widened to **8 bytes little-endian** (for `i64`, the
> two's-complement bit pattern).

`capture_dump` prints these for all columns; `python/cross_check.py` recomputes
them independently and compares. The definition is deliberately trivial so
that agreement is evidence about the *format*, not about a shared helper.

---

## 9. Reproducibility requirements

A conforming writer **must**:

1. Store the capture timestamp it observed, never regenerate one at write
   time. A writer that stamps "now" as it writes looks correct and silently
   destroys reproducibility.
2. Zero every field that does not apply to a record's message type, so
   re-encoding is deterministic.
3. Record `pkt_first`, so datagram framing is recoverable.

A conforming reader **must** return records in capture order, which is the
order they were appended, which is the order they arrived.

Given those, re-encoding a capture reproduces the original transport byte
stream exactly, and replaying it reproduces book state exactly — including
per-order queue position, and including any sequence gaps the original session
suffered. `replay_verify` checks all of this against SHA-256 digests recorded
by the live run.
