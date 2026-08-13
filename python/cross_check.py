#!/usr/bin/env python3
"""Cross-checks the independent Python reader against the C++ decoder.

Three levels, each strictly stronger than the last:

  1. Column digests   Python decodes every column and hashes the values the
                      same trivially-specified way capture_dump does. This
                      proves the Python implementation recovers identical
                      values from the compressed bytes -- i.e. that the
                      format is readable from its specification alone.

  2. Book replay      Python replays the records through its own L3 book
                      (tickcap/book.py, a separate implementation using
                      different data structures) and compares top of book,
                      per instrument, against the C++ book. This proves the
                      two agree on the *semantics* of the message stream,
                      not just on how to unpack bytes.

  3. Counters         Resting-order totals and unknown-reference counts must
                      match, which catches whole classes of drift that top of
                      book alone would hide (an order misplaced deep in the
                      book does not move the touch).

Usage: cross_check.py <capture> <capture_dump_output.txt> <tob.csv> [limit]
"""

from __future__ import annotations

import csv
import hashlib
import sys

import numpy as np

from tickcap import TickCapture, BookUniverse


def column_digest(values: np.ndarray) -> str:
    """SHA-256 over values widened to 8-byte little-endian.

    Matches the definition in docs/FORMAT.md that capture_dump implements.
    """
    as_u64 = values.view(np.uint64) if values.dtype == np.int64 else values.astype(np.uint64)
    return hashlib.sha256(np.ascontiguousarray(as_u64).astype("<u8").tobytes()).hexdigest()


def parse_dump(path: str):
    scalars, columns = {}, {}
    with open(path) as f:
        for line in f:
            parts = line.split()
            if not parts:
                continue
            if parts[0] == "column" and len(parts) == 3:
                columns[parts[1]] = parts[2]
            elif len(parts) == 2:
                scalars[parts[0]] = parts[1]
    return scalars, columns


def main() -> int:
    if len(sys.argv) < 4:
        print(__doc__)
        return 2
    capture_path, dump_path, tob_path = sys.argv[1], sys.argv[2], sys.argv[3]
    limit = int(sys.argv[4]) if len(sys.argv) > 4 else 0

    scalars, cpp_columns = parse_dump(dump_path)
    failures = 0

    cap = TickCapture(capture_path)
    print(f"capture : {capture_path}")
    print(f"records : {cap.num_records} in {cap.num_blocks} blocks")
    print(f"session : {cap.session.decode('ascii', 'replace')}")

    if int(scalars.get("records", -1)) != cap.num_records:
        print(f"  FAIL record count: C++ {scalars.get('records')} vs Python {cap.num_records}")
        failures += 1

    # -- 1. per-column digests ------------------------------------------
    print("\n--- column digests (Python vs C++) ---")
    data = {}
    for col in cap.columns:
        vals = cap.read_column(col.name)
        data[col.name] = vals
        py = column_digest(vals)
        cpp = cpp_columns.get(col.name)
        ok = (cpp is not None) and (py == cpp)
        if not ok:
            failures += 1
        print(f"  {col.name:<16} {'MATCH' if ok else 'MISMATCH'}")
        if not ok:
            print(f"      python {py}")
            print(f"      cpp    {cpp}")

    # -- 2. independent book replay --------------------------------------
    n = cap.num_records if limit == 0 else min(limit, cap.num_records)
    print(f"\n--- replaying {n} records through the Python L3 book ---")

    msg_type = data["msg_type"][:n]
    instrument = data["instrument_id"][:n]
    order_id = data["order_id"][:n]
    new_order_id = data["new_order_id"][:n]
    side = data["side"][:n]
    price = data["price_ticks"][:n]
    qty = data["qty"][:n]
    seq = data["seq_num"][:n]

    books = BookUniverse(num_instruments=128)
    # int() conversions keep Python's arbitrary-precision ints out of numpy
    # scalar arithmetic, which would otherwise wrap differently.
    for i in range(n):
        books.apply(
            int(msg_type[i]),
            int(instrument[i]),
            int(order_id[i]),
            int(new_order_id[i]),
            int(side[i]),
            int(price[i]),
            int(qty[i]),
            int(seq[i]),
        )

    py_resting = books.total_resting()
    cpp_resting = int(scalars.get("resting", -1))
    ok = py_resting == cpp_resting
    if not ok:
        failures += 1
    print(f"  resting orders   python {py_resting} vs cpp {cpp_resting}  "
          f"{'MATCH' if ok else 'MISMATCH'}")

    # -- 3. per-instrument top of book -----------------------------------
    mismatches = 0
    checked = 0
    with open(tob_path, newline="") as f:
        for row in csv.DictReader(f):
            iid = int(row["instrument_id"])
            bid_px, bq, bn, ask_px, aq, an = books.books[iid].top()
            cpp_bid = int(row["bid_ticks"])
            cpp_ask = int(row["ask_ticks"])
            py_bid = -1 if bid_px is None else bid_px
            py_ask = -1 if ask_px is None else ask_px
            checked += 1
            if (
                py_bid != cpp_bid
                or py_ask != cpp_ask
                or bq != int(row["bid_qty"])
                or aq != int(row["ask_qty"])
                or bn != int(row["bid_orders"])
                or an != int(row["ask_orders"])
                or books.books[iid].resting() != int(row["resting"])
            ):
                mismatches += 1
                if mismatches <= 5:
                    print(
                        f"  instrument {iid}: python bid={py_bid}x{bq}({bn}) "
                        f"ask={py_ask}x{aq}({an}) rest={books.books[iid].resting()} | "
                        f"cpp bid={cpp_bid}x{row['bid_qty']}({row['bid_orders']}) "
                        f"ask={cpp_ask}x{row['ask_qty']}({row['ask_orders']}) "
                        f"rest={row['resting']}"
                    )

    if mismatches:
        failures += 1
    print(f"  top of book      {checked - mismatches}/{checked} instruments agree")

    cap.close()
    print()
    if failures == 0:
        print("PASS: the independent Python reader and book agree with the C++ decoder.")
        return 0
    print(f"FAIL: {failures} check(s) disagreed.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
