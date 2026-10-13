#!/usr/bin/env python3
"""Cross-checks the published Parquet warehouse (python/publish_parquet.py)
against the raw TCAP captures it was published from.

Two independent paths compute the same per-(date, symbol) aggregates:

  1. This script reads each TCAP capture directly with tickcap.TickCapture
     (numpy, no pandas, no pyarrow) and computes record counts, total qty,
     and a msg_type histogram per (date, symbol) from the raw columns.
  2. DuckDB queries the published Parquet directory with Hive-style
     partitioning (`read_parquet(..., hive_partitioning=1)`) and computes
     the same three aggregates from the files publish_parquet.py wrote.

Agreement between the two is what makes "date and symbol partitioned
Parquet publication" a validated claim rather than "a script ran without
throwing". Neither path imports the other's aggregation code.
"""
from __future__ import annotations

import sys

import duckdb
import numpy as np

from tickcap import TickCapture

MSG_TYPE_NAMES = {65: "A", 69: "E", 88: "X", 68: "D", 85: "U"}  # 'A' 'E' 'X' 'D' 'U'


def symbol_name(instrument_id: int) -> str:
    return f"SYM{instrument_id:03d}"


def independent_aggregates(date: str, capture_path: str) -> dict:
    """Computes per-symbol (count, qty_sum, msg_type histogram) straight
    from the TCAP columns, with no pandas groupby and no pyarrow involved.
    """
    cap = TickCapture(capture_path)
    instrument_id = cap.read_column("instrument_id")
    qty = cap.read_column("qty")
    msg_type = cap.read_column("msg_type")
    cap.close()

    out = {}
    for inst in np.unique(instrument_id):
        mask = instrument_id == inst
        sym = symbol_name(int(inst))
        hist = {}
        mt = msg_type[mask]
        for code, name in MSG_TYPE_NAMES.items():
            c = int(np.sum(mt == code))
            if c:
                hist[name] = c
        out[(date, sym)] = {
            "count": int(np.sum(mask)),
            "qty_sum": int(np.sum(qty[mask])),
            "hist": hist,
        }
    return out


def duckdb_aggregates(out_dir: str) -> dict:
    con = duckdb.connect()
    # DuckDB infers the Hive `date=` partition value as a DATE column since
    # it is shaped like one; cast it back to text so it compares equal to
    # the plain date strings this script's own labels use.
    rows = con.execute(
        f"""
        SELECT CAST(date AS VARCHAR) AS date, symbol, msg_type, COUNT(*) AS n, SUM(qty) AS qty_sum
        FROM read_parquet('{out_dir}/**/*.parquet', hive_partitioning=1)
        GROUP BY date, symbol, msg_type
        """
    ).fetchall()
    out: dict = {}
    for date, symbol, msg_type, n, qty_sum in rows:
        key = (date, symbol)
        rec = out.setdefault(key, {"count": 0, "qty_sum": 0, "hist": {}})
        name = MSG_TYPE_NAMES.get(msg_type, chr(msg_type))
        rec["hist"][name] = rec["hist"].get(name, 0) + int(n)
        rec["count"] += int(n)
        rec["qty_sum"] += int(qty_sum)
    return out


def main() -> int:
    if len(sys.argv) < 3:
        print(f"usage: {sys.argv[0]} <published_dir> <date=capture.tcap> [date=capture.tcap ...]")
        return 1

    out_dir = sys.argv[1]
    independent: dict = {}
    for arg in sys.argv[2:]:
        date, path = arg.split("=", 1)
        independent.update(independent_aggregates(date, path))

    published = duckdb_aggregates(out_dir)

    keys = sorted(set(independent) | set(published))
    mismatches = []
    for key in keys:
        a = independent.get(key)
        b = published.get(key)
        if a is None:
            mismatches.append((key, "published partition with no matching capture data", a, b))
            continue
        if b is None:
            mismatches.append((key, "capture data missing from published partitions", a, b))
            continue
        if a["count"] != b["count"] or a["qty_sum"] != b["qty_sum"] or a["hist"] != b["hist"]:
            mismatches.append((key, "aggregate disagreement", a, b))

    total_records_independent = sum(v["count"] for v in independent.values())
    total_records_published = sum(v["count"] for v in published.values())

    print(f"partitions cross-checked: {len(keys)}")
    print(f"records (independent TCAP read): {total_records_independent}")
    print(f"records (DuckDB over published Parquet): {total_records_published}")

    if mismatches:
        for key, reason, a, b in mismatches[:20]:
            print(f"  MISMATCH {key}: {reason} independent={a} published={b}")
        print("FAIL")
        return 1

    print(f"PASS: {len(keys)} of {len(keys)} (date, symbol) partitions agree exactly "
          f"between the independent TCAP read and DuckDB over the published Parquet.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
