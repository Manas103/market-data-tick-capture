#!/usr/bin/env python3
"""Publishes one or more TCAP sessions into a date/symbol partitioned Parquet
warehouse, queryable with DuckDB.

A TCAP capture has no calendar date of its own -- docs/FORMAT.md's header
carries a session id and a record count, nothing calendar-shaped -- so a
real publication pipeline is told which trading day a capture belongs to,
exactly as this script is: each argument after the output directory is
`date=path.tcap`.

Partitioning is Hive-style (`date=YYYY-MM-DD/symbol=SYMnnn/part.parquet`),
the layout DuckDB's `read_parquet(..., hive_partitioning=1)` understands
without any extra metadata, and the layout
python/validate_publication.py's DuckDB side reads back.

Uses the SAME independent reader (tickcap.TickCapture) the rest of this
project's Python side already depends on -- nothing here re-implements TCAP
decoding.
"""
from __future__ import annotations

import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq

from tickcap import TickCapture


def symbol_name(instrument_id: int) -> str:
    return f"SYM{instrument_id:03d}"


def publish_one(date: str, capture_path: str, out_dir: str) -> tuple[int, int]:
    cap = TickCapture(capture_path)
    df = cap.to_dataframe()
    num_records = cap.num_records
    cap.close()

    df["symbol"] = df["instrument_id"].map(symbol_name)
    written = 0
    for symbol, group in df.groupby("symbol", sort=True):
        part_dir = os.path.join(out_dir, f"date={date}", f"symbol={symbol}")
        os.makedirs(part_dir, exist_ok=True)
        table = pa.Table.from_pandas(group.drop(columns=["symbol"]), preserve_index=False)
        pq.write_table(table, os.path.join(part_dir, "part.parquet"))
        written += len(group)
    return written, num_records


def main() -> int:
    if len(sys.argv) < 3:
        print(f"usage: {sys.argv[0]} <output_dir> <date=capture.tcap> [date=capture.tcap ...]")
        return 1

    out_dir = sys.argv[1]
    total_written = 0
    total_records = 0
    for arg in sys.argv[2:]:
        date, path = arg.split("=", 1)
        written, records = publish_one(date, path, out_dir)
        if written != records:
            print(f"FAIL: {path}: published {written} of {records} records")
            return 1
        print(f"{date}: published {written} records from {path} -> {out_dir}/date={date}/")
        total_written += written
        total_records += records

    print(f"total published: {total_written} records across {len(sys.argv) - 2} sessions")
    return 0


if __name__ == "__main__":
    sys.exit(main())
