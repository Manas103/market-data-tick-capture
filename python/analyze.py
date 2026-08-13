#!/usr/bin/env python3
"""Analytics over a captured session, using the independent Python reader.

This is the "why bother capturing it" half of the project. A capture format
is only worth having if the data comes back out in a shape you can actually
work with, so this loads a session into numpy/pandas and answers the
questions a tick store exists to answer: what was the message mix, how bursty
was the feed, how wide was the spread, how deep was the book.

It also demonstrates the format's selectivity directly -- reading one column
out of eleven touches a fraction of the file, and the script measures that
rather than asserting it.

Usage: analyze.py <capture> [replay_limit] [instrument_id]
"""

from __future__ import annotations

import sys
import time

import numpy as np
import pandas as pd

from tickcap import TickCapture, BookUniverse, ENCODING_NAMES

MSG_NAMES = {ord("A"): "Add", ord("E"): "Execute", ord("X"): "Cancel",
             ord("D"): "Delete", ord("U"): "Replace"}


def human_bytes(n: int) -> str:
    for unit in ("B", "KiB", "MiB", "GiB"):
        if n < 1024 or unit == "GiB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024.0
    return f"{n:.1f} GiB"


def main() -> int:
    capture_path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/session.tcap"
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 400000
    focus = int(sys.argv[3]) if len(sys.argv) > 3 else 0

    cap = TickCapture(capture_path)
    print("=" * 68)
    print(f"session {cap.session.decode('ascii', 'replace')}  --  {capture_path}")
    print("=" * 68)
    print(f"records            : {cap.num_records:,}")
    print(f"blocks             : {cap.num_blocks} x {cap.records_per_block:,} records")
    print(f"file size          : {human_bytes(cap.file_size())}")
    print(f"bytes per record   : {cap.file_size() / max(cap.num_records, 1):.2f}")

    # ---- selectivity: one column vs the whole file --------------------
    cap.bytes_read = 0
    t0 = time.perf_counter()
    prices = cap.read_column("price_ticks")
    one_col_bytes = cap.bytes_read
    one_col_time = time.perf_counter() - t0

    cap.bytes_read = 0
    t0 = time.perf_counter()
    frame = cap.to_dataframe()
    all_col_bytes = cap.bytes_read
    all_col_time = time.perf_counter() - t0

    print("\n--- columnar selectivity (measured) ---")
    print(f"read price_ticks only : {human_bytes(one_col_bytes):>10}  in {one_col_time:.3f}s")
    print(f"read all 11 columns   : {human_bytes(all_col_bytes):>10}  in {all_col_time:.3f}s")
    print(f"single-column read touches {one_col_bytes / max(all_col_bytes, 1) * 100:.1f}% "
          f"of the payload")

    # ---- per-column storage and codec ---------------------------------
    print("\n--- per-column storage ---")
    tally = cap.encodings()
    col_bytes = cap.column_bytes()
    rows = []
    for c in cap.columns:
        winners = tally[c.name]
        dominant = max(winners, key=winners.get) if winners else "-"
        rows.append({
            "column": c.name,
            "codec": dominant,
            "bytes": col_bytes[c.name],
            "bytes/rec": col_bytes[c.name] / max(cap.num_records, 1),
        })
    df_cols = pd.DataFrame(rows).sort_values("bytes", ascending=False)
    print(df_cols.to_string(index=False, float_format=lambda v: f"{v:.3f}"))

    # ---- message mix ---------------------------------------------------
    print("\n--- message mix ---")
    types = frame["msg_type"].map(lambda t: MSG_NAMES.get(int(t), str(t)))
    mix = types.value_counts()
    mix_df = pd.DataFrame({"count": mix, "pct": (mix / len(frame) * 100).round(2)})
    print(mix_df.to_string())

    # ---- feed burstiness ----------------------------------------------
    cap_ts = frame["capture_ts_ns"].to_numpy()
    span_s = (cap_ts.max() - cap_ts.min()) / 1e9 if len(cap_ts) > 1 else 0.0
    print("\n--- feed rate (from recorded capture timestamps) ---")
    print(f"session wall-clock span : {span_s:.3f} s")
    if span_s > 0:
        print(f"mean message rate       : {len(frame) / span_s:,.0f} msgs/sec")
        bucket_ns = 10_000_000  # 10 ms
        buckets = (cap_ts - cap_ts.min()) // bucket_ns
        counts = np.bincount(buckets.astype(np.int64))
        counts = counts[counts > 0]
        pcts = np.percentile(counts, [50, 90, 99, 100])
        print(f"messages per 10ms       : p50={pcts[0]:.0f}  p90={pcts[1]:.0f}  "
              f"p99={pcts[2]:.0f}  max={pcts[3]:.0f}")
        print(f"burstiness (max/median) : {pcts[3] / max(pcts[0], 1):.1f}x")

    # ---- exchange-side inter-message spacing ---------------------------
    ex_ts = frame["exchange_ts_ns"].to_numpy()
    gaps = np.diff(ex_ts.astype(np.int64))
    if gaps.size:
        g = np.percentile(gaps, [50, 90, 99])
        print(f"exchange ts spacing     : p50={g[0]:.0f}ns  p90={g[1]:.0f}ns  p99={g[2]:.0f}ns")

    # ---- price distribution -------------------------------------------
    live_prices = prices[prices != 0]   # 0 = field does not apply to that type
    print("\n--- prices (ticks; 1 tick = $0.01) ---")
    print(f"messages carrying a price : {len(live_prices):,} of {len(prices):,}")
    if len(live_prices):
        q = np.percentile(live_prices, [0, 25, 50, 75, 100])
        print(f"min/p25/median/p75/max    : {q[0]:.0f} / {q[1]:.0f} / {q[2]:.0f} / "
              f"{q[3]:.0f} / {q[4]:.0f}")

    # ---- book reconstruction + spread ----------------------------------
    n = min(limit, cap.num_records)
    print(f"\n--- replaying {n:,} records to reconstruct books ---")
    cols = {k: frame[k].to_numpy()[:n] for k in
            ("msg_type", "instrument_id", "order_id", "new_order_id",
             "side", "price_ticks", "qty", "seq_num")}

    books = BookUniverse(num_instruments=128)
    spreads = []
    sample_every = max(n // 2000, 1)
    t0 = time.perf_counter()
    for i in range(n):
        books.apply(int(cols["msg_type"][i]), int(cols["instrument_id"][i]),
                    int(cols["order_id"][i]), int(cols["new_order_id"][i]),
                    int(cols["side"][i]), int(cols["price_ticks"][i]),
                    int(cols["qty"][i]), int(cols["seq_num"][i]))
        if i % sample_every == 0:
            b = books.books[focus]
            if b.bids and b.asks:
                spreads.append(min(b.asks) - max(b.bids))
    replay_s = time.perf_counter() - t0
    print(f"replay took {replay_s:.2f}s  ({n / max(replay_s, 1e-9):,.0f} msgs/sec in Python)")
    print(f"resting orders across all instruments : {books.total_resting():,}")
    print(f"orders referencing unseen ids         : {books.unknown_order_refs:,}")

    if spreads:
        s = np.array(spreads)
        print(f"\n--- instrument {focus} spread over the session ({len(s)} samples) ---")
        q = np.percentile(s, [50, 90, 99])
        print(f"median {q[0]:.0f} ticks   p90 {q[1]:.0f}   p99 {q[2]:.0f}   "
              f"min {s.min():.0f}   max {s.max():.0f}")

    # ---- depth across instruments --------------------------------------
    depth_rows = []
    for iid in range(len(books.books)):
        b = books.books[iid]
        bid_px, bq, bn, ask_px, aq, an = b.top()
        if bid_px is None or ask_px is None:
            continue
        depth_rows.append({
            "instrument": iid,
            "bid": bid_px, "bid_qty": bq, "bid_orders": bn,
            "ask": ask_px, "ask_qty": aq, "ask_orders": an,
            "spread": ask_px - bid_px,
            "bid_levels": len(b.bids), "ask_levels": len(b.asks),
            "resting": b.resting(),
        })
    depth = pd.DataFrame(depth_rows)
    if not depth.empty:
        print(f"\n--- book depth across {len(depth)} two-sided instruments ---")
        print(f"price levels per side : median bid {depth['bid_levels'].median():.0f}, "
              f"median ask {depth['ask_levels'].median():.0f}")
        print(f"orders at touch       : median bid {depth['bid_orders'].median():.0f}, "
              f"median ask {depth['ask_orders'].median():.0f}")
        print(f"spread (ticks)        : median {depth['spread'].median():.0f}, "
              f"min {depth['spread'].min()}, max {depth['spread'].max()}")
        print("\nwidest five spreads:")
        print(depth.nlargest(5, "spread")[
            ["instrument", "bid", "ask", "spread", "resting"]
        ].to_string(index=False))

        # An order-by-order book knows something an aggregated one cannot:
        # how much size sits ahead of a specific order in the queue.
        busiest = int(depth.nlargest(1, "bid_orders")["instrument"].iloc[0])
        bb = books.books[busiest]
        px = max(bb.bids)
        queue = list(bb.bids[px].items())
        print(f"\nqueue at instrument {busiest} best bid {px} "
              f"({len(queue)} orders, L3-only view):")
        for oid, (seq, qty) in queue[:5]:
            ahead = bb.qty_ahead(ord("B"), px, oid)
            print(f"  order {oid:<9} qty {qty:<5} seq {seq:<9} qty ahead of it: {ahead}")

    cap.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
