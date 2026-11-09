"""Independent cross-check for the fee-aware smart order router (see
README, "Extended: Fee-Aware Smart Order Router").

Reads docs/order_router_sor_log.csv, which the C++ router
(src/order_router_bench.cpp, sor mode) wrote with the raw per-venue depth
it saw for every parent order, the fee-aware allocation it chose, and the
naive best-price allocation it chose. This script independently
recomputes, from scratch in Python, the fee-aware and naive allocations
from those same raw depths and fee schedule, and asserts the two agree
with the C++ router on every one of the 8,000 orders: the exact same set
of (venue, price, qty) fills, in the same order, for both strategies.

It also re-derives the venue fee totals under each strategy and reports
the fee savings the fee-aware router achieves over the naive one, as an
independent check on the number src/order_router_bench.cpp prints.
"""
import csv
import sys

# Must match include/fee_schedule.hpp FeeSchedule::five_venue_default()
# exactly -- this is a from-scratch reimplementation, not a call into the
# C++ schedule, so a drift here would be a real bug this check exists to
# catch.
TAKER_FEE_TICKS = [0.30, 0.28, 0.10, -0.15, -0.25]


def parse_levels(field):
    if not field:
        return []
    out = []
    for part in field.split(";"):
        price, qty = part.split(":")
        out.append((int(price), int(qty)))
    return out


def parse_fills(field):
    if not field:
        return []
    out = []
    for part in field.split("|"):
        venue, price, qty = part.split(":")
        out.append((int(venue), int(price), int(qty)))
    return out


def allocate(side, venue_levels, requested_qty, fee_aware):
    """venue_levels: list (one per venue) of list of (price_ticks, qty),
    best price first, exactly as logged. Returns (fills, filled_qty,
    fee_ticks) with fills as (venue, price, qty) tuples in walk order.
    """
    slices = []
    for v, levels in enumerate(venue_levels):
        fee = TAKER_FEE_TICKS[v]
        for price, qty in levels:
            eff = (price + fee if side == "B" else fee - price) if fee_aware else (
                price if side == "B" else -price
            )
            slices.append((eff, v, price, qty))
    slices.sort(key=lambda s: (s[0], s[1]))

    fills = []
    remaining = requested_qty
    filled = 0
    fee_total = 0.0
    for _, venue, price, qty in slices:
        if remaining <= 0:
            break
        take = min(remaining, qty)
        if take <= 0:
            continue
        fills.append((venue, price, take))
        filled += take
        fee_total += TAKER_FEE_TICKS[venue] * take
        remaining -= take
    return fills, filled, fee_total


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "docs/order_router_sor_log.csv"
    total = 0
    mismatches = []
    total_fee_aware_oracle = 0.0
    total_fee_naive_oracle = 0.0
    total_fee_aware_logged = 0.0
    total_fee_naive_logged = 0.0

    with open(path, newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            side = row["side"]
            qty = int(row["requested_qty"])
            venue_levels = [parse_levels(row[f"venue{v}_levels"]) for v in range(5)]

            oracle_aware_fills, oracle_aware_filled, oracle_aware_fee = allocate(
                side, venue_levels, qty, True
            )
            oracle_naive_fills, oracle_naive_filled, oracle_naive_fee = allocate(
                side, venue_levels, qty, False
            )

            logged_aware_fills = parse_fills(row["aware_fills"])
            logged_naive_fills = parse_fills(row["naive_fills"])
            logged_aware_fee = float(row["aware_fee_ticks"])
            logged_naive_fee = float(row["naive_fee_ticks"])

            total += 1
            ok = (
                oracle_aware_fills == logged_aware_fills
                and oracle_naive_fills == logged_naive_fills
                and abs(oracle_aware_fee - logged_aware_fee) < 1e-6
                and abs(oracle_naive_fee - logged_naive_fee) < 1e-6
            )
            if not ok:
                mismatches.append((row["session"], row["order_seq"]))

            total_fee_aware_oracle += oracle_aware_fee
            total_fee_naive_oracle += oracle_naive_fee
            total_fee_aware_logged += logged_aware_fee
            total_fee_naive_logged += logged_naive_fee

    print(f"orders cross-checked: {total}")
    print(f"mismatches: {len(mismatches)}")
    if mismatches:
        for m in mismatches[:20]:
            print(f"  session={m[0]} order_seq={m[1]}")
        print("FAIL")
        return 1

    savings_oracle = (
        100.0 * (total_fee_naive_oracle - total_fee_aware_oracle) / total_fee_naive_oracle
        if total_fee_naive_oracle
        else 0.0
    )
    savings_logged = (
        100.0 * (total_fee_naive_logged - total_fee_aware_logged) / total_fee_naive_logged
        if total_fee_naive_logged
        else 0.0
    )
    print(
        f"PASS: independent Python oracle matched the C++ router's fee-aware "
        f"and naive allocations on all {total} orders."
    )
    print(f"independent fee-savings recomputation: {savings_oracle:.4f}%")
    print(f"C++-logged fee-savings (for comparison): {savings_logged:.4f}%")
    return 0


if __name__ == "__main__":
    sys.exit(main())
