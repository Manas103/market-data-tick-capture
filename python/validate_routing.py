"""Independent cross-check for the multi-venue order router (see README,
"Extended: Multi-Venue Order Router with Pre-Trade Risk").

Reads docs/order_router_route_log.csv, which the C++ router
(src/order_router_bench.cpp, route mode) wrote with the raw per-venue top-of-
book quotes it saw for every order and the venue index it chose. This script
independently recomputes, in Python, which venue should have been chosen
from those same raw quotes -- a from-scratch oracle, not a call into
venue_router.hpp -- and asserts the two agree on every single order across
all 40 simulated sessions.
"""
import csv
import sys


def oracle_route(side, venue_prices_and_has):
    """venue_prices_and_has: list of (price, has) tuples, one per venue.
    Buy wants the lowest-priced venue that has a quote; Sell wants the
    highest-priced venue that has a quote. Ties break to the lowest index.
    """
    best_idx = -1
    best_price = None
    for i, (price, has) in enumerate(venue_prices_and_has):
        if not has:
            continue
        if best_idx == -1:
            best_idx, best_price = i, price
            continue
        better = (price < best_price) if side == "B" else (price > best_price)
        if better:
            best_idx, best_price = i, price
    return best_idx


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "docs/order_router_route_log.csv"
    total = 0
    mismatches = []

    with open(path, newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            side = row["side"]
            venues = [
                (int(row["venue0_price"]), row["venue0_has"] == "1"),
                (int(row["venue1_price"]), row["venue1_has"] == "1"),
                (int(row["venue2_price"]), row["venue2_has"] == "1"),
            ]
            expected = oracle_route(side, venues)
            actual = int(row["chosen_venue"])
            total += 1
            if expected != actual:
                mismatches.append((row["session"], row["order_seq"], expected, actual))

    print(f"orders cross-checked: {total}")
    print(f"mismatches: {len(mismatches)}")
    if mismatches:
        for m in mismatches[:20]:
            print(f"  session={m[0]} order_seq={m[1]} oracle_expected={m[2]} router_chose={m[3]}")
        print("FAIL")
        return 1
    print(f"PASS: independent Python oracle matched the C++ router's venue choice on all {total} orders.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
