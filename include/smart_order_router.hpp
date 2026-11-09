// Fee-aware smart order router: splits one parent order across up to five
// simulated venues' displayed depth, choosing the allocation that minimizes
// net-of-fee cost rather than displayed price alone.
//
// VenueRouter (venue_router.hpp) answers "which single venue has the best
// price" from one quote per venue. This is a different and strictly harder
// question: given a parent order that may be larger than any one venue's
// top-of-book size, and given that each venue charges or rebates a
// different taker fee (fee_schedule.hpp), what is the cheapest way to fill
// it, walking multiple price levels and multiple venues, net of fees?
//
// "Best execution" here means lowest net-of-fee cost: for a Buy, price paid
// plus fee; for a Sell, fee paid minus price received (so that lower is
// always better for both sides, and a single sort direction covers both --
// see effective_cost below). A venue's negative fee (a taker rebate) can
// make a worse displayed price the cheaper net choice, which is the entire
// reason a fee-aware router earns its keep over a best-price-only one.
#pragma once
#include <algorithm>
#include <cstdint>
#include <vector>

#include "fee_schedule.hpp"
#include "wire_protocol.hpp"

namespace tickcap {

struct VenueLevel {
    int64_t  price_ticks = 0;
    uint64_t qty = 0;
};

// One venue's visible depth, best (most aggressive) price first. Up to
// kMaxDepth levels; a venue with fewer resting levels than that is fine,
// an empty book (no liquidity on the required side at all) is fine too.
using VenueBook = std::vector<VenueLevel>;

struct SorFill {
    int      venue = -1;
    int64_t  price_ticks = 0;
    uint64_t qty = 0;
};

struct SorResult {
    std::vector<SorFill> fills;      // in the order the router walked them
    uint64_t requested_qty = 0;
    uint64_t filled_qty = 0;
    double   gross_notional_ticks = 0.0;  // sum(price_ticks * qty), unsigned
    double   total_fee_ticks = 0.0;       // sum(fee_per_share * qty); can be
                                           // negative (a net rebate overall)
};

class SmartOrderRouter {
public:
    // fee_aware = false reproduces the naive baseline this is measured
    // against: a router that walks venues and levels by displayed price
    // alone, blind to fees, even though the fees it actually incurs are
    // still the real schedule (a client who ignored fees still pays them).
    static SorResult route(Side side, uint64_t qty,
                            const std::vector<VenueBook>& books,
                            const FeeSchedule& fees,
                            bool fee_aware) {
        struct Slice {
            double   effective_cost;
            int      venue;
            int64_t  price_ticks;
            uint64_t qty;
        };
        std::vector<Slice> slices;
        for (size_t v = 0; v < books.size(); ++v) {
            double fee = fees.taker_fee_ticks[v];
            for (const VenueLevel& lvl : books[v]) {
                double eff;
                if (fee_aware) {
                    eff = (side == Side::Buy) ? static_cast<double>(lvl.price_ticks) + fee
                                               : fee - static_cast<double>(lvl.price_ticks);
                } else {
                    eff = (side == Side::Buy) ? static_cast<double>(lvl.price_ticks)
                                               : -static_cast<double>(lvl.price_ticks);
                }
                slices.push_back({eff, static_cast<int>(v), lvl.price_ticks, lvl.qty});
            }
        }

        // Cheapest net cost first; ties break to the lowest venue index,
        // deterministically, for the same reason VenueRouter's tie-break is
        // deterministic -- it is what makes "the oracle matched the router
        // on every order" a meaningful claim rather than a vacuous one.
        std::stable_sort(slices.begin(), slices.end(), [](const Slice& a, const Slice& b) {
            if (a.effective_cost != b.effective_cost) return a.effective_cost < b.effective_cost;
            return a.venue < b.venue;
        });

        SorResult result;
        result.requested_qty = qty;
        uint64_t remaining = qty;
        for (const Slice& s : slices) {
            if (remaining == 0) break;
            uint64_t take = std::min<uint64_t>(remaining, s.qty);
            if (take == 0) continue;
            result.fills.push_back({s.venue, s.price_ticks, take});
            result.filled_qty += take;
            result.gross_notional_ticks += static_cast<double>(s.price_ticks) * static_cast<double>(take);
            result.total_fee_ticks += fees.taker_fee_ticks[s.venue] * static_cast<double>(take);
            remaining -= take;
        }
        return result;
    }
};

} // namespace tickcap
