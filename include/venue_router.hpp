// Multi-venue best-price routing.
//
// Given the current top-of-book quote from each of N simulated venues for
// one instrument, chooses the venue offering the best price for a client
// order: the lowest ask for a Buy, the highest bid for a Sell. A venue with
// no resting liquidity on the required side is simply not a candidate, not
// an error (a thin or empty venue is an ordinary, expected state, not a
// fault). Ties (two venues quoting the identical best price) break to the
// lowest venue index, deterministically -- the independent Python oracle in
// python/validate_routing.py applies the exact same tie-break rule, which is
// what makes "matched the oracle on every order" a meaningful claim rather
// than one that is vacuously true whenever ties happen not to occur.
#pragma once
#include <cstdint>
#include <vector>
#include "wire_protocol.hpp"

namespace tickcap {

struct VenueQuote {
    bool    has_price   = false;
    int64_t price_ticks = 0;
};

class VenueRouter {
public:
    // Returns the chosen venue's index, or -1 if no venue quotes a price on
    // the required side at all.
    static int route(Side side, const std::vector<VenueQuote>& quotes) {
        int best_idx = -1;
        int64_t best_price = 0;
        for (size_t i = 0; i < quotes.size(); ++i) {
            if (!quotes[i].has_price) continue;
            if (best_idx < 0) {
                best_idx = static_cast<int>(i);
                best_price = quotes[i].price_ticks;
                continue;
            }
            bool better = (side == Side::Buy) ? (quotes[i].price_ticks < best_price)
                                               : (quotes[i].price_ticks > best_price);
            if (better) {
                best_idx = static_cast<int>(i);
                best_price = quotes[i].price_ticks;
            }
        }
        return best_idx;
    }
};

} // namespace tickcap
