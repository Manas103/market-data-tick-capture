// Multi-venue order router demo and measurement harness.
//
// Two modes, each proving one claim:
//
//   risk  - seeds exactly 500 breach orders (position, notional and
//           message-rate, mixed) into a stream of otherwise-valid orders
//           and checks that PreTradeRiskChecker rejects every seeded breach
//           and accepts every valid order. Writes docs/pre_trade_risk_output.txt.
//
//   route - runs 40 independent simulated sessions, each with 3 venues
//           running their own synthetic order flow (so their books quote
//           different prices), routes a stream of client orders to the
//           best-priced venue, and logs every decision (instrument, side,
//           all 3 venues' quotes, chosen venue) to a CSV that
//           python/validate_routing.py re-derives independently and
//           compares against.
//
// Neither mode puts anything on a real wire -- that half of the claim
// ("before the wire") is validated by construction: risk-rejected orders
// never reach VenueRouter::route at all, and the actual OUCH order-entry
// transport with its own measured tick-to-order latency is the existing
// Sep. 2026 extension (ouch_session_test), reused unchanged rather than
// re-measured here.
#include <algorithm>
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "book_universe.hpp"
#include "market_simulator.hpp"
#include "pre_trade_risk.hpp"
#include "venue_router.hpp"
#include "wire_protocol.hpp"

using namespace tickcap;

namespace {

struct BreachCase {
    int kind; // 0 = position, 1 = notional, 2 = message_rate
};

int run_risk_mode() {
    RiskLimits limits;
    PreTradeRiskChecker checker(limits);

    const int kValidOrders = 4500;
    const int kSeededBreaches = 500;

    int seeded_rejected = 0;
    int valid_accepted = 0;
    int valid_total = 0;
    int false_rejects = 0;
    int breach_counts[3] = {0, 0, 0};

    SplitMix64 rng(0xBEEFC0FFEEull);
    uint64_t ts = 1'000'000'000ull;

    // Interleave: for every 9 valid orders, 1 seeded breach, until both
    // quotas are exhausted. Instrument ids are spread out so each
    // instrument's own position/rate state stays small and predictable.
    int valid_emitted = 0, breach_emitted = 0;
    int next_breach_kind = 0;
    while (valid_emitted < kValidOrders || breach_emitted < kSeededBreaches) {
        ts += 2'000'000ull; // 2ms apart -> well under the 1s/50-msg rate limit for valid flow
        bool emit_breach = (breach_emitted < kSeededBreaches) &&
                            (valid_emitted >= kValidOrders ||
                             (breach_emitted * kValidOrders <= valid_emitted * kSeededBreaches));

        uint16_t instrument = static_cast<uint16_t>(100 + (breach_emitted + valid_emitted) % 20);
        Side side = (rng.below(2) == 0) ? Side::Buy : Side::Sell;

        if (emit_breach) {
            int kind = next_breach_kind;
            next_breach_kind = (next_breach_kind + 1) % 3;
            breach_counts[kind]++;
            RiskDecision d;
            if (kind == 0) {
                // Position breach: single order whose quantity alone exceeds
                // max_position on a fresh instrument id reserved for this case.
                uint16_t pos_instr = static_cast<uint16_t>(900 + breach_emitted);
                d = checker.check(pos_instr, Side::Buy, 10'000, limits.max_position + 1, ts);
            } else if (kind == 1) {
                // Notional breach: price * qty exceeds max_notional_ticks.
                uint16_t not_instr = static_cast<uint16_t>(1200 + breach_emitted);
                int64_t qty = (limits.max_notional_ticks / 10'000) + 10;
                d = checker.check(not_instr, side, 10'000, static_cast<uint32_t>(qty), ts);
            } else {
                // Message-rate breach: burst max_msgs_per_window+1 messages
                // on one fresh instrument within a 1ms span (<< window_ns).
                uint16_t rate_instr = static_cast<uint16_t>(1500 + breach_emitted);
                RiskDecision last{true, RiskReject::None};
                for (uint32_t k = 0; k < limits.max_msgs_per_window + 1; ++k) {
                    last = checker.check(rate_instr, Side::Buy, 10'000, 10, ts + k);
                }
                d = last;
            }
            if (!d.accepted) seeded_rejected++;
            breach_emitted++;
        } else {
            // Valid order: small size, moderate price, well inside every limit.
            int64_t price = 9'000 + static_cast<int64_t>(rng.below(2000));
            uint32_t qty = 1 + rng.below(50);
            RiskDecision d = checker.check(instrument, side, price, qty, ts);
            valid_total++;
            if (d.accepted) valid_accepted++;
            else false_rejects++;
            valid_emitted++;
        }
    }

    std::ofstream out("docs/pre_trade_risk_output.txt");
    auto report = [&](std::ostream& os) {
        os << "seeded breaches injected       : " << kSeededBreaches << "\n";
        os << "  position breaches            : " << breach_counts[0] << "\n";
        os << "  notional breaches             : " << breach_counts[1] << "\n";
        os << "  message-rate breaches         : " << breach_counts[2] << "\n";
        os << "seeded breaches rejected       : " << seeded_rejected << " / " << kSeededBreaches << "\n";
        os << "valid orders submitted         : " << valid_total << "\n";
        os << "valid orders accepted          : " << valid_accepted << " / " << valid_total << "\n";
        os << "valid orders falsely rejected  : " << false_rejects << "\n";
        if (seeded_rejected == kSeededBreaches && false_rejects == 0) {
            os << "PASS: all seeded breaches rejected, zero false rejects on valid orders.\n";
        } else {
            os << "FAIL\n";
        }
    };
    report(std::cout);
    report(out);

    return (seeded_rejected == kSeededBreaches && false_rejects == 0) ? 0 : 1;
}

// Advances `sim`/`books` by one message each, directly (no network --
// transport is covered by the existing OUCH/gateway extension).
void step_venue(MarketSimulator& sim, BookUniverse& books, uint64_t& seq) {
    DecodedMsg m = sim.next();
    books.apply(m, seq++);
}

VenueQuote top_of_book(const BookUniverse& books, uint16_t instrument, Side side);

int run_route_mode() {
    const int kNumVenues = 3;
    const int kNumSessions = 40;
    const int kWarmupMsgsPerVenue = 4000;
    const int kClientOrdersPerSession = 200;
    const int kNumInstrumentsUsed = 32; // subset, so venues' books stay populated

    std::ofstream log("docs/order_router_route_log.csv");
    log << "session,order_seq,instrument,side,venue0_price,venue0_has,venue1_price,venue1_has,"
           "venue2_price,venue2_has,chosen_venue\n";

    int total_orders = 0;

    for (int session = 0; session < kNumSessions; ++session) {
        std::vector<BookUniverse> books;
        std::vector<MarketSimulator> sims;
        books.reserve(kNumVenues);
        sims.reserve(kNumVenues);
        for (int v = 0; v < kNumVenues; ++v) {
            books.emplace_back(kNumInstrumentsUsed);
            sims.emplace_back(static_cast<uint64_t>(session) * 1000 + v * 97 + 1, kNumInstrumentsUsed);
        }
        std::vector<uint64_t> seqs(kNumVenues, 1);

        for (int v = 0; v < kNumVenues; ++v) {
            for (int i = 0; i < kWarmupMsgsPerVenue; ++i) step_venue(sims[v], books[v], seqs[v]);
        }

        SplitMix64 rng(static_cast<uint64_t>(session) * 7919 + 13);
        for (int o = 0; o < kClientOrdersPerSession; ++o) {
            // Keep each venue's book alive and quoting throughout the session.
            for (int v = 0; v < kNumVenues; ++v) step_venue(sims[v], books[v], seqs[v]);

            uint16_t instrument = static_cast<uint16_t>(rng.below(kNumInstrumentsUsed));
            Side side = (rng.below(2) == 0) ? Side::Buy : Side::Sell;

            std::vector<VenueQuote> quotes(kNumVenues);
            for (int v = 0; v < kNumVenues; ++v) {
                quotes[v] = top_of_book(books[v], instrument, side);
            }
            int chosen = VenueRouter::route(side, quotes);

            log << session << ',' << o << ',' << instrument << ','
                << (side == Side::Buy ? 'B' : 'S') << ',';
            for (int v = 0; v < kNumVenues; ++v) {
                log << quotes[v].price_ticks << ',' << (quotes[v].has_price ? 1 : 0) << ',';
            }
            log << chosen << '\n';
            total_orders++;
        }
    }
    log.close();

    std::cout << "sessions=" << kNumSessions << " total_orders=" << total_orders
              << " -> docs/order_router_route_log.csv (cross-check with python/validate_routing.py)\n";
    return 0;
}

VenueQuote top_of_book(const BookUniverse& books, uint16_t instrument, Side side) {
    // Buy order wants the best (lowest) ask it could lift; Sell order wants
    // the best (highest) bid it could hit. BookUniverse exposes per-instrument
    // L3 books via book(); see book_universe.hpp.
    const OrderBookL3& book = books.book(instrument);
    VenueQuote q;
    if (side == Side::Buy) {
        if (book.has_ask()) { q.has_price = true; q.price_ticks = book.best_ask_ticks(); }
    } else {
        if (book.has_bid()) { q.has_price = true; q.price_ticks = book.best_bid_ticks(); }
    }
    return q;
}

} // namespace

int main(int argc, char** argv) {
    std::string mode = argc > 1 ? argv[1] : "route";
    if (mode == "risk") return run_risk_mode();
    if (mode == "route") return run_route_mode();
    std::cerr << "usage: order_router_bench [risk|route]\n";
    return 2;
}
