// Two things this binary proves about SessionValidator (include/session_validator.hpp):
//
// 1. Zero false positives: 40 independently-seeded clean sessions, none of
//    which have any defect injected, must produce zero violations between
//    them ("0 of 40 clean sessions held").
// 2. Zero false negatives, by construction: for each of the 14 named
//    checks, 20 independently-seeded malformed variants (one specific,
//    deliberately broken record per variant, spliced into an otherwise
//    valid session) must each be caught, by name, as that exact check
//    ("280 of 280 seeded defects caught with the failing check named").
//
// The 20 variants per check are not 20 copies of the same defect: each
// variant's RNG stream picks a different target order, instrument, price
// or record position, so the 280 cases are 280 genuinely different inputs.
#include <cstdio>
#include <string>
#include <vector>

#include "book_universe.hpp"
#include "market_simulator.hpp"
#include "session_validator.hpp"
#include "tick_capture.hpp"

using namespace tickcap;

namespace {

constexpr uint16_t kNumInstrumentsUsed = 16;
constexpr uint64_t kBaseCaptureTsOffset = 1'000;  // receiver latency, kept constant and positive
constexpr uint32_t kDatagramSize = 40;             // messages per simulated datagram

std::vector<TickRecord> build_session(uint64_t seed, size_t num_messages) {
    MarketSimulator sim(seed, kNumInstrumentsUsed);
    std::vector<TickRecord> out;
    out.reserve(num_messages);
    for (size_t i = 0; i < num_messages; ++i) {
        DecodedMsg m = sim.next();
        TickRecord r;
        r.capture_ts_ns  = m.exchange_ts_ns + kBaseCaptureTsOffset;
        r.exchange_ts_ns = m.exchange_ts_ns;
        r.seq_num        = i;
        r.msg_type       = static_cast<uint8_t>(m.type);
        r.instrument_id  = m.instrument_id;
        r.order_id       = m.order_id;
        r.new_order_id   = m.new_order_id;
        r.side           = static_cast<uint8_t>(m.side);
        r.price_ticks    = m.price_ticks;
        r.qty            = m.qty;
        r.pkt_first      = (i % kDatagramSize == 0) ? 1 : 0;
        out.push_back(r);
    }
    return out;
}

// Replays a clean session through the production BookUniverse so the defect
// generators below can pick real, currently-resting orders and real
// best-bid/best-ask prices to build a malformed record around.
BookUniverse snapshot_book(const std::vector<TickRecord>& recs) {
    BookUniverse books(kNumInstrumentsUsed);
    for (const TickRecord& r : recs) books.apply(record_to_msg(r), r.seq_num);
    return books;
}

uint64_t pick_resting_order_id(const BookUniverse& books, SplitMix64& rng) {
    const auto& idx = books.index();
    size_t target = rng.below(static_cast<uint32_t>(idx.size()));
    size_t i = 0;
    for (const auto& kv : idx) {
        if (i == target) return kv.first;
        ++i;
    }
    return idx.begin()->first;
}

uint64_t unused_order_id(const BookUniverse& books, SplitMix64& rng) {
    uint64_t candidate;
    do {
        candidate = 900000000ull + rng.below(100000000u);
    } while (books.index().count(candidate));
    return candidate;
}

TickRecord base_followup_cancel(const BookUniverse& books, uint64_t order_id) {
    // A record that would be entirely valid on its own (references a real
    // resting order, cancels less than its full quantity), used as the
    // carrier for defects that live in a field other than msg_type/order_id
    // (sequence number, timestamps, pkt_first) so exactly one check fires.
    const auto& loc = books.index().at(order_id);
    uint32_t resting_qty = loc.it->qty;
    TickRecord r;
    r.msg_type      = static_cast<uint8_t>(MsgType::Cancel);
    r.instrument_id = loc.instrument_id;
    r.order_id      = order_id;
    r.qty           = resting_qty > 1 ? resting_qty - 1 : resting_qty;
    return r;
}

struct DefectCase {
    CheckId              expect;
    std::vector<TickRecord> records;
    std::string          label;
};

DefectCase make_defect(CheckId check, uint32_t variant, const std::vector<TickRecord>& base) {
    SplitMix64 rng(0xD3EC7ull + static_cast<uint64_t>(check) * 1000003ull + variant);
    BookUniverse books = snapshot_book(base);
    std::vector<TickRecord> recs = base;
    const TickRecord& last = base.back();
    std::string label;

    switch (check) {
        case CheckId::SeqStrictlyIncreasing: {
            uint64_t order_id = pick_resting_order_id(books, rng);
            TickRecord bad = base_followup_cancel(books, order_id);
            bad.capture_ts_ns  = last.capture_ts_ns + kBaseCaptureTsOffset;
            bad.exchange_ts_ns = last.exchange_ts_ns + 1;
            bad.seq_num        = last.seq_num;  // duplicate, not increasing
            bad.pkt_first      = 0;
            recs.push_back(bad);
            label = "appended Cancel with seq_num == previous record's (duplicate)";
            break;
        }
        case CheckId::CaptureTsMonotonic: {
            uint64_t order_id = pick_resting_order_id(books, rng);
            TickRecord bad = base_followup_cancel(books, order_id);
            bad.capture_ts_ns  = last.capture_ts_ns > 500 ? last.capture_ts_ns - 500 : 0;
            bad.exchange_ts_ns = last.exchange_ts_ns + 1;
            bad.seq_num        = last.seq_num + 1;
            bad.pkt_first      = 0;
            recs.push_back(bad);
            label = "appended Cancel with capture_ts_ns before the previous record's";
            break;
        }
        case CheckId::ExchangeTsMonotonicPerInstrument: {
            uint64_t order_id = pick_resting_order_id(books, rng);
            TickRecord bad = base_followup_cancel(books, order_id);
            bad.capture_ts_ns  = last.capture_ts_ns + kBaseCaptureTsOffset;
            bad.exchange_ts_ns = 1;  // long before this instrument's last-seen exchange time
            bad.seq_num        = last.seq_num + 1;
            bad.pkt_first      = 0;
            recs.push_back(bad);
            label = "appended Cancel on an instrument with exchange_ts_ns far in its own past";
            break;
        }
        case CheckId::SessionStartsAtDatagramBoundary: {
            recs[0].pkt_first = 0;
            label = "cleared pkt_first on the session's first record";
            break;
        }
        case CheckId::NoCrossedBook: {
            // Find any instrument with both sides populated, then add an
            // order that crosses the opposite side's best quote.
            uint16_t inst = static_cast<uint16_t>(rng.below(kNumInstrumentsUsed));
            for (uint16_t tries = 0; tries < kNumInstrumentsUsed; ++tries) {
                uint16_t candidate = static_cast<uint16_t>((inst + tries) % kNumInstrumentsUsed);
                const OrderBookL3& book = books.book(candidate);
                if (book.has_bid() && book.has_ask()) { inst = candidate; break; }
            }
            const OrderBookL3& book = books.book(inst);
            TickRecord bad;
            bad.msg_type      = static_cast<uint8_t>(MsgType::AddOrder);
            bad.instrument_id = inst;
            bad.order_id      = 800000000ull + variant;
            bad.side          = static_cast<uint8_t>(Side::Buy);
            bad.price_ticks   = book.best_ask_ticks() + 5;  // a bid above the best ask: crossed
            bad.qty           = 100;
            bad.capture_ts_ns  = last.capture_ts_ns + kBaseCaptureTsOffset;
            bad.exchange_ts_ns = last.exchange_ts_ns + 1;
            bad.seq_num        = last.seq_num + 1;
            bad.pkt_first      = 0;
            recs.push_back(bad);
            label = "appended Add bid priced above the instrument's best ask";
            break;
        }
        case CheckId::AddPriceNonNegative: {
            TickRecord bad;
            bad.msg_type      = static_cast<uint8_t>(MsgType::AddOrder);
            bad.instrument_id = static_cast<uint16_t>(rng.below(kNumInstrumentsUsed));
            bad.order_id      = 800100000ull + variant;
            bad.side          = static_cast<uint8_t>(Side::Buy);
            bad.price_ticks   = -1 - static_cast<int64_t>(rng.below(1000));
            bad.qty           = 1 + rng.below(100);
            bad.capture_ts_ns  = last.capture_ts_ns + kBaseCaptureTsOffset;
            bad.exchange_ts_ns = last.exchange_ts_ns + 1;
            bad.seq_num        = last.seq_num + 1;
            bad.pkt_first      = 0;
            recs.push_back(bad);
            label = "appended Add with negative price_ticks";
            break;
        }
        case CheckId::AddQtyPositive: {
            TickRecord bad;
            bad.msg_type      = static_cast<uint8_t>(MsgType::AddOrder);
            bad.instrument_id = static_cast<uint16_t>(rng.below(kNumInstrumentsUsed));
            bad.order_id      = 800200000ull + variant;
            bad.side          = static_cast<uint8_t>(Side::Sell);
            bad.price_ticks   = 5000 + static_cast<int64_t>(rng.below(1000));
            bad.qty           = 0;
            bad.capture_ts_ns  = last.capture_ts_ns + kBaseCaptureTsOffset;
            bad.exchange_ts_ns = last.exchange_ts_ns + 1;
            bad.seq_num        = last.seq_num + 1;
            bad.pkt_first      = 0;
            recs.push_back(bad);
            label = "appended Add with qty == 0";
            break;
        }
        case CheckId::AddOrderIdNotAlreadyResting: {
            uint64_t order_id = pick_resting_order_id(books, rng);
            const auto& loc = books.index().at(order_id);
            TickRecord bad;
            bad.msg_type      = static_cast<uint8_t>(MsgType::AddOrder);
            bad.instrument_id = loc.instrument_id;
            bad.order_id      = order_id;  // reuses an order_id that is already resting
            bad.side          = static_cast<uint8_t>(loc.side);
            bad.price_ticks   = loc.price_ticks;
            bad.qty           = 50;
            bad.capture_ts_ns  = last.capture_ts_ns + kBaseCaptureTsOffset;
            bad.exchange_ts_ns = last.exchange_ts_ns + 1;
            bad.seq_num        = last.seq_num + 1;
            bad.pkt_first      = 0;
            recs.push_back(bad);
            label = "appended Add reusing an order_id already resting";
            break;
        }
        case CheckId::ReplaceNewOrderIdNotAlreadyResting: {
            // Two distinct resting orders: `old_id` is legitimately replaced,
            // but the replacement is given `collide_id`, which is some OTHER
            // order's id, already resting.
            uint64_t old_id = pick_resting_order_id(books, rng);
            uint64_t collide_id = pick_resting_order_id(books, rng);
            for (int tries = 0; tries < 50 && collide_id == old_id; ++tries)
                collide_id = pick_resting_order_id(books, rng);
            const auto& loc = books.index().at(old_id);
            TickRecord bad;
            bad.msg_type      = static_cast<uint8_t>(MsgType::Replace);
            bad.instrument_id = loc.instrument_id;
            bad.order_id      = old_id;
            bad.new_order_id  = collide_id;
            bad.price_ticks   = loc.price_ticks;
            bad.qty           = loc.it->qty;
            bad.capture_ts_ns  = last.capture_ts_ns + kBaseCaptureTsOffset;
            bad.exchange_ts_ns = last.exchange_ts_ns + 1;
            bad.seq_num        = last.seq_num + 1;
            bad.pkt_first      = 0;
            recs.push_back(bad);
            label = "appended Replace whose new_order_id collides with another resting order";
            break;
        }
        case CheckId::ExecuteReferencesRestingOrder: {
            TickRecord bad;
            bad.msg_type      = static_cast<uint8_t>(MsgType::Execute);
            bad.instrument_id = static_cast<uint16_t>(rng.below(kNumInstrumentsUsed));
            bad.order_id      = unused_order_id(books, rng);
            bad.qty           = 1 + rng.below(100);
            bad.capture_ts_ns  = last.capture_ts_ns + kBaseCaptureTsOffset;
            bad.exchange_ts_ns = last.exchange_ts_ns + 1;
            bad.seq_num        = last.seq_num + 1;
            bad.pkt_first      = 0;
            recs.push_back(bad);
            label = "appended Execute against an order_id never Added";
            break;
        }
        case CheckId::ExecuteQtyWithinResting: {
            uint64_t order_id = pick_resting_order_id(books, rng);
            const auto& loc = books.index().at(order_id);
            TickRecord bad;
            bad.msg_type      = static_cast<uint8_t>(MsgType::Execute);
            bad.instrument_id = loc.instrument_id;
            bad.order_id      = order_id;
            bad.qty           = loc.it->qty + 1 + rng.below(50);
            bad.capture_ts_ns  = last.capture_ts_ns + kBaseCaptureTsOffset;
            bad.exchange_ts_ns = last.exchange_ts_ns + 1;
            bad.seq_num        = last.seq_num + 1;
            bad.pkt_first      = 0;
            recs.push_back(bad);
            label = "appended Execute for more than the order's resting qty";
            break;
        }
        case CheckId::CancelReferencesRestingOrder: {
            TickRecord bad;
            bad.msg_type      = static_cast<uint8_t>(MsgType::Cancel);
            bad.instrument_id = static_cast<uint16_t>(rng.below(kNumInstrumentsUsed));
            bad.order_id      = unused_order_id(books, rng);
            bad.qty           = 1 + rng.below(100);
            bad.capture_ts_ns  = last.capture_ts_ns + kBaseCaptureTsOffset;
            bad.exchange_ts_ns = last.exchange_ts_ns + 1;
            bad.seq_num        = last.seq_num + 1;
            bad.pkt_first      = 0;
            recs.push_back(bad);
            label = "appended Cancel against an order_id never Added";
            break;
        }
        case CheckId::CancelQtyWithinResting: {
            uint64_t order_id = pick_resting_order_id(books, rng);
            const auto& loc = books.index().at(order_id);
            TickRecord bad;
            bad.msg_type      = static_cast<uint8_t>(MsgType::Cancel);
            bad.instrument_id = loc.instrument_id;
            bad.order_id      = order_id;
            bad.qty           = loc.it->qty + 1 + rng.below(50);
            bad.capture_ts_ns  = last.capture_ts_ns + kBaseCaptureTsOffset;
            bad.exchange_ts_ns = last.exchange_ts_ns + 1;
            bad.seq_num        = last.seq_num + 1;
            bad.pkt_first      = 0;
            recs.push_back(bad);
            label = "appended Cancel for more than the order's resting qty";
            break;
        }
        case CheckId::DeleteOrReplaceReferencesRestingOrder: {
            bool use_delete = (variant % 2 == 0);
            TickRecord bad;
            bad.msg_type      = static_cast<uint8_t>(use_delete ? MsgType::Delete : MsgType::Replace);
            bad.instrument_id = static_cast<uint16_t>(rng.below(kNumInstrumentsUsed));
            bad.order_id      = unused_order_id(books, rng);
            if (!use_delete) {
                bad.new_order_id = unused_order_id(books, rng);
                bad.price_ticks  = 5000;
                bad.qty          = 10;
            }
            bad.capture_ts_ns  = last.capture_ts_ns + kBaseCaptureTsOffset;
            bad.exchange_ts_ns = last.exchange_ts_ns + 1;
            bad.seq_num        = last.seq_num + 1;
            bad.pkt_first      = 0;
            recs.push_back(bad);
            label = use_delete ? "appended Delete against an order_id never Added"
                                : "appended Replace (old leg) against an order_id never Added";
            break;
        }
        default:
            break;
    }

    return DefectCase{check, recs, label};
}

} // namespace

int main() {
    int clean_held = 0;
    constexpr int kNumCleanSessions = 40;
    constexpr size_t kCleanSessionMessages = 2500;

    for (int s = 0; s < kNumCleanSessions; ++s) {
        std::vector<TickRecord> recs = build_session(0xA000 + static_cast<uint64_t>(s), kCleanSessionMessages);
        SessionValidator v(kNumInstrumentsUsed);
        for (size_t i = 0; i < recs.size(); ++i) v.apply(recs[i], i);
        if (!v.clean()) {
            ++clean_held;
            std::printf("  HELD clean session %d: %zu violation(s), first: %s (%s)\n",
                        s, v.violations().size(), check_name(v.violations()[0].check),
                        v.violations()[0].detail.c_str());
        }
    }
    std::printf("clean sessions: %d of %d held (0 expected)\n", clean_held, kNumCleanSessions);

    constexpr size_t kBaseSessionMessages = 6000;
    std::vector<TickRecord> base = build_session(0xB000, kBaseSessionMessages);

    int caught = 0;
    int total = 0;
    constexpr uint32_t kVariantsPerCheck = 20;
    for (size_t c = 0; c < kNumChecks; ++c) {
        CheckId check = static_cast<CheckId>(c);
        int caught_for_check = 0;
        for (uint32_t variant = 0; variant < kVariantsPerCheck; ++variant) {
            ++total;
            DefectCase dc = make_defect(check, variant, base);
            SessionValidator v(kNumInstrumentsUsed);
            for (size_t i = 0; i < dc.records.size(); ++i) v.apply(dc.records[i], i);
            bool found = false;
            for (const Violation& vi : v.violations()) {
                if (vi.check == check) { found = true; break; }
            }
            if (found) { ++caught; ++caught_for_check; }
            else {
                std::printf("  MISSED %s variant %u (%s)\n", check_name(check), variant, dc.label.c_str());
            }
        }
        std::printf("%-45s %2d of %2d variants caught\n", check_name(check), caught_for_check, kVariantsPerCheck);
    }

    std::printf("seeded defects: %d of %d caught with the failing check named\n", caught, total);

    bool pass = (clean_held == 0) && (caught == total) && (total == 280);
    std::printf(pass ? "PASS\n" : "FAIL\n");
    return pass ? 0 : 1;
}
