// Canonical serialization of book state -- the thing the reproducibility
// claim is actually measured against.
//
// "The replayed session produced the same book" is only a real claim if
// "the same" is defined byte-exactly, so this walks the entire universe in
// a fully determined order and emits a byte stream that is then hashed.
// Everything that could vary between two runs of correct code is excluded
// by construction:
//
//   * No pointers or addresses are serialized -- only ids, prices, and
//     quantities.
//   * No floating point. Prices are integer ticks end to end; a double
//     would make the digest hostage to the compiler's FP contraction
//     settings.
//   * No hash-map iteration. Instruments go in ascending id order, sides in
//     a fixed order, price levels in the ordered map's price order, and
//     orders in list (queue) order.
//   * Empty levels cannot appear, because OrderBookL3 erases a level when
//     its last order leaves. Otherwise "level present with zero quantity"
//     and "level absent" would be different byte streams for the same book.
//
// Order queue position is included (orders are emitted in FIFO order with
// their time-priority sequence), so this digest is strictly stronger than
// one taken over an aggregated book: two books with identical quantity at
// every price but a different queue order produce different digests here.
#pragma once
#include <cstdint>
#include <vector>
#include <string>
#include "book_universe.hpp"
#include "sha256.hpp"
#include "le_bytes.hpp"

namespace tickcap {

// Appends one instrument's canonical bytes to `out`.
inline void canonical_instrument_bytes(const OrderBookL3& b, uint16_t instrument_id,
                                       std::vector<uint8_t>& out) {
    put_u16(out, instrument_id);

    put_u8(out, static_cast<uint8_t>(Side::Buy));
    put_u32(out, static_cast<uint32_t>(b.bids().size()));
    for (const auto& kv : b.bids()) {
        put_i64(out, kv.first);
        put_u64(out, kv.second.total_qty);
        put_u32(out, static_cast<uint32_t>(kv.second.orders.size()));
        for (const RestingOrder& o : kv.second.orders) {
            put_u64(out, o.order_id);
            put_u64(out, o.seq_added);
            put_u32(out, o.qty);
        }
    }

    put_u8(out, static_cast<uint8_t>(Side::Sell));
    put_u32(out, static_cast<uint32_t>(b.asks().size()));
    for (const auto& kv : b.asks()) {
        put_i64(out, kv.first);
        put_u64(out, kv.second.total_qty);
        put_u32(out, static_cast<uint32_t>(kv.second.orders.size()));
        for (const RestingOrder& o : kv.second.orders) {
            put_u64(out, o.order_id);
            put_u64(out, o.seq_added);
            put_u32(out, o.qty);
        }
    }
}

// Full-universe canonical bytes. Materialized in one buffer; fine for the
// book sizes this project runs, and it is what the byte-level equality test
// compares when it wants to report *where* two states diverge.
inline std::vector<uint8_t> canonical_book_bytes(const BookUniverse& u) {
    std::vector<uint8_t> out;
    put_u64(out, static_cast<uint64_t>(u.num_instruments()));
    for (uint16_t i = 0; i < u.num_instruments(); ++i) {
        canonical_instrument_bytes(u.book(i), i, out);
    }
    return out;
}

// Same byte stream, hashed incrementally so the whole state never has to be
// resident at once.
inline std::string canonical_book_digest(const BookUniverse& u) {
    Sha256 h;
    std::vector<uint8_t> hdr;
    put_u64(hdr, static_cast<uint64_t>(u.num_instruments()));
    h.update(hdr);
    std::vector<uint8_t> buf;
    for (uint16_t i = 0; i < u.num_instruments(); ++i) {
        buf.clear();
        canonical_instrument_bytes(u.book(i), i, buf);
        h.update(buf);
    }
    return h.hex();
}

// A compact top-of-book summary used to cross-check the independent Python
// reader against the C++ decoder. Deliberately a different, simpler view
// than the full canonical form: agreeing on both a rich structure and a
// reduced projection of it is a stronger cross-check than agreeing on one.
struct TopOfBook {
    uint16_t instrument_id = 0;
    bool     has_bid = false;
    bool     has_ask = false;
    int64_t  bid_ticks = 0;
    int64_t  ask_ticks = 0;
    uint64_t bid_qty = 0;
    uint64_t ask_qty = 0;
    uint32_t bid_orders = 0;
    uint32_t ask_orders = 0;
    uint64_t resting = 0;
};

inline TopOfBook top_of_book(const BookUniverse& u, uint16_t id) {
    const OrderBookL3& b = u.book(id);
    TopOfBook t;
    t.instrument_id = id;
    t.has_bid = b.has_bid();
    t.has_ask = b.has_ask();
    if (t.has_bid) {
        t.bid_ticks = b.best_bid_ticks();
        t.bid_qty   = b.best_bid_qty();
        t.bid_orders = static_cast<uint32_t>(b.bids().begin()->second.orders.size());
    }
    if (t.has_ask) {
        t.ask_ticks = b.best_ask_ticks();
        t.ask_qty   = b.best_ask_qty();
        t.ask_orders = static_cast<uint32_t>(b.asks().begin()->second.orders.size());
    }
    t.resting = b.resting_orders();
    return t;
}

} // namespace tickcap
