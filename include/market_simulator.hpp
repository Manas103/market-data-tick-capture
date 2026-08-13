// Synthetic but self-consistent order flow.
//
// "Self-consistent" is the property that matters: every Execute, Cancel,
// Delete and Replace names an order that is actually resting in the book at
// that moment. A generator that emitted random order ids would exercise
// only the decoder's error paths and would leave the book permanently
// empty, so the simulator keeps its own shadow list of live orders and
// draws from it.
//
// Determinism is the second property that matters, because the whole
// project is about reproducibility. The PRNG here is splitmix64, written
// out explicitly, rather than <random>: std::mt19937 is specified exactly,
// but std::uniform_int_distribution is not -- the same engine and seed can
// yield different draws across standard-library implementations. Anything
// that fed a reproducibility claim through a library-defined distribution
// would be reproducible only by accident.
//
// This is not captured exchange data and does not claim to model real
// market microstructure. It produces order flow with the right *shape* for
// exercising a decoder: many instruments, prices clustered in a band around
// a slowly drifting reference, a realistic mix of event types, and a
// timestamp series that advances at a near-constant rate with jitter.
#pragma once
#include <cstdint>
#include <vector>
#include "wire_protocol.hpp"

namespace tickcap {

class SplitMix64 {
public:
    explicit SplitMix64(uint64_t seed) : s_(seed) {}

    uint64_t next() {
        uint64_t z = (s_ += 0x9E3779B97F4A7C15ull);
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
        return z ^ (z >> 31);
    }

    // Unbiased enough for simulation purposes and, more importantly,
    // identical everywhere -- no library distribution in the path.
    uint32_t below(uint32_t n) { return static_cast<uint32_t>(next() % n); }

    int64_t in_range(int64_t lo, int64_t hi) {
        return lo + static_cast<int64_t>(next() % static_cast<uint64_t>(hi - lo + 1));
    }

private:
    uint64_t s_;
};

struct LiveOrder {
    uint64_t order_id;
    uint16_t instrument_id;
    Side     side;
    int64_t  price_ticks;
    uint32_t qty;
};

class MarketSimulator {
public:
    // How far the reference price may wander from its anchor, and the
    // minimum distance from the reference at which an order may be placed.
    //
    // The invariant kMinHalfSpread > kMaxDrift is what keeps the book
    // uncrossed, and it is not decoration. An earlier version let the
    // reference random-walk without bound while placing orders only one tick
    // away from it; over a session the reference wandered ~25 ticks, orders
    // resting from early on ended up on the wrong side of newer ones, and the
    // reconstructed books came out persistently crossed (median spread -6
    // ticks). The decoder was faithfully reproducing a feed that no real
    // exchange could emit -- on a real venue an order that would cross
    // matches instead of resting. Bounding the drift below the half-spread
    // makes crossing impossible by construction:
    //   worst-case bid = (anchor + kMaxDrift) - kMinHalfSpread
    //   worst-case ask = (anchor - kMaxDrift) + kMinHalfSpread
    // and bid < ask holds whenever kMinHalfSpread > kMaxDrift.
    static constexpr int64_t kMaxDrift      = 8;
    static constexpr int64_t kMinHalfSpread = 10;

    MarketSimulator(uint64_t seed, int num_instruments = kNumInstruments)
        : rng_(seed), num_instruments_(num_instruments) {
        anchor_price_.resize(static_cast<size_t>(num_instruments));
        ref_price_.resize(static_cast<size_t>(num_instruments));
        for (int i = 0; i < num_instruments; ++i) {
            anchor_price_[i] = 5000 + rng_.in_range(0, 15000);  // $50.00 .. $200.00
            ref_price_[i] = anchor_price_[i];
        }
    }

    // Produces the next message in the session. Always succeeds.
    DecodedMsg next() {
        clock_ns_ += 200 + static_cast<uint64_t>(rng_.below(1800));  // ~1us mean spacing, jittered

        // Keep a healthy resting population so the book is never trivial and
        // the destructive events always have something to reference.
        bool must_add = live_.size() < 4096;
        uint32_t roll = rng_.below(100);

        if (must_add || roll < 50) return gen_add();
        if (roll < 65) return gen_reduce(MsgType::Execute);
        if (roll < 80) return gen_reduce(MsgType::Cancel);
        if (roll < 95) return gen_delete();
        return gen_replace();
    }

    size_t live_orders() const { return live_.size(); }
    uint64_t clock_ns() const { return clock_ns_; }

private:
    DecodedMsg gen_add() {
        DecodedMsg m;
        m.type = MsgType::AddOrder;
        m.exchange_ts_ns = clock_ns_;
        m.instrument_id  = static_cast<uint16_t>(rng_.below(static_cast<uint32_t>(num_instruments_)));
        m.order_id       = next_order_id_++;
        m.side           = (rng_.below(2) == 0) ? Side::Buy : Side::Sell;
        m.qty            = static_cast<uint32_t>(1 + rng_.below(500));

        // Slow reference drift, so the price column has real structure
        // instead of being constant (which would make the compression
        // result flattering and meaningless), but clamped to a band so the
        // book cannot end up crossed -- see kMaxDrift above.
        drift(m.instrument_id);
        m.price_ticks = quote_price(m.instrument_id, m.side);

        live_.push_back(LiveOrder{m.order_id, m.instrument_id, m.side, m.price_ticks, m.qty});
        return m;
    }

    DecodedMsg gen_reduce(MsgType type) {
        size_t i = pick_live();
        LiveOrder& o = live_[i];
        DecodedMsg m;
        m.type = type;
        m.exchange_ts_ns = clock_ns_;
        m.instrument_id  = o.instrument_id;
        m.order_id       = o.order_id;
        uint32_t amount  = 1 + rng_.below(o.qty);
        m.qty = amount;
        if (amount >= o.qty) remove_live(i);
        else o.qty -= amount;
        return m;
    }

    DecodedMsg gen_delete() {
        size_t i = pick_live();
        DecodedMsg m;
        m.type = MsgType::Delete;
        m.exchange_ts_ns = clock_ns_;
        m.instrument_id  = live_[i].instrument_id;
        m.order_id       = live_[i].order_id;
        remove_live(i);
        return m;
    }

    DecodedMsg gen_replace() {
        size_t i = pick_live();
        LiveOrder& o = live_[i];
        DecodedMsg m;
        m.type = MsgType::Replace;
        m.exchange_ts_ns = clock_ns_;
        m.instrument_id  = o.instrument_id;
        m.order_id       = o.order_id;
        m.new_order_id   = next_order_id_++;
        m.price_ticks = quote_price(o.instrument_id, o.side);
        m.qty = static_cast<uint32_t>(1 + rng_.below(500));

        o.order_id    = m.new_order_id;
        o.price_ticks = m.price_ticks;
        o.qty         = m.qty;
        return m;
    }

    // Random walk on the reference price, clamped to the anchor's band.
    void drift(uint16_t id) {
        if (rng_.below(64) != 0) return;
        int64_t& ref = ref_price_[id];
        int64_t anchor = anchor_price_[id];
        ref += rng_.in_range(-2, 2);
        if (ref > anchor + kMaxDrift) ref = anchor + kMaxDrift;
        if (ref < anchor - kMaxDrift) ref = anchor - kMaxDrift;
    }

    // A price at least kMinHalfSpread away from the reference, on the
    // correct side, within a 40-tick band beyond that.
    int64_t quote_price(uint16_t id, Side side) {
        int64_t ref = ref_price_[id];
        int64_t offset = rng_.in_range(0, 40);
        return (side == Side::Buy) ? ref - kMinHalfSpread - offset
                                   : ref + kMinHalfSpread + offset;
    }

    size_t pick_live() { return rng_.below(static_cast<uint32_t>(live_.size())); }

    // Swap-and-pop: O(1) and fully deterministic given the same draw
    // sequence, which is all reproducibility requires.
    void remove_live(size_t i) {
        live_[i] = live_.back();
        live_.pop_back();
    }

    SplitMix64 rng_;
    int num_instruments_;
    std::vector<int64_t> anchor_price_;
    std::vector<int64_t> ref_price_;
    std::vector<LiveOrder> live_;
    uint64_t next_order_id_ = 1;
    uint64_t clock_ns_ = 1'700'000'000'000'000'000ull;  // arbitrary fixed epoch
};

} // namespace tickcap
