// Inline pre-trade risk checks: position, notional and message-rate limits,
// evaluated before a client order is allowed onto the wire.
//
// "Inline" means in the hot path, not a post-trade report: check() runs on
// every order a client submits, in the same call that decides whether the
// order is routed at all (see venue_router.hpp / src/order_router_bench.cpp).
// A rejected order never reaches a venue and never touches position state --
// state is committed only on acceptance, which is what makes the position
// check meaningful rather than cosmetic (an order that would have breached
// the limit must not be allowed to move the position and then get caught
// one order later).
#pragma once
#include <cstdint>
#include <deque>
#include <unordered_map>
#include "wire_protocol.hpp"

namespace tickcap {

struct RiskLimits {
    int64_t  max_position         = 10'000;      // net shares, per instrument, either sign
    int64_t  max_notional_ticks    = 50'000'000;  // |price_ticks * qty| per single order
    uint32_t max_msgs_per_window   = 50;          // messages per instrument inside window_ns
    uint64_t window_ns             = 1'000'000'000ull;  // 1 second sliding window
};

enum class RiskReject : uint8_t { None, Position, Notional, MessageRate };

inline const char* risk_reject_name(RiskReject r) {
    switch (r) {
        case RiskReject::None:        return "none";
        case RiskReject::Position:    return "position";
        case RiskReject::Notional:    return "notional";
        case RiskReject::MessageRate: return "message_rate";
    }
    return "unknown";
}

struct RiskDecision {
    bool       accepted;
    RiskReject reason;
};

// Per-instrument state: net position and a sliding window of recent message
// timestamps. Both only advance on an accepted order.
class PreTradeRiskChecker {
public:
    explicit PreTradeRiskChecker(RiskLimits limits) : limits_(limits) {}

    RiskDecision check(uint16_t instrument_id, Side side, int64_t price_ticks,
                        uint32_t qty, uint64_t ts_ns) {
        int64_t notional = price_ticks * static_cast<int64_t>(qty);
        if (notional < 0) notional = -notional;
        if (notional > limits_.max_notional_ticks) {
            return {false, RiskReject::Notional};
        }

        int64_t delta = (side == Side::Buy) ? static_cast<int64_t>(qty)
                                             : -static_cast<int64_t>(qty);
        int64_t current = position_.count(instrument_id) ? position_[instrument_id] : 0;
        int64_t projected = current + delta;
        if (projected > limits_.max_position || projected < -limits_.max_position) {
            return {false, RiskReject::Position};
        }

        std::deque<uint64_t>& window = msg_times_[instrument_id];
        while (!window.empty() && ts_ns >= window.front() &&
               ts_ns - window.front() > limits_.window_ns) {
            window.pop_front();
        }
        if (window.size() >= limits_.max_msgs_per_window) {
            return {false, RiskReject::MessageRate};
        }

        // Only an accepted order mutates state.
        window.push_back(ts_ns);
        position_[instrument_id] = projected;
        return {true, RiskReject::None};
    }

    int64_t position(uint16_t instrument_id) const {
        auto it = position_.find(instrument_id);
        return it == position_.end() ? 0 : it->second;
    }

private:
    RiskLimits limits_;
    std::unordered_map<uint16_t, int64_t> position_;
    std::unordered_map<uint16_t, std::deque<uint64_t>> msg_times_;
};

} // namespace tickcap
