// Independent post-hoc validation of a TickRecord stream against 14 named
// invariants, grouped into three categories: sequence continuity, crossed
// or malformed books, and trade-versus-book consistency.
//
// This is deliberately a second, independent bookkeeper, not a wrapper
// around OrderBookL3/BookUniverse. BookUniverse's reduce() clamps an
// over-sized Execute/Cancel to "whatever remains" because that is the right
// production behavior after a real gap (see its own comment there). A
// validator that reused that code path could never observe the violation it
// exists to catch -- it would already have been silently clamped away. So
// this class keeps its own minimal order/level state and raises a named
// violation at the moment a record would require clamping or would
// otherwise corrupt the book, instead of clamping or applying it.
//
// Every check examines a record BEFORE applying its effect to this
// validator's own state, so e.g. "exceeds resting quantity" is checked
// against the pre-image. On any violation the record's effect is NOT
// applied (mirroring BookUniverse's own "skip and count" handling of a
// malformed event), so one bad record cannot corrupt the state used to
// judge every record after it.
#pragma once
#include <cstdint>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>
#include "tick_capture.hpp"
#include "wire_protocol.hpp"

namespace tickcap {

enum class CheckId {
    SeqStrictlyIncreasing = 0,
    CaptureTsMonotonic,
    ExchangeTsMonotonicPerInstrument,
    SessionStartsAtDatagramBoundary,
    NoCrossedBook,
    AddPriceNonNegative,
    AddQtyPositive,
    AddOrderIdNotAlreadyResting,
    ReplaceNewOrderIdNotAlreadyResting,
    ExecuteReferencesRestingOrder,
    ExecuteQtyWithinResting,
    CancelReferencesRestingOrder,
    CancelQtyWithinResting,
    DeleteOrReplaceReferencesRestingOrder,
    kCount
};

inline const char* check_name(CheckId id) {
    switch (id) {
        case CheckId::SeqStrictlyIncreasing:               return "SEQ_STRICTLY_INCREASING";
        case CheckId::CaptureTsMonotonic:                   return "CAPTURE_TS_MONOTONIC";
        case CheckId::ExchangeTsMonotonicPerInstrument:     return "EXCHANGE_TS_MONOTONIC_PER_INSTRUMENT";
        case CheckId::SessionStartsAtDatagramBoundary:      return "SESSION_STARTS_AT_DATAGRAM_BOUNDARY";
        case CheckId::NoCrossedBook:                        return "NO_CROSSED_BOOK";
        case CheckId::AddPriceNonNegative:                  return "ADD_PRICE_NON_NEGATIVE";
        case CheckId::AddQtyPositive:                       return "ADD_QTY_POSITIVE";
        case CheckId::AddOrderIdNotAlreadyResting:          return "ADD_ORDER_ID_NOT_ALREADY_RESTING";
        case CheckId::ReplaceNewOrderIdNotAlreadyResting:   return "REPLACE_NEW_ORDER_ID_NOT_ALREADY_RESTING";
        case CheckId::ExecuteReferencesRestingOrder:        return "EXECUTE_REFERENCES_RESTING_ORDER";
        case CheckId::ExecuteQtyWithinResting:              return "EXECUTE_QTY_WITHIN_RESTING";
        case CheckId::CancelReferencesRestingOrder:         return "CANCEL_REFERENCES_RESTING_ORDER";
        case CheckId::CancelQtyWithinResting:                return "CANCEL_QTY_WITHIN_RESTING";
        case CheckId::DeleteOrReplaceReferencesRestingOrder: return "DELETE_OR_REPLACE_REFERENCES_RESTING_ORDER";
        default: return "UNKNOWN";
    }
}

constexpr size_t kNumChecks = static_cast<size_t>(CheckId::kCount);

struct Violation {
    CheckId     check;
    size_t      record_index;
    uint16_t    instrument_id;
    std::string detail;
};

struct OrderShadow {
    uint16_t instrument_id;
    Side     side;
    int64_t  price_ticks;
    uint32_t qty;
};

class SessionValidator {
public:
    explicit SessionValidator(int num_instruments = kNumInstruments)
        : inst_(static_cast<size_t>(num_instruments)) {}

    // Processes one record in capture order. Appends to `violations_` on
    // any invariant breach; never throws and never stops processing, so a
    // single bad record does not hide the rest of the session's defects.
    void apply(const TickRecord& r, size_t index) {
        check_sequence_continuity(r, index);

        switch (static_cast<MsgType>(r.msg_type)) {
            case MsgType::AddOrder:  apply_add(r, index);     break;
            case MsgType::Execute:   apply_execute(r, index); break;
            case MsgType::Cancel:    apply_cancel(r, index);  break;
            case MsgType::Delete:    apply_delete(r, index);  break;
            case MsgType::Replace:   apply_replace(r, index); break;
        }
    }

    const std::vector<Violation>& violations() const { return violations_; }
    bool clean() const { return violations_.empty(); }

private:
    // Price levels are per instrument, but order ids are a single global
    // namespace in this feed (BookUniverse's own index_ is likewise one
    // unordered_map, not one per instrument), so order identity is tracked
    // globally in `orders_` and only the price-level quantity maps below
    // are split per instrument.
    struct Instrument {
        std::map<int64_t, uint64_t, std::greater<int64_t>> bid_qty;  // price -> total resting qty
        std::map<int64_t, uint64_t, std::less<int64_t>>    ask_qty;
        uint64_t last_exchange_ts = 0;
        bool     have_exchange_ts = false;
    };

    void add_violation(CheckId c, size_t idx, uint16_t inst, const std::string& detail) {
        violations_.push_back(Violation{c, idx, inst, detail});
    }

    void check_sequence_continuity(const TickRecord& r, size_t index) {
        if (index == 0 && r.pkt_first != 1) {
            add_violation(CheckId::SessionStartsAtDatagramBoundary, index, r.instrument_id,
                          "first record of the session did not have pkt_first=1");
        }
        if (have_last_seq_ && r.seq_num <= last_seq_) {
            add_violation(CheckId::SeqStrictlyIncreasing, index, r.instrument_id,
                          "seq_num " + std::to_string(r.seq_num) +
                          " did not increase past previous " + std::to_string(last_seq_));
        }
        last_seq_ = r.seq_num;
        have_last_seq_ = true;

        if (have_last_cap_ts_ && r.capture_ts_ns < last_cap_ts_) {
            add_violation(CheckId::CaptureTsMonotonic, index, r.instrument_id,
                          "capture_ts_ns went backwards: " + std::to_string(r.capture_ts_ns) +
                          " < " + std::to_string(last_cap_ts_));
        }
        last_cap_ts_ = r.capture_ts_ns;
        have_last_cap_ts_ = true;

        if (r.instrument_id >= inst_.size()) return;  // out-of-range instrument, nothing more to check against
        Instrument& in = inst_[r.instrument_id];
        if (in.have_exchange_ts && r.exchange_ts_ns < in.last_exchange_ts) {
            add_violation(CheckId::ExchangeTsMonotonicPerInstrument, index, r.instrument_id,
                          "exchange_ts_ns went backwards for this instrument: " +
                          std::to_string(r.exchange_ts_ns) + " < " + std::to_string(in.last_exchange_ts));
        }
        in.last_exchange_ts = r.exchange_ts_ns;
        in.have_exchange_ts = true;
    }

    void check_crossed_book(Instrument& in, size_t index, uint16_t instrument_id) {
        if (in.bid_qty.empty() || in.ask_qty.empty()) return;
        int64_t best_bid = in.bid_qty.begin()->first;
        int64_t best_ask = in.ask_qty.begin()->first;
        if (best_bid >= best_ask) {
            add_violation(CheckId::NoCrossedBook, index, instrument_id,
                          "best_bid " + std::to_string(best_bid) +
                          " >= best_ask " + std::to_string(best_ask));
        }
    }

    void insert_resting(uint16_t instrument_id, uint64_t order_id, Side side, int64_t price, uint32_t qty) {
        orders_[order_id] = OrderShadow{instrument_id, side, price, qty};
        Instrument& in = inst_[instrument_id];
        if (side == Side::Buy) in.bid_qty[price] += qty;
        else                   in.ask_qty[price] += qty;
    }

    void remove_resting(std::unordered_map<uint64_t, OrderShadow>::iterator it) {
        OrderShadow o = it->second;
        Instrument& in = inst_[o.instrument_id];
        if (o.side == Side::Buy) {
            in.bid_qty[o.price_ticks] -= o.qty;
            if (in.bid_qty[o.price_ticks] == 0) in.bid_qty.erase(o.price_ticks);
        } else {
            in.ask_qty[o.price_ticks] -= o.qty;
            if (in.ask_qty[o.price_ticks] == 0) in.ask_qty.erase(o.price_ticks);
        }
        orders_.erase(it);
    }

    void apply_add(const TickRecord& r, size_t index) {
        bool price_ok = r.price_ticks >= 0;
        bool qty_ok   = r.qty > 0;
        if (!price_ok) {
            add_violation(CheckId::AddPriceNonNegative, index, r.instrument_id,
                          "AddOrder price_ticks " + std::to_string(r.price_ticks) + " is negative");
        }
        if (!qty_ok) {
            add_violation(CheckId::AddQtyPositive, index, r.instrument_id,
                          "AddOrder qty is zero for order " + std::to_string(r.order_id));
        }
        if (orders_.count(r.order_id)) {
            add_violation(CheckId::AddOrderIdNotAlreadyResting, index, r.instrument_id,
                          "AddOrder reuses order_id " + std::to_string(r.order_id) + " already resting");
            return;  // do not apply: a real decoder skips a duplicate Add too
        }
        if (!price_ok || !qty_ok) return;  // do not let a malformed Add enter the book at all
        if (r.instrument_id >= inst_.size()) return;
        Side side = static_cast<Side>(r.side);
        insert_resting(r.instrument_id, r.order_id, side, r.price_ticks, r.qty);
        check_crossed_book(inst_[r.instrument_id], index, r.instrument_id);
    }

    // Shared by Execute and Cancel: both reduce a resting order by `qty` and
    // differ only in which two named checks fire on failure.
    void reduce_order(const TickRecord& r, size_t index, CheckId missing_check, CheckId exceeds_check) {
        auto it = orders_.find(r.order_id);
        if (it == orders_.end()) {
            add_violation(missing_check, index, r.instrument_id,
                          "references order " + std::to_string(r.order_id) + " not currently resting");
            return;
        }
        OrderShadow& o = it->second;
        if (r.qty > o.qty) {
            add_violation(exceeds_check, index, r.instrument_id,
                          "qty " + std::to_string(r.qty) + " exceeds resting " + std::to_string(o.qty) +
                          " for order " + std::to_string(r.order_id));
            return;  // do not apply a reduction that would go negative
        }
        Instrument& in = inst_[o.instrument_id];
        int64_t price = o.price_ticks;
        if (o.side == Side::Buy) in.bid_qty[price] -= r.qty;
        else                     in.ask_qty[price] -= r.qty;
        o.qty -= r.qty;
        bool emptied = (o.qty == 0);
        if (emptied) {
            if (o.side == Side::Buy) { if (in.bid_qty[price] == 0) in.bid_qty.erase(price); }
            else                     { if (in.ask_qty[price] == 0) in.ask_qty.erase(price); }
            orders_.erase(it);
        }
        check_crossed_book(in, index, r.instrument_id);
    }

    void apply_execute(const TickRecord& r, size_t index) {
        reduce_order(r, index, CheckId::ExecuteReferencesRestingOrder, CheckId::ExecuteQtyWithinResting);
    }

    void apply_cancel(const TickRecord& r, size_t index) {
        reduce_order(r, index, CheckId::CancelReferencesRestingOrder, CheckId::CancelQtyWithinResting);
    }

    void apply_delete(const TickRecord& r, size_t index) {
        auto it = orders_.find(r.order_id);
        if (it == orders_.end()) {
            add_violation(CheckId::DeleteOrReplaceReferencesRestingOrder, index, r.instrument_id,
                          "Delete references order " + std::to_string(r.order_id) + " not currently resting");
            return;
        }
        uint16_t instrument_id = it->second.instrument_id;
        remove_resting(it);
        check_crossed_book(inst_[instrument_id], index, instrument_id);
    }

    void apply_replace(const TickRecord& r, size_t index) {
        auto it = orders_.find(r.order_id);
        if (it == orders_.end()) {
            add_violation(CheckId::DeleteOrReplaceReferencesRestingOrder, index, r.instrument_id,
                          "Replace references old order " + std::to_string(r.order_id) + " not currently resting");
            return;
        }
        if (orders_.count(r.new_order_id)) {
            add_violation(CheckId::ReplaceNewOrderIdNotAlreadyResting, index, r.instrument_id,
                          "Replace new_order_id " + std::to_string(r.new_order_id) +
                          " collides with another order already resting");
            return;  // do not apply: leave the old order resting rather than overwrite the collision
        }
        uint16_t instrument_id = it->second.instrument_id;
        Side side = it->second.side;  // a replace cannot change side; there is no field for it
        remove_resting(it);
        if (instrument_id >= inst_.size()) return;
        insert_resting(instrument_id, r.new_order_id, side, r.price_ticks, r.qty);
        check_crossed_book(inst_[instrument_id], index, instrument_id);
    }

    std::vector<Instrument> inst_;
    std::unordered_map<uint64_t, OrderShadow> orders_;  // global order_id namespace, as in BookUniverse::index_
    std::vector<Violation>  violations_;
    uint64_t last_seq_ = 0;
    bool     have_last_seq_ = false;
    uint64_t last_cap_ts_ = 0;
    bool     have_last_cap_ts_ = false;
};

} // namespace tickcap
