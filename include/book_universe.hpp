// All instruments' L3 books, plus the order_id -> location index that makes
// Cancel/Execute/Delete/Replace O(1).
//
// The index is an unordered_map on purpose and the price levels are an
// ordered map on purpose. The index is only ever probed by key, never
// iterated, so its iteration order (which is implementation-defined and can
// differ between library versions) never reaches any output. Anything that
// does reach an output -- the canonical digest, the analytics -- walks the
// ordered structures instead. That separation is what keeps a replayed
// session byte-identical rather than merely equivalent.
#pragma once
#include <cstdint>
#include <vector>
#include <unordered_map>
#include "order_book_l3.hpp"
#include "wire_protocol.hpp"

namespace tickcap {

struct OrderLocator {
    uint16_t  instrument_id = 0;
    Side      side{};
    int64_t   price_ticks = 0;
    OrderIter it{};
};

struct BookStats {
    uint64_t applied          = 0;
    uint64_t adds             = 0;
    uint64_t executes         = 0;
    uint64_t cancels          = 0;
    uint64_t deletes          = 0;
    uint64_t replaces         = 0;
    uint64_t executed_qty     = 0;
    uint64_t unknown_order_refs = 0;  // event referencing an order we never saw
    uint64_t duplicate_adds     = 0;  // Add for an order_id already resting
};

class BookUniverse {
public:
    explicit BookUniverse(int num_instruments = kNumInstruments)
        : books_(static_cast<size_t>(num_instruments)) {}

    // Applies one decoded message. `seq` is the feed sequence number, stored
    // on the resting order as its time-priority key.
    //
    // Events that reference an order the book does not know about are
    // counted and skipped rather than treated as fatal. That is not
    // defensive hand-waving: after a real multicast gap, the Add that
    // created an order can be exactly the message that was lost, so its
    // later Cancel legitimately arrives for an unknown id. A feed handler
    // that threw here would die on the first dropped datagram.
    void apply(const DecodedMsg& m, uint64_t seq) {
        ++stats_.applied;
        switch (m.type) {
            case MsgType::AddOrder: {
                ++stats_.adds;
                if (index_.find(m.order_id) != index_.end()) { ++stats_.duplicate_adds; return; }
                if (m.instrument_id >= books_.size()) return;
                RestingOrder o;
                o.order_id  = m.order_id;
                o.seq_added = seq;
                o.qty       = m.qty;
                OrderIter it = books_[m.instrument_id].add(m.side, m.price_ticks, o);
                index_[m.order_id] = OrderLocator{m.instrument_id, m.side, m.price_ticks, it};
                break;
            }
            case MsgType::Execute: {
                ++stats_.executes;
                auto f = index_.find(m.order_id);
                if (f == index_.end()) { ++stats_.unknown_order_refs; return; }
                const OrderLocator& loc = f->second;
                uint32_t before = loc.it->qty;
                uint32_t gone = books_[loc.instrument_id].reduce(loc.side, loc.price_ticks, loc.it, m.qty);
                stats_.executed_qty += gone;
                if (gone >= before) index_.erase(f);
                break;
            }
            case MsgType::Cancel: {
                ++stats_.cancels;
                auto f = index_.find(m.order_id);
                if (f == index_.end()) { ++stats_.unknown_order_refs; return; }
                const OrderLocator& loc = f->second;
                uint32_t before = loc.it->qty;
                uint32_t gone = books_[loc.instrument_id].reduce(loc.side, loc.price_ticks, loc.it, m.qty);
                if (gone >= before) index_.erase(f);
                break;
            }
            case MsgType::Delete: {
                ++stats_.deletes;
                auto f = index_.find(m.order_id);
                if (f == index_.end()) { ++stats_.unknown_order_refs; return; }
                const OrderLocator& loc = f->second;
                books_[loc.instrument_id].remove(loc.side, loc.price_ticks, loc.it);
                index_.erase(f);
                break;
            }
            case MsgType::Replace: {
                ++stats_.replaces;
                auto f = index_.find(m.order_id);
                if (f == index_.end()) { ++stats_.unknown_order_refs; return; }
                OrderLocator loc = f->second;
                books_[loc.instrument_id].remove(loc.side, loc.price_ticks, loc.it);
                index_.erase(f);
                // A replace goes to the back of the queue at its new price.
                // Real exchanges treat a price or size-increase modification
                // as a cancel/replace precisely so it cannot jump the queue,
                // so the new order takes `seq` as its time-priority key.
                RestingOrder o;
                o.order_id  = m.new_order_id;
                o.seq_added = seq;
                o.qty       = m.qty;
                OrderIter it = books_[loc.instrument_id].add(loc.side, m.price_ticks, o);
                index_[m.new_order_id] =
                    OrderLocator{loc.instrument_id, loc.side, m.price_ticks, it};
                break;
            }
        }
    }

    const OrderBookL3& book(uint16_t instrument_id) const { return books_[instrument_id]; }
    OrderBookL3&       book(uint16_t instrument_id)       { return books_[instrument_id]; }
    size_t num_instruments() const { return books_.size(); }
    const BookStats& stats() const { return stats_; }

    size_t total_resting_orders() const {
        size_t n = 0;
        for (const auto& b : books_) n += b.resting_orders();
        return n;
    }

    // Queue position for a live order, by id -- the L3-only query.
    // Returns false if the order is not resting.
    bool queue_position(uint64_t order_id, uint64_t& qty_ahead_out) const {
        auto f = index_.find(order_id);
        if (f == index_.end()) return false;
        const OrderLocator& loc = f->second;
        qty_ahead_out = books_[loc.instrument_id].qty_ahead(loc.side, loc.price_ticks, loc.it);
        return true;
    }

    const std::unordered_map<uint64_t, OrderLocator>& index() const { return index_; }

private:
    std::vector<OrderBookL3> books_;
    std::unordered_map<uint64_t, OrderLocator> index_;
    BookStats stats_;
};

} // namespace tickcap
