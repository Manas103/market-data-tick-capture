// Full-depth, order-by-order (L3) limit order book for one instrument.
//
// Every resting order is tracked individually, in arrival order, inside the
// price level it rests at. This is deliberately a different structure from
// the aggregated (L2) "quantity per price level" book, and the difference is
// not cosmetic:
//
//   An L2 book can tell you there are 4,000 shares at 101.25.
//   An L3 book can tell you those are six specific orders, in queue order,
//   and that yours is fourth with 2,300 shares ahead of it.
//
// That second question is the one that decides whether a passive order
// actually fills, so it is the question execution modelling and any
// queue-position research is built on -- and an aggregated book physically
// cannot answer it, because the information was destroyed at ingest. See
// qty_ahead() below, which is the capability the whole structure exists for.
//
// The cost, stated plainly: L3 is heavier. Every order is a node, cancels
// chase pointers, and memory scales with resting order count rather than
// with the number of occupied price levels. A feed handler that only ever
// needs top-of-book should use the flat aggregated array instead -- it is
// O(1) per update with no allocation, and strictly better for that job.
//
// Container choices, and why they matter for reproducibility:
//   std::map      for price levels -- ordered iteration is part of the
//                 contract. The canonical digest walks levels in price
//                 order, so the container must have a defined order. A hash
//                 map would still be correct for lookups but would make
//                 canonical serialization order implementation-defined.
//   std::list     for the FIFO at each level -- iterators stay valid across
//                 insertions and erasures elsewhere in the list, which is
//                 what lets an order_id index hold a direct iterator and
//                 cancel in O(1). Vector and deque iterators do not survive
//                 that, so they would force an O(n) scan per cancel.
#pragma once
#include <cstdint>
#include <map>
#include <list>
#include <functional>
#include "wire_protocol.hpp"

namespace tickcap {

struct RestingOrder {
    uint64_t order_id  = 0;
    uint64_t seq_added = 0;   // feed sequence number at insertion -> time priority
    uint32_t qty       = 0;
};

using OrderList = std::list<RestingOrder>;
using OrderIter = OrderList::iterator;

struct PriceLevel {
    OrderList orders;          // FIFO: front() has the best time priority
    uint64_t  total_qty = 0;   // maintained incrementally, never recomputed by scan
};

// Bids iterate high -> low, asks low -> high, so in both cases begin() is the
// most aggressive price. That symmetry keeps the top-of-book and canonical
// serialization code from needing a side-specific special case.
using BidLevels = std::map<int64_t, PriceLevel, std::greater<int64_t>>;
using AskLevels = std::map<int64_t, PriceLevel, std::less<int64_t>>;

class OrderBookL3 {
public:
    OrderIter add(Side side, int64_t price_ticks, const RestingOrder& o) {
        PriceLevel& lvl = level_for(side, price_ticks);
        lvl.total_qty += o.qty;
        lvl.orders.push_back(o);
        ++resting_count_;
        return std::prev(lvl.orders.end());
    }

    // Reduces a resting order by up to `qty`. Returns the amount actually
    // removed (an execution or cancel for more than remains is clamped, not
    // an error -- see the note in feed_decoder.hpp about why that race is
    // ordinary rather than exceptional). Fully depleted orders are erased,
    // and a level that empties is erased with them so that "a level exists"
    // always means "a level has orders".
    uint32_t reduce(Side side, int64_t price_ticks, OrderIter it, uint32_t qty) {
        PriceLevel* lvl = find_level(side, price_ticks);
        if (!lvl) return 0;
        uint32_t removed = qty < it->qty ? qty : it->qty;
        it->qty -= removed;
        lvl->total_qty -= removed;
        if (it->qty == 0) {
            lvl->orders.erase(it);
            --resting_count_;
            if (lvl->orders.empty()) erase_level(side, price_ticks);
        }
        return removed;
    }

    // Removes a resting order outright regardless of remaining quantity.
    uint32_t remove(Side side, int64_t price_ticks, OrderIter it) {
        PriceLevel* lvl = find_level(side, price_ticks);
        if (!lvl) return 0;
        uint32_t gone = it->qty;
        lvl->total_qty -= gone;
        lvl->orders.erase(it);
        --resting_count_;
        if (lvl->orders.empty()) erase_level(side, price_ticks);
        return gone;
    }

    // The reason this book is order-by-order: total resting quantity ahead
    // of `it` in its own queue. Strictly impossible on an aggregated book.
    uint64_t qty_ahead(Side side, int64_t price_ticks, OrderIter it) const {
        const PriceLevel* lvl = find_level(side, price_ticks);
        if (!lvl) return 0;
        uint64_t ahead = 0;
        for (auto i = lvl->orders.begin(); i != lvl->orders.end(); ++i) {
            if (i == it) break;
            ahead += i->qty;
        }
        return ahead;
    }

    bool has_bid() const { return !bids_.empty(); }
    bool has_ask() const { return !asks_.empty(); }
    int64_t  best_bid_ticks() const { return bids_.begin()->first; }
    int64_t  best_ask_ticks() const { return asks_.begin()->first; }
    uint64_t best_bid_qty()   const { return bids_.begin()->second.total_qty; }
    uint64_t best_ask_qty()   const { return asks_.begin()->second.total_qty; }
    size_t   bid_level_count() const { return bids_.size(); }
    size_t   ask_level_count() const { return asks_.size(); }
    size_t   resting_orders() const { return resting_count_; }

    const BidLevels& bids() const { return bids_; }
    const AskLevels& asks() const { return asks_; }

    // Number of individual orders resting at a price -- another L3-only fact.
    size_t order_count_at(Side side, int64_t price_ticks) const {
        const PriceLevel* lvl = find_level(side, price_ticks);
        return lvl ? lvl->orders.size() : 0;
    }

    uint64_t qty_at(Side side, int64_t price_ticks) const {
        const PriceLevel* lvl = find_level(side, price_ticks);
        return lvl ? lvl->total_qty : 0;
    }

private:
    PriceLevel& level_for(Side side, int64_t price) {
        return side == Side::Buy ? bids_[price] : asks_[price];
    }

    PriceLevel* find_level(Side side, int64_t price) {
        if (side == Side::Buy) {
            auto it = bids_.find(price);
            return it == bids_.end() ? nullptr : &it->second;
        }
        auto it = asks_.find(price);
        return it == asks_.end() ? nullptr : &it->second;
    }

    const PriceLevel* find_level(Side side, int64_t price) const {
        if (side == Side::Buy) {
            auto it = bids_.find(price);
            return it == bids_.end() ? nullptr : &it->second;
        }
        auto it = asks_.find(price);
        return it == asks_.end() ? nullptr : &it->second;
    }

    void erase_level(Side side, int64_t price) {
        if (side == Side::Buy) bids_.erase(price);
        else asks_.erase(price);
    }

    BidLevels bids_;
    AskLevels asks_;
    size_t resting_count_ = 0;
};

} // namespace tickcap
