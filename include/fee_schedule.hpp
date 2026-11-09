// Per-venue maker-taker fee and rebate schedule for the smart order router
// (see smart_order_router.hpp).
//
// Real US equity venues price access in two opposite ways: "maker-taker"
// venues charge the order that takes liquidity (crosses the spread) and pay
// a rebate to the order that was resting; "taker-maker" (inverted) venues do
// the reverse, paying the taker a rebate and charging the resting side a
// fee, to attract marketable flow away from the standard venues. This
// router only ever takes liquidity (it fills a client's aggressive parent
// order against whatever is resting), so the only side of each schedule
// that matters here is the taker fee: positive on a standard venue, negative
// (a net rebate to the taker) on an inverted one.
//
// The five figures below are fixed once, before any measurement is taken,
// at magnitudes modeled on real published access-fee schedules (SEC Rule
// 610's $0.0030/share taker-fee cap on protected quotes, and inverted-venue
// taker rebates in the $0.0010-0.0025/share range) -- not reverse-engineered
// from a target savings percentage. Fee units are "ticks per share", and a
// tick is $0.01 (see wire_protocol.hpp, kTicksPerCent), so 0.30 means
// $0.0030/share.
#pragma once
#include <vector>

namespace tickcap {

struct FeeSchedule {
    // Index i is the taker fee (positive) or taker rebate (negative, i.e. the
    // taker is paid) for venue i, in ticks per share.
    std::vector<double> taker_fee_ticks;

    static FeeSchedule five_venue_default() {
        return FeeSchedule{{0.30, 0.28, 0.10, -0.15, -0.25}};
    }
};

} // namespace tickcap
