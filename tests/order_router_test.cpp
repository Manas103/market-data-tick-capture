// Hand-constructed correctness checks for PreTradeRiskChecker and
// VenueRouter, independent of the larger seeded/session runs in
// src/order_router_bench.cpp.
#include <cstdio>
#include <vector>

#include "pre_trade_risk.hpp"
#include "venue_router.hpp"

using namespace tickcap;

static int g_checks = 0;
static int g_failures = 0;

#define CHECK(cond)                                                      \
    do {                                                                 \
        ++g_checks;                                                      \
        if (!(cond)) {                                                   \
            ++g_failures;                                                \
            std::printf("  FAIL %s:%d  %s\n", __FILE__, __LINE__, #cond); \
        }                                                                 \
    } while (0)

static void test_position_limit_boundary() {
    RiskLimits limits;
    limits.max_position = 100;
    PreTradeRiskChecker checker(limits);

    // Exactly at the limit: accepted.
    RiskDecision d1 = checker.check(1, Side::Buy, 10000, 100, 1000);
    CHECK(d1.accepted);
    CHECK(checker.position(1) == 100);

    // One more share would breach: rejected, and position must NOT move.
    RiskDecision d2 = checker.check(1, Side::Buy, 10000, 1, 2000);
    CHECK(!d2.accepted);
    CHECK(d2.reason == RiskReject::Position);
    CHECK(checker.position(1) == 100);

    // A sell back within the limit is accepted and reduces position.
    RiskDecision d3 = checker.check(1, Side::Sell, 10000, 50, 3000);
    CHECK(d3.accepted);
    CHECK(checker.position(1) == 50);
}

static void test_notional_limit_boundary() {
    RiskLimits limits;
    limits.max_notional_ticks = 1000;
    PreTradeRiskChecker checker(limits);

    RiskDecision ok = checker.check(2, Side::Buy, 100, 10, 1000);  // 100*10 = 1000, exactly at limit
    CHECK(ok.accepted);

    RiskDecision rejected = checker.check(3, Side::Buy, 100, 11, 1000);  // 1100 > 1000
    CHECK(!rejected.accepted);
    CHECK(rejected.reason == RiskReject::Notional);
}

static void test_message_rate_limit() {
    RiskLimits limits;
    limits.max_msgs_per_window = 3;
    limits.window_ns = 1000;
    PreTradeRiskChecker checker(limits);

    CHECK(checker.check(4, Side::Buy, 100, 1, 0).accepted);
    CHECK(checker.check(4, Side::Buy, 100, 1, 100).accepted);
    CHECK(checker.check(4, Side::Buy, 100, 1, 200).accepted);
    RiskDecision fourth = checker.check(4, Side::Buy, 100, 1, 300);  // 4th inside the window
    CHECK(!fourth.accepted);
    CHECK(fourth.reason == RiskReject::MessageRate);

    // After the window elapses, the slot frees up again.
    RiskDecision later = checker.check(4, Side::Buy, 100, 1, 300 + limits.window_ns + 1);
    CHECK(later.accepted);
}

static void test_risk_state_is_per_instrument() {
    RiskLimits limits;
    limits.max_position = 10;
    PreTradeRiskChecker checker(limits);

    CHECK(checker.check(10, Side::Buy, 100, 10, 0).accepted);
    // A different instrument's limit is untouched by instrument 10's position.
    CHECK(checker.check(11, Side::Buy, 100, 10, 0).accepted);
    CHECK(checker.position(10) == 10);
    CHECK(checker.position(11) == 10);
}

static void test_router_picks_best_price() {
    // Buy wants the lowest ask.
    std::vector<VenueQuote> asks = {{true, 105}, {true, 101}, {true, 110}};
    CHECK(VenueRouter::route(Side::Buy, asks) == 1);

    // Sell wants the highest bid.
    std::vector<VenueQuote> bids = {{true, 99}, {true, 102}, {true, 100}};
    CHECK(VenueRouter::route(Side::Sell, bids) == 1);
}

static void test_router_tie_breaks_to_lowest_index() {
    std::vector<VenueQuote> asks = {{true, 100}, {true, 100}, {true, 105}};
    CHECK(VenueRouter::route(Side::Buy, asks) == 0);
}

static void test_router_skips_venues_with_no_quote() {
    std::vector<VenueQuote> asks = {{false, 0}, {true, 100}, {false, 0}};
    CHECK(VenueRouter::route(Side::Buy, asks) == 1);
}

static void test_router_returns_negative_one_when_nothing_quotes() {
    std::vector<VenueQuote> asks = {{false, 0}, {false, 0}};
    CHECK(VenueRouter::route(Side::Buy, asks) == -1);
}

int main() {
    test_position_limit_boundary();
    test_notional_limit_boundary();
    test_message_rate_limit();
    test_risk_state_is_per_instrument();
    test_router_picks_best_price();
    test_router_tie_breaks_to_lowest_index();
    test_router_skips_venues_with_no_quote();
    test_router_returns_negative_one_when_nothing_quotes();

    std::printf("%d checks, %d failures\n", g_checks, g_failures);
    if (g_failures == 0) {
        std::printf("PASS\n");
        return 0;
    }
    std::printf("%d CHECK(S) FAILED.\n", g_failures);
    return 1;
}
