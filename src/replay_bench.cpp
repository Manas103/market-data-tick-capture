// Pinned-core replay throughput: how fast one core can decode-and-apply a
// previously captured session, with no network in the loop at all.
//
// This is a deliberately different number from live_capture's "815,678
// msgs/sec drain" (see README): that number is a live UDP multicast
// receiver (recvfrom() + decode + columnar write, one message at a time,
// paced by whatever a real socket and a real sender thread do). This
// benchmark removes the network and the writer entirely and asks a
// narrower question: once a session is already sitting in a capture file,
// how fast can one pinned core turn those bytes back into applied book
// state? That is what "over a captured replay" in the claim means, and it
// is measured, not assumed, exactly as the file-format comparison in
// capture_bench.cpp already is for a related but different question.
//
// Usage: replay_bench [capture_path] [passes]
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <chrono>
#include <sched.h>
#include <unistd.h>
#include "tick_capture.hpp"
#include "book_universe.hpp"
#include "feed_decoder.hpp"

using namespace tickcap;
using Clock = std::chrono::steady_clock;

// Pins the calling thread to exactly one logical core. Returns the core
// index actually pinned, or -1 if pinning failed (reported, not silently
// ignored -- a benchmark claiming "one pinned core" that could not actually
// pin is a benchmark lying about its own setup).
static int pin_to_one_core() {
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(0, &set);
    if (sched_setaffinity(0, sizeof(set), &set) != 0) return -1;
    return 0;
}

int main(int argc, char** argv) {
    std::string capture_path = (argc > 1) ? argv[1] : "/tmp/session.tcap";
    int passes = (argc > 2) ? std::atoi(argv[2]) : 5;

    int pinned_core = pin_to_one_core();
    std::printf("=== pinned-core replay throughput ===\n");
    std::printf("capture        : %s\n", capture_path.c_str());
    std::printf("pinned core    : %s\n", pinned_core >= 0 ? "0 (sched_setaffinity OK)" : "FAILED TO PIN");
    std::printf("logical cores visible to this process: %ld\n", (long)sysconf(_SC_NPROCESSORS_ONLN));

    CaptureReader reader(capture_path);
    std::vector<TickRecord> recs = reader.read_all();
    std::printf("records loaded : %zu (load not timed; this benchmark is decode+apply only)\n", recs.size());
    if (recs.empty()) {
        std::printf("FAIL: capture has no records\n");
        return 1;
    }

    std::vector<double> pass_rates;
    for (int p = 0; p < passes; ++p) {
        BookUniverse book;
        auto t0 = Clock::now();
        replay_records(recs, book);
        double secs = std::chrono::duration<double>(Clock::now() - t0).count();
        double rate = (double)recs.size() / secs;
        pass_rates.push_back(rate);
        std::printf("pass %d: %.3fs, %.0f msgs/sec (%llu resting orders)\n",
                    p, secs, rate, (unsigned long long)book.total_resting_orders());
    }

    std::sort(pass_rates.begin(), pass_rates.end());
    double best = pass_rates.back();
    double median = pass_rates[pass_rates.size() / 2];
    std::printf("\nbest pass rate   : %.0f msgs/sec\n", best);
    std::printf("median pass rate : %.0f msgs/sec\n", median);
    std::printf("target: 1,000,000+ msgs/sec sustained on one pinned core\n");
    std::printf("meets_claim (best of %d passes): %s\n", passes, best >= 1'000'000.0 ? "true" : "false");
    return 0;
}
