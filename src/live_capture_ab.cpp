// A/B redundant multicast session with seeded line drops, arbitrated on the
// receive side, with true double-line gaps recovered over a TCP retransmit
// path served from an authoritative retransmit buffer.
//
// Two passes over the same deterministic message stream (same simulator
// seed both times):
//   Pass 1 (dry run, no sockets): packs the stream into datagrams exactly
//     as pass 2 will, only to learn how many datagrams there are, so the
//     seeded-loss schedule below can pick *exact* counts rather than an
//     expected value from independent per-datagram probabilities.
//   Pass 2 (the real run): regenerates the identical stream, feeds every
//     message straight into the oracle book and the retransmit buffer
//     (both are exchange-side truth, independent of what the multicast
//     transport does to it), then sends each datagram on line A and line B
//     according to the precomputed schedule -- dropped entirely on one line
//     (arbitration recovers it from the other, no TCP needed), dropped on
//     both (a true gap; only the TCP retransmit path can recover it), or
//     dropped on neither.
//
// The oracle book (direct application, no transport) and the arbitrated
// book (real multicast sockets, real seeded loss, real TCP recovery) are
// compared by canonical digest at the end. Equal digests is the "zero book
// divergence" the claim asks for; this is measured, not assumed.
//
// Usage: live_capture_ab [messages] [capture_path] [both_drops] [a_only_drops] [b_only_drops] [rate]
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <atomic>
#include <chrono>
#include <algorithm>
#include "multicast_posix.hpp"
#include "market_simulator.hpp"
#include "line_arbitrator.hpp"
#include "gap_recovery.hpp"
#include "book_universe.hpp"
#include "canonical.hpp"
#include "tick_capture.hpp"

using namespace tickcap;
using Clock = std::chrono::steady_clock;

constexpr const char* kLineAGroup = "239.7.13.21";
constexpr uint16_t    kLineAPort  = 34712;
constexpr const char* kLineBGroup = "239.7.13.25";
constexpr uint16_t    kLineBPort  = 34715;

static uint64_t now_ns() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            Clock::now().time_since_epoch()).count());
}

// Category assigned to each datagram by index, decided once total datagram
// count is known (see pass 1).
enum class DropCat : uint8_t { None = 0, AOnly = 1, BOnly = 2, Both = 3 };

// Packs `num_messages` deterministic messages (same seed every call) into
// MoldUDP64-style datagrams, invoking `on_datagram(first_seq, count, bytes,
// is_last)` for each completed datagram. Used identically by both passes so
// datagram boundaries cannot drift between them.
template <typename OnMsg, typename OnDatagram>
static uint64_t pack_session(uint64_t seed, uint64_t num_messages,
                             const std::array<uint8_t, kSessionIdLen>& session,
                             OnMsg on_msg, OnDatagram on_datagram) {
    MarketSimulator sim(seed);
    std::vector<uint8_t> body;
    uint64_t first_seq = 0;
    size_t count = 0;
    uint64_t datagram_idx = 0;

    auto flush = [&] {
        if (body.empty()) return;
        PacketHeader h;
        h.session = session;
        h.first_seq = first_seq;
        h.msg_count = static_cast<uint16_t>(count);
        std::vector<uint8_t> pkt;
        pkt.reserve(kPacketHeaderSize + body.size());
        encode_packet_header(pkt, h);
        pkt.insert(pkt.end(), body.begin(), body.end());
        on_datagram(datagram_idx, first_seq, static_cast<uint32_t>(count), pkt);
        ++datagram_idx;
        body.clear();
        count = 0;
    };

    for (uint64_t i = 0; i < num_messages; ++i) {
        DecodedMsg m = sim.next();
        uint64_t seq = i + 1;
        size_t need = 2 + message_size(m.type);
        if (!body.empty() && kPacketHeaderSize + body.size() + need > kMaxDatagram) flush();
        bool pkt_first = body.empty();
        if (pkt_first) first_seq = seq;
        on_msg(m, seq, pkt_first);
        encode_message(body, m);
        ++count;
    }
    flush();
    return datagram_idx;  // total datagrams
}

int main(int argc, char** argv) {
    uint64_t num_messages = (argc > 1) ? std::strtoull(argv[1], nullptr, 10) : 2000000;
    std::string capture_path = (argc > 2) ? argv[2] : "/tmp/session_ab.tcap";
    uint32_t both_drops  = (argc > 3) ? static_cast<uint32_t>(std::strtoul(argv[3], nullptr, 10)) : 5000;
    uint32_t a_only_drops = (argc > 4) ? static_cast<uint32_t>(std::strtoul(argv[4], nullptr, 10)) : 3000;
    uint32_t b_only_drops = (argc > 5) ? static_cast<uint32_t>(std::strtoul(argv[5], nullptr, 10)) : 3000;
    uint64_t rate = (argc > 6) ? std::strtoull(argv[6], nullptr, 10) : 400000;
    const uint64_t seed = 0xA13CADEULL;

    auto session = make_session_id("TCAPAB001");

    std::printf("=== A/B line arbitration + TCP gap recovery ===\n");
    std::printf("messages         : %llu\n", (unsigned long long)num_messages);
    std::printf("line A           : %s:%u\n", kLineAGroup, (unsigned)kLineAPort);
    std::printf("line B           : %s:%u\n", kLineBGroup, (unsigned)kLineBPort);
    std::printf("capture          : %s\n", capture_path.c_str());

    // ---- pass 1: dry run, just to learn the datagram count ----
    uint64_t total_datagrams = pack_session(
        seed, num_messages, session,
        [](const DecodedMsg&, uint64_t, bool) {},
        [](uint64_t, uint64_t, uint32_t, const std::vector<uint8_t>&) {});

    std::printf("datagrams (dry run count) : %llu\n", (unsigned long long)total_datagrams);
    if (uint64_t(both_drops) + a_only_drops + b_only_drops >= total_datagrams) {
        std::fprintf(stderr, "seeded drop counts exceed datagram count; aborting\n");
        return 1;
    }

    // Deterministic shuffle of datagram indices to assign exact drop counts.
    std::vector<uint64_t> idx(total_datagrams);
    for (uint64_t i = 0; i < total_datagrams; ++i) idx[i] = i;
    SplitMix64 shuffle_rng(0xD1CE5EEDULL);
    for (uint64_t i = total_datagrams; i > 1; --i) {
        uint64_t j = shuffle_rng.below(static_cast<uint32_t>(i));
        std::swap(idx[i - 1], idx[j]);
    }
    std::vector<DropCat> category(total_datagrams, DropCat::None);
    uint64_t p = 0;
    for (uint32_t k = 0; k < both_drops; ++k)  category[idx[p++]] = DropCat::Both;
    for (uint32_t k = 0; k < a_only_drops; ++k) category[idx[p++]] = DropCat::AOnly;
    for (uint32_t k = 0; k < b_only_drops; ++k) category[idx[p++]] = DropCat::BOnly;

    // ---- shared infrastructure: retransmit service + arbitrated receiver ----
    RetransmitBuffer retransmit_buf(UINT64_MAX);  // full-session retention; see gap_recovery.hpp
    GapRecoveryServer gap_server(retransmit_buf, 0);
    GapRecoveryClient gap_client("127.0.0.1", gap_server.port());
    std::printf("gap recovery TCP : 127.0.0.1:%u\n", (unsigned)gap_server.port());

    BookUniverse oracle_books;   // ground truth: every message applied directly, no transport
    BookUniverse live_books;     // reconstructed via A/B multicast + arbitration + recovery
    CaptureWriter writer(capture_path, session);
    LineArbitrator arb(live_books, &writer, &gap_client);
    std::mutex arb_mu;   // serializes the two receiver threads' calls into arb (see line_arbitrator.hpp)

    MulticastReceiver rxA(kLineAGroup, kLineAPort);
    MulticastReceiver rxB(kLineBGroup, kLineBPort);
    rxA.set_recv_buffer(64 * 1024 * 1024);
    rxB.set_recv_buffer(64 * 1024 * 1024);
    rxA.set_timeout_ms(300);
    rxB.set_timeout_ms(300);

    std::atomic<bool> publisher_done{false};
    std::atomic<uint64_t> sent_a{0}, sent_b{0}, dropped_a{0}, dropped_b{0};

    std::thread publisher([&] {
        MulticastSender txA(kLineAGroup, kLineAPort);
        MulticastSender txB(kLineBGroup, kLineBPort);
        txA.set_send_buffer(8 * 1024 * 1024);
        txB.set_send_buffer(8 * 1024 * 1024);

        auto send_start = Clock::now();
        pack_session(
            seed, num_messages, session,
            [&](const DecodedMsg& m, uint64_t seq, bool pkt_first) {
                oracle_books.apply(m, seq);
                TickRecord rec;
                rec.capture_ts_ns  = m.exchange_ts_ns;  // exchange-side proxy timestamp
                rec.exchange_ts_ns = m.exchange_ts_ns;
                rec.seq_num        = seq;
                rec.msg_type       = static_cast<uint8_t>(m.type);
                rec.instrument_id  = m.instrument_id;
                rec.order_id       = m.order_id;
                rec.new_order_id   = m.new_order_id;
                rec.side           = static_cast<uint8_t>(m.side);
                rec.price_ticks    = m.price_ticks;
                rec.qty            = m.qty;
                rec.pkt_first      = pkt_first ? 1 : 0;
                retransmit_buf.append(rec);

                // Paced the same way live_capture.cpp is: unpaced publishing
                // on loopback overruns the receive buffers and injects real,
                // unseeded loss on top of the seeded schedule, which would
                // contaminate the "exactly N seeded drops" measurement below.
                if (rate > 0 && (seq & 0x3FF) == 0x3FF) {
                    double target = double(seq) / double(rate);
                    double actual = std::chrono::duration<double>(Clock::now() - send_start).count();
                    if (actual < target)
                        std::this_thread::sleep_for(std::chrono::duration<double>(target - actual));
                }
            },
            [&](uint64_t dgram_idx, uint64_t /*first_seq*/, uint32_t /*count*/, const std::vector<uint8_t>& pkt) {
                DropCat cat = category[dgram_idx];
                bool drop_a = (cat == DropCat::AOnly || cat == DropCat::Both);
                bool drop_b = (cat == DropCat::BOnly || cat == DropCat::Both);
                if (!drop_a) { txA.send(pkt.data(), pkt.size()); sent_a.fetch_add(1, std::memory_order_relaxed); }
                else dropped_a.fetch_add(1, std::memory_order_relaxed);
                if (!drop_b) { txB.send(pkt.data(), pkt.size()); sent_b.fetch_add(1, std::memory_order_relaxed); }
                else dropped_b.fetch_add(1, std::memory_order_relaxed);
            });
        publisher_done.store(true, std::memory_order_release);
    });

    // Receiver loops (one per line). Each decodes nothing itself -- raw
    // bytes go straight to the mutex-protected arbitrator, which is the
    // single logical consumer (see line_arbitrator.hpp's threading note).
    auto recv_loop = [&](MulticastReceiver& rx, int line, std::atomic<uint64_t>& recv_count) {
        std::vector<uint8_t> buf(kMaxDatagram + 64);
        int idle_rounds = 0;
        while (true) {
            ssize_t n = rx.recv(buf.data(), buf.size());
            if (n > 0) {
                idle_rounds = 0;
                uint64_t cap_ts = now_ns();
                {
                    std::lock_guard<std::mutex> lk(arb_mu);
                    arb.on_datagram(line, buf.data(), static_cast<size_t>(n), cap_ts);
                }
                recv_count.fetch_add(1, std::memory_order_relaxed);
            } else {
                if (publisher_done.load(std::memory_order_acquire) && ++idle_rounds >= 3) break;
            }
        }
    };

    std::atomic<uint64_t> recv_a{0}, recv_b{0};
    auto t0 = Clock::now();
    std::thread rxb_thread([&] { recv_loop(rxB, 1, recv_b); });
    recv_loop(rxA, 0, recv_a);
    rxb_thread.join();
    publisher.join();
    auto t1 = Clock::now();

    {
        std::lock_guard<std::mutex> lk(arb_mu);
        arb.finalize();
    }
    writer.close();
    gap_server.stop();

    double secs = std::chrono::duration<double>(t1 - t0).count();
    const ArbStats& st = arb.stats();

    std::printf("\n--- transport ---\n");
    std::printf("sent A / dropped A         : %llu / %llu\n", (unsigned long long)sent_a.load(), (unsigned long long)dropped_a.load());
    std::printf("sent B / dropped B         : %llu / %llu\n", (unsigned long long)sent_b.load(), (unsigned long long)dropped_b.load());
    std::printf("received A / received B    : %llu / %llu\n", (unsigned long long)recv_a.load(), (unsigned long long)recv_b.load());
    std::printf("elapsed                    : %.3f s\n", secs);

    std::printf("\n--- arbitration ---\n");
    std::printf("messages delivered         : %llu\n", (unsigned long long)st.messages);
    std::printf("delivered from line A      : %llu\n", (unsigned long long)st.delivered_from_a);
    std::printf("delivered from line B      : %llu\n", (unsigned long long)st.delivered_from_b);
    std::printf("duplicate datagrams dropped: %llu\n", (unsigned long long)st.duplicates_dropped);
    std::printf("malformed                  : %llu\n", (unsigned long long)st.malformed);

    std::printf("\n--- gap recovery (TCP retransmit path) ---\n");
    std::printf("seeded double-line drops   : %u (target)\n", both_drops);
    std::printf("gap events (both lines down): %llu\n", (unsigned long long)st.gap_events);
    std::printf("messages inside those gaps : %llu\n", (unsigned long long)st.messages_in_gaps);
    std::printf("messages recovered via TCP : %llu\n", (unsigned long long)st.messages_recovered);
    std::printf("messages unrecovered       : %llu\n", (unsigned long long)st.messages_unrecovered);
    std::printf("gap-recovery requests served: %llu\n", (unsigned long long)gap_server.requests_served());
    std::printf("gap-recovery records served : %llu\n", (unsigned long long)gap_server.records_served());

    std::string oracle_digest = canonical_book_digest(oracle_books);
    std::string live_digest   = canonical_book_digest(live_books);
    bool match = (oracle_digest == live_digest);

    std::printf("\n--- book divergence check (oracle: direct apply, no transport) ---\n");
    std::printf("oracle book sha256 : %s\n", oracle_digest.c_str());
    std::printf("live   book sha256 : %s\n", live_digest.c_str());
    std::printf("oracle resting orders : %llu\n", (unsigned long long)oracle_books.total_resting_orders());
    std::printf("live   resting orders : %llu\n", (unsigned long long)live_books.total_resting_orders());
    std::printf("%s: reconstructed book %s the oracle\n",
                match ? "PASS" : "FAIL", match ? "matches exactly" : "DIVERGES from");

    // `gap_events` is not the right thing to compare against `both_drops`:
    // it counts contiguous merged gap RANGES, and with both_drops datagrams
    // seeded independently at random across the session, two seeded drops
    // that land on adjacent datagrams merge into a single gap event covering
    // both (a real property of the recovery path, not a bug -- one TCP
    // request naturally covers a whole contiguous hole, seeded as one drop
    // or five in a row). The claim is about outcome, not event count: every
    // message that fell inside a seeded double-line-drop datagram was
    // recovered, and the reconstructed book is byte-identical to the
    // oracle. Both of those are checked directly below. (An earlier version
    // of this check compared gap_events == both_drops directly and reported
    // false even on a fully correct run, purely from this event-merging
    // effect; see README Findings.)
    bool all_seeded_drops_recovered = (st.messages_unrecovered == 0) && (st.gap_events > 0);
    std::printf("\ngap events observed (contiguous merged ranges, <= %u seeded double-line-drop datagrams "
                "when adjacent drops merge): %llu\n",
                both_drops, (unsigned long long)st.gap_events);
    std::printf("all %u seeded double-line-drop datagrams (%llu messages) recovered with zero book "
                "divergence: %s\n",
                both_drops, (unsigned long long)st.messages_in_gaps,
                (match && all_seeded_drops_recovered) ? "true" : "false");

    return (match && all_seeded_drops_recovered) ? 0 : 2;
}
