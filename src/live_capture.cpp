// Live session: publish a simulated feed over real UDP multicast, decode it
// off the socket into full-depth books, and capture every message.
//
// Both ends run in one process (sender thread, receiver on the main thread)
// against a real 239.x.x.x group, so the datagrams genuinely traverse the
// kernel's network stack rather than being handed over in memory. What this
// does *not* demonstrate is cross-host multicast routing, which needs more
// than one host; that limitation is stated in the README rather than
// implied away.
//
// Two digests are produced and written next to the capture:
//   wire    SHA-256 over every datagram as received, length-prefixed so
//           packet boundaries are part of the hash rather than washed out
//           by concatenation.
//   book    SHA-256 over the canonical serialization of final book state.
// replay_verify recomputes both from the capture file alone.
//
// Usage: live_capture [messages] [capture_path] [loss_permille] [rate_msgs_sec]
//   rate_msgs_sec = 0 means publish as fast as possible, which on loopback
//   overruns the receiver and produces real (uninjected) packet loss.
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <atomic>
#include <chrono>
#include <vector>
#include "multicast_posix.hpp"
#include "market_simulator.hpp"
#include "feed_decoder.hpp"
#include "book_universe.hpp"
#include "canonical.hpp"
#include "tick_capture.hpp"
#include "sha256.hpp"

using namespace tickcap;
using Clock = std::chrono::steady_clock;

static uint64_t now_ns() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            Clock::now().time_since_epoch()).count());
}

int main(int argc, char** argv) {
    uint64_t num_messages = (argc > 1) ? std::strtoull(argv[1], nullptr, 10) : 2000000;
    std::string capture_path = (argc > 2) ? argv[2] : "/tmp/session.tcap";
    uint32_t loss_permille = (argc > 3) ? static_cast<uint32_t>(std::strtoul(argv[3], nullptr, 10)) : 0;
    uint64_t rate = (argc > 4) ? std::strtoull(argv[4], nullptr, 10) : 400000;

    auto session = make_session_id("TCAPSIM001");

    std::printf("=== live multicast capture ===\n");
    std::printf("messages       : %llu\n", (unsigned long long)num_messages);
    std::printf("group          : %s:%u\n", kDefaultGroup, (unsigned)kDefaultPort);
    std::printf("capture        : %s\n", capture_path.c_str());
    std::printf("injected loss  : %.1f%% of datagrams\n", loss_permille / 10.0);
    if (rate) std::printf("publish rate   : %llu msgs/sec (paced)\n", (unsigned long long)rate);
    else      std::printf("publish rate   : unlimited (expect receiver overrun)\n");

    MulticastReceiver rx(kDefaultGroup, kDefaultPort);
    const int want_rcvbuf = 64 * 1024 * 1024;
    int got_rcvbuf = rx.set_recv_buffer(want_rcvbuf);
    rx.set_timeout_ms(300);
    std::printf("recv buffer    : requested %d KiB, granted %d KiB%s\n",
                want_rcvbuf / 1024, got_rcvbuf / 1024,
                got_rcvbuf < want_rcvbuf ? "  <-- CLAMPED by net.core.rmem_max" : "");
    if (got_rcvbuf < want_rcvbuf)
        std::printf("                 raise it with: sudo sysctl -w net.core.rmem_max=%d\n",
                    want_rcvbuf);
    if (rx.joined_on_any())
        std::printf("note           : joined group on INADDR_ANY (loopback join was refused)\n");

    std::atomic<bool> sender_done{false};
    std::atomic<uint64_t> sent_datagrams{0};
    std::atomic<uint64_t> dropped_datagrams{0};

    std::thread sender_thread([&] {
        MulticastSender tx(kDefaultGroup, kDefaultPort);
        tx.set_send_buffer(8 * 1024 * 1024);
        MarketSimulator sim(0xC0FFEEULL);
        SplitMix64 loss_rng(0xD1CEULL);

        std::vector<uint8_t> body;
        uint64_t first_seq = 0;
        size_t count = 0;
        uint64_t seq = 1;
        uint64_t packets = 0;

        auto flush = [&] {
            if (body.empty()) return;
            PacketHeader h;
            h.session   = session;
            h.first_seq = first_seq;
            h.msg_count = static_cast<uint16_t>(count);
            std::vector<uint8_t> pkt;
            pkt.reserve(kPacketHeaderSize + body.size());
            encode_packet_header(pkt, h);
            pkt.insert(pkt.end(), body.begin(), body.end());

            bool drop = loss_permille > 0 && (loss_rng.below(1000) < loss_permille);
            if (drop) {
                dropped_datagrams.fetch_add(1, std::memory_order_relaxed);
            } else {
                tx.send(pkt.data(), pkt.size());
                sent_datagrams.fetch_add(1, std::memory_order_relaxed);
            }
            ++packets;
            body.clear();
            count = 0;
        };

        // UDP has no flow control. Publishing as fast as the encoder can run
        // simply overruns the receiver's socket buffer and the kernel
        // discards the excess -- real loss, correctly reported as gaps, but
        // loss caused by the harness rather than by anything worth
        // demonstrating. Rate limiting lets the same binary show either
        // behaviour: a sustainable rate for the clean reproducibility run,
        // or rate=0 to deliberately provoke receiver overrun.
        auto send_start = Clock::now();
        for (uint64_t i = 0; i < num_messages; ++i) {
            DecodedMsg m = sim.next();
            size_t need = 2 + message_size(m.type);
            if (!body.empty() && kPacketHeaderSize + body.size() + need > kMaxDatagram) flush();
            if (body.empty()) first_seq = seq;
            encode_message(body, m);
            ++count;
            ++seq;

            if (rate > 0 && (i & 0x3FF) == 0x3FF) {
                double target = double(i + 1) / double(rate);
                double actual = std::chrono::duration<double>(Clock::now() - send_start).count();
                if (actual < target) {
                    std::this_thread::sleep_for(std::chrono::duration<double>(target - actual));
                }
            }
        }
        flush();
        sender_done.store(true, std::memory_order_release);
    });

    BookUniverse books;
    CaptureWriter writer(capture_path, session);
    FeedDecoder decoder(books, &writer);

    Sha256 wire_hash;
    std::vector<uint8_t> buf(kMaxDatagram + 64);
    uint64_t recv_datagrams = 0, recv_bytes = 0;
    auto t0 = Clock::now();
    int idle_rounds = 0;

    while (true) {
        ssize_t n = rx.recv(buf.data(), buf.size());
        if (n > 0) {
            idle_rounds = 0;
            uint64_t cap_ts = now_ns();
            // Length-prefixed so the digest is over the framed stream, not a
            // flat concatenation in which packet boundaries would vanish.
            uint8_t lenbuf[4];
            uint32_t ln = static_cast<uint32_t>(n);
            for (int b = 0; b < 4; ++b) lenbuf[b] = static_cast<uint8_t>(ln >> (8 * b));
            wire_hash.update(lenbuf, 4);
            wire_hash.update(buf.data(), static_cast<size_t>(n));
            decoder.on_datagram(buf.data(), static_cast<size_t>(n), cap_ts);
            ++recv_datagrams;
            recv_bytes += static_cast<uint64_t>(n);
        } else {
            if (sender_done.load(std::memory_order_acquire) && ++idle_rounds >= 3) break;
        }
    }
    auto t1 = Clock::now();
    sender_thread.join();
    writer.close();

    double secs = std::chrono::duration<double>(t1 - t0).count();
    const FeedStats& fs = decoder.stats();
    const BookStats& bs = books.stats();

    std::printf("\n--- transport ---\n");
    std::printf("datagrams sent      : %llu\n", (unsigned long long)sent_datagrams.load());
    std::printf("datagrams dropped   : %llu (injected)\n", (unsigned long long)dropped_datagrams.load());
    std::printf("datagrams received  : %llu\n", (unsigned long long)recv_datagrams);
    std::printf("bytes received      : %llu\n", (unsigned long long)recv_bytes);
    std::printf("messages decoded    : %llu\n", (unsigned long long)fs.messages);
    std::printf("avg msgs/datagram   : %.1f\n",
                recv_datagrams ? double(fs.messages) / double(recv_datagrams) : 0.0);
    std::printf("elapsed             : %.3f s (%.0f msgs/sec observed)\n",
                secs, secs > 0 ? fs.messages / secs : 0.0);

    std::printf("\n--- sequence integrity ---\n");
    std::printf("gap events          : %llu\n", (unsigned long long)fs.gap_events);
    std::printf("messages lost       : %llu\n", (unsigned long long)fs.messages_lost);
    std::printf("stale/dup packets   : %llu\n", (unsigned long long)fs.stale_packets);
    std::printf("malformed           : %llu\n", (unsigned long long)fs.malformed);

    std::printf("\n--- book ---\n");
    std::printf("adds/execs/cancels/deletes/replaces : %llu/%llu/%llu/%llu/%llu\n",
                (unsigned long long)bs.adds, (unsigned long long)bs.executes,
                (unsigned long long)bs.cancels, (unsigned long long)bs.deletes,
                (unsigned long long)bs.replaces);
    std::printf("executed qty        : %llu\n", (unsigned long long)bs.executed_qty);
    std::printf("unknown order refs  : %llu%s\n", (unsigned long long)bs.unknown_order_refs,
                fs.messages_lost ? "  (expected after a gap)" : "");
    std::printf("resting orders      : %llu\n", (unsigned long long)books.total_resting_orders());

    std::string wire_digest = wire_hash.hex();
    std::string book_digest = canonical_book_digest(books);
    std::printf("\n--- digests ---\n");
    std::printf("wire  sha256 : %s\n", wire_digest.c_str());
    std::printf("book  sha256 : %s\n", book_digest.c_str());

    // Sidecar so replay_verify has something independent to check against.
    std::string dpath = capture_path + ".digests";
    std::FILE* df = std::fopen(dpath.c_str(), "w");
    if (df) {
        std::fprintf(df, "wire %s\n", wire_digest.c_str());
        std::fprintf(df, "book %s\n", book_digest.c_str());
        std::fprintf(df, "messages %llu\n", (unsigned long long)fs.messages);
        std::fprintf(df, "datagrams %llu\n", (unsigned long long)recv_datagrams);
        std::fprintf(df, "gap_events %llu\n", (unsigned long long)fs.gap_events);
        std::fprintf(df, "messages_lost %llu\n", (unsigned long long)fs.messages_lost);
        std::fprintf(df, "resting %llu\n", (unsigned long long)books.total_resting_orders());
        std::fclose(df);
        std::printf("digests written to %s\n", dpath.c_str());
    }

    std::printf("\n--- capture encoding (winning codec per column) ---\n");
    const auto& hist = writer.encoding_histogram();
    const auto& cbytes = writer.column_bytes();
    uint64_t total_col_bytes = 0;
    for (uint64_t b : cbytes) total_col_bytes += b;
    for (size_t c = 0; c < kNumColumns; ++c) {
        size_t best = 0;
        for (size_t e = 1; e < 5; ++e) if (hist[c][e] > hist[c][best]) best = e;
        double bpr = writer.total_records() ? double(cbytes[c]) / double(writer.total_records()) : 0.0;
        std::printf("  %-16s %-6s %10llu bytes  (%.3f bytes/record)\n",
                    column_defs()[c].name, encoding_name(static_cast<Encoding>(best)),
                    (unsigned long long)cbytes[c], bpr);
    }
    std::printf("  column payload total: %llu bytes over %llu records\n",
                (unsigned long long)total_col_bytes, (unsigned long long)writer.total_records());
    return 0;
}
