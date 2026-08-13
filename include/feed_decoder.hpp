// Datagram -> messages -> book, with sequence-gap detection and capture.
//
// The receive path is deliberately single-consumer. The sibling project in
// this account shards the feed across threads to maximize decode
// throughput; here the goal is different, and the two goals are in direct
// tension: a sharded pipeline's *observable output* is deterministic, but
// the interleaving that produced it is not, so "replay produced the same
// bytes" would be a claim about the book's final state only. Keeping one
// consumer means the capture file records the exact order in which the
// process saw the world, and replay walks that order again. Determinism was
// worth more than parallelism for this project; that is a tradeoff, not a
// free win, and the README says so.
#pragma once
#include <cstdint>
#include <vector>
#include "wire_protocol.hpp"
#include "book_universe.hpp"
#include "tick_capture.hpp"

namespace tickcap {

struct FeedStats {
    uint64_t datagrams      = 0;
    uint64_t messages       = 0;
    uint64_t malformed      = 0;
    uint64_t gap_events     = 0;   // datagrams that arrived ahead of expectation
    uint64_t messages_lost  = 0;   // total sequence numbers skipped
    uint64_t stale_packets  = 0;   // arrived behind expectation (dup / reorder)
};

class FeedDecoder {
public:
    explicit FeedDecoder(BookUniverse& books, CaptureWriter* capture = nullptr)
        : books_(books), capture_(capture) {}

    // Feeds one datagram. `capture_ts_ns` is the receiver's arrival
    // timestamp and is *recorded*, never regenerated -- replaying a session
    // must reproduce the timestamps the original run observed, not new ones
    // from the replay machine's clock. This is the single most important
    // rule for the bit-for-bit property, and the easiest to violate by
    // accident: a capture writer that stamped "now" at write time would look
    // completely correct and quietly destroy reproducibility.
    void on_datagram(const uint8_t* data, size_t len, uint64_t capture_ts_ns) {
        ++stats_.datagrams;
        ByteReader r(data, len);
        PacketHeader h;
        if (!decode_packet_header(r, h)) { ++stats_.malformed; return; }

        if (!seen_first_) {
            expected_seq_ = h.first_seq;
            session_ = h.session;
            seen_first_ = true;
        }
        if (h.first_seq > expected_seq_) {
            ++stats_.gap_events;
            stats_.messages_lost += h.first_seq - expected_seq_;
            expected_seq_ = h.first_seq;
        } else if (h.first_seq < expected_seq_) {
            ++stats_.stale_packets;
        }

        for (uint16_t i = 0; i < h.msg_count; ++i) {
            DecodedMsg m;
            if (!decode_message(r, m)) { ++stats_.malformed; return; }
            uint64_t seq = h.first_seq + i;

            if (capture_) {
                TickRecord rec;
                rec.capture_ts_ns  = capture_ts_ns;
                rec.exchange_ts_ns = m.exchange_ts_ns;
                rec.seq_num        = seq;
                rec.msg_type       = static_cast<uint8_t>(m.type);
                rec.instrument_id  = m.instrument_id;
                rec.order_id       = m.order_id;
                rec.new_order_id   = m.new_order_id;
                rec.side           = static_cast<uint8_t>(m.side);
                rec.price_ticks    = m.price_ticks;
                rec.qty            = m.qty;
                rec.pkt_first      = (i == 0) ? 1 : 0;
                capture_->append(rec);
            }

            books_.apply(m, seq);
            ++stats_.messages;
        }
        expected_seq_ = h.first_seq + h.msg_count;
    }

    const FeedStats& stats() const { return stats_; }
    const std::array<uint8_t, kSessionIdLen>& session() const { return session_; }

private:
    BookUniverse& books_;
    CaptureWriter* capture_;
    FeedStats stats_;
    uint64_t expected_seq_ = 0;
    bool seen_first_ = false;
    std::array<uint8_t, kSessionIdLen> session_{};
};

// ------------------------------------------------------------ replay side

// Rebuilds datagrams from captured records. The pkt_first column marks
// where each original datagram began, so the transport framing -- not just
// the message contents -- is recoverable. Without that one flag column the
// capture could reproduce the message stream but not the packet stream, and
// the reproducibility claim would have a hole in it exactly where the
// transport layer sits.
inline std::vector<std::vector<uint8_t>>
reencode_datagrams(const std::vector<TickRecord>& recs,
                   const std::array<uint8_t, kSessionIdLen>& session) {
    std::vector<std::vector<uint8_t>> out;
    size_t i = 0;
    while (i < recs.size()) {
        size_t j = i + 1;
        while (j < recs.size() && recs[j].pkt_first == 0) ++j;
        PacketHeader h;
        h.session   = session;
        h.first_seq = recs[i].seq_num;
        h.msg_count = static_cast<uint16_t>(j - i);
        std::vector<uint8_t> pkt;
        encode_packet_header(pkt, h);
        for (size_t k = i; k < j; ++k) encode_message(pkt, record_to_msg(recs[k]));
        out.push_back(std::move(pkt));
        i = j;
    }
    return out;
}

// Concatenated message bytes, without transport framing.
inline std::vector<uint8_t> reencode_message_stream(const std::vector<TickRecord>& recs) {
    std::vector<uint8_t> out;
    for (const TickRecord& r : recs) encode_message(out, record_to_msg(r));
    return out;
}

// Rebuilds book state by replaying captured records directly (no transport).
inline void replay_records(const std::vector<TickRecord>& recs, BookUniverse& books) {
    for (const TickRecord& r : recs) books.apply(record_to_msg(r), r.seq_num);
}

} // namespace tickcap
