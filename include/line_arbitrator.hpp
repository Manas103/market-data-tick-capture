// A/B line arbitration with sequence-gap recovery over a TCP retransmit
// path.
//
// Real redundant multicast feeds (this is the standard architecture, not
// this project's invention -- Nasdaq TotalView and most exchange
// multicast feeds publish an identical primary (A) and secondary (B) line
// over independent network paths) exist because UDP has no retransmission:
// once a datagram is gone, the receiver's only recourse on the multicast
// side is that the *other* line probably still has it. Arbitration is the
// receive-side logic that exploits that: track the next sequence number
// this process needs, and whichever line delivers it first wins; the other
// line's copy of the same range is a duplicate, not a second event.
//
// A line only fails to help when *both* lines drop the same sequence
// range, which is materially rarer than either line dropping alone (two
// independent network paths would have to lose the same messages), but not
// impossible, and when it happens neither redundant line has an answer --
// only a service that can retransmit does. That is gap_recovery.hpp's job;
// this class is the one that decides when to call it.
//
// Threading: on_datagram() is called from whichever thread owns a given
// line's socket. This class assumes it is invoked from a single thread (the
// two lines are drained by one receive loop in this project's demos, the
// same single-consumer choice feed_decoder.hpp makes and explains); it is
// not internally synchronized.
#pragma once
#include <cstdint>
#include <cstddef>
#include <map>
#include <vector>
#include <array>
#include <algorithm>
#include "wire_protocol.hpp"
#include "tick_capture.hpp"
#include "book_universe.hpp"
#include "gap_recovery.hpp"

namespace tickcap {

struct ArbStats {
    uint64_t datagrams_a         = 0;
    uint64_t datagrams_b         = 0;
    uint64_t malformed           = 0;
    uint64_t duplicates_dropped  = 0;   // datagram wholly behind expected_seq_
    uint64_t messages            = 0;   // total messages delivered downstream
    uint64_t delivered_from_a    = 0;
    uint64_t delivered_from_b    = 0;
    uint64_t gap_events          = 0;   // both lines skipped past expected_seq_
    uint64_t messages_in_gaps    = 0;   // total messages those gaps covered
    uint64_t messages_recovered  = 0;   // filled by a TCP retransmit request
    uint64_t messages_unrecovered = 0;  // gap exceeded what recovery could return
};

class LineArbitrator {
public:
    // `recovery` may be null, in which case a double-line gap is skipped
    // (counted as unrecovered) exactly as an undefended single-line feed
    // handler would -- useful for measuring what arbitration alone buys
    // before recovery is added.
    LineArbitrator(BookUniverse& books, CaptureWriter* capture, GapRecoveryClient* recovery)
        : books_(books), capture_(capture), recovery_(recovery) {}

    void on_datagram(int line, const uint8_t* data, size_t len, uint64_t capture_ts_ns) {
        ByteReader r(data, len);
        PacketHeader h;
        if (!decode_packet_header(r, h)) { ++stats_.malformed; return; }
        if (!seen_first_) {
            expected_seq_ = h.first_seq;
            session_ = h.session;
            seen_first_ = true;
        }
        if (line == 0) ++stats_.datagrams_a; else ++stats_.datagrams_b;

        uint64_t count = h.msg_count;
        if (h.first_seq + count <= expected_seq_) {
            // Wholly behind what we've already delivered -- the redundant
            // line's copy of a range the other line (or this one, earlier)
            // already supplied. Not an error; this is arbitration working.
            ++stats_.duplicates_dropped;
            return;
        }

        std::vector<DecodedMsg> msgs;
        msgs.reserve(h.msg_count);
        for (uint16_t i = 0; i < h.msg_count; ++i) {
            DecodedMsg m;
            if (!decode_message(r, m)) { ++stats_.malformed; return; }
            msgs.push_back(m);
        }

        pending_[line][h.first_seq] = PendingDgram{h.first_seq, std::move(msgs), capture_ts_ns};
        drain();
    }

    // Call after the session ends (no more datagrams coming on either
    // line). Any sequence range still stuck behind a gap at this point is a
    // trailing loss with nothing left to reveal it -- the same
    // fundamentally-undetectable case feed_decoder.hpp's single-line path
    // calls out. This makes one last recovery attempt for whatever gap is
    // currently known, then reports what is left unresolved rather than
    // spinning forever waiting for a datagram that will never arrive.
    void finalize() {
        drain();
        if (!pending_[0].empty() || !pending_[1].empty()) {
            uint64_t gap_end = 0;
            if (!pending_[0].empty() && !pending_[1].empty())
                gap_end = std::min(pending_[0].begin()->first, pending_[1].begin()->first);
            else if (!pending_[0].empty())
                gap_end = pending_[0].begin()->first;
            else
                gap_end = pending_[1].begin()->first;
            force_recover_to(gap_end);
            drain();
        }
    }

    const ArbStats& stats() const { return stats_; }
    uint64_t expected_seq() const { return expected_seq_; }
    const std::array<uint8_t, kSessionIdLen>& session() const { return session_; }

private:
    struct PendingDgram {
        uint64_t first_seq = 0;
        std::vector<DecodedMsg> msgs;
        uint64_t capture_ts_ns = 0;
    };
    using PendingMap = std::map<uint64_t, PendingDgram>;

    void deliver_record(const TickRecord& rec) {
        if (capture_) capture_->append(rec);
        books_.apply(record_to_msg(rec), rec.seq_num);
        ++stats_.messages;
    }

    void deliver(const DecodedMsg& m, uint64_t seq, uint64_t capture_ts_ns, bool pkt_first) {
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
        rec.pkt_first      = pkt_first ? 1 : 0;
        deliver_record(rec);
    }

    // Consumes whatever this line has buffered that covers expected_seq_,
    // garbage-collecting stale entries along the way. Returns true if it
    // delivered anything.
    bool try_consume_from(int line) {
        PendingMap& q = pending_[line];
        while (!q.empty()) {
            auto it = q.upper_bound(expected_seq_);
            if (it == q.begin()) return false;      // nothing at or before expected_seq_
            --it;
            PendingDgram& d = it->second;
            uint64_t end = d.first_seq + d.msgs.size();
            if (end <= expected_seq_) { q.erase(it); continue; }  // stale, collect and keep looking
            if (d.first_seq > expected_seq_) return false;         // future only, not usable yet

            size_t start_idx = static_cast<size_t>(expected_seq_ - d.first_seq);
            for (size_t i = start_idx; i < d.msgs.size(); ++i)
                deliver(d.msgs[i], d.first_seq + i, d.capture_ts_ns, i == 0);
            size_t delivered = d.msgs.size() - start_idx;
            expected_seq_ = end;
            q.erase(it);
            if (line == 0) stats_.delivered_from_a += delivered;
            else            stats_.delivered_from_b += delivered;
            return true;
        }
        return false;
    }

    // Only declares a gap once BOTH lines have datagrams buffered strictly
    // ahead of expected_seq_ -- i.e. both lines have demonstrably moved
    // past the sequence number we need, so neither redundant copy exists.
    // An empty queue on one line just means that line hasn't sent its next
    // datagram yet, which is not a gap, only a race to be waited out.
    bool try_recover_gap() {
        if (pending_[0].empty() || pending_[1].empty()) return false;
        uint64_t gap_end = std::min(pending_[0].begin()->first, pending_[1].begin()->first);
        if (gap_end <= expected_seq_) return false;
        force_recover_to(gap_end);
        return true;
    }

    void force_recover_to(uint64_t gap_end) {
        uint32_t need = static_cast<uint32_t>(gap_end - expected_seq_);
        ++stats_.gap_events;
        stats_.messages_in_gaps += need;
        if (!recovery_) {
            stats_.messages_unrecovered += need;
            expected_seq_ = gap_end;
            return;
        }
        uint8_t status = 2;
        std::vector<TickRecord> recs = recovery_->request_range(expected_seq_, need, status);
        for (const TickRecord& rec : recs) {
            deliver_record(rec);
            ++expected_seq_;
        }
        stats_.messages_recovered += recs.size();
        if (recs.size() < need) {
            stats_.messages_unrecovered += (need - recs.size());
            expected_seq_ = gap_end;  // retention window exhausted; move on
        }
    }

    void drain() {
        bool progress = true;
        while (progress) {
            progress = try_consume_from(0) || try_consume_from(1);
            if (!progress) progress = try_recover_gap();
        }
    }

    BookUniverse& books_;
    CaptureWriter* capture_;
    GapRecoveryClient* recovery_;
    ArbStats stats_;
    uint64_t expected_seq_ = 0;
    bool seen_first_ = false;
    std::array<uint8_t, kSessionIdLen> session_{};
    std::array<PendingMap, 2> pending_;  // [0] = line A, [1] = line B
};

} // namespace tickcap
