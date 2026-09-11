// Sequence-gap recovery over a TCP retransmit path, modeled loosely on
// Nasdaq's SoupBinTCP/GLIMPSE recovery pattern: multicast carries the live
// feed and is unrecoverable by construction (UDP has no retransmission), so
// a gap discovered on the multicast side is filled by asking a TCP service
// for the missing sequence range, and that service answers out of a
// retransmit buffer built from everything it has already sent.
//
// This is explicitly GLIMPSE-*style*, not the Nasdaq GLIMPSE/SoupBinTCP wire
// format: the request/response framing below is this project's own, small
// enough to read in one sitting, but it plays the same role -- a client that
// knows it is missing sequence numbers [start, start+count) asks, and gets
// back exactly those records (or fewer, if the server's retention window
// does not go back that far, which is reported rather than hidden).
//
// The retransmit buffer here retains the whole session, which is a
// simplification stated plainly: a production retransmit service sizes its
// window to the worst outage it is willing to recover from and evicts
// older sequence numbers past that, at which point a gap recovery request
// for evicted sequence numbers comes back short. RetransmitBuffer supports
// that (see `retention_floor()`); the demo binaries just set the retention
// large enough to cover the whole seeded-loss session, and say so.
#pragma once
#include <cstdint>
#include <cstring>
#include <vector>
#include <mutex>
#include <thread>
#include <atomic>
#include <functional>
#include "tcp_io.hpp"
#include "tick_capture.hpp"
#include "le_bytes.hpp"

namespace tickcap {

// ---------------------------------------------------------- wire framing
//
// Request  (client -> server), 16 bytes:
//   u32 magic ('G','A','P','Q')   u64 start_seq   u32 count
//
// Response (server -> client), 9-byte header then `returned_count` fixed
// 57-byte records (the same field set as tick_capture.hpp's TickRecord,
// serialized RAW -- this is a small control-plane path, not the bulk
// columnar format, so there is no benefit to the codec machinery here):
//   u32 magic ('G','A','P','R')   u32 returned_count   u8 status
//   status: 0 = full range served, 1 = partial (retention ran out), 2 = none available

constexpr uint32_t kGapReqMagic  = 0x51504147u;  // "GAPQ" little-endian read as bytes G,A,P,Q
constexpr uint32_t kGapRespMagic = 0x52504147u;  // "GAPR"
constexpr size_t   kGapReqSize   = 16;
constexpr size_t   kGapRespHdrSize = 9;
constexpr size_t   kGapRecordSize  = 57;  // 8+8+8+1+2+8+8+1+8+4+1, see below

inline void encode_gap_record(std::vector<uint8_t>& out, const TickRecord& r) {
    put_u64(out, r.capture_ts_ns);
    put_u64(out, r.exchange_ts_ns);
    put_u64(out, r.seq_num);
    put_u8(out,  r.msg_type);
    put_u16(out, r.instrument_id);
    put_u64(out, r.order_id);
    put_u64(out, r.new_order_id);
    put_u8(out,  r.side);
    put_i64(out, r.price_ticks);
    put_u32(out, r.qty);
    put_u8(out,  r.pkt_first);
}

inline TickRecord decode_gap_record(ByteReader& br) {
    TickRecord r;
    r.capture_ts_ns  = br.u64();
    r.exchange_ts_ns = br.u64();
    r.seq_num        = br.u64();
    r.msg_type       = br.u8();
    r.instrument_id  = br.u16();
    r.order_id       = br.u64();
    r.new_order_id   = br.u64();
    r.side           = br.u8();
    r.price_ticks    = br.i64();
    r.qty            = br.u32();
    r.pkt_first      = br.u8();
    return r;
}

// -------------------------------------------------------- retransmit buffer

// Append-only log of every message the publisher has sent, indexed by
// sequence number (sequence numbers here start at 1 and are dense, matching
// the feed's own scheme in wire_protocol.hpp/feed_decoder.hpp). Thread-safe:
// the publisher thread appends while gap-recovery worker threads read.
class RetransmitBuffer {
public:
    explicit RetransmitBuffer(uint64_t retention = UINT64_MAX) : retention_(retention) {}

    void append(const TickRecord& r) {
        std::lock_guard<std::mutex> lk(mu_);
        records_.push_back(r);
        if (records_.size() > retention_) {
            // Evict from the front. Rare in the demos (retention is sized
            // to the whole session) but implemented for real, not stubbed,
            // because a retransmit service that could not evict would leak
            // memory over a real trading day.
            records_.erase(records_.begin());
            floor_seq_ += 1;
        }
    }

    // Returns up to `count` records starting at `start_seq`. `status` is set
    // to 0 (full), 1 (partial -- retention did not go back far enough or the
    // buffer does not yet have that many), or 2 (none available at all).
    std::vector<TickRecord> fetch(uint64_t start_seq, uint32_t count, uint8_t& status) const {
        std::lock_guard<std::mutex> lk(mu_);
        std::vector<TickRecord> out;
        uint64_t first_available = floor_seq_ + 1;
        uint64_t effective_start = start_seq < first_available ? first_available : start_seq;
        uint64_t last_seq = records_.empty() ? 0 : records_.back().seq_num;
        for (uint64_t s = effective_start; s < start_seq + count && s <= last_seq; ++s) {
            size_t idx = static_cast<size_t>(s - first_available);
            if (idx < records_.size()) out.push_back(records_[idx]);
        }
        if (out.size() == count) status = 0;
        else if (!out.empty()) status = 1;
        else status = 2;
        return out;
    }

    uint64_t retention_floor() const {
        std::lock_guard<std::mutex> lk(mu_);
        return floor_seq_;
    }

    size_t size() const {
        std::lock_guard<std::mutex> lk(mu_);
        return records_.size();
    }

private:
    mutable std::mutex mu_;
    std::vector<TickRecord> records_;
    uint64_t retention_;
    uint64_t floor_seq_ = 0;   // highest sequence number ever evicted
};

// -------------------------------------------------------------- server

// One accept loop, one handler thread per connection (gap recovery is a
// low-frequency, bursty control path relative to the multicast feed, so a
// thread per connection is the right amount of machinery -- not the
// thread-per-message design the feed handler itself deliberately avoids).
class GapRecoveryServer {
public:
    explicit GapRecoveryServer(const RetransmitBuffer& buf, uint16_t port = 0)
        : buf_(buf) {
        listen_fd_ = tcp_listen(port);
        port_ = tcp_bound_port(listen_fd_);
        running_.store(true);
        accept_thread_ = std::thread([this] { accept_loop(); });
    }

    ~GapRecoveryServer() { stop(); }

    uint16_t port() const { return port_; }
    uint64_t requests_served() const { return requests_served_.load(); }
    uint64_t records_served() const { return records_served_.load(); }

    void stop() {
        bool was_running = running_.exchange(false);
        if (!was_running) return;
        ::shutdown(listen_fd_, SHUT_RDWR);
        ::close(listen_fd_);
        if (accept_thread_.joinable()) accept_thread_.join();
        std::lock_guard<std::mutex> lk(handlers_mu_);
        for (std::thread& t : handlers_) if (t.joinable()) t.join();
    }

private:
    void accept_loop() {
        while (running_.load()) {
            int fd = tcp_accept(listen_fd_);
            if (fd < 0) {
                if (!running_.load()) return;
                continue;
            }
            std::lock_guard<std::mutex> lk(handlers_mu_);
            handlers_.emplace_back([this, fd] { handle_connection(fd); });
        }
    }

    void handle_connection(int fd) {
        while (running_.load()) {
            uint8_t req[kGapReqSize];
            if (!read_all(fd, req, sizeof(req))) break;
            ByteReader r(req, sizeof(req));
            uint32_t magic = r.u32();
            uint64_t start_seq = r.u64();
            uint32_t count = r.u32();
            if (magic != kGapReqMagic) break;

            uint8_t status = 2;
            std::vector<TickRecord> recs = buf_.fetch(start_seq, count, status);

            std::vector<uint8_t> hdr;
            put_u32(hdr, kGapRespMagic);
            put_u32(hdr, static_cast<uint32_t>(recs.size()));
            put_u8(hdr, status);
            if (!write_all(fd, hdr.data(), hdr.size())) break;

            std::vector<uint8_t> body;
            body.reserve(recs.size() * kGapRecordSize);
            for (const TickRecord& rec : recs) encode_gap_record(body, rec);
            if (!body.empty() && !write_all(fd, body.data(), body.size())) break;

            requests_served_.fetch_add(1, std::memory_order_relaxed);
            records_served_.fetch_add(recs.size(), std::memory_order_relaxed);
        }
        ::close(fd);
    }

    const RetransmitBuffer& buf_;
    int listen_fd_ = -1;
    uint16_t port_ = 0;
    std::atomic<bool> running_{false};
    std::thread accept_thread_;
    std::mutex handlers_mu_;
    std::vector<std::thread> handlers_;
    std::atomic<uint64_t> requests_served_{0};
    std::atomic<uint64_t> records_served_{0};
};

// -------------------------------------------------------------- client

// Opens one fresh TCP connection per gap. Gaps are rare relative to message
// volume (that is the whole premise of sizing a feed handler this way), so
// paying a connect() per gap is the honest cost model rather than hiding
// gap-recovery latency inside a warm, pre-established connection that a
// real outage recovery path would not get to assume either.
class GapRecoveryClient {
public:
    GapRecoveryClient(std::string host, uint16_t port) : host_(std::move(host)), port_(port) {}

    // Returns the records the server actually had (may be fewer than
    // `count` -- see RetransmitBuffer::fetch). `status` reports which.
    std::vector<TickRecord> request_range(uint64_t start_seq, uint32_t count, uint8_t& status) {
        int fd = tcp_connect(host_, port_);
        std::vector<uint8_t> req;
        put_u32(req, kGapReqMagic);
        put_u64(req, start_seq);
        put_u32(req, count);
        std::vector<TickRecord> out;
        status = 2;
        if (!write_all(fd, req.data(), req.size())) { ::close(fd); return out; }

        uint8_t hdr[kGapRespHdrSize];
        if (!read_all(fd, hdr, sizeof(hdr))) { ::close(fd); return out; }
        ByteReader r(hdr, sizeof(hdr));
        uint32_t magic = r.u32();
        uint32_t returned = r.u32();
        status = r.u8();
        if (magic != kGapRespMagic) { ::close(fd); status = 2; return out; }

        if (returned > 0) {
            std::vector<uint8_t> body(returned * kGapRecordSize);
            if (!read_all(fd, body.data(), body.size())) { ::close(fd); status = 1; return out; }
            ByteReader br(body.data(), body.size());
            out.reserve(returned);
            for (uint32_t i = 0; i < returned; ++i) out.push_back(decode_gap_record(br));
        }
        ::close(fd);
        return out;
    }

private:
    std::string host_;
    uint16_t port_;
};

} // namespace tickcap
