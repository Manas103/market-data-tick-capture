// An OUCH-style order-entry session.
//
// Real Nasdaq OUCH order-entry messages ride inside SoupBinTCP, which is the
// layer that actually provides sequencing, login, heartbeats and resend --
// OUCH itself is just the message vocabulary (Enter/Replace/Cancel Order,
// Accepted/Replaced/Canceled/Rejected). This header follows that same split
// and is explicit about being *style*, not spec: the frame layout, message
// IDs and field widths below are this project's own, sized for the claims
// this repo measures rather than for wire compatibility with a real gateway.
//
// What is real: a session-level outbound sequence number assigned by the
// server to every order-lifecycle event it sends (never to heartbeats or to
// the login exchange, which are session housekeeping, matching real
// SoupBinTCP's split between "Sequenced Data" and unsequenced packets); a
// server-side log of every sequenced message ever sent, kept so a
// reconnecting client can ask to resume from sequence N and receive exactly
// what it missed, in order, with no gaps and no duplicates *beyond* what it
// explicitly re-requests; and a heartbeat sent on a timer so a silently
// dead connection is detectable without waiting for the OS's TCP timeouts.
//
// Deliberately out of scope: order rejection (every EnterOrder is
// accepted), multiple concurrent client sessions (one session at a time,
// which is what "session" means for a single trading connection anyway),
// and authentication. Named here rather than glossed over.
#pragma once
#include <cstdint>
#include <cstddef>
#include <string>
#include <vector>
#include <mutex>
#include <thread>
#include <atomic>
#include <chrono>
#include "tcp_io.hpp"
#include "le_bytes.hpp"

namespace tickcap {

// ---------------------------------------------------------------- messages

struct EnterOrderMsg   { uint64_t client_order_id; uint16_t instrument_id; uint8_t side; int64_t price_ticks; uint32_t qty; };
struct CancelOrderMsg  { uint64_t client_order_id; uint32_t qty; };
struct ReplaceOrderMsg { uint64_t orig_client_order_id; uint64_t new_client_order_id; int64_t price_ticks; uint32_t qty; };

struct OrderAcceptedMsg { uint64_t client_order_id; uint64_t exch_order_id; uint16_t instrument_id; uint8_t side; int64_t price_ticks; uint32_t qty; };
struct OrderCanceledMsg { uint64_t client_order_id; uint32_t cancelled_qty; };
struct OrderReplacedMsg { uint64_t orig_client_order_id; uint64_t new_client_order_id; uint64_t exch_order_id; int64_t price_ticks; uint32_t qty; };

inline std::vector<uint8_t> encode_enter_order(const EnterOrderMsg& m) {
    std::vector<uint8_t> b;
    put_u64(b, m.client_order_id); put_u16(b, m.instrument_id);
    put_u8(b, m.side); put_i64(b, m.price_ticks); put_u32(b, m.qty);
    return b;
}
inline EnterOrderMsg decode_enter_order(ByteReader& r) {
    EnterOrderMsg m{};
    m.client_order_id = r.u64(); m.instrument_id = r.u16();
    m.side = r.u8(); m.price_ticks = r.i64(); m.qty = r.u32();
    return m;
}

inline std::vector<uint8_t> encode_cancel_order(const CancelOrderMsg& m) {
    std::vector<uint8_t> b;
    put_u64(b, m.client_order_id); put_u32(b, m.qty);
    return b;
}
inline CancelOrderMsg decode_cancel_order(ByteReader& r) {
    CancelOrderMsg m{};
    m.client_order_id = r.u64(); m.qty = r.u32();
    return m;
}

inline std::vector<uint8_t> encode_replace_order(const ReplaceOrderMsg& m) {
    std::vector<uint8_t> b;
    put_u64(b, m.orig_client_order_id); put_u64(b, m.new_client_order_id);
    put_i64(b, m.price_ticks); put_u32(b, m.qty);
    return b;
}
inline ReplaceOrderMsg decode_replace_order(ByteReader& r) {
    ReplaceOrderMsg m{};
    m.orig_client_order_id = r.u64(); m.new_client_order_id = r.u64();
    m.price_ticks = r.i64(); m.qty = r.u32();
    return m;
}

inline std::vector<uint8_t> encode_order_accepted(const OrderAcceptedMsg& m) {
    std::vector<uint8_t> b;
    put_u64(b, m.client_order_id); put_u64(b, m.exch_order_id); put_u16(b, m.instrument_id);
    put_u8(b, m.side); put_i64(b, m.price_ticks); put_u32(b, m.qty);
    return b;
}
inline OrderAcceptedMsg decode_order_accepted(ByteReader& r) {
    OrderAcceptedMsg m{};
    m.client_order_id = r.u64(); m.exch_order_id = r.u64(); m.instrument_id = r.u16();
    m.side = r.u8(); m.price_ticks = r.i64(); m.qty = r.u32();
    return m;
}

inline std::vector<uint8_t> encode_order_canceled(const OrderCanceledMsg& m) {
    std::vector<uint8_t> b;
    put_u64(b, m.client_order_id); put_u32(b, m.cancelled_qty);
    return b;
}
inline OrderCanceledMsg decode_order_canceled(ByteReader& r) {
    OrderCanceledMsg m{};
    m.client_order_id = r.u64(); m.cancelled_qty = r.u32();
    return m;
}

inline std::vector<uint8_t> encode_order_replaced(const OrderReplacedMsg& m) {
    std::vector<uint8_t> b;
    put_u64(b, m.orig_client_order_id); put_u64(b, m.new_client_order_id); put_u64(b, m.exch_order_id);
    put_i64(b, m.price_ticks); put_u32(b, m.qty);
    return b;
}
inline OrderReplacedMsg decode_order_replaced(ByteReader& r) {
    OrderReplacedMsg m{};
    m.orig_client_order_id = r.u64(); m.new_client_order_id = r.u64(); m.exch_order_id = r.u64();
    m.price_ticks = r.i64(); m.qty = r.u32();
    return m;
}

// Inner message type bytes.
constexpr uint8_t kOuchLogin           = 'L';  // client -> server, unsequenced
constexpr uint8_t kOuchLoginAccepted   = 'A';  // server -> client, unsequenced
constexpr uint8_t kOuchClientHeartbeat = 'C';  // client -> server, unsequenced
constexpr uint8_t kOuchServerHeartbeat = 'H';  // server -> client, unsequenced
constexpr uint8_t kOuchEnterOrder      = 'O';  // client -> server, unsequenced
constexpr uint8_t kOuchCancelOrder     = 'X';  // client -> server, unsequenced
constexpr uint8_t kOuchReplaceOrder    = 'U';  // client -> server, unsequenced
constexpr uint8_t kOuchOrderAccepted   = 'a';  // server -> client, SEQUENCED
constexpr uint8_t kOuchOrderCanceled   = 'x';  // server -> client, SEQUENCED
constexpr uint8_t kOuchOrderReplaced   = 'u';  // server -> client, SEQUENCED

// ------------------------------------------------------------ frame layer
//
// [u16 total_len][u8 category]['S': u64 seq]['N' has none][u8 inner_type][payload]
// total_len counts everything after itself (category onward).

struct OuchFrame {
    bool sequenced = false;
    uint64_t seq = 0;
    uint8_t inner_type = 0;
    std::vector<uint8_t> payload;
};

inline bool send_unsequenced(int fd, std::mutex& wmu, uint8_t inner_type,
                              const std::vector<uint8_t>& payload) {
    std::vector<uint8_t> body;
    put_u8(body, inner_type);
    body.insert(body.end(), payload.begin(), payload.end());
    std::vector<uint8_t> out;
    put_u16(out, static_cast<uint16_t>(1 + body.size()));
    put_u8(out, 'N');
    out.insert(out.end(), body.begin(), body.end());
    std::lock_guard<std::mutex> lk(wmu);
    return write_all(fd, out.data(), out.size());
}

inline bool send_sequenced(int fd, std::mutex& wmu, uint64_t seq, uint8_t inner_type,
                            const std::vector<uint8_t>& payload) {
    std::vector<uint8_t> body;
    put_u64(body, seq);
    put_u8(body, inner_type);
    body.insert(body.end(), payload.begin(), payload.end());
    std::vector<uint8_t> out;
    put_u16(out, static_cast<uint16_t>(1 + body.size()));
    put_u8(out, 'S');
    out.insert(out.end(), body.begin(), body.end());
    std::lock_guard<std::mutex> lk(wmu);
    return write_all(fd, out.data(), out.size());
}

inline bool recv_frame(int fd, OuchFrame& out) {
    uint8_t lenbuf[2];
    if (!read_all(fd, lenbuf, 2)) return false;
    uint16_t total_len = static_cast<uint16_t>(lenbuf[0] | (static_cast<uint16_t>(lenbuf[1]) << 8));
    if (total_len == 0) return false;
    std::vector<uint8_t> body(total_len);
    if (!read_all(fd, body.data(), total_len)) return false;
    ByteReader r(body.data(), body.size());
    uint8_t category = r.u8();
    out.sequenced = (category == 'S');
    if (out.sequenced) out.seq = r.u64();
    out.inner_type = r.u8();
    size_t rem = r.remaining();
    const uint8_t* start = body.data() + r.pos();
    out.payload.assign(start, start + rem);
    return true;
}

// ---------------------------------------------------------------- server

// One session at a time: a listening socket accepts a connection, serves
// it until it drops, then waits for the next one. A reconnect after a
// simulated disconnect is exactly "the next accepted connection", which is
// what lets the demo/test drive a disconnect with a plain socket close.
class OuchServer {
public:
    explicit OuchServer(uint16_t port = 0, int heartbeat_ms = 50) : hb_interval_ms_(heartbeat_ms) {
        listen_fd_ = tcp_listen(port);
        port_ = tcp_bound_port(listen_fd_);
        running_.store(true);
        accept_thread_ = std::thread([this] { accept_loop(); });
    }

    ~OuchServer() { stop(); }

    uint16_t port() const { return port_; }
    uint64_t orders_accepted() const { return orders_accepted_.load(); }
    uint64_t resends_sent() const { return resends_sent_.load(); }
    uint64_t connections_served() const { return connections_served_.load(); }
    uint64_t outbound_log_size() const {
        std::lock_guard<std::mutex> lk(log_mu_);
        return log_.size();
    }

    void stop() {
        bool was_running = running_.exchange(false);
        if (!was_running) return;
        ::shutdown(listen_fd_, SHUT_RDWR);
        ::close(listen_fd_);
        {
            std::lock_guard<std::mutex> lk(conn_mu_);
            if (cur_fd_ >= 0) ::shutdown(cur_fd_, SHUT_RDWR);
        }
        if (accept_thread_.joinable()) accept_thread_.join();
    }

private:
    struct OutboundLogEntry { uint64_t seq; uint8_t inner_type; std::vector<uint8_t> payload; };

    void accept_loop() {
        while (running_.load()) {
            int fd = tcp_accept(listen_fd_);
            if (fd < 0) { if (!running_.load()) return; continue; }
            connections_served_.fetch_add(1, std::memory_order_relaxed);
            handle_connection(fd);
        }
    }

    void handle_connection(int fd) {
        {
            std::lock_guard<std::mutex> lk(conn_mu_);
            cur_fd_ = fd;
        }
        std::atomic<bool> hb_running{true};
        std::thread hb([this, fd, &hb_running] {
            while (hb_running.load() && running_.load()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(hb_interval_ms_));
                if (!hb_running.load() || !running_.load()) break;
                send_unsequenced(fd, send_mu_, kOuchServerHeartbeat, {});
            }
        });

        while (running_.load()) {
            OuchFrame f;
            if (!recv_frame(fd, f)) break;
            if (f.sequenced) continue;  // clients never send sequenced frames in this protocol
            ByteReader r(f.payload.data(), f.payload.size());
            switch (f.inner_type) {
                case kOuchLogin: {
                    uint64_t requested = r.u64();
                    resend_from(fd, requested);
                    std::vector<uint8_t> ack;
                    put_u64(ack, peek_next_seq());
                    send_unsequenced(fd, send_mu_, kOuchLoginAccepted, ack);
                    break;
                }
                case kOuchClientHeartbeat:
                    break;
                case kOuchEnterOrder: {
                    EnterOrderMsg in = decode_enter_order(r);
                    uint64_t exch_id = next_exch_order_id_++;
                    OrderAcceptedMsg out{in.client_order_id, exch_id, in.instrument_id,
                                          in.side, in.price_ticks, in.qty};
                    log_and_send(fd, kOuchOrderAccepted, encode_order_accepted(out));
                    orders_accepted_.fetch_add(1, std::memory_order_relaxed);
                    break;
                }
                case kOuchCancelOrder: {
                    CancelOrderMsg in = decode_cancel_order(r);
                    OrderCanceledMsg out{in.client_order_id, in.qty};
                    log_and_send(fd, kOuchOrderCanceled, encode_order_canceled(out));
                    break;
                }
                case kOuchReplaceOrder: {
                    ReplaceOrderMsg in = decode_replace_order(r);
                    uint64_t exch_id = next_exch_order_id_++;
                    OrderReplacedMsg out{in.orig_client_order_id, in.new_client_order_id,
                                          exch_id, in.price_ticks, in.qty};
                    log_and_send(fd, kOuchOrderReplaced, encode_order_replaced(out));
                    break;
                }
                default:
                    break;
            }
        }

        hb_running.store(false);
        // A blocked heartbeat sleep just has to elapse; heartbeat interval is
        // short (tens of ms) by design so this join is bounded and quick.
        if (hb.joinable()) hb.join();
        {
            std::lock_guard<std::mutex> lk(conn_mu_);
            cur_fd_ = -1;
        }
        ::close(fd);
    }

    uint64_t peek_next_seq() {
        std::lock_guard<std::mutex> lk(log_mu_);
        return next_out_seq_;
    }

    void resend_from(int fd, uint64_t from_seq) {
        std::lock_guard<std::mutex> lk(log_mu_);
        for (const OutboundLogEntry& e : log_) {
            if (e.seq < from_seq) continue;
            send_sequenced(fd, send_mu_, e.seq, e.inner_type, e.payload);
            resends_sent_.fetch_add(1, std::memory_order_relaxed);
        }
    }

    void log_and_send(int fd, uint8_t inner_type, const std::vector<uint8_t>& payload) {
        uint64_t seq;
        {
            std::lock_guard<std::mutex> lk(log_mu_);
            seq = next_out_seq_++;
            log_.push_back(OutboundLogEntry{seq, inner_type, payload});
        }
        // A send that fails here (the client vanished mid-flight) is not an
        // error for the log: the message is already durably recorded, and
        // the client's own reconnect + LoginRequest(seq) is what re-delivers
        // it. That is the entire point of logging before sending rather than
        // only on confirmed delivery -- there is no delivery confirmation in
        // this protocol, only sequence continuity.
        send_sequenced(fd, send_mu_, seq, inner_type, payload);
    }

    int listen_fd_ = -1;
    uint16_t port_ = 0;
    int hb_interval_ms_;
    std::atomic<bool> running_{false};
    std::thread accept_thread_;

    std::mutex conn_mu_;
    int cur_fd_ = -1;
    std::mutex send_mu_;

    mutable std::mutex log_mu_;
    std::vector<OutboundLogEntry> log_;
    uint64_t next_out_seq_ = 1;
    uint64_t next_exch_order_id_ = 1;

    std::atomic<uint64_t> orders_accepted_{0};
    std::atomic<uint64_t> resends_sent_{0};
    std::atomic<uint64_t> connections_served_{0};
};

// ---------------------------------------------------------------- client

class OuchClient {
public:
    OuchClient(std::string host, uint16_t port) : host_(std::move(host)), port_(port) {}
    ~OuchClient() { disconnect(); }

    // Connects and sends a LoginRequest asking the server to resume (resend)
    // from `requested_seq`. Any sequenced messages the server has logged at
    // or after that sequence arrive before LoginAccepted; the caller reads
    // them via recv() same as live traffic.
    bool connect_and_login(uint64_t requested_seq) {
        fd_ = tcp_connect(host_, port_);
        std::vector<uint8_t> payload;
        put_u64(payload, requested_seq);
        return send_unsequenced(fd_, send_mu_, kOuchLogin, payload);
    }

    void disconnect() {
        if (fd_ >= 0) { ::shutdown(fd_, SHUT_RDWR); ::close(fd_); fd_ = -1; }
    }

    bool send_enter_order(const EnterOrderMsg& m)   { return send_unsequenced(fd_, send_mu_, kOuchEnterOrder, encode_enter_order(m)); }
    bool send_cancel_order(const CancelOrderMsg& m) { return send_unsequenced(fd_, send_mu_, kOuchCancelOrder, encode_cancel_order(m)); }
    bool send_replace_order(const ReplaceOrderMsg& m) { return send_unsequenced(fd_, send_mu_, kOuchReplaceOrder, encode_replace_order(m)); }
    bool send_heartbeat() { return send_unsequenced(fd_, send_mu_, kOuchClientHeartbeat, {}); }

    bool recv(OuchFrame& out) { return recv_frame(fd_, out); }
    int fd() const { return fd_; }

private:
    std::string host_;
    uint16_t port_;
    int fd_ = -1;
    std::mutex send_mu_;
};

} // namespace tickcap
