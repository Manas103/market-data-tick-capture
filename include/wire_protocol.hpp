// Simulated exchange multicast feed: transport framing + message encoding.
//
// Two layers, mirroring how real exchange multicast actually works:
//
//   Transport   A MoldUDP64-style datagram header (session id, sequence
//               number of the first message in the datagram, message count)
//               followed by length-prefixed messages. This is the layer that
//               makes gap detection possible: a receiver that sees a
//               datagram whose first sequence number is ahead of what it
//               expected knows exactly how many messages it lost, without
//               any help from the message layer. Nasdaq's real MoldUDP64
//               works this way for the same reason.
//
//   Message     ITCH-category order events (Add / Execute / Cancel / Delete /
//               Replace). Those five categories are inherent to any
//               order-by-order book feed, so they are not a novel choice --
//               what matters here is that the book they feed is a full
//               order-by-order (L3) book, and that every message is
//               losslessly capturable (see tick_capture.hpp).
//
// This is NOT the real Nasdaq ITCH 5.0 or MoldUDP64 specification. It
// borrows their structure and states its own field layout explicitly below.
// All integers are little-endian on the wire by definition, written through
// le_bytes.hpp rather than by copying packed structs, so the encoding is
// independent of host byte order and compiler struct layout.
#pragma once
#include <cstdint>
#include <vector>
#include <array>
#include <string>
#include "le_bytes.hpp"

namespace tickcap {

enum class MsgType : uint8_t {
    AddOrder = 'A',
    Execute  = 'E',
    Cancel   = 'X',
    Delete   = 'D',
    Replace  = 'U',
};

enum class Side : uint8_t { Buy = 'B', Sell = 'S' };

inline bool is_valid_msg_type(uint8_t t) {
    return t == 'A' || t == 'E' || t == 'X' || t == 'D' || t == 'U';
}

// A fully decoded message. Every field of every message type lives here so
// a single flat record can round-trip any message without loss -- this is
// the shape the columnar capture stores, and the shape it re-encodes from.
// Fields that do not apply to a given type are always exactly zero, on both
// the encode and decode paths, which is what makes byte-identical
// re-encoding from a capture possible.
struct DecodedMsg {
    MsgType  type{};
    uint64_t exchange_ts_ns = 0;
    uint16_t instrument_id  = 0;
    uint64_t order_id       = 0;   // Replace: the OLD order id
    uint64_t new_order_id   = 0;   // Replace only, else 0
    Side     side{};               // AddOrder only, else 0
    int64_t  price_ticks    = 0;   // AddOrder / Replace, else 0
    uint32_t qty            = 0;   // Add/Replace qty, Execute exec qty, Cancel cancel qty
};

// ------------------------------------------------------------ constants

constexpr int      kNumInstruments   = 128;
constexpr size_t   kSessionIdLen     = 10;
constexpr size_t   kPacketHeaderSize = kSessionIdLen + 8 + 2;  // 20 bytes
constexpr size_t   kMaxDatagram      = 1400;                   // conservative sub-MTU
constexpr int64_t  kTicksPerCent     = 1;                      // 1 tick == $0.01

// Message payload sizes (after the 2-byte length prefix).
//   common: type(1) + exchange_ts(8) + instrument(2)          = 11
constexpr size_t kCommonSize   = 11;
constexpr size_t kAddSize      = kCommonSize + 8 + 1 + 8 + 4;  // 32
constexpr size_t kExecuteSize  = kCommonSize + 8 + 4;          // 23
constexpr size_t kCancelSize   = kCommonSize + 8 + 4;          // 23
constexpr size_t kDeleteSize   = kCommonSize + 8;              // 19
constexpr size_t kReplaceSize  = kCommonSize + 8 + 8 + 8 + 4;  // 39

inline size_t message_size(MsgType t) {
    switch (t) {
        case MsgType::AddOrder: return kAddSize;
        case MsgType::Execute:  return kExecuteSize;
        case MsgType::Cancel:   return kCancelSize;
        case MsgType::Delete:   return kDeleteSize;
        case MsgType::Replace:  return kReplaceSize;
    }
    return 0;
}

// -------------------------------------------------------------- encoding

// Appends one length-prefixed message: uint16 length, then the payload.
inline void encode_message(std::vector<uint8_t>& out, const DecodedMsg& m) {
    put_u16(out, static_cast<uint16_t>(message_size(m.type)));
    put_u8(out, static_cast<uint8_t>(m.type));
    put_u64(out, m.exchange_ts_ns);
    put_u16(out, m.instrument_id);
    switch (m.type) {
        case MsgType::AddOrder:
            put_u64(out, m.order_id);
            put_u8(out, static_cast<uint8_t>(m.side));
            put_i64(out, m.price_ticks);
            put_u32(out, m.qty);
            break;
        case MsgType::Execute:
        case MsgType::Cancel:
            put_u64(out, m.order_id);
            put_u32(out, m.qty);
            break;
        case MsgType::Delete:
            put_u64(out, m.order_id);
            break;
        case MsgType::Replace:
            put_u64(out, m.order_id);
            put_u64(out, m.new_order_id);
            put_i64(out, m.price_ticks);
            put_u32(out, m.qty);
            break;
    }
}

// Reads one length-prefixed message. Returns false on malformed input
// (unknown type, wrong declared length, or truncation).
inline bool decode_message(ByteReader& r, DecodedMsg& m) {
    uint16_t len = r.u16();
    if (!r.ok()) return false;
    size_t body_start = r.pos();

    uint8_t t = r.u8();
    if (!is_valid_msg_type(t)) return false;
    m = DecodedMsg{};
    m.type = static_cast<MsgType>(t);
    if (len != message_size(m.type)) return false;

    m.exchange_ts_ns = r.u64();
    m.instrument_id  = r.u16();
    switch (m.type) {
        case MsgType::AddOrder:
            m.order_id = r.u64();
            m.side     = static_cast<Side>(r.u8());
            m.price_ticks = r.i64();
            m.qty      = r.u32();
            break;
        case MsgType::Execute:
        case MsgType::Cancel:
            m.order_id = r.u64();
            m.qty      = r.u32();
            break;
        case MsgType::Delete:
            m.order_id = r.u64();
            break;
        case MsgType::Replace:
            m.order_id     = r.u64();
            m.new_order_id = r.u64();
            m.price_ticks  = r.i64();
            m.qty          = r.u32();
            break;
    }
    if (!r.ok()) return false;
    return r.pos() - body_start == len;
}

// ------------------------------------------------- MoldUDP64-style packet

struct PacketHeader {
    std::array<uint8_t, kSessionIdLen> session{};
    uint64_t first_seq  = 0;   // sequence number of the first message inside
    uint16_t msg_count  = 0;
};

inline void encode_packet_header(std::vector<uint8_t>& out, const PacketHeader& h) {
    put_bytes(out, h.session.data(), h.session.size());
    put_u64(out, h.first_seq);
    put_u16(out, h.msg_count);
}

inline bool decode_packet_header(ByteReader& r, PacketHeader& h) {
    r.bytes(h.session.data(), h.session.size());
    h.first_seq = r.u64();
    h.msg_count = r.u16();
    return r.ok();
}

// Packs messages into datagrams up to max_bytes, appending each completed
// datagram to `out`. Batching matters: one sendto() per message is
// syscall-bound long before the decode logic is, which is why real feeds
// carry many messages per packet.
class PacketBuilder {
public:
    PacketBuilder(const std::array<uint8_t, kSessionIdLen>& session, size_t max_bytes)
        : session_(session), max_bytes_(max_bytes) {}

    // Returns true if adding this message forced the current datagram to be
    // flushed first (the caller does not need to care; it is exposed for
    // tests that reason about packet boundaries).
    bool add(const DecodedMsg& m, uint64_t seq) {
        size_t need = 2 + message_size(m.type);
        bool flushed = false;
        if (!body_.empty() && kPacketHeaderSize + body_.size() + need > max_bytes_) {
            flush();
            flushed = true;
        }
        if (body_.empty()) first_seq_ = seq;
        encode_message(body_, m);
        ++count_;
        return flushed;
    }

    void flush() {
        if (body_.empty()) return;
        PacketHeader h;
        h.session   = session_;
        h.first_seq = first_seq_;
        h.msg_count = static_cast<uint16_t>(count_);
        std::vector<uint8_t> pkt;
        pkt.reserve(kPacketHeaderSize + body_.size());
        encode_packet_header(pkt, h);
        pkt.insert(pkt.end(), body_.begin(), body_.end());
        packets_.push_back(std::move(pkt));
        body_.clear();
        count_ = 0;
    }

    std::vector<std::vector<uint8_t>>& packets() { return packets_; }
    const std::vector<std::vector<uint8_t>>& packets() const { return packets_; }

private:
    std::array<uint8_t, kSessionIdLen> session_;
    size_t max_bytes_;
    std::vector<uint8_t> body_;
    std::vector<std::vector<uint8_t>> packets_;
    uint64_t first_seq_ = 0;
    size_t count_ = 0;
};

inline std::array<uint8_t, kSessionIdLen> make_session_id(const std::string& s) {
    std::array<uint8_t, kSessionIdLen> out{};
    for (size_t i = 0; i < kSessionIdLen; ++i) {
        out[i] = i < s.size() ? static_cast<uint8_t>(s[i]) : static_cast<uint8_t>(' ');
    }
    return out;
}

} // namespace tickcap
