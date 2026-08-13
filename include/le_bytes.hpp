// Explicit little-endian byte primitives + varint/zigzag encoding.
//
// Every byte this project writes -- to the wire, to a capture file, or into
// a canonical digest buffer -- goes through these helpers rather than
// memcpy-ing a packed struct. That costs a little speed and buys two things
// the project actually depends on:
//
//   1. Architecture independence. A memcpy of a packed struct bakes in the
//      host's byte order and the compiler's struct layout. Explicit
//      per-byte serialization means the same logical record produces the
//      same bytes anywhere, which is the precondition for the bit-for-bit
//      reproducibility claim in the README being meaningful rather than
//      "meaningful on this machine, with this compiler".
//   2. Bounds-checked reads. ByteReader refuses to read past the end of a
//      buffer instead of walking off it, so a truncated datagram or a
//      corrupt capture block is a clean error rather than undefined
//      behaviour.
#pragma once
#include <cstdint>
#include <cstddef>
#include <cstring>
#include <vector>
#include <string>

namespace tickcap {

// ---------------------------------------------------------------- writing

inline void put_u8(std::vector<uint8_t>& out, uint8_t v) { out.push_back(v); }

inline void put_u16(std::vector<uint8_t>& out, uint16_t v) {
    out.push_back(static_cast<uint8_t>(v));
    out.push_back(static_cast<uint8_t>(v >> 8));
}

inline void put_u32(std::vector<uint8_t>& out, uint32_t v) {
    for (int i = 0; i < 4; ++i) out.push_back(static_cast<uint8_t>(v >> (8 * i)));
}

inline void put_u64(std::vector<uint8_t>& out, uint64_t v) {
    for (int i = 0; i < 8; ++i) out.push_back(static_cast<uint8_t>(v >> (8 * i)));
}

inline void put_i64(std::vector<uint8_t>& out, int64_t v) {
    put_u64(out, static_cast<uint64_t>(v));
}

inline void put_bytes(std::vector<uint8_t>& out, const void* p, size_t n) {
    const uint8_t* b = static_cast<const uint8_t*>(p);
    out.insert(out.end(), b, b + n);
}

// ------------------------------------------------------- varint / zigzag

// LEB128 unsigned varint. Small magnitudes cost one byte, which is the whole
// point of the delta/frame-of-reference column codecs: after differencing,
// most values are small even though the raw column is 8 bytes wide.
inline void put_uvarint(std::vector<uint8_t>& out, uint64_t v) {
    while (v >= 0x80) {
        out.push_back(static_cast<uint8_t>(v) | 0x80);
        v >>= 7;
    }
    out.push_back(static_cast<uint8_t>(v));
}

// ZigZag maps small-magnitude signed values onto small unsigned ones
// (-1 -> 1, 1 -> 2, -2 -> 3, ...) so that negative deltas stay cheap.
// Without it, -1 would encode as 0xFFFFFFFFFFFFFFFF and cost 10 bytes.
inline uint64_t zigzag_encode(int64_t v) {
    return (static_cast<uint64_t>(v) << 1) ^ static_cast<uint64_t>(v >> 63);
}

inline int64_t zigzag_decode(uint64_t v) {
    return static_cast<int64_t>((v >> 1) ^ (~(v & 1) + 1));
}

inline void put_svarint(std::vector<uint8_t>& out, int64_t v) {
    put_uvarint(out, zigzag_encode(v));
}

// ---------------------------------------------------------------- reading

// A bounds-checked cursor over a byte range. Any read that would run past
// the end sets ok_ = false and returns zero; callers check ok() once at the
// end of a parse rather than after every field.
class ByteReader {
public:
    ByteReader(const uint8_t* data, size_t size) : data_(data), size_(size) {}

    bool ok() const { return ok_; }
    size_t pos() const { return pos_; }
    size_t remaining() const { return pos_ <= size_ ? size_ - pos_ : 0; }
    void seek(size_t p) { pos_ = p; if (pos_ > size_) ok_ = false; }

    uint8_t u8() {
        if (remaining() < 1) { ok_ = false; return 0; }
        return data_[pos_++];
    }

    uint16_t u16() {
        if (remaining() < 2) { ok_ = false; pos_ = size_; return 0; }
        uint16_t v = static_cast<uint16_t>(data_[pos_]) |
                     (static_cast<uint16_t>(data_[pos_ + 1]) << 8);
        pos_ += 2;
        return v;
    }

    uint32_t u32() {
        if (remaining() < 4) { ok_ = false; pos_ = size_; return 0; }
        uint32_t v = 0;
        for (int i = 0; i < 4; ++i) v |= static_cast<uint32_t>(data_[pos_ + i]) << (8 * i);
        pos_ += 4;
        return v;
    }

    uint64_t u64() {
        if (remaining() < 8) { ok_ = false; pos_ = size_; return 0; }
        uint64_t v = 0;
        for (int i = 0; i < 8; ++i) v |= static_cast<uint64_t>(data_[pos_ + i]) << (8 * i);
        pos_ += 8;
        return v;
    }

    int64_t i64() { return static_cast<int64_t>(u64()); }

    uint64_t uvarint() {
        uint64_t result = 0;
        int shift = 0;
        while (true) {
            if (remaining() < 1 || shift > 63) { ok_ = false; return 0; }
            uint8_t b = data_[pos_++];
            result |= static_cast<uint64_t>(b & 0x7F) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
        }
        return result;
    }

    int64_t svarint() { return zigzag_decode(uvarint()); }

    void bytes(void* dst, size_t n) {
        if (remaining() < n) { ok_ = false; pos_ = size_; std::memset(dst, 0, n); return; }
        std::memcpy(dst, data_ + pos_, n);
        pos_ += n;
    }

    const uint8_t* raw() const { return data_; }

private:
    const uint8_t* data_;
    size_t size_;
    size_t pos_ = 0;
    bool ok_ = true;
};

} // namespace tickcap
