// Per-column encodings for the TCAP capture format.
//
// A columnar layout is what makes these encodings possible at all. In a
// row-oriented file, consecutive bytes are a timestamp, then a type byte,
// then an order id, then a price -- adjacent values have nothing in common,
// so there is no structure to exploit. Group the same field from a million
// consecutive ticks together and the structure is obvious: timestamps rise
// almost linearly, sequence numbers rise by exactly one, prices cluster in a
// narrow band around a reference, and side/type columns are a handful of
// distinct bytes repeated forever. Each of those shapes has a cheap
// encoding, and each of those encodings is only reachable once the data is
// laid out by column.
//
//   Raw    fixed-width little-endian. The fallback, and the winner for
//          genuinely high-entropy columns.
//   Delta  first value, then zigzag-varint successive differences. For
//          anything monotonic-ish.
//   Dod    delta-of-delta. For near-constant-rate series (wall-clock
//          capture timestamps, and sequence numbers that step by one, where
//          the second difference is almost always exactly zero -> 1 byte).
//   For    frame of reference: subtract the block minimum, varint the
//          residuals. For values clustered in a band with no ordering, which
//          is exactly what a price column looks like.
//   Rle    run-length. For low-cardinality columns (side, message type, the
//          packet-boundary flag) that hold long constant runs.
//
// The writer does not guess which applies. It encodes every column with all
// five and keeps the smallest result, storing the winner's id in the block
// directory -- the same "try the candidates, keep the best" approach Parquet
// and ORC take, and the reason the measured compression in the README is not
// contingent on hand-tuning per column.
//
// All arithmetic runs on uint64 bit patterns with defined wraparound, so a
// delta between two arbitrary uint64 values round-trips exactly even when it
// overflows a signed 64-bit range.
#pragma once
#include <cstdint>
#include <vector>
#include <string>
#include <limits>
#include "le_bytes.hpp"

namespace tickcap {

enum class Encoding : uint8_t { Raw = 0, Delta = 1, Dod = 2, For = 3, Rle = 4 };

enum class PhysType : uint8_t { U8 = 0, U16 = 1, U32 = 2, U64 = 3, I64 = 4 };

inline const char* encoding_name(Encoding e) {
    switch (e) {
        case Encoding::Raw:   return "RAW";
        case Encoding::Delta: return "DELTA";
        case Encoding::Dod:   return "DOD";
        case Encoding::For:   return "FOR";
        case Encoding::Rle:   return "RLE";
    }
    return "?";
}

inline size_t phys_width(PhysType t) {
    switch (t) {
        case PhysType::U8:  return 1;
        case PhysType::U16: return 2;
        case PhysType::U32: return 4;
        case PhysType::U64: return 8;
        case PhysType::I64: return 8;
    }
    return 8;
}

inline bool phys_is_signed(PhysType t) { return t == PhysType::I64; }

inline uint64_t mask_to_width(uint64_t v, PhysType t) {
    switch (t) {
        case PhysType::U8:  return v & 0xFFull;
        case PhysType::U16: return v & 0xFFFFull;
        case PhysType::U32: return v & 0xFFFFFFFFull;
        default: return v;
    }
}

// A single scalar, varint-coded according to the column's signedness.
inline void put_scalar(std::vector<uint8_t>& out, uint64_t bits, bool is_signed) {
    if (is_signed) put_svarint(out, static_cast<int64_t>(bits));
    else           put_uvarint(out, bits);
}

inline uint64_t get_scalar(ByteReader& r, bool is_signed) {
    return is_signed ? static_cast<uint64_t>(r.svarint()) : r.uvarint();
}

// -------------------------------------------------------------- encoders

inline void encode_raw(const std::vector<uint64_t>& v, PhysType t, std::vector<uint8_t>& out) {
    size_t w = phys_width(t);
    out.reserve(out.size() + v.size() * w);
    for (uint64_t x : v)
        for (size_t b = 0; b < w; ++b)
            out.push_back(static_cast<uint8_t>(x >> (8 * b)));
}

inline void encode_delta(const std::vector<uint64_t>& v, bool is_signed, std::vector<uint8_t>& out) {
    if (v.empty()) return;
    put_scalar(out, v[0], is_signed);
    for (size_t i = 1; i < v.size(); ++i)
        put_svarint(out, static_cast<int64_t>(v[i] - v[i - 1]));
}

inline void encode_dod(const std::vector<uint64_t>& v, bool is_signed, std::vector<uint8_t>& out) {
    if (v.empty()) return;
    put_scalar(out, v[0], is_signed);
    if (v.size() == 1) return;
    uint64_t d_prev = v[1] - v[0];
    put_svarint(out, static_cast<int64_t>(d_prev));
    for (size_t i = 2; i < v.size(); ++i) {
        uint64_t d = v[i] - v[i - 1];
        put_svarint(out, static_cast<int64_t>(d - d_prev));
        d_prev = d;
    }
}

inline void encode_for(const std::vector<uint64_t>& v, bool is_signed, std::vector<uint8_t>& out) {
    if (v.empty()) return;
    if (is_signed) {
        int64_t mn = std::numeric_limits<int64_t>::max();
        for (uint64_t x : v) mn = std::min(mn, static_cast<int64_t>(x));
        put_svarint(out, mn);
        for (uint64_t x : v)
            put_uvarint(out, static_cast<uint64_t>(static_cast<int64_t>(x) - mn));
    } else {
        uint64_t mn = std::numeric_limits<uint64_t>::max();
        for (uint64_t x : v) mn = std::min(mn, x);
        put_uvarint(out, mn);
        for (uint64_t x : v) put_uvarint(out, x - mn);
    }
}

inline void encode_rle(const std::vector<uint64_t>& v, bool is_signed, std::vector<uint8_t>& out) {
    std::vector<uint8_t> runs;
    uint64_t run_count = 0;
    size_t i = 0;
    while (i < v.size()) {
        size_t j = i;
        while (j < v.size() && v[j] == v[i]) ++j;
        put_uvarint(runs, static_cast<uint64_t>(j - i));
        put_scalar(runs, v[i], is_signed);
        ++run_count;
        i = j;
    }
    put_uvarint(out, run_count);
    out.insert(out.end(), runs.begin(), runs.end());
}

// Encodes with every candidate and keeps the smallest.
inline Encoding encode_column_best(const std::vector<uint64_t>& v, PhysType t,
                                   std::vector<uint8_t>& out) {
    bool sgn = phys_is_signed(t);
    struct Candidate { Encoding enc; std::vector<uint8_t> bytes; };
    Candidate best;
    best.enc = Encoding::Raw;
    encode_raw(v, t, best.bytes);

    auto consider = [&](Encoding e, void (*fn)(const std::vector<uint64_t>&, bool, std::vector<uint8_t>&)) {
        std::vector<uint8_t> b;
        fn(v, sgn, b);
        if (b.size() < best.bytes.size()) { best.enc = e; best.bytes.swap(b); }
    };
    consider(Encoding::Delta, encode_delta);
    consider(Encoding::Dod,   encode_dod);
    consider(Encoding::For,   encode_for);
    consider(Encoding::Rle,   encode_rle);

    out.insert(out.end(), best.bytes.begin(), best.bytes.end());
    return best.enc;
}

// -------------------------------------------------------------- decoders

inline bool decode_column(ByteReader& r, Encoding enc, PhysType t, size_t n,
                          std::vector<uint64_t>& out) {
    bool sgn = phys_is_signed(t);
    out.clear();
    out.reserve(n);
    switch (enc) {
        case Encoding::Raw: {
            size_t w = phys_width(t);
            for (size_t i = 0; i < n; ++i) {
                uint64_t x = 0;
                for (size_t b = 0; b < w; ++b) x |= static_cast<uint64_t>(r.u8()) << (8 * b);
                if (t == PhysType::I64 || t == PhysType::U64) { /* full width */ }
                out.push_back(x);
            }
            break;
        }
        case Encoding::Delta: {
            if (n == 0) break;
            uint64_t prev = get_scalar(r, sgn);
            out.push_back(prev);
            for (size_t i = 1; i < n; ++i) {
                prev = prev + static_cast<uint64_t>(r.svarint());
                out.push_back(prev);
            }
            break;
        }
        case Encoding::Dod: {
            if (n == 0) break;
            uint64_t prev = get_scalar(r, sgn);
            out.push_back(prev);
            if (n == 1) break;
            uint64_t d = static_cast<uint64_t>(r.svarint());
            prev = prev + d;
            out.push_back(prev);
            for (size_t i = 2; i < n; ++i) {
                d = d + static_cast<uint64_t>(r.svarint());
                prev = prev + d;
                out.push_back(prev);
            }
            break;
        }
        case Encoding::For: {
            if (n == 0) break;
            if (sgn) {
                int64_t mn = r.svarint();
                for (size_t i = 0; i < n; ++i)
                    out.push_back(static_cast<uint64_t>(mn + static_cast<int64_t>(r.uvarint())));
            } else {
                uint64_t mn = r.uvarint();
                for (size_t i = 0; i < n; ++i) out.push_back(mn + r.uvarint());
            }
            break;
        }
        case Encoding::Rle: {
            uint64_t runs = r.uvarint();
            for (uint64_t k = 0; k < runs && r.ok(); ++k) {
                uint64_t len = r.uvarint();
                uint64_t val = get_scalar(r, sgn);
                for (uint64_t j = 0; j < len; ++j) out.push_back(val);
            }
            break;
        }
        default:
            return false;
    }
    return r.ok() && out.size() == n;
}

} // namespace tickcap
