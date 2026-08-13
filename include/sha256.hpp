// Self-contained SHA-256 (FIPS 180-4).
//
// The bit-for-bit reproducibility claim is only worth as much as the
// comparison behind it, so the project hashes multi-megabyte byte streams
// rather than eyeballing a few fields. This is implemented here instead of
// linking OpenSSL purely to keep the build a plain `cmake && make` with no
// external dependency; it is validated against the published FIPS test
// vectors in tests/correctness_test.cpp before it is trusted for anything.
#pragma once
#include <cstdint>
#include <cstddef>
#include <cstring>
#include <string>
#include <vector>

namespace tickcap {

class Sha256 {
public:
    Sha256() { reset(); }

    void reset() {
        state_[0] = 0x6a09e667; state_[1] = 0xbb67ae85;
        state_[2] = 0x3c6ef372; state_[3] = 0xa54ff53a;
        state_[4] = 0x510e527f; state_[5] = 0x9b05688c;
        state_[6] = 0x1f83d9ab; state_[7] = 0x5be0cd19;
        bitlen_ = 0;
        buflen_ = 0;
    }

    // Bulk path: whole 64-byte blocks are compressed straight out of the
    // caller's buffer. An earlier version appended byte-at-a-time into the
    // internal block and measured 267 MB/s, which was enough to matter --
    // the live capture hashes every datagram it receives on the receive
    // thread, so the hash sat directly in the path that was losing packets.
    void update(const void* data, size_t len) {
        const uint8_t* p = static_cast<const uint8_t*>(data);

        if (buflen_ > 0) {                       // finish any partial block
            size_t take = 64 - buflen_;
            if (take > len) take = len;
            std::memcpy(buf_ + buflen_, p, take);
            buflen_ += take;
            p += take;
            len -= take;
            if (buflen_ == 64) {
                transform(buf_);
                bitlen_ += 512;
                buflen_ = 0;
            }
        }
        while (len >= 64) {                      // full blocks, no copy
            transform(p);
            bitlen_ += 512;
            p += 64;
            len -= 64;
        }
        if (len > 0) {                           // stash the remainder
            std::memcpy(buf_ + buflen_, p, len);
            buflen_ += len;
        }
    }

    void update(const std::vector<uint8_t>& v) { update(v.data(), v.size()); }

    // Returns the 32-byte digest. Finalizing does not disturb the object's
    // usefulness for a fresh hash after reset().
    std::vector<uint8_t> digest() {
        uint8_t out[32];
        uint32_t st[8];
        std::memcpy(st, state_, sizeof(st));
        uint8_t tmp[64];
        std::memcpy(tmp, buf_, 64);
        size_t i = buflen_;
        uint64_t total_bits = bitlen_ + static_cast<uint64_t>(buflen_) * 8;

        tmp[i++] = 0x80;
        if (i > 56) {
            while (i < 64) tmp[i++] = 0x00;
            transform_into(st, tmp);
            i = 0;
        }
        while (i < 56) tmp[i++] = 0x00;
        for (int b = 7; b >= 0; --b) tmp[i++] = static_cast<uint8_t>(total_bits >> (8 * b));
        transform_into(st, tmp);

        for (int w = 0; w < 8; ++w) {
            out[w * 4 + 0] = static_cast<uint8_t>(st[w] >> 24);
            out[w * 4 + 1] = static_cast<uint8_t>(st[w] >> 16);
            out[w * 4 + 2] = static_cast<uint8_t>(st[w] >> 8);
            out[w * 4 + 3] = static_cast<uint8_t>(st[w]);
        }
        return std::vector<uint8_t>(out, out + 32);
    }

    std::string hex() {
        static const char* kHex = "0123456789abcdef";
        std::vector<uint8_t> d = digest();
        std::string s;
        s.reserve(64);
        for (uint8_t b : d) {
            s.push_back(kHex[b >> 4]);
            s.push_back(kHex[b & 0x0F]);
        }
        return s;
    }

    static std::string hex_of(const void* data, size_t len) {
        Sha256 h;
        h.update(data, len);
        return h.hex();
    }

    static std::string hex_of(const std::vector<uint8_t>& v) {
        return hex_of(v.data(), v.size());
    }

private:
    static uint32_t rotr(uint32_t x, int n) { return (x >> n) | (x << (32 - n)); }

    void transform(const uint8_t block[64]) { transform_into(state_, block); }

    static void transform_into(uint32_t st[8], const uint8_t block[64]) {
        static const uint32_t K[64] = {
            0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,
            0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,
            0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,
            0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,
            0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,
            0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,
            0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,
            0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2
        };
        uint32_t w[64];
        for (int i = 0; i < 16; ++i) {
            w[i] = (static_cast<uint32_t>(block[i * 4 + 0]) << 24) |
                   (static_cast<uint32_t>(block[i * 4 + 1]) << 16) |
                   (static_cast<uint32_t>(block[i * 4 + 2]) << 8) |
                   (static_cast<uint32_t>(block[i * 4 + 3]));
        }
        for (int i = 16; i < 64; ++i) {
            uint32_t s0 = rotr(w[i - 15], 7) ^ rotr(w[i - 15], 18) ^ (w[i - 15] >> 3);
            uint32_t s1 = rotr(w[i - 2], 17) ^ rotr(w[i - 2], 19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16] + s0 + w[i - 7] + s1;
        }
        uint32_t a = st[0], b = st[1], c = st[2], d = st[3];
        uint32_t e = st[4], f = st[5], g = st[6], h = st[7];
        for (int i = 0; i < 64; ++i) {
            uint32_t S1 = rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25);
            uint32_t ch = (e & f) ^ ((~e) & g);
            uint32_t t1 = h + S1 + ch + K[i] + w[i];
            uint32_t S0 = rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22);
            uint32_t maj = (a & b) ^ (a & c) ^ (b & c);
            uint32_t t2 = S0 + maj;
            h = g; g = f; f = e; e = d + t1;
            d = c; c = b; b = a; a = t1 + t2;
        }
        st[0] += a; st[1] += b; st[2] += c; st[3] += d;
        st[4] += e; st[5] += f; st[6] += g; st[7] += h;
    }

    uint32_t state_[8];
    uint64_t bitlen_;
    uint8_t  buf_[64];
    size_t   buflen_;
};

} // namespace tickcap
