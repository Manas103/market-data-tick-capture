// TCAP: the column-oriented tick capture format (writer + reader).
//
// The full byte-level specification lives in docs/FORMAT.md and is the
// document the independent Python reader was written from. If the two ever
// disagree, the spec is authoritative -- that is the point of writing it
// down separately rather than letting this header be the only definition.
//
// Layout, in one picture:
//
//   [FileHeader 56B][ColumnSchema 40B x N]
//   [Block 0][Block 1]...[Block K-1]
//   [BlockIndex 16B x K]
//
// and each block is
//
//   [magic|record_count|num_columns|dir_bytes][directory 8B x N][payloads...]
//
// The per-block directory is what makes single-column reads actually cheap.
// It records every column chunk's encoding and byte length up front, so the
// offset of column j's payload is computable from the directory alone --
// a reader that wants one column preads the 16-byte prefix, the directory,
// and that column's bytes, and never touches the other ten columns. Without
// the directory a reader would have to walk chunk headers sequentially and
// would end up faulting in most of the block anyway, which would make the
// columnar layout a filesize win but not an I/O win.
//
// Blocking (65,536 records by default) is the usual columnar tradeoff:
// bigger blocks compress better because encodings amortize their per-chunk
// setup over more values, smaller blocks make selective reads finer-grained
// and bound the writer's memory. It is a knob here, not a constant.
//
// POSIX only. Reads go through pread() so a column scan can address exact
// byte ranges without disturbing a shared file offset.
#pragma once
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <array>
#include <stdexcept>
#include <fcntl.h>
#include <unistd.h>
#include "column_codec.hpp"
#include "le_bytes.hpp"
#include "wire_protocol.hpp"

namespace tickcap {

// One captured message, flattened so every field of every message type has a
// home. Fields that do not apply to a record's type are always zero, which
// is both what makes the columns compressible and what makes byte-exact
// re-encoding possible.
struct TickRecord {
    uint64_t capture_ts_ns  = 0;  // receiver-side arrival time, RECORDED not regenerated
    uint64_t exchange_ts_ns = 0;  // from the message itself
    uint64_t seq_num        = 0;  // feed sequence number
    uint8_t  msg_type       = 0;
    uint16_t instrument_id  = 0;
    uint64_t order_id       = 0;
    uint64_t new_order_id   = 0;
    uint8_t  side           = 0;
    int64_t  price_ticks    = 0;
    uint32_t qty            = 0;
    uint8_t  pkt_first      = 0;  // 1 if this message began a datagram
};

constexpr size_t kNumColumns        = 11;
constexpr size_t kFileHeaderSize    = 56;
constexpr size_t kColumnSchemaSize  = 40;
constexpr size_t kBlockPrefixSize   = 16;
constexpr size_t kBlockDirEntrySize = 8;
constexpr size_t kBlockIndexEntry   = 16;
constexpr uint32_t kBlockMagic      = 0x004B4C42u;  // 'B','L','K',0 little-endian
constexpr const char* kFileMagic    = "TCAPv001";

// Width of one record if the same fields were stored row-oriented, used as
// the honest baseline in the size comparison.
constexpr size_t kRowRecordBytes = 8 + 8 + 8 + 1 + 2 + 8 + 8 + 1 + 8 + 4 + 1;  // 57

struct ColumnDef {
    const char* name;
    PhysType    type;
};

inline const std::array<ColumnDef, kNumColumns>& column_defs() {
    static const std::array<ColumnDef, kNumColumns> defs = {{
        {"capture_ts_ns",  PhysType::U64},
        {"exchange_ts_ns", PhysType::U64},
        {"seq_num",        PhysType::U64},
        {"msg_type",       PhysType::U8},
        {"instrument_id",  PhysType::U16},
        {"order_id",       PhysType::U64},
        {"new_order_id",   PhysType::U64},
        {"side",           PhysType::U8},
        {"price_ticks",    PhysType::I64},
        {"qty",            PhysType::U32},
        {"pkt_first",      PhysType::U8},
    }};
    return defs;
}

// Byte offset of a field within a packed row-oriented record, derived from
// the schema rather than written out by hand. The row baseline's writer and
// reader both go through this, so they cannot disagree about the layout --
// an earlier version hardcoded the price offset as 39 instead of 44 and the
// benchmark's own cross-check caught it, which is the argument for
// computing it in one place.
inline size_t row_field_offset(size_t col) {
    size_t off = 0;
    for (size_t c = 0; c < col && c < kNumColumns; ++c)
        off += phys_width(column_defs()[c].type);
    return off;
}

inline size_t row_record_bytes() { return row_field_offset(kNumColumns); }

inline uint64_t record_field(const TickRecord& r, size_t col) {
    switch (col) {
        case 0:  return r.capture_ts_ns;
        case 1:  return r.exchange_ts_ns;
        case 2:  return r.seq_num;
        case 3:  return r.msg_type;
        case 4:  return r.instrument_id;
        case 5:  return r.order_id;
        case 6:  return r.new_order_id;
        case 7:  return r.side;
        case 8:  return static_cast<uint64_t>(r.price_ticks);
        case 9:  return r.qty;
        case 10: return r.pkt_first;
    }
    return 0;
}

inline void set_record_field(TickRecord& r, size_t col, uint64_t v) {
    switch (col) {
        case 0:  r.capture_ts_ns  = v; break;
        case 1:  r.exchange_ts_ns = v; break;
        case 2:  r.seq_num        = v; break;
        case 3:  r.msg_type       = static_cast<uint8_t>(v); break;
        case 4:  r.instrument_id  = static_cast<uint16_t>(v); break;
        case 5:  r.order_id       = v; break;
        case 6:  r.new_order_id   = v; break;
        case 7:  r.side           = static_cast<uint8_t>(v); break;
        case 8:  r.price_ticks    = static_cast<int64_t>(v); break;
        case 9:  r.qty            = static_cast<uint32_t>(v); break;
        case 10: r.pkt_first      = static_cast<uint8_t>(v); break;
    }
}

// Converts a captured record back into the decoded message it came from.
// Exact by construction: every wire field is stored, and inapplicable
// fields are zero on both paths.
inline DecodedMsg record_to_msg(const TickRecord& r) {
    DecodedMsg m;
    m.type            = static_cast<MsgType>(r.msg_type);
    m.exchange_ts_ns  = r.exchange_ts_ns;
    m.instrument_id   = r.instrument_id;
    m.order_id        = r.order_id;
    m.new_order_id    = r.new_order_id;
    m.side            = static_cast<Side>(r.side);
    m.price_ticks     = r.price_ticks;
    m.qty             = r.qty;
    return m;
}

// ------------------------------------------------------------- the writer

class CaptureWriter {
public:
    CaptureWriter(const std::string& path,
                  const std::array<uint8_t, kSessionIdLen>& session,
                  uint32_t records_per_block = 65536)
        : session_(session), records_per_block_(records_per_block) {
        f_ = std::fopen(path.c_str(), "wb+");
        if (!f_) throw std::runtime_error("CaptureWriter: cannot open " + path);
        write_header_placeholder_and_schema();
        columns_.resize(kNumColumns);
    }

    ~CaptureWriter() { if (f_) close(); }

    void append(const TickRecord& r) {
        for (size_t c = 0; c < kNumColumns; ++c)
            columns_[c].push_back(mask_to_width(record_field(r, c), column_defs()[c].type));
        ++pending_;
        ++total_records_;
        if (pending_ >= records_per_block_) flush_block();
    }

    void close() {
        if (!f_) return;
        flush_block();
        long index_off = std::ftell(f_);
        std::vector<uint8_t> idx;
        for (const auto& b : block_index_) {
            put_u64(idx, b.first);
            put_u32(idx, b.second);
            put_u32(idx, 0);
        }
        if (!idx.empty()) std::fwrite(idx.data(), 1, idx.size(), f_);
        patch_header(static_cast<uint64_t>(index_off));
        std::fclose(f_);
        f_ = nullptr;
    }

    // Which encoding won for each column, aggregated over all blocks --
    // reported by the benchmark so the README's numbers can name the codec
    // that actually produced them.
    const std::vector<std::array<uint64_t, 5>>& encoding_histogram() const { return enc_hist_; }
    const std::vector<uint64_t>& column_bytes() const { return col_bytes_; }
    uint64_t total_records() const { return total_records_; }

private:
    void write_header_placeholder_and_schema() {
        std::vector<uint8_t> hdr;
        put_bytes(hdr, kFileMagic, 8);
        put_u32(hdr, 1);
        put_u32(hdr, static_cast<uint32_t>(kNumColumns));
        put_u64(hdr, 0);                       // num_records, patched at close
        put_u64(hdr, 0);                       // num_blocks, patched at close
        put_u64(hdr, 0);                       // block_index_offset, patched at close
        put_bytes(hdr, session_.data(), session_.size());
        put_u32(hdr, records_per_block_);
        put_u16(hdr, 0);
        std::fwrite(hdr.data(), 1, hdr.size(), f_);

        std::vector<uint8_t> schema;
        for (const ColumnDef& d : column_defs()) {
            char name[32];
            std::memset(name, 0, sizeof(name));
            std::strncpy(name, d.name, sizeof(name) - 1);
            put_bytes(schema, name, sizeof(name));
            put_u8(schema, static_cast<uint8_t>(d.type));
            for (int i = 0; i < 7; ++i) put_u8(schema, 0);
        }
        std::fwrite(schema.data(), 1, schema.size(), f_);

        enc_hist_.assign(kNumColumns, {0, 0, 0, 0, 0});
        col_bytes_.assign(kNumColumns, 0);
    }

    void flush_block() {
        if (pending_ == 0) return;
        std::vector<uint8_t> directory;
        std::vector<uint8_t> payloads;
        for (size_t c = 0; c < kNumColumns; ++c) {
            std::vector<uint8_t> chunk;
            Encoding e = encode_column_best(columns_[c], column_defs()[c].type, chunk);
            put_u8(directory, static_cast<uint8_t>(e));
            put_u8(directory, 0); put_u8(directory, 0); put_u8(directory, 0);
            put_u32(directory, static_cast<uint32_t>(chunk.size()));
            payloads.insert(payloads.end(), chunk.begin(), chunk.end());
            enc_hist_[c][static_cast<size_t>(e)] += 1;
            col_bytes_[c] += chunk.size();
            columns_[c].clear();
        }

        long off = std::ftell(f_);
        std::vector<uint8_t> prefix;
        put_u32(prefix, kBlockMagic);
        put_u32(prefix, pending_);
        put_u32(prefix, static_cast<uint32_t>(kNumColumns));
        put_u32(prefix, static_cast<uint32_t>(directory.size()));
        std::fwrite(prefix.data(), 1, prefix.size(), f_);
        std::fwrite(directory.data(), 1, directory.size(), f_);
        if (!payloads.empty()) std::fwrite(payloads.data(), 1, payloads.size(), f_);

        block_index_.emplace_back(static_cast<uint64_t>(off), pending_);
        pending_ = 0;
    }

    void patch_header(uint64_t index_off) {
        std::vector<uint8_t> patch;
        put_u64(patch, total_records_);
        put_u64(patch, static_cast<uint64_t>(block_index_.size()));
        put_u64(patch, index_off);
        std::fseek(f_, 16, SEEK_SET);
        std::fwrite(patch.data(), 1, patch.size(), f_);
    }

    std::FILE* f_ = nullptr;
    std::array<uint8_t, kSessionIdLen> session_;
    uint32_t records_per_block_;
    uint32_t pending_ = 0;
    uint64_t total_records_ = 0;
    std::vector<std::vector<uint64_t>> columns_;
    std::vector<std::pair<uint64_t, uint32_t>> block_index_;
    std::vector<std::array<uint64_t, 5>> enc_hist_;
    std::vector<uint64_t> col_bytes_;
};

// ------------------------------------------------------------- the reader

class CaptureReader {
public:
    explicit CaptureReader(const std::string& path) {
        fd_ = ::open(path.c_str(), O_RDONLY);
        if (fd_ < 0) throw std::runtime_error("CaptureReader: cannot open " + path);
        uint8_t hdr[kFileHeaderSize];
        if (pread_exact(hdr, kFileHeaderSize, 0) != kFileHeaderSize)
            throw std::runtime_error("CaptureReader: short header");
        ByteReader r(hdr, kFileHeaderSize);
        char magic[8];
        r.bytes(magic, 8);
        if (std::memcmp(magic, kFileMagic, 8) != 0)
            throw std::runtime_error("CaptureReader: bad magic");
        version_     = r.u32();
        num_columns_ = r.u32();
        num_records_ = r.u64();
        num_blocks_  = r.u64();
        index_off_   = r.u64();
        r.bytes(session_.data(), session_.size());
        records_per_block_ = r.u32();
        if (num_columns_ != kNumColumns)
            throw std::runtime_error("CaptureReader: unexpected column count");

        std::vector<uint8_t> idx(num_blocks_ * kBlockIndexEntry);
        if (num_blocks_ && pread_exact(idx.data(), idx.size(), index_off_) != idx.size())
            throw std::runtime_error("CaptureReader: short block index");
        ByteReader ir(idx.data(), idx.size());
        for (uint64_t i = 0; i < num_blocks_; ++i) {
            uint64_t off = ir.u64();
            uint32_t cnt = ir.u32();
            ir.u32();
            blocks_.emplace_back(off, cnt);
        }
    }

    ~CaptureReader() { if (fd_ >= 0) ::close(fd_); }

    uint64_t num_records() const { return num_records_; }
    uint64_t num_blocks()  const { return num_blocks_; }
    uint32_t records_per_block() const { return records_per_block_; }
    const std::array<uint8_t, kSessionIdLen>& session() const { return session_; }
    uint64_t bytes_read() const { return bytes_read_; }
    void reset_bytes_read() { bytes_read_ = 0; }

    // Reads only column `col` from block `bi`. Touches the block prefix, the
    // directory, and that column's payload -- nothing else.
    bool read_block_column(size_t bi, size_t col, std::vector<uint64_t>& out) {
        if (bi >= blocks_.size() || col >= kNumColumns) return false;
        uint64_t boff = blocks_[bi].first;
        uint8_t prefix[kBlockPrefixSize];
        if (pread_exact(prefix, kBlockPrefixSize, boff) != kBlockPrefixSize) return false;
        ByteReader pr(prefix, kBlockPrefixSize);
        if (pr.u32() != kBlockMagic) return false;
        uint32_t rec_count = pr.u32();
        uint32_t ncols     = pr.u32();
        uint32_t dir_bytes = pr.u32();
        if (ncols != kNumColumns) return false;

        std::vector<uint8_t> dir(dir_bytes);
        if (pread_exact(dir.data(), dir_bytes, boff + kBlockPrefixSize) != dir_bytes) return false;
        ByteReader dr(dir.data(), dir.size());
        uint64_t payload_off = boff + kBlockPrefixSize + dir_bytes;
        Encoding enc = Encoding::Raw;
        uint32_t len = 0;
        for (size_t c = 0; c < kNumColumns; ++c) {
            Encoding e = static_cast<Encoding>(dr.u8());
            dr.u8(); dr.u8(); dr.u8();
            uint32_t n = dr.u32();
            if (c == col) { enc = e; len = n; break; }
            payload_off += n;
        }
        std::vector<uint8_t> payload(len);
        if (len && pread_exact(payload.data(), len, payload_off) != len) return false;
        ByteReader vr(payload.data(), payload.size());
        return decode_column(vr, enc, column_defs()[col].type, rec_count, out);
    }

    // Reads every column of a block (the full-record path).
    bool read_block(size_t bi, std::vector<std::vector<uint64_t>>& cols, uint32_t& rec_count) {
        if (bi >= blocks_.size()) return false;
        uint64_t boff = blocks_[bi].first;
        uint8_t prefix[kBlockPrefixSize];
        if (pread_exact(prefix, kBlockPrefixSize, boff) != kBlockPrefixSize) return false;
        ByteReader pr(prefix, kBlockPrefixSize);
        if (pr.u32() != kBlockMagic) return false;
        rec_count          = pr.u32();
        uint32_t ncols     = pr.u32();
        uint32_t dir_bytes = pr.u32();
        if (ncols != kNumColumns) return false;

        std::vector<uint8_t> dir(dir_bytes);
        if (pread_exact(dir.data(), dir_bytes, boff + kBlockPrefixSize) != dir_bytes) return false;
        ByteReader dr(dir.data(), dir.size());
        std::vector<Encoding> encs(kNumColumns);
        std::vector<uint32_t> lens(kNumColumns);
        uint64_t total = 0;
        for (size_t c = 0; c < kNumColumns; ++c) {
            encs[c] = static_cast<Encoding>(dr.u8());
            dr.u8(); dr.u8(); dr.u8();
            lens[c] = dr.u32();
            total += lens[c];
        }
        std::vector<uint8_t> payload(total);
        if (total && pread_exact(payload.data(), total, boff + kBlockPrefixSize + dir_bytes) != total)
            return false;

        cols.assign(kNumColumns, {});
        size_t off = 0;
        for (size_t c = 0; c < kNumColumns; ++c) {
            ByteReader vr(payload.data() + off, lens[c]);
            if (!decode_column(vr, encs[c], column_defs()[c].type, rec_count, cols[c])) return false;
            off += lens[c];
        }
        return true;
    }

    // Materializes the whole capture as records, in capture order.
    std::vector<TickRecord> read_all() {
        std::vector<TickRecord> out;
        out.reserve(num_records_);
        std::vector<std::vector<uint64_t>> cols;
        uint32_t n = 0;
        for (size_t b = 0; b < blocks_.size(); ++b) {
            if (!read_block(b, cols, n)) throw std::runtime_error("CaptureReader: bad block");
            for (uint32_t i = 0; i < n; ++i) {
                TickRecord r;
                for (size_t c = 0; c < kNumColumns; ++c) set_record_field(r, c, cols[c][i]);
                out.push_back(r);
            }
        }
        return out;
    }

private:
    size_t pread_exact(void* buf, size_t n, uint64_t off) {
        size_t done = 0;
        while (done < n) {
            ssize_t k = ::pread(fd_, static_cast<uint8_t*>(buf) + done, n - done, static_cast<off_t>(off + done));
            if (k <= 0) break;
            done += static_cast<size_t>(k);
        }
        bytes_read_ += done;
        return done;
    }

    int fd_ = -1;
    uint32_t version_ = 0;
    uint32_t num_columns_ = 0;
    uint64_t num_records_ = 0;
    uint64_t num_blocks_ = 0;
    uint64_t index_off_ = 0;
    uint32_t records_per_block_ = 0;
    std::array<uint8_t, kSessionIdLen> session_{};
    std::vector<std::pair<uint64_t, uint32_t>> blocks_;
    uint64_t bytes_read_ = 0;
};

} // namespace tickcap
