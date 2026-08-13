// Columnar vs row-oriented, measured rather than asserted.
//
// Two things get compared, and they are different claims:
//
//   size        How many bytes the same session occupies in the columnar
//               format versus a packed row-oriented file holding exactly
//               the same fields at their natural widths (57 bytes/record,
//               no padding). This is a fair baseline -- it is not compared
//               against CSV or against JSON, which would be a strawman.
//
//   scan cost   How many bytes have to be touched, and how long it takes,
//               to answer a question about ONE field across the whole
//               session. This is the access-pattern argument for columnar
//               layout, and it is the one that survives even if the
//               compression ratio were 1.0.
//
// Both scans compute the same aggregate and the results are compared: a
// benchmark whose two sides disagree on the answer is not measuring
// anything, so the check is part of the benchmark rather than an afterthought.
//
// Timings are warm-cache. Both files are read immediately after being
// written, so these numbers measure decode and memory-bandwidth cost, not
// disk. The bytes-touched figures are the storage-independent measure and
// are the ones to trust if your workload is I/O bound.
//
// Usage: capture_bench [capture_path] [row_path]
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <chrono>
#include <limits>
#include <fcntl.h>
#include <unistd.h>
#include "tick_capture.hpp"
#include "le_bytes.hpp"

using namespace tickcap;
using Clock = std::chrono::steady_clock;

static double secs_since(Clock::time_point t) {
    return std::chrono::duration<double>(Clock::now() - t).count();
}

// Packed row-oriented baseline: every record written field-by-field at its
// natural width, little-endian, no padding and no per-record framing.
// Driven by the same schema the columnar writer uses, so the two formats are
// storing identical information and the size comparison is apples to apples.
static void write_row_file(const std::string& path, const std::vector<TickRecord>& recs) {
    std::FILE* f = std::fopen(path.c_str(), "wb");
    if (!f) { std::perror("row file"); std::exit(1); }
    std::vector<uint8_t> buf;
    buf.reserve(1 << 20);
    for (const TickRecord& r : recs) {
        for (size_t c = 0; c < kNumColumns; ++c) {
            uint64_t v = record_field(r, c);
            size_t w = phys_width(column_defs()[c].type);
            for (size_t b = 0; b < w; ++b) buf.push_back(static_cast<uint8_t>(v >> (8 * b)));
        }
        if (buf.size() >= (1 << 20)) { std::fwrite(buf.data(), 1, buf.size(), f); buf.clear(); }
    }
    if (!buf.empty()) std::fwrite(buf.data(), 1, buf.size(), f);
    std::fclose(f);
}

static uint64_t file_size(const std::string& path) {
    std::FILE* f = std::fopen(path.c_str(), "rb");
    if (!f) return 0;
    std::fseek(f, 0, SEEK_END);
    long n = std::ftell(f);
    std::fclose(f);
    return static_cast<uint64_t>(n < 0 ? 0 : n);
}

struct ScanResult {
    int64_t  min_price = std::numeric_limits<int64_t>::max();
    int64_t  max_price = std::numeric_limits<int64_t>::min();
    int64_t  sum_price = 0;
    uint64_t count     = 0;
    uint64_t bytes     = 0;
    double   seconds   = 0;
    bool operator==(const ScanResult& o) const {
        return min_price == o.min_price && max_price == o.max_price &&
               sum_price == o.sum_price && count == o.count;
    }
};

// The columnar path: only the price_ticks chunk of each block is preaded.
static ScanResult scan_price_columnar(const std::string& path) {
    CaptureReader r(path);
    r.reset_bytes_read();
    ScanResult out;
    auto t = Clock::now();
    std::vector<uint64_t> col;
    for (size_t b = 0; b < r.num_blocks(); ++b) {
        if (!r.read_block_column(b, 8 /* price_ticks */, col)) { std::printf("column read failed\n"); std::exit(1); }
        for (uint64_t bits : col) {
            int64_t p = static_cast<int64_t>(bits);
            if (p < out.min_price) out.min_price = p;
            if (p > out.max_price) out.max_price = p;
            out.sum_price += p;
            ++out.count;
        }
    }
    out.seconds = secs_since(t);
    out.bytes   = r.bytes_read();
    return out;
}

// The row path: there is no way to reach the price field of record N+1
// without stepping over every other field of record N. A row store can skip
// bytes with seeks, but on any real storage that still faults in the pages
// they live on, so a sequential read is the honest model.
static ScanResult scan_price_rows(const std::string& path) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) { std::perror("open row file"); std::exit(1); }
    ScanResult out;
    const size_t kBuf = 1 << 20;
    std::vector<uint8_t> buf(kBuf);
    std::vector<uint8_t> carry;
    auto t = Clock::now();
    while (true) {
        ssize_t n = ::read(fd, buf.data(), kBuf);
        if (n <= 0) break;
        out.bytes += static_cast<uint64_t>(n);
        carry.insert(carry.end(), buf.begin(), buf.begin() + n);
        size_t usable = (carry.size() / kRowRecordBytes) * kRowRecordBytes;
        const size_t price_off = row_field_offset(8);   // price_ticks
        for (size_t off = 0; off < usable; off += kRowRecordBytes) {
            const uint8_t* p = carry.data() + off + price_off;
            uint64_t bits = 0;
            for (int i = 0; i < 8; ++i) bits |= static_cast<uint64_t>(p[i]) << (8 * i);
            int64_t v = static_cast<int64_t>(bits);
            if (v < out.min_price) out.min_price = v;
            if (v > out.max_price) out.max_price = v;
            out.sum_price += v;
            ++out.count;
        }
        carry.erase(carry.begin(), carry.begin() + usable);
    }
    out.seconds = secs_since(t);
    ::close(fd);
    return out;
}

int main(int argc, char** argv) {
    std::string capture_path = (argc > 1) ? argv[1] : "/tmp/session.tcap";
    std::string row_path     = (argc > 2) ? argv[2] : "/tmp/session.rows";

    std::printf("=== capture format benchmark ===\n");
    std::printf("capture : %s\n", capture_path.c_str());
    if (row_record_bytes() != kRowRecordBytes) {
        std::printf("FAIL: schema widths sum to %zu but kRowRecordBytes is %zu\n",
                    row_record_bytes(), kRowRecordBytes);
        return 1;
    }

    std::vector<TickRecord> recs;
    {
        CaptureReader r(capture_path);
        recs = r.read_all();
    }
    std::printf("records : %zu\n\n", recs.size());

    write_row_file(row_path, recs);
    uint64_t col_bytes = file_size(capture_path);
    uint64_t row_bytes = file_size(row_path);

    std::printf("--- on-disk size ---\n");
    std::printf("row-oriented (%zu B/record) : %llu bytes (%.2f MiB)\n",
                kRowRecordBytes, (unsigned long long)row_bytes, row_bytes / 1048576.0);
    std::printf("columnar TCAP               : %llu bytes (%.2f MiB)\n",
                (unsigned long long)col_bytes, col_bytes / 1048576.0);
    std::printf("ratio                       : %.2fx smaller (%.2f bytes/record vs %zu)\n",
                row_bytes ? double(row_bytes) / double(col_bytes) : 0.0,
                recs.empty() ? 0.0 : double(col_bytes) / double(recs.size()),
                kRowRecordBytes);

    std::printf("\n--- single-column scan (min/max/sum of price_ticks) ---\n");
    ScanResult c = scan_price_columnar(capture_path);
    ScanResult r = scan_price_rows(row_path);

    if (!(c == r)) {
        std::printf("FAIL: the two scans disagree -- benchmark is invalid.\n");
        std::printf("  columnar: n=%llu min=%lld max=%lld sum=%lld\n",
                    (unsigned long long)c.count, (long long)c.min_price,
                    (long long)c.max_price, (long long)c.sum_price);
        std::printf("  rows    : n=%llu min=%lld max=%lld sum=%lld\n",
                    (unsigned long long)r.count, (long long)r.min_price,
                    (long long)r.max_price, (long long)r.sum_price);
        return 1;
    }

    std::printf("both scans agree: n=%llu min=%lld max=%lld sum=%lld\n",
                (unsigned long long)c.count, (long long)c.min_price,
                (long long)c.max_price, (long long)c.sum_price);
    std::printf("columnar : %10llu bytes touched, %.4f s\n",
                (unsigned long long)c.bytes, c.seconds);
    std::printf("rows     : %10llu bytes touched, %.4f s\n",
                (unsigned long long)r.bytes, r.seconds);
    std::printf("columnar reads %.2fx fewer bytes and is %.2fx faster (warm cache)\n",
                c.bytes ? double(r.bytes) / double(c.bytes) : 0.0,
                c.seconds > 0 ? r.seconds / c.seconds : 0.0);

    std::printf("\n--- full-record scan (all 11 columns) ---\n");
    {
        CaptureReader rd(capture_path);
        rd.reset_bytes_read();
        auto t = Clock::now();
        std::vector<std::vector<uint64_t>> cols;
        uint32_t n = 0;
        uint64_t total = 0;
        for (size_t b = 0; b < rd.num_blocks(); ++b) {
            if (!rd.read_block(b, cols, n)) { std::printf("block read failed\n"); return 1; }
            total += n;
        }
        double cs = secs_since(t);
        std::printf("columnar : %10llu bytes touched, %.4f s (%llu records)\n",
                    (unsigned long long)rd.bytes_read(), cs, (unsigned long long)total);
    }
    {
        int fd = ::open(row_path.c_str(), O_RDONLY);
        std::vector<uint8_t> buf(1 << 20);
        uint64_t bytes = 0;
        auto t = Clock::now();
        while (true) {
            ssize_t n = ::read(fd, buf.data(), buf.size());
            if (n <= 0) break;
            bytes += static_cast<uint64_t>(n);
        }
        double rs = secs_since(t);
        ::close(fd);
        std::printf("rows     : %10llu bytes touched, %.4f s (read only, no field extraction)\n",
                    (unsigned long long)bytes, rs);
        std::printf("note     : the row store is competitive here -- when every field is\n"
                    "           needed, columnar has to pay decode on all 11 columns.\n");
    }
    return 0;
}
