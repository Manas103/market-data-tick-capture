// The typed predicate query path: counts messages matching
// (instrument_id == X AND msg_type == Y) by decoding ONLY the two columns
// the predicate needs, across every block, never touching the other nine
// columns (order_id, qty, price_ticks, timestamps, ...). This is the
// columnar selectivity CaptureReader::read_block_column already proves in
// the base README's "columnar selectivity (measured)" section, used here
// to answer a real feed question instead of a raw byte-count benchmark.
//
// Usage: tcap_query <capture.tcap> <instrument_id> <msg_type_char>
#include <cstdio>
#include <cstdlib>
#include <chrono>
#include <string>
#include <vector>
#include "tick_capture.hpp"

using namespace tickcap;

int main(int argc, char** argv) {
    if (argc < 4) {
        std::fprintf(stderr, "usage: %s <capture.tcap> <instrument_id> <msg_type_char>\n", argv[0]);
        return 1;
    }
    std::string path = argv[1];
    uint16_t want_instrument = static_cast<uint16_t>(std::atoi(argv[2]));
    uint8_t want_type = static_cast<uint8_t>(argv[3][0]);

    auto t0 = std::chrono::steady_clock::now();
    CaptureReader reader(path);

    // Column indices from tick_capture.hpp's column_defs(): msg_type=3, instrument_id=4.
    constexpr size_t kMsgTypeCol = 3;
    constexpr size_t kInstrumentCol = 4;

    uint64_t matched = 0;
    uint64_t scanned = 0;
    std::vector<uint64_t> types, instruments;
    for (uint64_t bi = 0; bi < reader.num_blocks(); ++bi) {
        if (!reader.read_block_column(bi, kMsgTypeCol, types)) { std::fprintf(stderr, "read failed\n"); return 1; }
        if (!reader.read_block_column(bi, kInstrumentCol, instruments)) { std::fprintf(stderr, "read failed\n"); return 1; }
        size_t n = types.size();
        for (size_t i = 0; i < n; ++i) {
            if (types[i] == want_type && instruments[i] == want_instrument) ++matched;
        }
        scanned += n;
    }
    auto t1 = std::chrono::steady_clock::now();
    double elapsed_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

    std::printf("scanned=%llu matched=%llu instrument_id=%u msg_type=%c elapsed_ms=%.3f bytes_touched_cols=2\n",
                (unsigned long long)scanned, (unsigned long long)matched, want_instrument, want_type, elapsed_ms);
    return 0;
}
