// Emits the C++ decoder's view of a capture in forms an independent
// implementation can be compared against.
//
// This exists so the Python reader can be cross-checked against something
// other than itself. Two artifacts are produced:
//
//   per-column digests   SHA-256 over each column's values, every value
//                        widened to 8 bytes little-endian. Deliberately a
//                        trivial canonical form: any implementation that
//                        decoded the column correctly will produce the same
//                        hash, and the definition is small enough to state
//                        in one sentence in docs/FORMAT.md.
//
//   top-of-book CSV      the book state after replaying the first N records,
//                        so the cross-check covers not just "can Python
//                        parse the bytes" but "does Python's book agree".
//
// Usage: capture_dump <capture> [tob_csv] [replay_limit]
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>
#include "tick_capture.hpp"
#include "feed_decoder.hpp"
#include "book_universe.hpp"
#include "canonical.hpp"
#include "sha256.hpp"
#include "le_bytes.hpp"

using namespace tickcap;

int main(int argc, char** argv) {
    if (argc < 2) {
        std::printf("usage: capture_dump <capture> [tob_csv] [replay_limit]\n");
        return 2;
    }
    std::string path = argv[1];
    std::string tob_csv = (argc > 2) ? argv[2] : "";
    size_t limit = (argc > 3) ? static_cast<size_t>(std::strtoull(argv[3], nullptr, 10)) : 0;

    CaptureReader rd(path);
    std::vector<TickRecord> recs = rd.read_all();

    std::printf("records %zu\n", recs.size());
    std::printf("blocks %llu\n", (unsigned long long)rd.num_blocks());
    std::printf("records_per_block %u\n", rd.records_per_block());
    std::string sess;
    for (uint8_t c : rd.session()) sess.push_back(static_cast<char>(c));
    std::printf("session %s\n", sess.c_str());

    // Per-column digests over the widened little-endian value stream.
    for (size_t c = 0; c < kNumColumns; ++c) {
        Sha256 h;
        std::vector<uint8_t> buf;
        buf.reserve(1 << 16);
        for (const TickRecord& r : recs) {
            put_u64(buf, record_field(r, c));
            if (buf.size() >= (1 << 16)) { h.update(buf); buf.clear(); }
        }
        if (!buf.empty()) h.update(buf);
        std::printf("column %s %s\n", column_defs()[c].name, h.hex().c_str());
    }

    if (limit == 0 || limit > recs.size()) limit = recs.size();
    BookUniverse books;
    for (size_t i = 0; i < limit; ++i) books.apply(record_to_msg(recs[i]), recs[i].seq_num);
    std::printf("replayed %zu\n", limit);
    std::printf("resting %llu\n", (unsigned long long)books.total_resting_orders());
    std::printf("book_digest %s\n", canonical_book_digest(books).c_str());

    if (!tob_csv.empty()) {
        std::FILE* f = std::fopen(tob_csv.c_str(), "w");
        if (!f) { std::perror("tob csv"); return 1; }
        std::fprintf(f, "instrument_id,bid_ticks,bid_qty,bid_orders,ask_ticks,ask_qty,ask_orders,resting\n");
        for (uint16_t i = 0; i < books.num_instruments(); ++i) {
            TopOfBook t = top_of_book(books, i);
            std::fprintf(f, "%u,%lld,%llu,%u,%lld,%llu,%u,%llu\n",
                         (unsigned)t.instrument_id,
                         t.has_bid ? (long long)t.bid_ticks : -1LL,
                         (unsigned long long)t.bid_qty, (unsigned)t.bid_orders,
                         t.has_ask ? (long long)t.ask_ticks : -1LL,
                         (unsigned long long)t.ask_qty, (unsigned)t.ask_orders,
                         (unsigned long long)t.resting);
        }
        std::fclose(f);
        std::printf("tob_csv %s\n", tob_csv.c_str());
    }
    return 0;
}
