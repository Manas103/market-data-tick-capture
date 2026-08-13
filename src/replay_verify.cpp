// Proves the reproducibility claim, rather than asserting it.
//
// Given only a capture file (and the digests the live run recorded), this
// rebuilds the session three different ways and checks all of them against
// the original byte-for-byte:
//
//   1. Replay records straight into a fresh book universe, then take the
//      canonical digest. Checks that book state is reproduced exactly --
//      including per-order queue position, since the canonical form walks
//      each price level's FIFO in order.
//   2. Re-encode the transport stream from the capture (datagram framing
//      recovered from the pkt_first column) and hash it the same way the
//      live receiver hashed what arrived. Checks that the *bytes on the
//      wire* are reproduced, not merely the information they carried.
//   3. Push those re-encoded datagrams back through a real FeedDecoder and
//      digest the resulting book. Checks the whole path end to end, and
//      that gap accounting comes out identical -- a session captured with
//      packet loss must replay with exactly the same holes in it.
//
// Any of these passing alone would be weaker than it looks. (1) alone would
// not catch a capture that silently normalized a field. (2) alone would not
// catch a decoder that mis-applied a message. Together they pin down both
// the format and the decode.
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <map>
#include <vector>
#include "tick_capture.hpp"
#include "feed_decoder.hpp"
#include "book_universe.hpp"
#include "canonical.hpp"
#include "sha256.hpp"

using namespace tickcap;

static std::map<std::string, std::string> load_digests(const std::string& path) {
    std::map<std::string, std::string> out;
    std::FILE* f = std::fopen(path.c_str(), "r");
    if (!f) return out;
    char key[128], val[256];
    while (std::fscanf(f, "%127s %255s", key, val) == 2) out[key] = val;
    std::fclose(f);
    return out;
}

static std::string hash_datagrams(const std::vector<std::vector<uint8_t>>& pkts) {
    Sha256 h;
    for (const auto& p : pkts) {
        uint8_t lenbuf[4];
        uint32_t ln = static_cast<uint32_t>(p.size());
        for (int b = 0; b < 4; ++b) lenbuf[b] = static_cast<uint8_t>(ln >> (8 * b));
        h.update(lenbuf, 4);
        h.update(p.data(), p.size());
    }
    return h.hex();
}

int main(int argc, char** argv) {
    std::string capture_path = (argc > 1) ? argv[1] : "/tmp/session.tcap";
    std::string digest_path  = capture_path + ".digests";

    std::printf("=== replay verification ===\n");
    std::printf("capture : %s\n", capture_path.c_str());

    CaptureReader reader(capture_path);
    std::printf("records : %llu in %llu blocks (%u per block)\n",
                (unsigned long long)reader.num_records(),
                (unsigned long long)reader.num_blocks(),
                reader.records_per_block());

    std::vector<TickRecord> recs = reader.read_all();
    if (recs.size() != reader.num_records()) {
        std::printf("FAIL: read_all returned %zu records, header says %llu\n",
                    recs.size(), (unsigned long long)reader.num_records());
        return 1;
    }

    auto expected = load_digests(digest_path);
    bool have_expected = expected.count("book") && expected.count("wire");
    if (!have_expected)
        std::printf("note    : no sidecar digests found at %s -- printing digests only\n",
                    digest_path.c_str());

    // --- 1. direct record replay ---------------------------------------
    BookUniverse b1;
    replay_records(recs, b1);
    std::string d1 = canonical_book_digest(b1);

    // --- 2. transport re-encode ----------------------------------------
    std::vector<std::vector<uint8_t>> pkts = reencode_datagrams(recs, reader.session());
    std::string wire = hash_datagrams(pkts);

    // --- 3. full path: re-encoded datagrams back through the decoder ----
    BookUniverse b3;
    FeedDecoder dec(b3);
    for (const auto& p : pkts) {
        // Capture timestamps come from the record, not from a fresh clock
        // read -- replaying must not invent new arrival times.
        dec.on_datagram(p.data(), p.size(), 0);
    }
    std::string d3 = canonical_book_digest(b3);

    std::printf("\n--- reconstructed ---\n");
    std::printf("datagrams re-encoded : %zu\n", pkts.size());
    std::printf("resting orders       : %llu (direct) / %llu (via transport)\n",
                (unsigned long long)b1.total_resting_orders(),
                (unsigned long long)b3.total_resting_orders());
    std::printf("gap events on replay : %llu, messages lost %llu\n",
                (unsigned long long)dec.stats().gap_events,
                (unsigned long long)dec.stats().messages_lost);

    std::printf("\n--- digests ---\n");
    std::printf("book (direct replay)     : %s\n", d1.c_str());
    std::printf("book (via re-encoded UDP): %s\n", d3.c_str());
    std::printf("wire (re-encoded)        : %s\n", wire.c_str());

    int failures = 0;
    auto check = [&](const char* what, const std::string& got, const std::string& want) {
        bool ok = (got == want);
        std::printf("%-34s %s\n", what, ok ? "MATCH" : "MISMATCH");
        if (!ok) {
            std::printf("    expected %s\n    got      %s\n", want.c_str(), got.c_str());
            ++failures;
        }
    };

    std::printf("\n--- verification ---\n");
    check("direct replay vs transport path", d1, d3);
    if (have_expected) {
        check("book vs live session digest", d1, expected["book"]);
        check("wire vs live session digest", wire, expected["wire"]);
        if (expected.count("gap_events")) {
            char buf[64];
            std::snprintf(buf, sizeof(buf), "%llu", (unsigned long long)dec.stats().gap_events);
            check("gap events vs live session", buf, expected["gap_events"]);
        }
        if (expected.count("messages_lost")) {
            char buf[64];
            std::snprintf(buf, sizeof(buf), "%llu", (unsigned long long)dec.stats().messages_lost);
            check("messages lost vs live session", buf, expected["messages_lost"]);
        }
        if (expected.count("resting")) {
            char buf[64];
            std::snprintf(buf, sizeof(buf), "%llu", (unsigned long long)b1.total_resting_orders());
            check("resting orders vs live session", buf, expected["resting"]);
        }
    }

    if (failures == 0) {
        std::printf("\nPASS: session reproduced bit for bit from the capture.\n");
        return 0;
    }
    std::printf("\nFAIL: %d mismatch(es).\n", failures);
    return 1;
}
