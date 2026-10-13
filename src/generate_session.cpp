// Writes a TCAP capture directly from the simulator, with no network
// involved at all. live_capture's job is to prove the capture survives a
// real multicast receive path; this tool exists only to produce the inputs
// the publication pipeline (python/publish_parquet.py) and its validator
// (python/validate_publication.py) are demonstrated against, where the
// network path is not what is being measured.
//
// Usage: generate_session <seed> <num_messages> <output.tcap> [num_instruments]
#include <cstdio>
#include <cstdlib>
#include <string>

#include "market_simulator.hpp"
#include "tick_capture.hpp"

using namespace tickcap;

int main(int argc, char** argv) {
    if (argc < 4) {
        std::fprintf(stderr, "usage: %s <seed> <num_messages> <output.tcap> [num_instruments]\n", argv[0]);
        return 1;
    }
    uint64_t seed         = std::strtoull(argv[1], nullptr, 10);
    size_t   num_messages = std::strtoull(argv[2], nullptr, 10);
    std::string out_path  = argv[3];
    int num_instruments   = argc > 4 ? std::atoi(argv[4]) : kNumInstruments;
    constexpr uint32_t kDatagramSize = 40;

    MarketSimulator sim(seed, num_instruments);
    auto session = make_session_id("GENSESSION ");
    CaptureWriter writer(out_path, session);

    for (size_t i = 0; i < num_messages; ++i) {
        DecodedMsg m = sim.next();
        TickRecord r;
        r.capture_ts_ns  = m.exchange_ts_ns + 1000;
        r.exchange_ts_ns = m.exchange_ts_ns;
        r.seq_num        = i;
        r.msg_type       = static_cast<uint8_t>(m.type);
        r.instrument_id  = m.instrument_id;
        r.order_id       = m.order_id;
        r.new_order_id   = m.new_order_id;
        r.side           = static_cast<uint8_t>(m.side);
        r.price_ticks    = m.price_ticks;
        r.qty            = m.qty;
        r.pkt_first      = (i % kDatagramSize == 0) ? 1 : 0;
        writer.append(r);
    }
    writer.close();

    std::printf("wrote %zu records (%d instruments, seed %llu) to %s\n",
                num_messages, num_instruments, static_cast<unsigned long long>(seed), out_path.c_str());
    return 0;
}
