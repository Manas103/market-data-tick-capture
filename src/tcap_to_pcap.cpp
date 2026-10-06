// Exports a TCAP capture to a standard pcap file, wrapping each re-encoded
// MoldUDP64-style datagram in a synthetic Ethernet+IPv4+UDP frame so an
// unmodified tshark can read it. This is what lets "25x faster than a
// tshark-plus-grep baseline on the same capture" be measured on the exact
// same bytes rather than on a separately regenerated session: the pcap and
// the TCAP file both come from re-encoding the identical capture records.
//
// The Ethernet/IP addressing below is entirely synthetic (see README
// "Honest framing"); only the UDP payload -- the re-encoded datagram bytes
// -- is real, re-encoded capture content, identical to what
// replay_verify's wire-digest check already proves byte-for-byte.
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include "tick_capture.hpp"
#include "feed_decoder.hpp"

using namespace tickcap;

namespace {

void put_u16_be(std::vector<uint8_t>& out, uint16_t v) {
    out.push_back(static_cast<uint8_t>(v >> 8));
    out.push_back(static_cast<uint8_t>(v));
}
void put_u32_le(std::vector<uint8_t>& out, uint32_t v) {
    out.push_back(static_cast<uint8_t>(v));
    out.push_back(static_cast<uint8_t>(v >> 8));
    out.push_back(static_cast<uint8_t>(v >> 16));
    out.push_back(static_cast<uint8_t>(v >> 24));
}

// 20-byte IPv4 header, UDP checksum left as 0 (optional for IPv4; tshark
// reads payloads fine without a valid checksum).
void write_ip_udp_frame(std::FILE* f, const std::vector<uint8_t>& payload, uint32_t ts_sec, uint32_t ts_usec) {
    std::vector<uint8_t> pkt;
    // Ethernet: dst, src (synthetic locally-administered MACs), ethertype IPv4
    const uint8_t dst_mac[6] = {0x02, 0x00, 0x00, 0x00, 0x00, 0x01};
    const uint8_t src_mac[6] = {0x02, 0x00, 0x00, 0x00, 0x00, 0x02};
    pkt.insert(pkt.end(), dst_mac, dst_mac + 6);
    pkt.insert(pkt.end(), src_mac, src_mac + 6);
    put_u16_be(pkt, 0x0800);

    uint16_t udp_len = static_cast<uint16_t>(8 + payload.size());
    uint16_t ip_total_len = static_cast<uint16_t>(20 + udp_len);

    // IPv4 header
    pkt.push_back(0x45);              // version 4, IHL 5
    pkt.push_back(0x00);              // DSCP/ECN
    put_u16_be(pkt, ip_total_len);
    put_u16_be(pkt, 0x0000);          // identification
    put_u16_be(pkt, 0x0000);          // flags/fragment offset
    pkt.push_back(0x40);              // TTL
    pkt.push_back(17);                // protocol: UDP
    put_u16_be(pkt, 0x0000);          // header checksum (unvalidated, see comment above)
    pkt.push_back(10); pkt.push_back(0); pkt.push_back(0); pkt.push_back(1);   // src 10.0.0.1
    pkt.push_back(239); pkt.push_back(1); pkt.push_back(1); pkt.push_back(1);  // dst 239.1.1.1 (multicast)

    // UDP header
    put_u16_be(pkt, 30001);   // src port
    put_u16_be(pkt, 30002);   // dst port
    put_u16_be(pkt, udp_len);
    put_u16_be(pkt, 0x0000);  // checksum (0 = unused, valid for IPv4)

    pkt.insert(pkt.end(), payload.begin(), payload.end());

    // pcap per-packet record header (classic pcap, little-endian, since the
    // global header below declares magic 0xa1b2c3d4)
    std::vector<uint8_t> rec_hdr;
    put_u32_le(rec_hdr, ts_sec);
    put_u32_le(rec_hdr, ts_usec);
    put_u32_le(rec_hdr, static_cast<uint32_t>(pkt.size()));
    put_u32_le(rec_hdr, static_cast<uint32_t>(pkt.size()));
    std::fwrite(rec_hdr.data(), 1, rec_hdr.size(), f);
    std::fwrite(pkt.data(), 1, pkt.size(), f);
}

} // namespace

int main(int argc, char** argv) {
    if (argc < 3) {
        std::fprintf(stderr, "usage: %s <capture.tcap> <out.pcap>\n", argv[0]);
        return 1;
    }
    std::string capture_path = argv[1];
    std::string pcap_path = argv[2];

    CaptureReader reader(capture_path);
    std::vector<TickRecord> recs = reader.read_all();
    std::printf("records: %zu\n", recs.size());

    std::FILE* f = std::fopen(pcap_path.c_str(), "wb");
    if (!f) { std::perror("fopen"); return 1; }

    // pcap global header, classic format, microsecond resolution, Ethernet link type
    auto put16_le = [](std::vector<uint8_t>& o, uint16_t v) { o.push_back(v & 0xFF); o.push_back((v >> 8) & 0xFF); };
    std::vector<uint8_t> gh;
    put_u32_le(gh, 0xa1b2c3d4);  // magic: classic pcap, little-endian, microsecond timestamps
    put16_le(gh, 2); put16_le(gh, 4);  // version 2.4
    put_u32_le(gh, 0);           // thiszone
    put_u32_le(gh, 0);           // sigfigs
    put_u32_le(gh, 65535);       // snaplen
    put_u32_le(gh, 1);           // network = Ethernet
    std::fwrite(gh.data(), 1, gh.size(), f);

    size_t i = 0, n_pkts = 0;
    while (i < recs.size()) {
        size_t j = i + 1;
        while (j < recs.size() && recs[j].pkt_first == 0) ++j;
        PacketHeader h;
        h.session = reader.session();
        h.first_seq = recs[i].seq_num;
        h.msg_count = static_cast<uint16_t>(j - i);
        std::vector<uint8_t> payload;
        encode_packet_header(payload, h);
        for (size_t k = i; k < j; ++k) encode_message(payload, record_to_msg(recs[k]));

        // Real recorded arrival timestamp, not a placeholder.
        uint64_t ts_ns = recs[i].capture_ts_ns;
        uint32_t ts_sec = static_cast<uint32_t>(ts_ns / 1'000'000'000ull);
        uint32_t ts_usec = static_cast<uint32_t>((ts_ns % 1'000'000'000ull) / 1000ull);
        write_ip_udp_frame(f, payload, ts_sec, ts_usec);

        i = j;
        ++n_pkts;
    }
    std::fclose(f);
    std::printf("wrote %zu packets (datagrams) to %s\n", n_pkts, pcap_path.c_str());
    return 0;
}
