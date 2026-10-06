"""The "tshark-plus-grep" baseline this project's typed query path is
measured against. tshark has no dissector for this project's own
ITCH/MoldUDP64-*style* protocol (see README "Honest framing"), so the
realistic baseline workflow for a custom binary protocol is: use tshark to
pull each packet's raw UDP payload bytes out of the pcap, then parse and
filter those bytes in a script, which is what "grep" stands in for here.
This is NOT a byte-offset tshark display filter (which would only work for
a fixed-width protocol); it decodes the same variable-length message
framing src/tcap_query.cpp's typed path already knows how to skip past.

Usage: python tshark_grep_baseline.py <capture.pcap> <instrument_id> <msg_type_char>
"""
from __future__ import annotations

import subprocess
import sys
import time

PACKET_HEADER_LEN = 20  # 10-byte session + 8-byte first_seq + 2-byte msg_count


def count_matches(pcap_path: str, want_instrument: int, want_type: bytes) -> tuple[int, int]:
    # "-e udp.payload" reads the raw bytes at the UDP layer itself, before
    # any subdissector runs. The first attempt used "-e data" (the generic
    # Data-dissector field) instead, and about 0.1% of packets in a
    # 3.8M-message capture came back with an empty "data" field: a tiny
    # fraction of this project's own synthetic payloads happen to start
    # with bytes that coincidentally match some other protocol's heuristic
    # signature, so a different subdissector claimed them instead of
    # falling through to "Data", and tshark never silently lost anything on
    # the wire (see README Findings for the full account). "udp.payload" is
    # populated for every UDP packet regardless of which subdissector (if
    # any) runs afterward, which is the fix.
    proc = subprocess.run(
        ["tshark", "-r", pcap_path, "-Y", "udp", "-T", "fields", "-e", "udp.payload"],
        capture_output=True, text=True, check=True,
    )
    matched = 0
    scanned = 0
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        payload = bytes.fromhex(line.replace(":", ""))
        pos = PACKET_HEADER_LEN
        while pos + 2 <= len(payload):
            msg_len = int.from_bytes(payload[pos:pos + 2], "little")
            pos += 2
            if pos + msg_len > len(payload):
                break
            msg = payload[pos:pos + msg_len]
            pos += msg_len
            scanned += 1
            msg_type = msg[0:1]
            instrument_id = int.from_bytes(msg[9:11], "little")
            if msg_type == want_type and instrument_id == want_instrument:
                matched += 1
    return scanned, matched


def main() -> None:
    pcap_path = sys.argv[1]
    want_instrument = int(sys.argv[2])
    want_type = sys.argv[3].encode("ascii")

    t0 = time.perf_counter()
    scanned, matched = count_matches(pcap_path, want_instrument, want_type)
    elapsed_ms = (time.perf_counter() - t0) * 1000.0

    print(f"scanned={scanned} matched={matched} instrument_id={want_instrument} "
          f"msg_type={want_type.decode()} elapsed_ms={elapsed_ms:.3f}")


if __name__ == "__main__":
    main()
