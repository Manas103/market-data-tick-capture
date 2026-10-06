"""Cross-checks the typed columnar query path (tcap_query, C++) against the
independent tshark-plus-grep baseline (tshark_grep_baseline.py) on several
different predicates over the same capture, asserting both report the exact
same scanned/matched counts. Agreement across two independently-coded paths
reading two independently-generated files (TCAP vs. the pcap export) is the
evidence that the speed comparison is measuring two ways of answering the
same question, not two different questions.

Usage: python validate_typed_query.py <tcap_query_binary> <capture.tcap> <capture.pcap>
"""
from __future__ import annotations

import re
import subprocess
import sys

from tshark_grep_baseline import count_matches

PREDICATES = [(5, "A"), (5, "E"), (17, "A"), (42, "X"), (90, "D"), (1, "U")]


def run_cpp(binary: str, tcap_path: str, instrument: int, msg_type: str) -> tuple[int, int]:
    out = subprocess.run([binary, tcap_path, str(instrument), msg_type],
                          capture_output=True, text=True, check=True).stdout
    scanned = int(re.search(r"scanned=(\d+)", out).group(1))
    matched = int(re.search(r"matched=(\d+)", out).group(1))
    return scanned, matched


def main() -> int:
    binary, tcap_path, pcap_path = sys.argv[1], sys.argv[2], sys.argv[3]
    failures = 0
    for instrument, msg_type in PREDICATES:
        cpp_scanned, cpp_matched = run_cpp(binary, tcap_path, instrument, msg_type)
        py_scanned, py_matched = count_matches(pcap_path, instrument, msg_type.encode("ascii"))
        ok = (cpp_scanned == py_scanned) and (cpp_matched == py_matched)
        status = "MATCH" if ok else "MISMATCH"
        print(f"instrument={instrument:>4} type={msg_type}  "
              f"cpp(scanned={cpp_scanned}, matched={cpp_matched})  "
              f"python(scanned={py_scanned}, matched={py_matched})  {status}")
        if not ok:
            failures += 1

    print()
    if failures == 0:
        print(f"PASS: typed query path and tshark-plus-grep baseline agree on all {len(PREDICATES)} predicates.")
        return 0
    print(f"FAIL: {failures} of {len(PREDICATES)} predicate(s) disagreed.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
