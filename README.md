# Market Data Decoder & Tick Capture Pipeline

A C++ decoder for a simulated exchange's UDP multicast feed that reconstructs
**full-depth, order-by-order limit order books**, plus **TCAP** — a
column-oriented capture format with recorded timestamps that makes any session
reproducible **bit for bit** — plus an **independent Python reader** written
from the format specification, used to cross-validate the C++ decoder and to do
the analytics a tick store exists for.

The headline claim is the reproducibility one, so it is proved rather than
asserted: a live session's final book state and its entire 57.5 MiB datagram
stream are hashed with SHA-256, and replaying the capture reproduces **both
digests exactly**, including any packet loss the original session suffered.
Extended with a full connectivity gateway: A/B line arbitration, TCP gap
recovery for double-line drops, and an OUCH-style order-entry session with
sequenced resend on reconnect (see "Extended: Equity Exchange Connectivity
Gateway" below).

---

## Why this exists

Two problems, and the interesting part is that they pull in opposite
directions.

**Reconstructing a book from a feed** is a decoding problem: parse a binary
message stream, apply order events, keep the result correct at high rates.
**Capturing that session so it can be replayed exactly** is a determinism
problem: every choice that makes a decoder fast — threading, hash maps,
regenerating cheap values instead of storing them — is a choice that can make a
session irreproducible. This project takes determinism as the requirement and
pays for it where necessary, then measures what it cost.

The capture format is columnar because tick data is overwhelmingly read one
field at a time ("what did the spread do", "what was the message rate") and
storing it by column makes both the compression and the access pattern an order
of magnitude better. That claim gets measured too, against a fair row-oriented
baseline rather than against a strawman like CSV.

---

## Honest framing, up front

- **The protocol is ITCH-*inspired*, not ITCH 5.0**, and the transport is
  MoldUDP64-*style*, not MoldUDP64. Both borrow real structure (five order-event
  message categories; a session id + sequence number + message count datagram
  header) and then define their own field layout, spelled out in
  `include/wire_protocol.hpp`. No spec conformance is claimed.
- **The feed is synthetic.** `include/market_simulator.hpp` generates
  self-consistent order flow — every Execute/Cancel/Delete/Replace references an
  order actually resting at that moment — across 128 instruments. There is no
  real exchange data behind any of this.
- **Sender and receiver run on one host.** The multicast path is real
  (`IP_ADD_MEMBERSHIP`, a real 239.x.x.x group, datagrams through the kernel's
  network stack), but cross-host multicast routing needs more than one host and
  was not exercised.
- **POSIX only.** Built and tested on Linux (WSL2, Ubuntu 22.04, g++ 11.4). The
  socket layer uses POSIX multicast and the capture reader uses `pread`. This is
  a deliberate choice — see [Portability](#portability) — but it does mean the
  project does not build on Windows.
- **Every number below was measured on this machine** (16 logical cores), not
  targeted. The commands to reproduce each one are given.
- **All timings are warm-cache.** Files are read straight after being written.
  The bytes-touched figures are the storage-independent measure; trust those if
  your workload is I/O bound.

---

## Architecture

```
include/le_bytes.hpp          explicit little-endian primitives, varint/zigzag,
                              bounds-checked reader
include/wire_protocol.hpp     message encoding + MoldUDP64-style datagram framing
include/market_simulator.hpp  deterministic, self-consistent synthetic order flow
include/multicast_posix.hpp   POSIX UDP multicast sender/receiver
include/order_book_l3.hpp     full-depth ORDER-BY-ORDER book for one instrument
include/book_universe.hpp     all instruments + O(1) order_id -> location index
include/feed_decoder.hpp      datagram -> messages -> book, with gap detection
include/column_codec.hpp      RAW / DELTA / DOD / FOR / RLE column encodings
include/tick_capture.hpp      TCAP columnar capture writer + selective reader
include/canonical.hpp         deterministic book serialization for digests
include/sha256.hpp            self-contained SHA-256 (FIPS test vectors in tests)

src/live_capture.cpp          live multicast publish + decode + capture
src/replay_verify.cpp         proves bit-for-bit reproduction from a capture
src/capture_bench.cpp         columnar vs row-oriented, size and scan cost
src/capture_dump.cpp          emits the C++ view for cross-implementation checks
tests/correctness_test.cpp    600 checks across every layer

python/tickcap/reader.py      INDEPENDENT TCAP reader, written from the spec
python/tickcap/book.py        INDEPENDENT L3 book, different data structures
python/cross_check.py         Python vs C++: column digests, books, top of book
python/analyze.py             session analytics with numpy/pandas

docs/FORMAT.md                the authoritative TCAP specification
```

---

## The order book is order-by-order (L3), not aggregated (L2)

This is the structural choice the rest of the project builds on.

An **aggregated** book stores quantity per price level: *4,000 shares at
101.25*. An **order-by-order** book stores every resting order individually, in
queue order within its level: *six orders at 101.25, and yours is fourth with
2,300 shares ahead of it*.

That second question is the one that decides whether a passive order actually
fills, and an aggregated book **cannot** answer it — the information was
destroyed at ingest. `OrderBookL3::qty_ahead()` exists for exactly that query,
and `analyze.py` prints a real queue from a reconstructed book:

```
queue at instrument 22 best bid 6802 (11 orders, L3-only view):
  order 93918     qty 9     seq 167089    qty ahead of it: 0
  order 127834    qty 259   seq 228555    qty ahead of it: 9
  order 128446    qty 66    seq 229671    qty ahead of it: 268
  order 130214    qty 421   seq 232870    qty ahead of it: 334
  order 134719    qty 250   seq 240847    qty ahead of it: 755
```

**The cost, stated plainly:** L3 is heavier. Every order is a node, cancels
chase pointers, and memory scales with resting order count rather than with
occupied price levels. A feed handler that only needs top-of-book should use a
flat aggregated array instead — O(1) per update, no allocation, strictly better
for that job.

**Container choices matter for determinism, not just speed:**

- `std::map` for price levels — ordered iteration is part of the contract,
  because the canonical digest walks levels in price order. A hash map would be
  correct for lookups but would make serialization order implementation-defined.
- `std::list` for each level's FIFO — iterators survive insertions and erasures
  elsewhere, which is what lets the order index hold a direct iterator and cancel
  in O(1). Vector and deque iterators do not, forcing an O(n) scan per cancel.
- `std::unordered_map` **only** for the `order_id` index, which is probed by key
  and never iterated, so its implementation-defined order never reaches output.

---

## Bit-for-bit reproducibility

### What makes this hard

The failure modes are all quiet ones:

1. **Regenerated timestamps.** A capture writer that stamps "now" as it writes
   looks completely correct and silently destroys reproducibility. Arrival times
   must be *recorded*.
2. **Thread interleaving.** A sharded decoder's final state is deterministic,
   but the order it observed the world in is not. The receive path here is
   deliberately single-consumer — determinism was worth more than parallelism
   for this project. That is a real tradeoff, and the sibling
   [exchange-market-data-decoder](https://github.com/Manas103/exchange-market-data-decoder)
   makes the opposite one.
3. **Hash-map iteration order.** Excluded by construction; see above.
4. **Floating point.** There is none. Prices are integer ticks end to end.
5. **Empty levels.** A level that empties is erased, so "level present with zero
   quantity" and "level absent" cannot be two encodings of the same book.
6. **Transport framing.** Reproducing the *messages* is easier than reproducing
   the *datagrams*. The `pkt_first` column records where each datagram began, so
   the packet stream is recoverable too — without it the claim would have a hole
   exactly where the transport layer sits.

### How it is proved

`replay_verify` rebuilds the session three ways from the capture alone and
compares against digests the live run recorded:

| Check | What it would catch |
|---|---|
| Direct record replay → canonical book digest | a capture that silently normalized a field |
| Re-encode datagrams → SHA-256 of the framed stream | a format that lost transport structure |
| Re-encoded datagrams → real decoder → book digest | a decoder that mis-applied a message |

Any one alone would be weaker than it looks; together they pin down both the
format and the decode.

### Measured — clean session

`live_capture 2000000 /tmp/session.tcap 0 400000`

```
datagrams sent      : 43529          datagrams received : 43529
messages decoded    : 2,000,000      avg msgs/datagram  : 45.9
gap events          : 0              messages lost      : 0
malformed           : 0              resting orders     : 682,394

wire  sha256 : 69098fc2e83094f33c0b99658c3183a65b00e87ad7d24337caf31dbe5a5e294f
book  sha256 : 03f8f2eb4973220aed17f5360d54761327696a2f87b9d8b33c3ebb878f5fc0c3
```

`replay_verify /tmp/session.tcap`

```
direct replay vs transport path    MATCH
book vs live session digest        MATCH
wire vs live session digest        MATCH
gap events vs live session         MATCH
messages lost vs live session      MATCH
resting orders vs live session     MATCH

PASS: session reproduced bit for bit from the capture.
```

The wire digest covers all 43,529 datagrams and 57.5 MiB of framed bytes.

### Measured — session with real packet loss

A capture is only useful if it reproduces what actually happened, holes
included. With 2% of datagrams deliberately never sent:

`live_capture 1000000 /tmp/lossy.tcap 20 400000`

```
datagrams dropped   : 431 (injected)
messages decoded    : 980,231
gap events          : 415
messages lost       : 19,769
unknown order refs  : 10,713   (expected after a gap)
```

Replay reproduces that session exactly — same book digest, same wire digest,
**and the same 415 gaps and 19,769 lost messages**. The 10,713 dangling
references are the honest consequence of losing the Add that created an order:
its later Cancel arrives for an id the book never saw. A feed handler that threw
there would die on the first dropped datagram, so those are counted, not fatal.

---

## The capture format

Full byte-level specification: **[docs/FORMAT.md](docs/FORMAT.md)**. That
document is authoritative and is what the Python reader was written from.

```
[FileHeader 56B][ColumnSchema 40B x 11]
[Block 0][Block 1]...[Block 30]
[BlockIndex 16B x 31]
```

Each block carries a **directory** of `(encoding, payload_bytes)` per column
before the payloads, so column *j*'s offset is computable without reading any
other column. That is what makes a single-column read cheap in I/O and not just
in file size: a reader preads the 16-byte prefix, the directory, and one
payload.

### Encodings, and why columnar makes them possible

In a row-oriented file, adjacent bytes are a timestamp, then a type byte, then
an order id — neighbouring values have nothing in common. Group the same field
across a million ticks and the structure is obvious. The writer encodes every
column-block with all five codecs and keeps the smallest (the same
try-the-candidates approach Parquet and ORC take), so the result below is not
hand-tuned:

| Column | Codec chosen | Bytes | Bytes/record | Why |
|---|---|---:|---:|---|
| `order_id` | DELTA | 4,898,480 | 2.449 | ids issued in near-sequence |
| `exchange_ts_ns` | DOD | 3,860,466 | 1.930 | large but near-constant spacing |
| `price_ticks` | FOR | 3,386,460 | 1.693 | clustered in a band around a reference |
| `qty` | FOR | 3,004,024 | 1.502 | small values, no ordering |
| `seq_num` | DELTA | 2,000,060 | 1.000 | steps by exactly 1 |
| `instrument_id` | FOR | 2,000,031 | 1.000 | 128 values, one varint byte each |
| `side` | RAW | 2,000,000 | 1.000 | one byte already minimal |
| `msg_type` | RAW | 2,000,000 | 1.000 | one byte already minimal |
| `new_order_id` | RLE | 588,399 | 0.294 | zero except on Replace (5% of messages) |
| `capture_ts_ns` | RLE | 304,961 | 0.152 | one arrival time per *datagram* → runs of ~46 |
| `pkt_first` | RLE | 174,234 | 0.087 | one 1 every ~46 records |

`capture_ts_ns` collapsing to 0.152 bytes/record is the clearest illustration of
why layout matters: every message in a datagram shares an arrival timestamp, so
the column is long constant runs — structure that is invisible in a row layout
and free to exploit in a columnar one.

A detail worth knowing: for a sequence stepping by exactly 1, DELTA and DOD
produce **identical** sizes (5,000 bytes each on a 5,000-value test column), so
the tie goes to whichever is tried first. DOD earns its place on a different
shape — timestamps whose differences are large (~1,000 ns, two varint bytes) but
nearly constant, where it measures 7,328 bytes against DELTA's 10,007.

### Measured — size and scan cost

`capture_bench /tmp/session.tcap /tmp/session.rows`

The baseline is a packed row file holding **exactly the same fields at their
natural widths** — 57 bytes/record, no padding, no framing.

```
row-oriented (57 B/record) : 114,000,000 bytes (108.72 MiB)
columnar TCAP              :  24,221,331 bytes ( 23.10 MiB)
ratio                      : 4.71x smaller (12.11 bytes/record vs 57)
```

Single-column scan (min/max/sum of `price_ticks` across all 2M records) — **both
scans compute the same aggregate and the result is compared, because a benchmark
whose two sides disagree is measuring nothing**:

```
both scans agree: n=2000000 min=0 max=19992 sum=13897633155
columnar :   3,389,684 bytes touched, 0.0106 s
rows     : 114,000,000 bytes touched, 0.0347 s
-> 33.63x fewer bytes, 3.28x faster (warm cache)
```

The **33.63x** byte ratio is exact and reproduces identically every run. The
wall-clock ratio does not: across runs on this machine it moves between roughly
3.3x and 3.5x, which is ordinary timing noise on a non-isolated process. Quote
the byte figure if you want the deterministic one.

And the case where columnar **loses**, reported rather than omitted:

```
full-record scan (all 11 columns)
columnar :  24,220,339 bytes touched, 0.0490 s
rows     : 114,000,000 bytes touched, 0.0199 s (read only, no field extraction)
```

When every field is needed, columnar pays decode on all eleven columns and the
row store's straight sequential read wins on warm-cache time despite touching
4.7x more bytes. That is the honest shape of the tradeoff.

---

## Python: an independent implementation, not a binding

`python/tickcap/` shares no code with the C++ and is written from
`docs/FORMAT.md`. That independence is the point — a reader generated from the
writer would prove only that the code is self-consistent. Two implementations
built from a written spec agreeing byte for byte is evidence that the
specification is complete and the format is genuinely portable.

The Python L3 book is likewise a separate implementation with **different data
structures**: dict-of-dicts relying on Python's insertion-ordered dicts for FIFO
time priority, versus `std::map` + `std::list` in C++. It reaches the same
answers by a different route, which is what makes agreement meaningful.

`cross_check.py` compares three levels:

```
--- column digests (Python vs C++) ---
  capture_ts_ns    MATCH      order_id       MATCH
  exchange_ts_ns   MATCH      new_order_id   MATCH
  seq_num          MATCH      side           MATCH
  msg_type         MATCH      price_ticks    MATCH
  instrument_id    MATCH      qty            MATCH
                              pkt_first      MATCH

--- replaying 400000 records through the Python L3 book ---
  resting orders   python 139534 vs cpp 139534  MATCH
  top of book      128/128 instruments agree

PASS: the independent Python reader and book agree with the C++ decoder.
```

Decoding is vectorized with numpy rather than looped — a capture of 2M ticks
holds 22M encoded values, and a byte-at-a-time varint loop in CPython turns a
1.5-second read into minutes. The decoder computes each value's byte group and
position with array operations and sums the 7-bit payloads with
`np.add.reduceat`, keeping the whole thing in compiled code.

### Analytics

`analyze.py` loads a session into pandas and answers what a tick store is for:

```
--- columnar selectivity (measured) ---
read price_ticks only :    3.2 MiB  in 0.168s
read all 11 columns   :   23.1 MiB  in 1.499s
single-column read touches 14.0% of the payload

--- message mix ---
Add 1002046 (50.10%)   Cancel 300240 (15.01%)   Delete 299084 (14.95%)
Execute 298747 (14.94%)   Replace 99883 (4.99%)

--- feed rate (from recorded capture timestamps) ---
session wall-clock span : 5.000 s
mean message rate       : 400,010 msgs/sec
messages per 10ms       : p50=4087  p90=4134  p99=13278  max=13985
burstiness (max/median) : 3.4x

--- book depth across 128 two-sided instruments ---
price levels per side : median bid 47, median ask 47
orders at touch       : median bid 2, median ask 2
spread (ticks)        : median 13, min 6, max 18
```

The rate percentiles come from recorded wall-clock arrival times, so the tail
figures shift a little between runs (p90 moves around 4.1k–5.4k per 10 ms) —
the sender is paced to 400k msgs/sec and the burstiness is the scheduler's, not
the feed's. Everything derived from message content — the mix, the book depth,
the spreads — is identical on every run of the same session.

---

## Engineering findings

Three real problems found while building this, kept because the diagnosis is
the interesting part.

### 1. A 208 KB socket buffer, requested as 64 MB, granted silently

The first live runs lost **68% of datagrams**. The cause was not throughput:
`setsockopt(SO_RCVBUF)` does **not** fail when the request exceeds
`net.core.rmem_max` — it silently clamps. The program asked for 64 MiB, got
416 KiB, and received no error.

Same binary, same workload, one sysctl apart:

| `net.core.rmem_max` | Granted | Datagrams received | Messages | Gap events |
|---|---:|---|---:|---:|
| 212,992 (Ubuntu default) | 416 KiB | 13,851 / 43,529 (**68% lost**) | 635,917 | 258 |
| 67,108,864 (raised) | 128 MiB | **43,529 / 43,529 (0% lost)** | 2,000,000 | 0 |

The fix in the code is not the sysctl — it is that `set_recv_buffer()` now
returns what the kernel actually granted and `live_capture` prints it with the
command to raise the cap. A silent clamp became a visible one.

```
recv buffer    : requested 65536 KiB, granted 416 KiB  <-- CLAMPED by net.core.rmem_max
                 raise it with: sudo sysctl -w net.core.rmem_max=67108864
```

Tuning receive buffers is exactly what real multicast market-data receivers do,
so this is a real deployment concern rather than a harness artifact.

### 2. The books were persistently crossed, and the analytics found it

Reconstructed books showed a **median spread of −6 ticks** — bids above asks,
which no real venue can emit, because an order that would cross matches instead
of resting.

The decoder was innocent; it was faithfully reproducing an impossible feed. The
simulator placed orders 1 tick from a reference price that random-walked without
bound. Measured in isolation, that reference wandered **25 ticks** over a
session while the minimum half-spread was 1 tick, so orders resting from early
on ended up on the wrong side of newer ones.

The fix bounds the drift strictly below the half-spread
(`kMinHalfSpread=10 > kMaxDrift=8`), which makes crossing impossible by
construction. Spreads afterwards: **median 13 ticks, min 6, max 18**. The
invariant now has its own test so it cannot regress quietly.

### 3. A benchmark that disagreed with itself

`capture_bench` computes the same aggregate two ways and compares them. It
failed immediately: the row-oriented reader was reading `price_ticks` at byte 39
of each 57-byte record, when the field actually starts at byte 44
(8+8+8+1+2+8+8+1). The fix was not to correct the constant but to delete it —
row offsets are now derived from the same schema the writer uses
(`row_field_offset()`), so the two cannot drift apart. Without the cross-check
the benchmark would have reported a plausible-looking speedup computed from
garbage.

---

## Correctness

`correctness_test` — **600 checks, 0 failures**. Ordered so nothing is used as
evidence before it has been checked: SHA-256 is validated against the published
FIPS 180-4 vectors before any digest comparison is trusted, codecs are
round-tripped before the capture format is, and the format is round-tripped
before `replay_verify`'s result means anything.

Coverage includes:

- SHA-256 against four FIPS vectors, including the one-million-`a` case fed in
  awkward chunk sizes to exercise block-boundary handling.
- Every codec round-tripped on eight structured shapes plus forced round-trips
  of all five codecs on all physical types, including `u64` values chosen to
  make deltas overflow a signed 64-bit range (the codecs rely on defined
  wraparound).
- Wire format: round-trips, plus **actual byte assertions** for little-endian
  layout — a round-trip alone would pass even if the encoder wrote big-endian —
  plus rejection of truncated input, unknown message types, and declared lengths
  that disagree with the type.
- L3 book: queue position, partial execution shifting the queue, over-reduction
  clamped rather than going negative, level erased when its last order leaves.
- Replace losing time priority (it joins the back of the queue at its new
  price, as on a real exchange).
- Canonical digest **distinguishing two books with identical quantity at every
  price but different queue order** — the property an aggregated book cannot
  have — and being blind to history, so a level that filled and emptied digests
  identically to one that never existed.
- Gap detection against an independently computed expectation, including the two
  genuinely undetectable cases that must *not* be counted: loss before the first
  delivered datagram, and trailing loss with nothing after it to reveal it.
- The simulated feed never producing a crossed book.

---

## Building and running

Requires CMake ≥ 3.15, a C++17 compiler, and Linux. Tested with g++ 11.4 and
CMake 3.22.1 on Ubuntu 22.04 (WSL2).

```bash
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j

./correctness_test

# Raise the receive-buffer cap first, or expect heavy loss (see finding 1).
sudo sysctl -w net.core.rmem_max=67108864

# Live multicast session -> columnar capture. Args:
#   messages  capture_path  loss_permille  publish_rate(msgs/sec, 0 = unthrottled)
./live_capture 2000000 /tmp/session.tcap 0 400000

./replay_verify /tmp/session.tcap
./capture_bench /tmp/session.tcap /tmp/session.rows

# Session with 2% injected datagram loss
./live_capture 1000000 /tmp/lossy.tcap 20 400000
./replay_verify /tmp/lossy.tcap
```

Write captures to a native Linux path such as `/tmp`. Under WSL, writing to a
Windows-mounted path (`/mnt/c/...`) crosses the 9p filesystem boundary and the
I/O cost swamps what the benchmark is trying to measure.

Python side:

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cd python

# Emit the C++ view, then check the independent Python implementation against it
../build/capture_dump /tmp/session.tcap /tmp/tob.csv 400000 > /tmp/dump.txt
python cross_check.py /tmp/session.tcap /tmp/dump.txt /tmp/tob.csv 400000

python analyze.py /tmp/session.tcap 400000 0
```

---

## Throughput

Letting the publisher run unthrottled (`publish_rate = 0`) measures the receive
path's drain rate — recv → decode → L3 book maintenance → columnar capture →
SHA-256 over the wire stream, all on one thread:

```
messages decoded : 2,000,000 in 2.452 s  ->  815,678 msgs/sec
```

Honest caveat: with a 128 MiB receive buffer and a 57.5 MiB session, the buffer
absorbs the entire burst, so this measures how fast the receiver *processes*
messages, not proof that it could sustain that rate indefinitely against a live
wire. The paced runs are the ones that demonstrate lossless end-to-end
operation.

---

## Portability

POSIX-only, deliberately. The sibling
[exchange-market-data-decoder](https://github.com/Manas103/exchange-market-data-decoder)
is Windows/Winsock-only and lists that as a limitation; this project takes the
other platform and makes the multicast path real on Linux — genuine
`IP_ADD_MEMBERSHIP` group membership rather than unicast wearing a multicast
address.

Two things would need abstracting for a portable build: `multicast_posix.hpp`
(Winsock differs mainly in initialization and in taking `const char*` for
`setsockopt`) and `CaptureReader`'s use of `pread` (Windows needs
`OVERLAPPED`-based `ReadFile`). Everything else — protocol, codecs, book,
canonical form, SHA-256 — is portable standard C++ with no endianness or layout
assumptions.

---

## Extended (Sep. 2026): Equity Exchange Connectivity Gateway (feed handler, gap recovery, order entry)

**Not a real exchange protocol.** A and B line arbitration, the TCP
retransmit path, and the OUCH-style order-entry session below are all this
project's own small implementations of the *ideas* behind Nasdaq's
redundant-multicast-plus-SoupBinTCP/GLIMPSE/OUCH architecture, not the real
wire formats. The base decoder above already establishes that framing for
the feed itself; this extension carries it into the two things a receive-only
decoder cannot do: recover from a real outage, and send anything back.

**What existed before this extension and what is new.** Before this, the
repo decoded one multicast line and never recovered from loss beyond what
`live_capture`'s own loss-injection test already covered (see "Measured —
session with real packet loss" above, which accepts loss rather than
recovering it). Three things are new: A/B line arbitration
(`include/line_arbitrator.hpp`) so a drop on one line is silently filled
from its redundant twin; TCP gap recovery (`include/gap_recovery.hpp`) for
the rarer case where *both* lines drop the same range, modeled loosely on
SoupBinTCP/GLIMPSE; and an OUCH-style order-entry session
(`include/ouch_session.hpp`) with sequenced, logged, resend-on-reconnect
delivery, none of which the base repo had any reason to include (it only
ever received).

### Architecture (additions)

```
include/tcp_io.hpp          Minimal POSIX TCP helpers (listen/accept/connect,
                             exact-length read_all/write_all) shared by gap
                             recovery and order entry. Ignores SIGPIPE once,
                             process-wide (see Findings).
include/line_arbitrator.hpp LineArbitrator: tracks the next sequence number
                             needed, takes whichever of line A or B delivers
                             it first, drops the other line's copy as a
                             duplicate, and calls gap recovery only when
                             BOTH lines have moved past the needed sequence
                             with neither having delivered it.
include/gap_recovery.hpp    RetransmitBuffer (append-only log of every sent
                             record), GapRecoveryServer (one thread per TCP
                             connection, answers [start_seq, start_seq+count)
                             requests from the buffer), GapRecoveryClient
                             (one fresh connection per gap; gaps are rare, so
                             this is the honest cost model rather than a
                             pre-warmed connection).
include/ouch_session.hpp    OuchServer/OuchClient: a session-sequenced,
                             length-prefixed frame protocol for
                             Enter/Cancel/Replace order and their Accepted/
                             Canceled/Replaced acks, with a server-side
                             outbound log so a reconnecting client's
                             LoginRequest(seq) resumes exactly where it left
                             off.
src/live_capture_ab.cpp     End-to-end demo/benchmark: two real multicast
                             lines, a precomputed seeded-loss schedule
                             (exact counts, not per-datagram probabilities),
                             the arbitrator and gap-recovery service wired
                             together, and a byte-for-byte digest comparison
                             between an oracle book (direct apply, no
                             transport) and the reconstructed one.
src/replay_bench.cpp        Pinned-core replay throughput: decode-and-apply
                             a capture file with no network involved at all.
tests/ouch_session_test.cpp Resend-on-reconnect correctness (a client
                             disconnects mid-stream, a fresh client resumes
                             from the right sequence, every sequence number
                             1..20 present exactly once across both
                             connections) and tick-to-order latency.
```

### Validation

1. **Book divergence check.** `live_capture_ab` builds the same deterministic
   2,000,000-message session two ways: an oracle book fed every message
   directly (no transport at all), and a "live" book reconstructed purely
   from what arrived over two real UDP multicast sockets with a precomputed
   loss schedule, arbitrated and gap-recovered. The two books' canonical
   SHA-256 digests are compared. Both digests, and both `resting orders`
   counts, are printed in the raw output so a mismatch would be visible, not
   just a boolean (`docs/live_capture_ab_output.txt`).
2. **Resend-on-reconnect correctness.** `ouch_session_test` pipelines 20
   orders, reads only the first 12 acks, disconnects (a legitimate model of
   "the exchange already sent responses 13-20, this client never received
   them"), reconnects requesting resume from sequence 13, and checks the
   union of both connections' sequence numbers is exactly {1..20}, each
   appearing once (`docs/ouch_session_test_output.txt`).
3. **ASan+UBSan.** `correctness_test`, `ouch_session_test`, and a
   scaled-down `live_capture_ab` run all clean under
   `-fsanitize=address,undefined`
   (`docs/correctness_test_san.txt`, `docs/ouch_session_test_san.txt`,
   `docs/live_capture_ab_san.txt`).

### Findings

**A process-wide SIGPIPE kill, found by the resend-on-reconnect test itself
exercising the exact scenario it exists to recover from.** The first run of
`ouch_session_test` produced zero output and exit code 0 -- looking like a
no-op, not a failure. `strace -f` showed what actually happened
(`docs/ouch_session_sigpipe_repro.txt`): after the test client disconnects
mid-stream, the server's handler thread is still mid-way through writing its
queued replies to that now-closed socket; `write()` to a socket whose peer
already closed its read side raises SIGPIPE, and SIGPIPE's default
disposition kills the *entire process*, every thread, not just the writer.
`write_all()` (`tcp_io.hpp`) already checks its return value and treats a
failed write as a clean session drop, which is exactly correct -- the bug
was that the process never survived long enough to see that return value.
The fix is one line, made once, process-wide:
`std::signal(SIGPIPE, SIG_IGN)`, called from `tcp_listen()`/`tcp_connect()`
so every executable that uses `tcp_io.hpp` gets it automatically. This is
precisely the kind of bug a disconnect-and-resume test is supposed to catch,
and it did, immediately, on the first genuine attempt at that scenario.

**Tick-to-order latency took three genuine attempts to characterize
correctly, and the honest number is the third one, not the first.** Attempt
1 alternated send-then-block-on-recv per iteration: p99 46.5us, target
missed. Attempt 2 moved ack-draining to a concurrent thread to avoid the
per-iteration block: p99 46.5 -> 56.9us, *worse* -- cross-thread scheduling
contention on a shared, busy machine cost more than the alternation it
replaced. Attempt 3 diagnosed the actual problem: with a default-sized
socket buffer and a per-iteration blocking read (attempt 1) or a contending
reader thread (attempt 2), the timed `send()` call was being charged for
scheduler wake-up latency that had nothing to do with the cost of building
and sending an OUCH frame. Growing both socket buffers to 8 MiB, running the
loop as pure back-to-back sends with nothing else touching the socket, and
draining all 20,000 acks afterward (untimed) isolated the number the claim
is actually about: p99 8.2us. This is reported as the measured result
because it is the version of the experiment that actually measures what the
claim describes, not because it is the smallest number of the three --
attempts 1 and 2 are disclosed in the source comments and here rather than
discarded.

**Seeded double-line drops and gap events are not the same count, and
conflating them nearly produced a false failure.** The seeded-loss schedule
marks exactly 5,000 datagrams as dropped on both lines, but adjacent seeded
drops (common at roughly 11% datagram-loss density across 43,528 datagrams)
merge into one contiguous gap when the arbitrator notices, so `gap_events`
(4,230-4,265 across runs) is consistently lower than 5,000 even on a fully
correct recovery. An early version of the pass/fail check compared
`gap_events == 5000` directly and reported `false` on a run that had, in
fact, recovered every single message with zero book divergence. The check
now compares `messages_unrecovered == 0` and the digest match instead, which
is what the claim is actually about, and states the merging behavior in the
program's own output rather than leaving it to look like a discrepancy.

### Measured results

Machine: AMD Ryzen 7 7800X3D, WSL2 Ubuntu 22.04 (12 logical cores visible to
WSL under its `.wslconfig` cap; 8 physical / 16 logical on the host), Windows
11 build 10.0.26200, g++ 11.4.0 with `-O2`, CMake 3.22.1. Raw output in
`docs/`.

| Claim | Target | Measured | Meets claim |
|---|---|---|---|
| Pinned-core replay throughput | 1M+ msgs/sec sustained, one core | **6.87M msgs/sec best of 5 passes** (median 6.21M), `sched_setaffinity` pinned to core 0, 2,000,000-record capture, decode+apply only, no network | Yes |
| Seeded double-line drops recovered | all 5,000, zero book divergence | **5,000/5,000 seeded double-line-drop datagrams** (308,648 messages inside those gaps), **0 unrecovered**, oracle and live book SHA-256 digests **identical** | Yes |
| Tick-to-order latency | p99 < 10us on loopback | **p99 8.2us** (mean 3.77us, p50 4.21us, p90 5.90us), third genuine attempt (see Findings) | Yes |
| A/B line arbitration with sequence-gap recovery over a TCP retransmit path | implemented and correct | Implemented (`line_arbitrator.hpp` + `gap_recovery.hpp`); validated by the book-divergence check above | Yes |
| OUCH-style order-entry session with sequenced resend on reconnect | implemented and correct | Implemented (`ouch_session.hpp`); 20/20 sequence numbers recovered exactly once across a disconnect-and-resume, see Findings for the SIGPIPE bug this test caught | Yes |

The pinned-core replay number and the earlier "815,678 msgs/sec drain"
number (Throughput, above) measure different things and are not comparable:
the drain number is a live UDP receiver (`recvfrom()` + decode + columnar
write, real socket syscalls, real sender pacing); the replay number removes
the network entirely and measures only decode-and-apply cost against an
in-memory record vector already loaded from a capture file. That the replay
number is roughly 7-8x higher is exactly what removing socket I/O from the
hot path should do, not a discrepancy between the two measurements.

### Building and running

```bash
# WSL2 Ubuntu 22.04, g++ 11.4
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j"$(( $(nproc) / 2 ))" live_capture_ab replay_bench ouch_session_test correctness_test

./ouch_session_test                                   # resend-on-reconnect + tick-to-order latency
./live_capture_ab 2000000 /tmp/session_ab.tcap 5000 3000 3000 400000  # A/B + gap recovery, 5000 seeded double-drops
taskset -c 0 ./replay_bench /tmp/session_ab.tcap 5     # pinned-core replay throughput, 5 passes
```

ASan+UBSan build (found the SIGPIPE bug above):

```bash
mkdir -p ~/build/mdtc-san && cd ~/build/mdtc-san
cmake /path/to/market-data-tick-capture \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_CXX_FLAGS="-fsanitize=address,undefined -fno-omit-frame-pointer -g -O1" \
  -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=address,undefined"
make -j"$(( $(nproc) / 2 ))" correctness_test ouch_session_test live_capture_ab
./correctness_test && ./ouch_session_test
./live_capture_ab 200000 /tmp/session_ab_san.tcap 500 300 300 100000   # scaled down; ASan is slow
```

## Extended (Oct. 2026): Typed Predicate Query Path

**What existed before this extension and what is new.** Before this, the
capture format could be read back only column-by-column
(`CaptureReader::read_block_column`) or wholesale; nothing answered a
question like "how many Execute messages hit instrument 5" without either
writing a one-off loop against the C++ reader or decoding every column by
hand in Python. Three things are new: `tcap_query` (a typed predicate
counter that decodes only the two columns a predicate needs across every
block), `tcap_to_pcap` (re-encodes a capture's datagrams into a standard
pcap so the exact same capture can be queried the other way), and
`python/tshark_grep_baseline.py` (the realistic baseline for a protocol
tshark has no dissector for: extract raw bytes with tshark, decode and
filter them in a script).

### Architecture (additions)

```
src/tcap_query.cpp   Typed predicate path: decodes only msg_type and
                     instrument_id per block (2 of 11 columns), counts
                     matches, reports elapsed time.
src/tcap_to_pcap.cpp Re-encodes a capture's datagrams (same code path
                     replay_verify already proves byte-for-byte) into a
                     standard pcap: synthetic Ethernet/IPv4/UDP framing
                     around the real, re-encoded MoldUDP64-style payload,
                     with the real recorded capture_ts_ns as the pcap
                     timestamp.
python/tshark_grep_baseline.py   tshark extracts udp.payload, a Python
                     loop decodes the same variable-length message framing
                     tcap_query already knows how to skip past, and counts
                     matches -- the "grep" in the baseline's name is this
                     script, not a literal grep(1) invocation, because the
                     protocol is custom and tshark cannot dissect it.
python/validate_typed_query.py   cross-checks tcap_query against the
                     tshark-plus-grep baseline on 6 different predicates
                     over the same capture, asserting identical
                     scanned/matched counts from two independently-coded
                     paths reading two independently-generated files.
```

### Validation

`validate_typed_query.py` runs 6 (instrument_id, msg_type) predicates
through both paths on the same session and asserts exact agreement:

```
instrument=   5 type=A  cpp(scanned=4011877, matched=15715)  python(scanned=4011877, matched=15715)  MATCH
instrument=   5 type=E  cpp(scanned=4011877, matched=4631)   python(scanned=4011877, matched=4631)   MATCH
instrument=  17 type=A  cpp(scanned=4011877, matched=15787)  python(scanned=4011877, matched=15787)  MATCH
instrument=  42 type=X  cpp(scanned=4011877, matched=4735)   python(scanned=4011877, matched=4735)   MATCH
instrument=  90 type=D  cpp(scanned=4011877, matched=4581)   python(scanned=4011877, matched=4581)   MATCH
instrument=   1 type=U  cpp(scanned=4011877, matched=1599)   python(scanned=4011877, matched=1599)   MATCH

PASS: typed query path and tshark-plus-grep baseline agree on all 6 predicates.
```

### Findings

**The first cross-check attempt disagreed on every predicate by a fixed
amount, which pointed straight at the root cause rather than at either
decoder.** `tcap_query` reported 4,011,877 messages scanned on every one of
6 different predicates (correct: the scanned count does not depend on the
predicate); the first version of the tshark baseline reported a few
thousand fewer, also on every predicate, the same gap every time. A
constant gap independent of the predicate meant the two paths disagreed
about the *total* message count, not about matching logic. The cause:
`-e data` reads tshark's generic "Data" dissector field, which is only
populated when no other dissector claims the packet first; about 0.1% of
this project's synthetic payloads happen to start with bytes that
coincidentally match some other protocol's heuristic signature, so a
different dissector claimed them and the "data" field came back empty for
those packets, with no messages actually lost on the wire or in either
file. Switching to `-e udp.payload` (the raw bytes at the UDP layer itself,
populated for every UDP packet regardless of which subdissector runs
afterward) fixed it; the agreement above is from that version.

**The measured speedup (about 318x) overstates the steady-state
advantage, because tshark's fixed per-invocation startup cost dominates at
this capture's size, and that is disclosed rather than hidden behind a
single flattering number.** Three runs each, same predicate, same
session (4,011,877 messages, 46.7 MiB capture): `tcap_query` measured
21.1-22.8ms; `tshark_grep_baseline.py` measured 6,824-6,896ms. Spawning
tshark and loading its full dissector table is itself on the order of
hundreds of milliseconds to a few seconds before it reads a single packet,
which is a large fraction of a ~6.9 second run on a file this size; a
capture two or three orders of magnitude larger would amortize that fixed
cost over far more bytes and should narrow the ratio toward the
cost actually being measured, that of the slower, byte-oriented decode
path versus the typed columnar one, rather than the ratio reported here.

### Measured results

Machine: AMD Ryzen 7 7800X3D, WSL2 Ubuntu 22.04, g++ 11.4.0 `-O2`, tshark
3.6.2. Raw output in `docs/benchmark_output.txt`.

| Claim | Target | Measured | Met |
|---|---|---|---|
| 815,000+ messages/sec | 815,000+ | unchanged from the base repo (815,678 msgs/sec live drain) | Yes |
| Nanosecond timestamps | implemented | unchanged (`exchange_ts_ns`, `capture_ts_ns`) | Yes |
| Capture volume | 120 GB | **46.7 MiB** (4,011,877 messages; see note) | No |
| Typed query vs. tshark-plus-grep on the same capture | 25x faster | **about 318x** (21.1-22.8ms vs. 6,824-6,896ms, 3 runs each), overstated by tshark's fixed startup cost at this size (see Findings) | Yes (but see caveat) |

**The 120 GB capture target was not reached, and the honest reason is a
time and disk budget on a shared machine, not a format limit.** Three
genuine attempts were made at increasing scale: a 200,000,000-message
unthrottled capture was started and had to be abandoned mid-run when the
work session restarted; a 20,000,000-message retry lost 11,843,888
messages to receive-buffer overflow at that burst size even with
`net.core.rmem_max` raised to 128 MiB, which this project's own base
README already documents as a real failure mode at high enough unthrottled
burst sizes; the reported 4,011,877-message, 46.7 MiB capture is the
largest *complete, loss-accounted-for* run that fit in the time actually
available. At this capture's measured 12.21 bytes/record, 120 GB would be
roughly 9.8 billion records, which would take on the order of hours of
single-threaded decode throughput alone at this project's own measured
rate, well beyond what a shared, in-use machine affords in one sitting.
The query-path speedup itself does not depend on the capture being 120 GB;
it is the same two decoders reading the same bytes either way.

### Building and running

```bash
# from build/, after the base repo's cmake/make
sudo sysctl -w net.core.rmem_max=134217728
./live_capture 5000000 /tmp/session.tcap 0 0
./tcap_to_pcap /tmp/session.tcap /tmp/session.pcap

# cross-validate the two query paths agree
cd ../python
python3 validate_typed_query.py ../build/tcap_query /tmp/session.tcap /tmp/session.pcap

# the comparison itself
../build/tcap_query /tmp/session.tcap 5 A
time python3 tshark_grep_baseline.py /tmp/session.pcap 5 A
```

## Limitations

- Typed-query extension: the capture is 46.7 MiB, not 120 GB (see that
  section's Findings for the three-attempt account); the measured 318x
  query speedup is overstated by tshark's fixed per-invocation startup
  cost at this size, also disclosed there rather than reported as a clean
  number.
- Simplified ITCH-style protocol and MoldUDP64-style framing; not the real
  specs.
- Synthetic order flow, not captured exchange data.
- Single-host multicast; no cross-host routing exercised.
- The receive path is single-threaded by design (determinism over throughput).
  Parallel decode with deterministic capture is possible — capture pre-shard,
  reassemble by sequence number on replay — but was not built here.
- The simulator generates order *events* without matching semantics. Crossing is
  prevented by construction (finding 2) rather than by orders trading against
  each other, so there is no trade-price series to analyse.
- TCAP has no per-block checksums, no null/missing-value support, no schema
  evolution, and no nested types. A production format would want at least the
  first of those; the reproducibility checks here hash the whole file's
  reconstructed content instead.
- The codecs are integer-oriented. A column of genuinely high-entropy 64-bit
  values falls back to RAW, which is correct but means TCAP is not a general
  compressor — it exploits the specific structure of tick data.
- No kernel-bypass networking. At the rates a real feed handler runs, socket
  syscalls dominate and production systems reach for DPDK or similar; that is
  out of scope here and named rather than glossed over.
- The gateway extension's A/B line arbitration assumes a single logical
  consumer (both receive threads call into `LineArbitrator` under one mutex;
  see the threading note in `line_arbitrator.hpp`), not a lock-free or
  per-core design.
- Gap recovery's retransmit buffer retains the whole session in the demo
  binaries rather than a bounded, production-sized retention window;
  `RetransmitBuffer` supports eviction (`retention_floor()`) but the demos
  do not exercise it.
- The OUCH-style session is one session at a time with no authentication, no
  order rejection (every EnterOrder is accepted), and no
  Intermarket-Sweep-Order or self-help exceptions; it is the sequencing and
  resend mechanics, not a matching engine or a compliance-aware gateway.
- Tick-to-order latency measures the client-side cost of building an OUCH
  frame and handing it to the kernel's socket send buffer, not a full
  order-to-acknowledgment round trip and not any market-data decode time;
  see Findings for exactly what the three measurement attempts isolated.
