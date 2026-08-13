// Correctness checks for every layer the reproducibility claim rests on.
//
// The ordering here is deliberate: nothing is used as evidence before it has
// been checked. SHA-256 is validated against the published FIPS vectors
// before any digest comparison is trusted, the codecs are round-tripped
// before the capture format is trusted, and the capture format is
// round-tripped before replay_verify's result means anything.
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <random>
#include "sha256.hpp"
#include "le_bytes.hpp"
#include "column_codec.hpp"
#include "wire_protocol.hpp"
#include "order_book_l3.hpp"
#include "book_universe.hpp"
#include "canonical.hpp"
#include "tick_capture.hpp"
#include "feed_decoder.hpp"
#include "market_simulator.hpp"

using namespace tickcap;

static int g_checks = 0;
static int g_failures = 0;

#define CHECK(cond)                                                            \
    do {                                                                       \
        ++g_checks;                                                            \
        if (!(cond)) {                                                         \
            ++g_failures;                                                      \
            std::printf("  FAIL %s:%d  %s\n", __FILE__, __LINE__, #cond);      \
        }                                                                      \
    } while (0)

#define CHECK_EQ_STR(a, b)                                                     \
    do {                                                                       \
        ++g_checks;                                                            \
        if ((a) != (b)) {                                                      \
            ++g_failures;                                                      \
            std::printf("  FAIL %s:%d\n    got      %s\n    expected %s\n",    \
                        __FILE__, __LINE__, std::string(a).c_str(),            \
                        std::string(b).c_str());                               \
        }                                                                      \
    } while (0)

// ------------------------------------------------------------------ sha256

static void test_sha256() {
    std::printf("sha256 (FIPS 180-4 vectors)\n");
    CHECK_EQ_STR(Sha256::hex_of("", 0),
                 "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    CHECK_EQ_STR(Sha256::hex_of("abc", 3),
                 "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    const char* v3 = "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq";
    CHECK_EQ_STR(Sha256::hex_of(v3, std::strlen(v3)),
                 "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1");

    // One million 'a', fed in awkward chunk sizes so the streaming buffer's
    // block-boundary handling is exercised rather than just the fast path.
    Sha256 h;
    std::string chunk(1000, 'a');
    for (int i = 0; i < 1000; ++i) h.update(chunk.data(), chunk.size());
    CHECK_EQ_STR(h.hex(),
                 "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0");

    Sha256 h2;
    for (int i = 0; i < 1000000; ++i) h2.update("a", 1);
    CHECK_EQ_STR(h2.hex(),
                 "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0");
}

// ------------------------------------------------------------------ varint

static void test_varint() {
    std::printf("varint / zigzag\n");
    std::vector<uint64_t> vals = {0, 1, 2, 127, 128, 129, 300, 65535, 1u << 20,
                                  0x7FFFFFFFFFFFFFFFull, 0xFFFFFFFFFFFFFFFFull};
    for (uint64_t v : vals) {
        std::vector<uint8_t> b;
        put_uvarint(b, v);
        ByteReader r(b.data(), b.size());
        CHECK(r.uvarint() == v);
        CHECK(r.ok());
    }
    std::vector<int64_t> svals = {0, -1, 1, -2, 2, -1000, 1000,
                                  std::numeric_limits<int64_t>::min(),
                                  std::numeric_limits<int64_t>::max()};
    for (int64_t v : svals) {
        std::vector<uint8_t> b;
        put_svarint(b, v);
        ByteReader r(b.data(), b.size());
        CHECK(r.svarint() == v);
        CHECK(r.ok());
    }
    // Small negative numbers must stay cheap -- that is the entire reason
    // zigzag exists, and if it regressed the delta codecs would silently
    // become worse than raw.
    std::vector<uint8_t> b;
    put_svarint(b, -1);
    CHECK(b.size() == 1);

    // A reader must refuse to run off the end rather than fabricate values.
    uint8_t tiny[2] = {0x01, 0x02};
    ByteReader tr(tiny, 2);
    tr.u64();
    CHECK(!tr.ok());
}

// ------------------------------------------------------------------ codecs

static void roundtrip_column(const std::vector<uint64_t>& vals, PhysType t, const char* label) {
    std::vector<uint8_t> bytes;
    Encoding e = encode_column_best(vals, t, bytes);
    ByteReader r(bytes.data(), bytes.size());
    std::vector<uint64_t> back;
    bool ok = decode_column(r, e, t, vals.size(), back);
    ++g_checks;
    if (!ok || back != vals) {
        ++g_failures;
        std::printf("  FAIL codec roundtrip [%s] enc=%s n=%zu\n",
                    label, encoding_name(e), vals.size());
        for (size_t i = 0; i < vals.size() && i < 8; ++i)
            std::printf("      [%zu] in=%llu out=%llu\n", i,
                        (unsigned long long)vals[i],
                        i < back.size() ? (unsigned long long)back[i] : 0ull);
    }
}

// Forces one specific encoding, so a codec that is never selected by the
// "smallest wins" rule still gets round-trip coverage.
static void roundtrip_forced(const std::vector<uint64_t>& vals, PhysType t, Encoding e) {
    bool sgn = phys_is_signed(t);
    std::vector<uint8_t> bytes;
    switch (e) {
        case Encoding::Raw:   encode_raw(vals, t, bytes); break;
        case Encoding::Delta: encode_delta(vals, sgn, bytes); break;
        case Encoding::Dod:   encode_dod(vals, sgn, bytes); break;
        case Encoding::For:   encode_for(vals, sgn, bytes); break;
        case Encoding::Rle:   encode_rle(vals, sgn, bytes); break;
    }
    ByteReader r(bytes.data(), bytes.size());
    std::vector<uint64_t> back;
    bool ok = decode_column(r, e, t, vals.size(), back);
    ++g_checks;
    if (!ok || back != vals) {
        ++g_failures;
        std::printf("  FAIL forced codec %s roundtrip (n=%zu)\n", encoding_name(e), vals.size());
    }
}

static void test_codecs() {
    std::printf("column codecs\n");
    std::mt19937_64 rng(12345);

    // Structured shapes that mirror the real columns.
    std::vector<uint64_t> monotonic, jittered, banded, lowcard, highentropy, allsame, single, empty;
    uint64_t t = 1'700'000'000'000'000'000ull;
    for (int i = 0; i < 5000; ++i) {
        monotonic.push_back(static_cast<uint64_t>(i));
        t += 900 + (rng() % 200);
        jittered.push_back(t);
        banded.push_back(static_cast<uint64_t>(static_cast<int64_t>(10000 + (rng() % 80) - 40)));
        lowcard.push_back(rng() % 2 ? 'B' : 'S');
        highentropy.push_back(rng());
        allsame.push_back(42);
    }
    single.push_back(999);

    roundtrip_column(empty,       PhysType::U64, "empty");
    roundtrip_column(single,      PhysType::U64, "single");
    roundtrip_column(monotonic,   PhysType::U64, "monotonic");
    roundtrip_column(jittered,    PhysType::U64, "jittered-timestamps");
    roundtrip_column(banded,      PhysType::I64, "banded-prices");
    roundtrip_column(lowcard,     PhysType::U8,  "low-cardinality");
    roundtrip_column(highentropy, PhysType::U64, "high-entropy");
    roundtrip_column(allsame,     PhysType::U64, "constant");

    // Negative and boundary values through the signed path.
    std::vector<uint64_t> negatives;
    for (int i = -2000; i < 2000; ++i) negatives.push_back(static_cast<uint64_t>(static_cast<int64_t>(i)));
    negatives.push_back(static_cast<uint64_t>(std::numeric_limits<int64_t>::min()));
    negatives.push_back(static_cast<uint64_t>(std::numeric_limits<int64_t>::max()));
    roundtrip_column(negatives, PhysType::I64, "signed-range");

    // Deltas between arbitrary uint64 values overflow int64; the codecs rely
    // on defined wraparound to round-trip anyway. Check that directly.
    std::vector<uint64_t> extremes = {0, 0xFFFFFFFFFFFFFFFFull, 1, 0x8000000000000000ull, 7};
    roundtrip_column(extremes, PhysType::U64, "wraparound-extremes");
    for (Encoding e : {Encoding::Raw, Encoding::Delta, Encoding::Dod, Encoding::For, Encoding::Rle}) {
        roundtrip_forced(extremes,  PhysType::U64, e);
        roundtrip_forced(monotonic, PhysType::U64, e);
        roundtrip_forced(negatives, PhysType::I64, e);
        roundtrip_forced(lowcard,   PhysType::U8,  e);
    }

    // Narrow types must survive their own width.
    std::vector<uint64_t> u8vals, u16vals, u32vals;
    for (int i = 0; i < 1000; ++i) {
        u8vals.push_back(rng() & 0xFF);
        u16vals.push_back(rng() & 0xFFFF);
        u32vals.push_back(rng() & 0xFFFFFFFF);
    }
    roundtrip_column(u8vals,  PhysType::U8,  "u8");
    roundtrip_column(u16vals, PhysType::U16, "u16");
    roundtrip_column(u32vals, PhysType::U32, "u32");

    // The "pick the smallest" rule must actually pick the right thing on
    // shapes where the answer is obvious, otherwise the compression numbers
    // are luck.
    {
        std::vector<uint8_t> b;
        Encoding e = encode_column_best(allsame, PhysType::U64, b);
        CHECK(e == Encoding::Rle);
        CHECK(b.size() < 32);
    }
    {
        // A sequence stepping by exactly 1 costs the SAME under delta and
        // delta-of-delta: delta writes n-1 one-byte differences, dod writes
        // one first difference plus n-2 one-byte zeros. Measured on this
        // input both are 5000 bytes, so the tie goes to whichever is tried
        // first. Asserting "dod wins here" would be asserting a tie-break,
        // not a property. What is actually guaranteed is that one of the two
        // differencing codecs wins and that it crushes raw 8-byte storage.
        std::vector<uint8_t> b;
        Encoding e = encode_column_best(monotonic, PhysType::U64, b);
        CHECK(e == Encoding::Delta || e == Encoding::Dod);
        CHECK(b.size() <= monotonic.size() + 8);          // ~1 byte/value
        CHECK(b.size() * 7 < monotonic.size() * 8);       // vs 8 bytes/value raw
    }
    {
        // Delta-of-delta earns its place on a different shape: timestamps
        // whose successive differences are LARGE (~1000 ns, two varint bytes
        // each) but nearly constant, so the second difference is small.
        // Measured: DELTA 10007 bytes, DOD 7328 bytes.
        std::vector<uint8_t> b;
        Encoding e = encode_column_best(jittered, PhysType::U64, b);
        CHECK(e == Encoding::Dod);
        std::vector<uint8_t> d;
        encode_delta(jittered, false, d);
        CHECK(b.size() < d.size());
    }
    {
        std::vector<uint8_t> b;
        encode_column_best(highentropy, PhysType::U64, b);
        // Random 64-bit values cannot be compressed; RAW must win, and the
        // result must not be larger than raw.
        CHECK(b.size() <= highentropy.size() * 8);
    }
}

// ------------------------------------------------------------ wire protocol

static void test_wire_protocol() {
    std::printf("wire protocol\n");
    std::vector<DecodedMsg> msgs;
    DecodedMsg a; a.type = MsgType::AddOrder; a.exchange_ts_ns = 111; a.instrument_id = 7;
    a.order_id = 900; a.side = Side::Buy; a.price_ticks = -12345; a.qty = 88;
    DecodedMsg e; e.type = MsgType::Execute; e.exchange_ts_ns = 112; e.instrument_id = 7;
    e.order_id = 900; e.qty = 10;
    DecodedMsg x; x.type = MsgType::Cancel; x.exchange_ts_ns = 113; x.instrument_id = 7;
    x.order_id = 900; x.qty = 5;
    DecodedMsg d; d.type = MsgType::Delete; d.exchange_ts_ns = 114; d.instrument_id = 7; d.order_id = 900;
    DecodedMsg u; u.type = MsgType::Replace; u.exchange_ts_ns = 115; u.instrument_id = 7;
    u.order_id = 900; u.new_order_id = 901; u.price_ticks = 555; u.qty = 44;
    msgs = {a, e, x, d, u};

    for (const DecodedMsg& m : msgs) {
        std::vector<uint8_t> b;
        encode_message(b, m);
        CHECK(b.size() == 2 + message_size(m.type));
        ByteReader r(b.data(), b.size());
        DecodedMsg back;
        CHECK(decode_message(r, back));
        CHECK(back.type == m.type);
        CHECK(back.exchange_ts_ns == m.exchange_ts_ns);
        CHECK(back.instrument_id == m.instrument_id);
        CHECK(back.order_id == m.order_id);
        CHECK(back.new_order_id == m.new_order_id);
        CHECK(back.price_ticks == m.price_ticks);
        CHECK(back.qty == m.qty);
        if (m.type == MsgType::AddOrder) CHECK(back.side == m.side);
    }

    // Explicit little-endian: the layout is part of the format contract, so
    // check actual bytes rather than only that a round-trip works. A
    // round-trip alone would pass even if the encoder wrote big-endian.
    {
        DecodedMsg m; m.type = MsgType::Delete; m.exchange_ts_ns = 0x0102030405060708ull;
        m.instrument_id = 0x1234; m.order_id = 1;
        std::vector<uint8_t> b;
        encode_message(b, m);
        CHECK(b[0] == static_cast<uint8_t>(kDeleteSize) && b[1] == 0);  // length LE
        CHECK(b[2] == 'D');
        CHECK(b[3] == 0x08 && b[10] == 0x01);                            // ts LE
        CHECK(b[11] == 0x34 && b[12] == 0x12);                           // instrument LE
    }

    // Truncated and malformed input must be rejected, not misparsed.
    {
        std::vector<uint8_t> b;
        encode_message(b, a);
        b.resize(b.size() - 3);
        ByteReader r(b.data(), b.size());
        DecodedMsg back;
        CHECK(!decode_message(r, back));
    }
    {
        std::vector<uint8_t> b;
        encode_message(b, a);
        b[2] = 'Z';  // unknown type
        ByteReader r(b.data(), b.size());
        DecodedMsg back;
        CHECK(!decode_message(r, back));
    }
    {
        std::vector<uint8_t> b;
        encode_message(b, a);
        b[0] = 99;   // declared length disagrees with the type
        ByteReader r(b.data(), b.size());
        DecodedMsg back;
        CHECK(!decode_message(r, back));
    }

    // Packet framing round-trip.
    {
        auto sess = make_session_id("TESTSESS01");
        PacketBuilder pb(sess, 200);
        MarketSimulator sim(42);
        std::vector<DecodedMsg> gen;
        for (int i = 0; i < 100; ++i) gen.push_back(sim.next());
        for (size_t i = 0; i < gen.size(); ++i) pb.add(gen[i], 1 + i);
        pb.flush();
        CHECK(pb.packets().size() > 1);   // 200-byte cap must force several

        size_t idx = 0;
        for (const auto& pkt : pb.packets()) {
            ByteReader r(pkt.data(), pkt.size());
            PacketHeader h;
            CHECK(decode_packet_header(r, h));
            CHECK(h.session == sess);
            CHECK(h.first_seq == 1 + idx);
            CHECK(pkt.size() <= 200);
            for (uint16_t k = 0; k < h.msg_count; ++k) {
                DecodedMsg back;
                CHECK(decode_message(r, back));
                CHECK(back.type == gen[idx].type);
                CHECK(back.order_id == gen[idx].order_id);
                ++idx;
            }
        }
        CHECK(idx == gen.size());
    }
}

// -------------------------------------------------------------- L3 book

static void test_order_book_l3() {
    std::printf("L3 order book\n");
    OrderBookL3 b;
    OrderIter i1 = b.add(Side::Buy, 100, RestingOrder{1, 1, 50});
    OrderIter i2 = b.add(Side::Buy, 100, RestingOrder{2, 2, 30});
    OrderIter i3 = b.add(Side::Buy, 100, RestingOrder{3, 3, 20});
    b.add(Side::Buy, 99, RestingOrder{4, 4, 10});
    b.add(Side::Sell, 101, RestingOrder{5, 5, 70});

    CHECK(b.best_bid_ticks() == 100);
    CHECK(b.best_ask_ticks() == 101);
    CHECK(b.best_bid_qty() == 100);
    CHECK(b.order_count_at(Side::Buy, 100) == 3);
    CHECK(b.resting_orders() == 5);
    CHECK(b.bid_level_count() == 2);

    // Queue position: the L3-only query. Order 1 is at the front.
    CHECK(b.qty_ahead(Side::Buy, 100, i1) == 0);
    CHECK(b.qty_ahead(Side::Buy, 100, i2) == 50);
    CHECK(b.qty_ahead(Side::Buy, 100, i3) == 80);

    // Partial execution of the front order shifts everyone forward.
    CHECK(b.reduce(Side::Buy, 100, i1, 20) == 20);
    CHECK(b.best_bid_qty() == 80);
    CHECK(b.qty_ahead(Side::Buy, 100, i2) == 30);
    CHECK(b.order_count_at(Side::Buy, 100) == 3);

    // Over-reducing is clamped to what remains, not negative.
    CHECK(b.reduce(Side::Buy, 100, i1, 999) == 30);
    CHECK(b.order_count_at(Side::Buy, 100) == 2);
    CHECK(b.best_bid_qty() == 50);
    CHECK(b.qty_ahead(Side::Buy, 100, i2) == 0);

    // Emptying a level must remove it, so best-bid rolls down to 99.
    b.remove(Side::Buy, 100, i2);
    b.remove(Side::Buy, 100, i3);
    CHECK(b.bid_level_count() == 1);
    CHECK(b.best_bid_ticks() == 99);
    CHECK(b.best_bid_qty() == 10);
    CHECK(b.resting_orders() == 2);
}

static void test_book_universe() {
    std::printf("book universe\n");
    BookUniverse u(4);

    DecodedMsg a; a.type = MsgType::AddOrder; a.instrument_id = 1; a.order_id = 10;
    a.side = Side::Buy; a.price_ticks = 500; a.qty = 100;
    u.apply(a, 1);
    DecodedMsg a2 = a; a2.order_id = 11; a2.qty = 40;
    u.apply(a2, 2);
    CHECK(u.book(1).order_count_at(Side::Buy, 500) == 2);

    uint64_t ahead = 999;
    CHECK(u.queue_position(11, ahead));
    CHECK(ahead == 100);

    // A replace must go to the back of the queue at its new price, not keep
    // the original order's time priority.
    DecodedMsg a3 = a; a3.order_id = 12; a3.qty = 7;
    u.apply(a3, 3);
    DecodedMsg rep; rep.type = MsgType::Replace; rep.instrument_id = 1;
    rep.order_id = 10; rep.new_order_id = 20; rep.price_ticks = 500; rep.qty = 100;
    u.apply(rep, 4);
    CHECK(u.queue_position(20, ahead));
    CHECK(ahead == 47);              // behind orders 11 (40) and 12 (7)
    CHECK(!u.queue_position(10, ahead));

    // Events for orders we never saw are counted and skipped -- the normal
    // situation downstream of a multicast gap.
    DecodedMsg orphan; orphan.type = MsgType::Cancel; orphan.instrument_id = 1;
    orphan.order_id = 99999; orphan.qty = 5;
    u.apply(orphan, 5);
    CHECK(u.stats().unknown_order_refs == 1);

    // A duplicate add must not create a second resting order.
    u.apply(a2, 6);
    CHECK(u.stats().duplicate_adds == 1);
    CHECK(u.book(1).order_count_at(Side::Buy, 500) == 3);
}

// ------------------------------------------------------------- canonical

static void test_canonical_digest() {
    std::printf("canonical serialization\n");

    auto build = [](const std::vector<std::pair<uint64_t, uint32_t>>& orders) {
        BookUniverse u(2);
        uint64_t seq = 1;
        for (auto& o : orders) {
            DecodedMsg m; m.type = MsgType::AddOrder; m.instrument_id = 0;
            m.order_id = o.first; m.side = Side::Buy; m.price_ticks = 100; m.qty = o.second;
            u.apply(m, seq++);
        }
        return canonical_book_digest(u);
    };

    // Same book built twice -> identical digest.
    CHECK_EQ_STR(build({{1, 10}, {2, 20}, {3, 30}}), build({{1, 10}, {2, 20}, {3, 30}}));

    // Same aggregate quantity at the same price, different QUEUE ORDER.
    // An aggregated (L2) book cannot tell these apart -- both are "60 at
    // 100". This digest must, or it would not be measuring what an L3 book
    // knows.
    std::string qa = build({{1, 10}, {2, 20}, {3, 30}});
    std::string qb = build({{3, 30}, {2, 20}, {1, 10}});
    ++g_checks;
    if (qa == qb) {
        ++g_failures;
        std::printf("  FAIL canonical digest is blind to queue order\n");
    }

    // Different quantity -> different digest (sanity in the other direction).
    ++g_checks;
    if (build({{1, 10}, {2, 20}}) == build({{1, 10}, {2, 21}})) {
        ++g_failures;
        std::printf("  FAIL canonical digest is blind to quantity\n");
    }

    // An emptied level must leave no trace: a book that had a level and lost
    // it must digest identically to one that never had it, otherwise replay
    // could diverge on history rather than state.
    {
        BookUniverse u1(2), u2(2);
        DecodedMsg m; m.type = MsgType::AddOrder; m.instrument_id = 0;
        m.order_id = 1; m.side = Side::Sell; m.price_ticks = 300; m.qty = 5;
        u1.apply(m, 1);
        DecodedMsg del; del.type = MsgType::Delete; del.instrument_id = 0; del.order_id = 1;
        u1.apply(del, 2);
        CHECK_EQ_STR(canonical_book_digest(u1), canonical_book_digest(u2));
    }
}

// --------------------------------------------------------- capture format

static void test_capture_roundtrip() {
    std::printf("capture format round-trip\n");
    const std::string path = "/tmp/tickcap_unit_test.tcap";
    auto sess = make_session_id("UNITTEST01");

    MarketSimulator sim(777);
    std::vector<TickRecord> original;
    {
        // Deliberately not a multiple of the block size, so the final
        // partial block is exercised.
        CaptureWriter w(path, sess, 1024);
        uint64_t seq = 1;
        for (int i = 0; i < 5000; ++i) {
            DecodedMsg m = sim.next();
            TickRecord r;
            r.capture_ts_ns  = 1'000'000'000ull + static_cast<uint64_t>(i) * 977;
            r.exchange_ts_ns = m.exchange_ts_ns;
            r.seq_num        = seq++;
            r.msg_type       = static_cast<uint8_t>(m.type);
            r.instrument_id  = m.instrument_id;
            r.order_id       = m.order_id;
            r.new_order_id   = m.new_order_id;
            r.side           = static_cast<uint8_t>(m.side);
            r.price_ticks    = m.price_ticks;
            r.qty            = m.qty;
            r.pkt_first      = (i % 37 == 0) ? 1 : 0;
            original.push_back(r);
            w.append(r);
        }
        w.close();
    }

    CaptureReader rd(path);
    CHECK(rd.num_records() == original.size());
    CHECK(rd.num_blocks() == 5);           // 5000 records / 1024 per block
    CHECK(rd.session() == sess);

    std::vector<TickRecord> back = rd.read_all();
    CHECK(back.size() == original.size());
    bool identical = true;
    for (size_t i = 0; i < back.size() && identical; ++i) {
        const TickRecord& x = original[i];
        const TickRecord& y = back[i];
        identical = x.capture_ts_ns == y.capture_ts_ns &&
                    x.exchange_ts_ns == y.exchange_ts_ns &&
                    x.seq_num == y.seq_num && x.msg_type == y.msg_type &&
                    x.instrument_id == y.instrument_id && x.order_id == y.order_id &&
                    x.new_order_id == y.new_order_id && x.side == y.side &&
                    x.price_ticks == y.price_ticks && x.qty == y.qty &&
                    x.pkt_first == y.pkt_first;
        if (!identical) std::printf("  first divergence at record %zu\n", i);
    }
    CHECK(identical);

    // A single-column read must agree with the full-record read, and must
    // touch far fewer bytes than reading everything.
    {
        rd.reset_bytes_read();
        std::vector<uint64_t> col;
        std::vector<uint64_t> all_prices;
        for (size_t b = 0; b < rd.num_blocks(); ++b) {
            CHECK(rd.read_block_column(b, 8, col));
            for (uint64_t v : col) all_prices.push_back(v);
        }
        uint64_t col_bytes = rd.bytes_read();
        CHECK(all_prices.size() == original.size());
        bool same = true;
        for (size_t i = 0; i < all_prices.size(); ++i)
            if (static_cast<int64_t>(all_prices[i]) != original[i].price_ticks) { same = false; break; }
        CHECK(same);

        rd.reset_bytes_read();
        std::vector<std::vector<uint64_t>> cols;
        uint32_t n = 0;
        for (size_t b = 0; b < rd.num_blocks(); ++b) CHECK(rd.read_block(b, cols, n));
        CHECK(col_bytes < rd.bytes_read());
    }

    // Re-encoding captured records must reproduce the original message bytes
    // exactly -- the property the whole reproducibility claim rests on.
    {
        std::vector<uint8_t> from_records = reencode_message_stream(back);
        std::vector<uint8_t> from_original;
        for (const TickRecord& r : original) encode_message(from_original, record_to_msg(r));
        CHECK(from_records == from_original);
        CHECK_EQ_STR(Sha256::hex_of(from_records), Sha256::hex_of(from_original));
    }

    std::remove(path.c_str());
}

// ------------------------------------------------------- end-to-end replay

static void test_end_to_end_replay() {
    std::printf("end-to-end capture -> replay (no network)\n");
    const std::string path = "/tmp/tickcap_unit_e2e.tcap";
    auto sess = make_session_id("E2ETEST001");

    // Build a packet stream, decode it while capturing, then replay the
    // capture and require the two books to be byte-identical.
    MarketSimulator sim(2024);
    PacketBuilder pb(sess, 512);
    std::vector<DecodedMsg> gen;
    for (int i = 0; i < 20000; ++i) gen.push_back(sim.next());
    for (size_t i = 0; i < gen.size(); ++i) pb.add(gen[i], 1 + i);
    pb.flush();

    BookUniverse live;
    std::string live_digest;
    {
        CaptureWriter w(path, sess, 4096);
        FeedDecoder dec(live, &w);
        uint64_t ts = 5'000'000'000ull;
        for (const auto& p : pb.packets()) {
            dec.on_datagram(p.data(), p.size(), ts);
            ts += 1500;
        }
        w.close();
        CHECK(dec.stats().messages == gen.size());
        CHECK(dec.stats().gap_events == 0);
        CHECK(dec.stats().malformed == 0);
        live_digest = canonical_book_digest(live);
    }

    CaptureReader rd(path);
    std::vector<TickRecord> recs = rd.read_all();
    CHECK(recs.size() == gen.size());

    BookUniverse replayed;
    replay_records(recs, replayed);
    CHECK_EQ_STR(canonical_book_digest(replayed), live_digest);

    // Byte-level comparison of the canonical forms, so a failure can point
    // at an offset rather than just "the hashes differ".
    std::vector<uint8_t> ba = canonical_book_bytes(live);
    std::vector<uint8_t> bb = canonical_book_bytes(replayed);
    CHECK(ba.size() == bb.size());
    CHECK(ba == bb);

    // The datagram stream must come back byte-identical too, framing and all.
    std::vector<std::vector<uint8_t>> re = reencode_datagrams(recs, rd.session());
    CHECK(re.size() == pb.packets().size());
    bool pkts_identical = (re.size() == pb.packets().size());
    for (size_t i = 0; i < re.size() && pkts_identical; ++i)
        pkts_identical = (re[i] == pb.packets()[i]);
    CHECK(pkts_identical);

    // And feeding those back through a decoder must land on the same book.
    BookUniverse via_wire;
    FeedDecoder dec2(via_wire);
    for (const auto& p : re) dec2.on_datagram(p.data(), p.size(), 0);
    CHECK_EQ_STR(canonical_book_digest(via_wire), live_digest);

    std::remove(path.c_str());
}

// ------------------------------------------------------- gap detection

static void test_gap_detection() {
    std::printf("sequence gap detection\n");
    auto sess = make_session_id("GAPTEST001");
    MarketSimulator sim(31337);
    PacketBuilder pb(sess, 300);
    std::vector<DecodedMsg> gen;
    // The simulator emits nothing but Adds until ~4,096 orders are resting:
    // a book has to fill before there is anything to cancel against. A
    // session short enough to stay inside that warm-up would be 100% Adds,
    // and losing an Add-only stream can never produce a dangling reference,
    // so the destructive-event path would go untested. 30,000 messages puts
    // the mix well past warm-up.
    for (int i = 0; i < 30000; ++i) gen.push_back(sim.next());
    for (size_t i = 0; i < gen.size(); ++i) pb.add(gen[i], 1 + i);
    pb.flush();

    const auto& pkts = pb.packets();
    auto dropped = [](size_t i) { return i % 10 == 3; };

    // Work out what a correct receiver could possibly detect, rather than
    // using a closed-form guess. Two cases are genuinely undetectable and
    // must not be counted: losses before the first delivered datagram (the
    // receiver has no baseline yet, so it adopts whatever sequence number
    // arrives first), and a trailing loss with no later datagram to reveal
    // it. A test that ignored either would be demanding wrong behaviour.
    uint64_t expected_lost = 0, expected_gaps = 0, pending = 0;
    bool seen_first = false;
    for (size_t i = 0; i < pkts.size(); ++i) {
        ByteReader r(pkts[i].data(), pkts[i].size());
        PacketHeader h;
        decode_packet_header(r, h);
        if (dropped(i)) {
            if (seen_first) pending += h.msg_count;
        } else if (!seen_first) {
            seen_first = true;
        } else if (pending > 0) {
            ++expected_gaps;
            expected_lost += pending;
            pending = 0;
        }
    }

    BookUniverse u;
    FeedDecoder dec(u);
    uint64_t delivered_msgs = 0;
    for (size_t i = 0; i < pkts.size(); ++i) {
        if (dropped(i)) continue;
        dec.on_datagram(pkts[i].data(), pkts[i].size(), 0);
        ByteReader r(pkts[i].data(), pkts[i].size());
        PacketHeader h;
        decode_packet_header(r, h);
        delivered_msgs += h.msg_count;
    }
    CHECK(expected_gaps > 0);
    CHECK(dec.stats().messages_lost == expected_lost);
    CHECK(dec.stats().gap_events == expected_gaps);
    CHECK(dec.stats().malformed == 0);
    CHECK(dec.stats().messages == delivered_msgs);
    // Losing the Add that created an order makes its later Cancel reference
    // an unknown id. That must be counted, not fatal.
    CHECK(u.stats().unknown_order_refs > 0);

    // A clean run of the same stream must report no gaps at all, so the
    // detector is not simply reporting gaps unconditionally.
    BookUniverse u2;
    FeedDecoder clean(u2);
    for (const auto& p : pkts) clean.on_datagram(p.data(), p.size(), 0);
    CHECK(clean.stats().gap_events == 0);
    CHECK(clean.stats().messages_lost == 0);
    CHECK(clean.stats().messages == gen.size());
    CHECK(u2.stats().unknown_order_refs == 0);
}

// The simulated feed must be one a real venue could actually emit. The book
// staying uncrossed is the property that matters: on a real exchange an
// order that would cross matches instead of resting, so a feed that produces
// a crossed book is describing something impossible. This regressed once
// already (an unbounded reference-price walk outran the half-spread), so it
// is pinned down here rather than left to be noticed in the analytics.
static void test_simulated_feed_is_uncrossed() {
    std::printf("simulated feed sanity (book never crosses)\n");
    BookUniverse u;
    MarketSimulator sim(20260813);
    uint64_t seq = 1;
    for (int i = 0; i < 400000; ++i) {
        DecodedMsg m = sim.next();
        u.apply(m, seq++);
    }

    int crossed = 0, two_sided = 0;
    int64_t worst = 0;
    for (uint16_t i = 0; i < u.num_instruments(); ++i) {
        const OrderBookL3& b = u.book(i);
        if (!b.has_bid() || !b.has_ask()) continue;
        ++two_sided;
        int64_t spread = b.best_ask_ticks() - b.best_bid_ticks();
        if (spread <= 0) {
            ++crossed;
            if (spread < worst) worst = spread;
        }
    }
    CHECK(two_sided > 100);          // the run must actually build books
    CHECK(crossed == 0);
    if (crossed) std::printf("  %d/%d instruments crossed, worst spread %lld\n",
                             crossed, two_sided, (long long)worst);

    // And the invariant the fix rests on.
    CHECK(MarketSimulator::kMinHalfSpread > MarketSimulator::kMaxDrift);
}

int main() {
    std::printf("=== market-data-tick-capture correctness ===\n\n");
    test_sha256();
    test_varint();
    test_codecs();
    test_wire_protocol();
    test_order_book_l3();
    test_book_universe();
    test_canonical_digest();
    test_capture_roundtrip();
    test_end_to_end_replay();
    test_gap_detection();
    test_simulated_feed_is_uncrossed();

    std::printf("\n%d checks, %d failures\n", g_checks, g_failures);
    if (g_failures == 0) {
        std::printf("PASS\n");
        return 0;
    }
    std::printf("FAIL\n");
    return 1;
}
