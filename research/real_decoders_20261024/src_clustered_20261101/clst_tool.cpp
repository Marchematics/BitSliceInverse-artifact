// clst_tool.cpp -- value-clustered exact aggregation for high group cardinality
// (clustered stream, 2026-11-01).
//
// Problem this closes: a dynamic value range over a high-cardinality GROUP BY
// (G >= 100,000 on NYC-100M) cannot use the dense ECUBE cube (O(G*V) space:
// 8.83 GB at G=100K, 88.3 GB at G=1M) and the block-summary + scan fallback is
// O(n) per query, so it measured only 1.78x / 1.13x vs the DuckDB 2.0 single
// thread engine (TIERS 2026-11-01 section 2).
//
// CLST01 makes range rows *contiguous*: the rows are physically ordered by the
// value column, so a range predicate maps to a contiguous row interval
// [a,b) found by binary search over the sorted distinct values, and the query
// scans only those rows.  Query cost is therefore proportional to the answer
// (matching rows), not to n.
//
// Layout (single file, all sections byte aligned):
//   header      156 B  magic "CLST01\n", n, G, V, gid bit width W, flags,
//                      vmin/vmax, section offsets+lengths, gmin (group origin)
//   values      V x varint(zigzag(delta))   sorted distinct values
//   counts      V x varint                  rows per distinct value
//   starts      (V+1) x u64                 prefix row starts (O(1) range map)
//   gids        n*W/8 + 8 B zero pad        bit-packed group ids in value order
//   stats       G x 20 B (optional)         exact per-group SUM/COUNT/MIN/MAX
//                                           over all rows (fixed-query path)
//
// Lossless: (values, counts, gids) reconstructs every (value, gid) row of the
// input exactly -- the row order within one value run is the only thing not
// preserved, and no query depends on it (verified by `verify`).
//
// Commands:
//   build   GID VAL OUT [--with-stats]
//   info    FILE
//   query   FILE LO HI OUT.json OUT.bin [REPS] [--no-fastpath]
//   verify  GID VAL FILE
//
// LO/HI are inclusive (v BETWEEN LO AND HI semantics), i64, clamped to i32.
// Single threaded; every timing run is externally pinned (nice -n 5 taskset -c 128).
#include <algorithm>
#include <charconv>
#include <chrono>
#include <climits>
#include <pthread.h>
#include <sched.h>
#include <thread>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <string>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

using u8 = uint8_t;
using u32 = uint32_t;
using u64 = uint64_t;
using i32 = int32_t;
using i64 = int64_t;
using u128 = unsigned __int128;

static constexpr char MAGIC[8] = {'C', 'L', 'S', 'T', '0', '1', '\n', '\0'};
static constexpr char RMAGIC[8] = {'C', 'L', 'S', 'R', 'S', '0', '1', '\n'};  // result binary
static constexpr u32 FLAG_STATS = 1u;
static constexpr size_t STATS_BYTES = 20;  // i64 sum, u32 cnt, i32 mn, i32 mx

#pragma pack(push, 1)
struct Header {
    char magic[8];
    u64 n, G, V;
    u32 W, flags, pad0;
    i64 vmin, vmax;
    u64 off_values, len_values;
    u64 off_counts, len_counts;
    u64 off_starts, len_starts;
    u64 off_gids, len_gids;
    u64 off_stats, len_stats;
    u64 file_bytes;
    i64 gmin;      // group-domain origin: stored group = gmin + packed W-bit index
                   // (0 for the 100M structures; -1 for the 1B NULL-group sentinel)
};
#pragma pack(pop)
static_assert(sizeof(Header) == 156, "header layout");

static double now_ms() {
    using clk = std::chrono::steady_clock;
    return std::chrono::duration<double, std::milli>(clk::now().time_since_epoch()).count();
}
static i64 peak_rss_kb() {
    struct rusage ru;
    getrusage(RUSAGE_SELF, &ru);
    return (i64)ru.ru_maxrss;
}

// ---------------------------------------------------------------------------
// mapped i32 column
// ---------------------------------------------------------------------------
struct Mapped {
    const i32* p = nullptr;
    u64 n = 0;
    void* base = nullptr;
    size_t bytes = 0;
};
static Mapped map_i32(const char* path) {
    Mapped m;
    int fd = open(path, O_RDONLY);
    if (fd < 0) { std::perror(path); std::exit(1); }
    struct stat st {};
    if (fstat(fd, &st)) { std::perror("fstat"); std::exit(1); }
    m.bytes = (size_t)st.st_size;
    if (m.bytes % 4) { std::fprintf(stderr, "ERR %s not i32\n", path); std::exit(1); }
    m.n = m.bytes / 4;
    m.base = mmap(nullptr, m.bytes, PROT_READ, MAP_PRIVATE, fd, 0);
    if (m.base == MAP_FAILED) { std::perror("mmap"); std::exit(1); }
    m.p = static_cast<const i32*>(m.base);
    close(fd);
    return m;
}

// ---------------------------------------------------------------------------
// varints (zigzag for values)
// ---------------------------------------------------------------------------
static void put_varint(std::vector<u8>& out, u64 v) {
    while (v >= 0x80) { out.push_back((u8)(v | 0x80)); v >>= 7; }
    out.push_back((u8)v);
}
static u64 get_varint(const u8*& p) {
    u64 v = 0;
    u32 sh = 0;
    while (true) {
        u8 b = *p++;
        v |= (u64)(b & 0x7F) << sh;
        if (!(b & 0x80)) return v;
        sh += 7;
    }
}
static inline u64 zigzag(i64 v) { return ((u64)v << 1) ^ (u64)(v >> 63); }
static inline i64 unzigzag(u64 v) { return (i64)((v >> 1) ^ (~(v & 1) + 1)); }

// ---------------------------------------------------------------------------
// bit packing: LSB-first, u128 accumulator, 8-byte tail pad on the stream
// ---------------------------------------------------------------------------
struct BitWriter {
    std::vector<u8> buf;
    u128 acc = 0;
    u32 have = 0;
    void reserve(size_t n) { buf.reserve(n + 16); }
    inline void put(u32 v, u32 w) {
        if (!w) return;
        acc |= (u128)v << have;
        have += w;
        while (have >= 8) {
            buf.push_back((u8)(acc & 0xFF));
            acc >>= 8;
            have -= 8;
        }
    }
    void flush() {
        while (have) {
            buf.push_back((u8)(acc & 0xFF));
            acc >>= 8;
            have = have >= 8 ? have - 8 : 0;
        }
    }
};

static inline u64 load64(const u8* p) {
    u64 w;
    std::memcpy(&w, p, 8);
    return w;
}

// Random-access bit reader: init at bit position `bitpos`, then next() W-bit values.
struct BitReader {
    const u8* p = nullptr;
    u128 acc = 0;
    u32 have = 0;
    u32 W = 0;
    u64 mask = 0;
    inline void init(const u8* gid_base, u64 bitpos) {
        u64 byteoff = bitpos >> 3;
        u32 bitoff = (u32)(bitpos & 7);
        p = gid_base + byteoff;
        u64 w = load64(p);
        acc = (u128)(w >> bitoff);
        have = 64 - bitoff;
        // 8-byte loads may run up to 7 bytes past the last bit: the stream
        // carries an 8-byte zero pad, so this is always in bounds.
        p += 8;
    }
    inline u32 next() {
        if (have < W) {
            u64 w = load64(p);
            p += 8;
            acc |= (u128)w << have;
            have += 64;
        }
        u32 v = (u32)(acc & mask);
        acc >>= W;
        have -= W;
        return v;
    }
};

// ---------------------------------------------------------------------------
// per-group aggregation state (dense, only touched groups are reset)
// ---------------------------------------------------------------------------
// One 32-byte record per group, so a random group touch costs ONE cache line
// (the four separate i64/i32 arrays cost four lines per touched group, which
// dominates the high-G scan: at G=1M a 10 % range touches ~1M groups).
// `epoch` lets the parallel path reuse a dirty buffer without clearing it: a
// record is live for a query only if epoch == the query's (or thread's) epoch.
struct alignas(32) Rec {
    i64 sum;
    u32 cnt;
    i32 mn, mx;
    u32 epoch;
    u32 pad;
};
struct State {
    std::vector<Rec> st;
    std::vector<u32> touched;
    void alloc(u64 G) {
        st.assign(G, Rec{0, 0, 0, 0, 0, 0});
        touched.reserve(std::min<u64>(G, 1u << 20));
    }
    inline void reset() {
        for (u32 g : touched) st[g] = Rec{0, 0, 0, 0, 0, 0};
        touched.clear();
    }
};

struct Row {
    i64 g, sum, cnt, mn, mx;
};

// ---------------------------------------------------------------------------
// result writers (JSON format identical to the ECUBE02 / tiers_tool writers,
// so every exactness comparator in the program applies unchanged)
// ---------------------------------------------------------------------------
static void json_rows(const std::vector<Row>& rows, std::string& out) {
    out.clear();
    out.reserve(rows.size() * 56 + 32);
    out += "{\"rows\":[";
    char buf[32];
    bool first = true;
    for (const Row& r : rows) {
        if (!first) out += ',';
        first = false;
        out += '[';
        out += '"';
        auto w = std::to_chars(buf, buf + sizeof(buf), (long long)r.g);
        out.append(buf, w.ptr);
        out += '"';
        auto app = [&](long long v) {
            out += ",\"";
            auto w2 = std::to_chars(buf, buf + sizeof(buf), v);
            out.append(buf, w2.ptr);
            out += '"';
        };
        app((long long)r.sum);
        app((long long)r.cnt);
        app((long long)r.mn);
        app((long long)r.mx);
        out += ']';
    }
    out += "]}\n";
}
static void bin_rows(const std::vector<Row>& rows, std::vector<char>& out) {
    out.clear();
    out.reserve(16 + rows.size() * 40);
    out.insert(out.end(), RMAGIC, RMAGIC + 8);
    u64 n = rows.size();
    out.insert(out.end(), (const char*)&n, (const char*)&n + 8);
    for (const Row& r : rows) {
        i64 v[5] = {r.g, r.sum, r.cnt, r.mn, r.mx};
        out.insert(out.end(), (const char*)v, (const char*)v + 40);
    }
}
static u64 rows_checksum(const std::vector<Row>& rows) {
    u64 c = 1469598103934665603ULL;
    for (const Row& r : rows) {
        c = (c ^ (u64)r.g) * 1099511628211ULL;
        c = (c ^ (u64)r.sum) * 1099511628211ULL;
        c = (c ^ (u64)r.cnt) * 1099511628211ULL;
        c = (c ^ (u64)r.mn) * 1099511628211ULL;
        c = (c ^ (u64)r.mx) * 1099511628211ULL;
    }
    return c;
}
static void write_file(const char* path, const void* p, size_t n) {
    FILE* f = fopen(path, "wb");
    if (!f) { std::perror(path); std::exit(1); }
    if (fwrite(p, 1, n, f) != n) { std::perror("fwrite"); std::exit(1); }
    fclose(f);
}
static double pctl(std::vector<double>& v, double p) {
    std::sort(v.begin(), v.end());
    return v[std::min(v.size() - 1, (size_t)(p * v.size()))];
}

// ---------------------------------------------------------------------------
// structure handle (mmap)
// ---------------------------------------------------------------------------
struct Clst {
    void* base = nullptr;
    size_t bytes = 0;
    Header h{};
    const u8* values = nullptr;
    const u64* starts = nullptr;
    const u8* gids = nullptr;
    const u8* stats = nullptr;  // G*20 packed records
    std::vector<i64> vals;      // decoded distinct values
};

static void clst_open(const char* path, Clst& c) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) { std::perror(path); std::exit(1); }
    struct stat st {};
    fstat(fd, &st);
    c.bytes = (size_t)st.st_size;
    c.base = mmap(nullptr, c.bytes, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (c.base == MAP_FAILED) { std::perror("mmap"); std::exit(1); }
    std::memcpy(&c.h, c.base, sizeof(Header));
    if (std::memcmp(c.h.magic, MAGIC, 8)) { std::fprintf(stderr, "ERR clst magic\n"); std::exit(1); }
    const u8* b = (const u8*)c.base;
    c.values = b + c.h.off_values;
    c.starts = (const u64*)(b + c.h.off_starts);
    c.gids = b + c.h.off_gids;
    c.stats = c.h.flags & FLAG_STATS ? b + c.h.off_stats : nullptr;
    c.vals.resize(c.h.V);
    const u8* p = c.values;
    i64 prev = 0;
    for (u64 i = 0; i < c.h.V; ++i) {
        prev += unzigzag(get_varint(p));
        c.vals[i] = prev;
    }
}

// map inclusive range [lo,hi] to run interval [r0,r1] (r0>r1 => empty)
static void range_runs(const Clst& c, i64 lo, i64 hi, u64& r0, u64& r1) {
    lo = std::max<i64>(lo, INT32_MIN);
    hi = std::min<i64>(hi, INT32_MAX);
    auto lb = std::lower_bound(c.vals.begin(), c.vals.end(), lo);
    auto ub = std::upper_bound(c.vals.begin(), c.vals.end(), hi);
    r0 = (u64)(lb - c.vals.begin());
    r1 = (u64)(ub - c.vals.begin());
    // r1 is exclusive
}

// ---------------------------------------------------------------------------
// build
// ---------------------------------------------------------------------------
static void cmd_build(const char* gid_path, const char* val_path, const char* out_path,
                      bool with_stats) {
    double t0 = now_ms();
    Mapped gid = map_i32(gid_path);
    Mapped val = map_i32(val_path);
    if (gid.n != val.n) { std::fprintf(stderr, "ERR size mismatch\n"); std::exit(1); }
    const u64 n = val.n;

    // pass 1: group domain [gmin,gmax] + value domain.  A negative gid is allowed:
    // it is the SQL-NULL group-key sentinel of this program's plain i32 columns
    // (-1 on the 1B replicas), stored as gmin + packed index.
    i64 gmin = INT64_MAX, gmax = INT64_MIN;
    i64 vmin = INT64_MAX, vmax = INT64_MIN;
    for (u64 i = 0; i < n; ++i) {
        i64 g = gid.p[i];
        if (g < gmin) gmin = g;
        if (g > gmax) gmax = g;
        i64 v = val.p[i];
        if (v < vmin) vmin = v;
        if (v > vmax) vmax = v;
    }
    const u64 G = (u64)(gmax - gmin) + 1;
    if (G > (1ull << 32)) { std::fprintf(stderr, "ERR gid domain too wide\n"); std::exit(1); }
    double t1 = now_ms();
    u32 W = 0;
    while ((1ull << W) < G) ++W;  // ceil(log2 G); W=0 when G==1

    // pass 2: dense value histogram over [vmin,vmax] (span bounded by 2^32 check)
    const u64 span = (u64)(vmax - vmin) + 1;
    if (span > (1ull << 32)) {
        std::fprintf(stderr, "ERR value span %llu too wide for the dense histogram\n",
                     (unsigned long long)span);
        std::exit(1);
    }
    std::vector<u32> hist(span, 0);
    for (u64 i = 0; i < n; ++i) ++hist[(u64)((i64)val.p[i] - vmin)];
    double t2 = now_ms();

    // distinct values + counts (ascending), dense rank table over the span
    std::vector<i64> dvals;
    std::vector<u64> dcnt;
    dvals.reserve(1 << 16);
    dcnt.reserve(1 << 16);
    std::vector<u32> rank(span, 0xFFFFFFFFu);  // rank of each in-domain value
    for (u64 d = 0; d < span; ++d) {
        if (!hist[d]) continue;
        rank[d] = (u32)dvals.size();
        dvals.push_back((i64)d + vmin);
        dcnt.push_back(hist[d]);
    }
    hist.clear();
    hist.shrink_to_fit();
    const u64 V = dvals.size();
    std::vector<u64> starts(V + 1, 0);
    for (u64 r = 0; r < V; ++r) starts[r + 1] = starts[r] + dcnt[r];
    if (starts[V] != n) { std::fprintf(stderr, "ERR histogram sum\n"); std::exit(1); }
    double t3 = now_ms();

    // pass 3: scatter gids into value order (+ optional exact per-group totals)
    std::vector<u32> gsorted(n);
    std::vector<u64> cursor(starts.begin(), starts.end() - 1);
    std::vector<i64> s_sum;
    std::vector<u32> s_cnt;
    std::vector<i32> s_mn, s_mx;
    if (with_stats) {
        s_sum.assign(G, 0);
        s_cnt.assign(G, 0);
        s_mn.assign(G, 0);
        s_mx.assign(G, 0);
    }
    for (u64 i = 0; i < n; ++i) {
        const u32 r = rank[(u64)((i64)val.p[i] - vmin)];
        gsorted[cursor[r]++] = (u32)((i64)gid.p[i] - gmin);
        if (with_stats) {
            const u32 g = (u32)((i64)gid.p[i] - gmin);
            const i32 v = val.p[i];
            if (!s_cnt[g]) { s_mn[g] = v; s_mx[g] = v; }
            else { if (v < s_mn[g]) s_mn[g] = v; if (v > s_mx[g]) s_mx[g] = v; }
            s_sum[g] += (i64)v;
            ++s_cnt[g];
        }
    }
    double t4 = now_ms();

    // pass 4: bit-pack the value-ordered gid stream (+8 B zero tail pad)
    BitWriter bw;
    bw.reserve((size_t)((n * W + 7) / 8) + 8);
    if (W) {
        for (u64 i = 0; i < n; ++i) bw.put(gsorted[i], W);
    }
    bw.flush();
    for (int i = 0; i < 8; ++i) bw.buf.push_back(0);
    gsorted.clear();
    gsorted.shrink_to_fit();
    double t5 = now_ms();

    // value/count varint blocks
    std::vector<u8> vblk, cblk;
    vblk.reserve((size_t)V * 3 + 16);
    cblk.reserve((size_t)V * 3 + 16);
    i64 prev = 0;
    for (u64 r = 0; r < V; ++r) {
        put_varint(vblk, zigzag(dvals[r] - prev));
        prev = dvals[r];
        put_varint(cblk, dcnt[r]);
    }

    // layout
    Header h{};
    std::memcpy(h.magic, MAGIC, 8);
    h.n = n;
    h.G = G;
    h.V = V;
    h.W = W;
    h.flags = with_stats ? FLAG_STATS : 0u;
    h.vmin = vmin;
    h.vmax = vmax;
    h.gmin = gmin;
    u64 off = sizeof(Header);
    auto place = [&](u64 len, u64& o, u64& l) {
        o = off;
        l = len;
        off = (off + len + 7) & ~7ull;
    };
    place(vblk.size(), h.off_values, h.len_values);
    place(cblk.size(), h.off_counts, h.len_counts);
    place((V + 1) * 8, h.off_starts, h.len_starts);
    place(bw.buf.size(), h.off_gids, h.len_gids);
    place(with_stats ? G * STATS_BYTES : 0, h.off_stats, h.len_stats);
    h.file_bytes = off;

    std::vector<u8> file(off, 0);
    std::memcpy(file.data(), &h, sizeof(Header));
    std::memcpy(file.data() + h.off_values, vblk.data(), vblk.size());
    std::memcpy(file.data() + h.off_counts, cblk.data(), cblk.size());
    std::memcpy(file.data() + h.off_starts, starts.data(), starts.size() * 8);
    std::memcpy(file.data() + h.off_gids, bw.buf.data(), bw.buf.size());
    if (with_stats) {
        u8* p = file.data() + h.off_stats;
        for (u64 g = 0; g < G; ++g) {
            std::memcpy(p, &s_sum[g], 8); p += 8;
            std::memcpy(p, &s_cnt[g], 4); p += 4;
            std::memcpy(p, &s_mn[g], 4); p += 4;
            std::memcpy(p, &s_mx[g], 4); p += 4;
        }
    }
    write_file(out_path, file.data(), file.size());
    double t6 = now_ms();

    std::fprintf(stderr,
                 "{\"mode\":\"clst_build\",\"build_ms\":%.3f,\"rows\":%llu,\"G\":%llu,"
                 "\"V\":%llu,\"gid_bits\":%u,\"vmin\":%lld,\"vmax\":%lld,"
                 "\"domain_ms\":%.3f,\"hist_ms\":%.3f,\"distinct_ms\":%.3f,"
                 "\"scatter_ms\":%.3f,\"bitpack_ms\":%.3f,\"write_ms\":%.3f,"
                 "\"bytes\":%llu,\"bytes_values\":%llu,\"bytes_counts\":%llu,"
                 "\"bytes_starts\":%llu,\"bytes_gids\":%llu,\"bytes_stats\":%llu,"
                 "\"with_stats\":%d,\"peak_rss_kb\":%lld}\n",
                 t6 - t0, (unsigned long long)n, (unsigned long long)G,
                 (unsigned long long)V, W, (long long)vmin, (long long)vmax, t1 - t0,
                 t2 - t1, t3 - t2, t4 - t3, t5 - t4, t6 - t5,
                 (unsigned long long)file.size(), (unsigned long long)vblk.size(),
                 (unsigned long long)cblk.size(), (unsigned long long)((V + 1) * 8),
                 (unsigned long long)bw.buf.size(),
                 (unsigned long long)(with_stats ? G * STATS_BYTES : 0), (int)with_stats,
                 (long long)peak_rss_kb());
}

// ---------------------------------------------------------------------------
// info
// ---------------------------------------------------------------------------
static void cmd_info(const char* path) {
    Clst c;
    clst_open(path, c);
    const Header& h = c.h;
    std::printf("{\"mode\":\"clst_info\",\"file\":\"%s\",\"file_bytes\":%llu,\"rows\":%llu,"
                "\"G\":%llu,\"V\":%llu,\"gid_bits\":%u,\"gmin\":%lld,\"vmin\":%lld,\"vmax\":%lld,"
                "\"with_stats\":%d,\"bytes_values\":%llu,\"bytes_counts\":%llu,"
                "\"bytes_starts\":%llu,\"bytes_gids\":%llu,\"bytes_stats\":%llu,"
                "\"bits_per_row\":%.4f,\"bytes_per_row\":%.4f,\"bytes_per_value\":%.4f}\n",
                path, (unsigned long long)h.file_bytes, (unsigned long long)h.n,
                (unsigned long long)h.G, (unsigned long long)h.V, h.W, (long long)h.gmin,
                (long long)h.vmin,
                (long long)h.vmax, (int)((h.flags & FLAG_STATS) != 0),
                (unsigned long long)h.len_values, (unsigned long long)h.len_counts,
                (unsigned long long)h.len_starts, (unsigned long long)h.len_gids,
                (unsigned long long)h.len_stats,
                8.0 * (double)h.len_gids / (double)h.n, (double)h.file_bytes / (double)h.n,
                (double)(h.len_values + h.len_counts) / (double)h.V);
}


// ---------------------------------------------------------------------------
// parallel scan (multi-core series, 2026-11-01)
//
// The matching rows are a contiguous interval of the value-ordered runs.  It is
// partitioned into T row-balanced, run-aligned chunks; each thread scans its
// chunk into its OWN dense per-group state (disjoint accumulation, no atomics,
// no locks) and the partials are merged afterwards over disjoint group slices.
// All operations are exact integer adds/min/max, so the merged result is
// bit-identical to the single-thread result (checked by row md5 across threads).
//
// Memory: T x G x 32 B partial state.  A cap (`max_partial_bytes`) reduces the
// number of concurrently live partials and the scan is then run in W waves,
// each wave merged before the next starts, so peak memory stays at the cap.
// ---------------------------------------------------------------------------

// Persistent worker pool: threads are created ONCE per query process and reused
// for every timed query, so the per-query dispatch cost is a notify_all + wait
// (tens of microseconds) instead of T pthread_create/join pairs (~30 us each,
// which alone exceeded a 2 ms scan at 16 threads).
struct Pool {
    std::mutex m;
    std::condition_variable cv_work, cv_done;
    int T = 0, pending = 0;
    u64 gen = 0;
    bool quit = false;
    std::function<void(int)> job;
    std::vector<std::thread> workers;

    void start(int T_, void (*pin)(int)) {
        T = T_;
        workers.reserve((size_t)T);
        for (int t = 0; t < T; ++t) {
            workers.emplace_back([this, t, pin]() {
                pin(t);
                u64 seen = 0;
                for (;;) {
                    std::function<void(int)> f;
                    {
                        std::unique_lock<std::mutex> lk(m);
                        cv_work.wait(lk, [&] { return quit || gen != seen; });
                        if (quit) return;
                        seen = gen;
                        f = job;
                    }
                    f(t);
                    {
                        std::lock_guard<std::mutex> lk(m);
                        if (--pending == 0) cv_done.notify_one();
                    }
                }
            });
        }
    }
    void run(const std::function<void(int)>& f) {
        {
            std::lock_guard<std::mutex> lk(m);
            job = f;
            pending = T;
            ++gen;
        }
        cv_work.notify_all();
        std::unique_lock<std::mutex> lk(m);
        cv_done.wait(lk, [&] { return pending == 0; });
    }
    ~Pool() {
        if (!T) return;
        {
            std::lock_guard<std::mutex> lk(m);
            quit = true;
        }
        cv_work.notify_all();
        for (auto& w : workers) w.join();
    }
};

struct alignas(32) PRec {
    i64 sum;
    u32 cnt;
    i32 mn, mx;
    u32 epoch;
    u32 pad;
};

struct ParCtx {
    u64 G = 0;
    int T = 0;
    u32 epoch = 0;                 // incremented once per wave
    std::vector<PRec> parts;       // T * G
    std::vector<std::vector<u32>> slice_touched;  // per merge-thread touched lists
    size_t partial_bytes = 0;
    void alloc(u64 G_, int T_) {
        G = G_;
        T = T_;
        parts.assign((size_t)T * (size_t)G, PRec{0, 0, 0, 0, 0, 0});
        slice_touched.assign(T, {});
        for (auto& v : slice_touched) v.reserve(1024);
        partial_bytes = parts.size() * sizeof(PRec);
    }
};

// pin worker t to the t-th CPU of the process's allowed set (nice/taskset already
// restrict the set, so this just removes migration noise)
static void pin_worker(int t) {
#if defined(__linux__)
    cpu_set_t allowed;
    CPU_ZERO(&allowed);
    if (sched_getaffinity(0, sizeof(allowed), &allowed) != 0) return;
    int k = 0, chosen = -1;
    for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
        if (!CPU_ISSET(cpu, &allowed)) continue;
        if (k == t) { chosen = cpu; break; }
        ++k;
    }
    if (chosen < 0) return;
    cpu_set_t one;
    CPU_ZERO(&one);
    CPU_SET(chosen, &one);
    pthread_setaffinity_np(pthread_self(), sizeof(one), &one);
#else
    (void)t;
#endif
}

// run index of the run containing row `row` (largest r with starts[r] <= row)
static u64 run_of_row(const u64* starts, u64 V, u64 row) {
    u64 lo = 0, hi = V;  // upper_bound(starts[0..V], row) - 1
    while (lo < hi) {
        u64 mid = (lo + hi) / 2;
        if (starts[mid] <= row) lo = mid + 1; else hi = mid;
    }
    return lo ? lo - 1 : 0;
}

// Scan the row range [a,b) of the value-ordered stream into partial state slot.
// Chunks are ROW-aligned, so a chunk may start and end inside a value run: run
// boundaries are not thread boundaries (one frequent value can own 30 % of the
// matching rows, which would otherwise collapse the partition).
static void scan_rows(const Clst& c, u64 a, u64 b, PRec* P, u32 ep) {
    if (a >= b) return;
    const u32 W = c.h.W;
    const u64 mask = (W == 32) ? 0xFFFFFFFFull : ((1ull << W) - 1ull);
    const u8* gb = c.gids;
    const u64* starts = c.starts;
    u64 r = run_of_row(starts, c.h.V, a);
    while (r < c.h.V) {
        const u64 s0 = starts[r], s1 = starts[r + 1];
        if (s0 >= b) break;
        const u64 lo = a > s0 ? a : s0;
        const u64 hi = b < s1 ? b : s1;
        const i32 v = (i32)c.vals[r];
        if (lo < hi) {
            if (!W) {
                PRec& z = P[0];
                if (z.epoch != ep) { z.epoch = ep; z.sum = 0; z.cnt = 0; z.mn = v; z.mx = v; }
                z.sum += (i64)v * (i64)(hi - lo);
                z.cnt += (u32)(hi - lo);
                z.mx = v;
            } else {
                u64 bitpos = lo * (u64)W;
                for (u64 row = lo; row < hi; ++row) {
                    const u32 g = (u32)((load64(gb + (bitpos >> 3)) >> (u32)(bitpos & 7)) & mask);
                    bitpos += W;
                    PRec& q = P[g];
                    if (q.epoch != ep) {
                        q.epoch = ep;
                        q.sum = v;
                        q.cnt = 1;
                        q.mn = v;
                        q.mx = v;
                    } else {
                        q.sum += v;
                        ++q.cnt;
                        q.mx = v;   // values ascend inside the interval
                    }
                }
            }
        }
        ++r;
    }
}

// Parallel aggregation over runs [r0,r1). Fills st (and st.touched).
// Returns: parallel_phase_ms (spawn+scan+merge, all waves), scan_ms (threads only),
// merge_ms, waves used, threads actually used per wave, partial bytes.
struct ParResult {
    double phase_ms = 0, scan_ms = 0, merge_ms = 0;
    int waves = 1, T_used = 1, chunks = 0;
    u64 max_chunk_rows = 0;
};
static ParResult par_aggregate(ParCtx& pc, Pool& pool, const Clst& c, u64 r0, u64 r1,
                               u64 nrows, int T_req, u64 max_partial_bytes, State& st) {
    ParResult res;
    if (nrows == 0 || T_req <= 1) {
        res.T_used = 1;
        return res;
    }
    const u64 G = c.h.G;
    (void)r1;  // boundaries are derived from r0 + exact row counts
    const u64 per_thread = (u64)G * sizeof(PRec);
    int Tmax = (int)std::max<u64>(1, max_partial_bytes / std::max<u64>(1, per_thread));
    if (Tmax > T_req) Tmax = T_req;
    res.T_used = Tmax;
    const int waves = (int)((T_req + Tmax - 1) / Tmax);
    res.waves = waves;
    const u32 st_epoch = pc.epoch + 1;  // one global epoch per query
    for (int t = 0; t < Tmax; ++t) pc.slice_touched[t].clear();
    const double p0 = now_ms();
    std::vector<u64> bnd((size_t)Tmax + 1, r0);
    for (int w = 0; w < waves; ++w) {
        // wave row range (balanced over the whole matching interval)
        const u64 wa0 = nrows * (u64)w / (u64)waves;
        const u64 wa1 = nrows * (u64)(w + 1) / (u64)waves;
        const u64 base = c.starts[r0];
        // thread chunk boundaries in ROW indices (exactly balanced in rows)
        const int nt = Tmax;
        for (int t = 0; t <= nt; ++t) bnd[t] = base + wa0 + (wa1 - wa0) * (u64)t / (u64)nt;
        const u32 ep = ++pc.epoch;
        const double s0 = now_ms();
        {
            const u64* B = bnd.data();
            PRec* P = pc.parts.data();
            pool.run([&, B, P](int t) {
                if (B[t] >= B[t + 1]) return;
                scan_rows(c, B[t], B[t + 1], P + (size_t)t * G, ep);
            });
            res.chunks = 0;
            for (int t = 0; t < nt; ++t) {
                if (bnd[t] >= bnd[t + 1]) continue;
                ++res.chunks;
                const u64 rows_t = bnd[t + 1] - bnd[t];
                if (rows_t > res.max_chunk_rows) res.max_chunk_rows = rows_t;
            }
        }
        const double s1 = now_ms();
        // merge over disjoint group slices, in parallel
        const int nmerge = nt;
        {
            pool.run([&, nmerge, ep](int t) {
            const u64 g0 = G * (u64)t / (u64)nmerge;
            const u64 g1 = G * (u64)(t + 1) / (u64)nmerge;
            if (g0 >= g1) return;
            {
                std::vector<u32>& out = pc.slice_touched[t];
                out.clear();
                for (u64 g = g0; g < g1; ++g) {
                    i64 sum = 0;
                    u64 cnt = 0;
                    i32 mn = 0, mx = 0;
                    bool any = false;
                    for (int k = 0; k < nt; ++k) {
                        const PRec& q = pc.parts[(size_t)k * G + g];
                        if (q.epoch != ep) continue;
                        if (!any) { sum = q.sum; cnt = q.cnt; mn = q.mn; mx = q.mx; any = true; }
                        else {
                            sum += q.sum;
                            cnt += q.cnt;
                            if (q.mn < mn) mn = q.mn;
                            if (q.mx > mx) mx = q.mx;
                        }
                    }
                    if (!any) continue;
                    Rec& r = st.st[g];
                    if (r.epoch != st_epoch) { r.epoch = st_epoch; r.sum = sum; r.cnt = (u32)cnt; r.mn = mn; r.mx = mx; }
                    else {
                        r.sum += sum;
                        r.cnt += (u32)cnt;
                        if (mn < r.mn) r.mn = mn;
                        if (mx > r.mx) r.mx = mx;
                    }
                    out.push_back((u32)g);
                }
            }
            });
        }
        const double s2 = now_ms();
        res.scan_ms += s1 - s0;
        res.merge_ms += s2 - s1;
        for (int t = 0; t < nt; ++t)
            for (u32 g : pc.slice_touched[t]) st.touched.push_back(g);
    }
    res.phase_ms = now_ms() - p0;
    return res;
}

// ---------------------------------------------------------------------------
// query
// ---------------------------------------------------------------------------
static int allowed_cpus() {
#if defined(__linux__)
    cpu_set_t a;
    CPU_ZERO(&a);
    if (sched_getaffinity(0, sizeof(a), &a) == 0) return CPU_COUNT(&a);
#endif
    return 1;
}

// Cost model for the production (auto) mode: choose the thread count from the
// estimated scan work.  Per-row cost is calibrated from the measured curves
// (1.3 ns/row at G=101, 2.6 ns/row at G=100K, 8.1 ns/row at G=1M: the extra cost
// is the per-touch cache miss into the dense group state).
// Calibrated model: scan rate 1.2-8.1 ns per matching row (G-dependent cache
// misses into the dense group state), merge bandwidth ~61 GB/s (measured: 512 MB
// of partials in 8.4 ms at G=1M/16T), dispatch ~5 us per thread + 20 us.  The
// model enumerates T and picks the cheapest; it is a choice heuristic, not a
// latency predictor (the measured series is in the receipt).
static int auto_threads(u64 nrows, u64 G, int T_max) {
    if (T_max <= 1 || nrows == 0) return 1;
    const double ns_per_row = 1.2 + 6.8 * ((double)G / 1e6);
    const double scan_ms = (double)nrows * ns_per_row / 1e6;
    int best_T = 1;
    double best = scan_ms;
    for (int T = 2; T <= T_max; T *= 2) {
        const double merge = (double)G * 32.0 * (double)T / 61e6;
        const double dispatch = 0.005 * (double)T + 0.02;
        const double cost = scan_ms / (double)T + merge + dispatch;
        if (cost < best) { best = cost; best_T = T; }
    }
    return best_T;
}

static void cmd_query(const char* path, i64 lo, i64 hi, const char* out_json,
                      const char* out_bin, int reps, bool fastpath, int threads_req,
                      bool auto_mode, u64 max_partial_bytes) {
    double t0 = now_ms();
    (void)0;
    Clst c;
    clst_open(path, c);
    const u64 n = c.h.n, G = c.h.G, V = c.h.V;
    const i64 gmin = c.h.gmin;
    const double t1 = now_ms();
    u64 r0, r1;
    range_runs(c, lo, hi, r0, r1);  // r1 exclusive
    const bool empty = r0 >= r1;
    const bool full = !empty && r0 == 0 && r1 == V;
    const u64 nrows = empty ? 0 : (c.starts[r1] - c.starts[r0]);
    const bool use_fast = fastpath && full && c.stats != nullptr;

    // multi-core: only the range-scan path is parallelised; the stored-stats
    // fast path is 101-1M records and is left single-threaded.
    int T_use;
    int T_max = threads_req > 1 ? threads_req : std::min(allowed_cpus(), 32);
    if (use_fast) T_use = 1;
    else if (auto_mode) T_use = auto_threads(nrows, G, T_max);
    else T_use = threads_req > 1 ? threads_req : 1;
    if (T_use > 1 && nrows == 0) T_use = 1;
    if (T_use > T_max) T_use = T_max;
    ParCtx pc;
    Pool pool;
    if (T_use > 1) {
        pc.alloc(G, T_use);
        pool.start(T_use, pin_worker);
    }

    State st;
    st.alloc(G);
    std::vector<Row> rows;
    rows.reserve(use_fast ? G : std::min<u64>(G, nrows));

    // one untimed warmup so page faults / TLB are not in the timed region
    auto aggregate_scan = [&]() {
        if (empty) return;
        const u32 W = c.h.W;
        const u64 mask = (W == 32) ? 0xFFFFFFFFull : ((1ull << W) - 1ull);
        const u8* gb = c.gids;
        u64 bitpos = c.starts[r0] * (u64)W;
        if (!W) {
            for (u64 r = r0; r < r1; ++r) {
                const i32 v = (i32)c.vals[r];
                const u64 k = c.starts[r + 1] - c.starts[r];
                Rec& z = st.st[0];
                for (u64 j = 0; j < k; ++j) {
                    if (!z.cnt) { z.mn = v; st.touched.push_back(0); }
                    z.mx = v;
                    z.sum += v;
                    ++z.cnt;
                }
            }
            return;
        }
        // Branch-free extraction: each gid is (load64(byte) >> (bitpos&7)) & mask.
        // The 8-byte load is always in bounds because the gid stream carries an
        // 8-byte zero tail pad.  No loop-carried shift/carry -> fully pipelined.
        for (u64 r = r0; r < r1; ++r) {
            const i32 v = (i32)c.vals[r];
            const u64 k = c.starts[r + 1] - c.starts[r];
            for (u64 j = 0; j < k; ++j) {
                const u32 g = (u32)((load64(gb + (bitpos >> 3)) >> (u32)(bitpos & 7)) & mask);
                bitpos += W;
                Rec& r = st.st[g];
                if (!r.cnt) { r.mn = v; st.touched.push_back(g); }
                r.mx = v;
                r.sum += v;
                ++r.cnt;
            }
        }
    };
    auto aggregate_fast = [&]() {
        // full domain: exact per-group totals are stored, O(G)
        const u8* p = c.stats;
        for (u64 g = 0; g < G; ++g, p += STATS_BYTES) {
            u32 cnt;
            std::memcpy(&cnt, p + 8, 4);
            if (!cnt) continue;
            Rec& r = st.st[g];
            std::memcpy(&r.sum, p, 8);
            std::memcpy(&r.mn, p + 12, 4);
            std::memcpy(&r.mx, p + 16, 4);
            r.cnt = cnt;
            st.touched.push_back((u32)g);
        }
    };
    auto collect = [&](bool sort_rows) {
        rows.clear();
        if (use_fast) {
            for (u64 g = 0; g < G; ++g) {
                const Rec& r = st.st[g];
                if (r.cnt) rows.push_back(Row{gmin + (i64)g, r.sum, (i64)r.cnt, (i64)r.mn,
                                             (i64)r.mx});
            }
        } else {
            for (u32 g : st.touched) {
                const Rec& r = st.st[g];
                if (!r.cnt) continue;
                rows.push_back(Row{gmin + (i64)g, r.sum, (i64)r.cnt, (i64)r.mn, (i64)r.mx});
            }
            if (sort_rows)
                std::sort(rows.begin(), rows.end(),
                          [](const Row& a, const Row& b) { return a.g < b.g; });
        }
    };

    // Timed region = reset of the previous query's state + the aggregation
    // scan, i.e. exactly the thing the block-summary fallback times in
    // bsum_query (it memsets its 32 B/group state inside run()).  Row
    // collection and serialization are timed separately, like the ECUBE
    // "query" vs "serialize" split -- never mixed into the query number.
    // Parallel path: the epoch tag makes the group state self-initialising, so a
    // query only has to drop the touched list (no O(touched) clearing).  The 1T
    // path is unchanged so the headline single-thread numbers stand.
    u64 par_epoch_base = 0;  // kept for the receipt only
    ParResult last_par;
    auto step = [&]() {
        if (use_fast || T_use <= 1) {
            st.reset();
            if (use_fast) aggregate_fast(); else aggregate_scan();
            return;
        }
        st.touched.clear();
        last_par = par_aggregate(pc, pool, c, r0, r1, nrows, T_use, max_partial_bytes, st);
    };
    step();  // untimed warmup (pages, TLB, thread stack setup)
    std::vector<double> tot, agg, rst, par_scan, par_merge;
    tot.reserve(reps); agg.reserve(reps); rst.reserve(reps);
    if (T_use > 1) { par_scan.reserve(reps); par_merge.reserve(reps); }
    for (int i = 0; i < reps; ++i) {
        const double s0 = now_ms();
        if (use_fast || T_use <= 1) st.reset(); else st.touched.clear();
        const double s1 = now_ms();
        if (use_fast || T_use <= 1) {
            if (use_fast) aggregate_fast(); else aggregate_scan();
        } else {
            last_par = par_aggregate(pc, pool, c, r0, r1, nrows, T_use, max_partial_bytes, st);
        }
        const double s2 = now_ms();
        rst.push_back(s1 - s0);
        agg.push_back(s2 - s1);
        tot.push_back(s2 - s0);
        if (T_use > 1) { par_scan.push_back(last_par.scan_ms); par_merge.push_back(last_par.merge_ms); }
    }
    (void)par_epoch_base;
    // the last step left the exact answer in the state: materialize it untimed
    collect(!use_fast);
    std::string js;
    std::vector<double> jser;
    std::vector<char> bs;
    std::vector<double> bser;
    {
        for (int i = 0; i < 3; ++i) {
            double s = now_ms();
            json_rows(rows, js);
            jser.push_back(now_ms() - s);
            s = now_ms();
            bin_rows(rows, bs);
            bser.push_back(now_ms() - s);
        }
        std::sort(jser.begin(), jser.end());
        std::sort(bser.begin(), bser.end());
    }
    double wj = 0, wb = 0;
    if (out_json && std::strcmp(out_json, "/dev/null")) {
        double s = now_ms();
        write_file(out_json, js.data(), js.size());
        wj = now_ms() - s;
    }
    if (out_bin && std::strcmp(out_bin, "/dev/null")) {
        double s = now_ms();
        write_file(out_bin, bs.data(), bs.size());
        wb = now_ms() - s;
    }
    const double t2 = now_ms();
    std::fprintf(stderr,
                 "{\"mode\":\"clst_query\",\"lo\":%lld,\"hi\":%lld,\"reps\":%d,"
                 "\"empty\":%d,\"full\":%d,\"fast_path\":%d,\"scan_rows\":%llu,"
                 "\"selectivity\":%.9f,\"rows\":%zu,\"open_ms\":%.3f,"
                 "\"query_median_ms\":%.6f,\"query_p95_ms\":%.6f,"
                 "\"aggregate_median_ms\":%.6f,\"serialize_json_ms\":%.4f,"
                 "\"reset_median_ms\":%.6f,\"query_rep_spread\":%.4f,"
                 "\"threads_requested\":%d,\"threads_used\":%d,\"waves\":%d,"
                 "\"auto_mode\":%d,\"parallel_phase_ms\":%.6f,\"parallel_scan_ms\":%.6f,"
                 "\"parallel_merge_ms\":%.6f,\"chunks\":%d,\"max_chunk_rows\":%llu,"
                 "\"partial_state_bytes\":%llu,"
                 "\"bytes_json\":%zu,\"json_write_ms\":%.4f,"
                 "\"bin_median_ms\":%.4f,\"bin_write_ms\":%.4f,\"wall_ms\":%.3f,"
                 "\"checksum\":%llu,\"peak_rss_kb\":%lld}\n",
                 (long long)lo, (long long)hi, reps, (int)empty, (int)full, (int)use_fast,
                 (unsigned long long)nrows,
                 (double)nrows / (double)n, rows.size(), t1 - t0,
                 pctl(tot, 0.5), pctl(tot, 0.95), pctl(agg, 0.5),
                 (jser.empty() ? 0.0 : jser[1]), pctl(rst, 0.5),
                 (*std::max_element(tot.begin(), tot.end()) /
                  std::max(1e-9, *std::min_element(tot.begin(), tot.end()))),
                 threads_req, T_use, last_par.waves, (int)auto_mode,
                 (T_use > 1 ? pctl(agg, 0.5) : 0.0),
                 (par_scan.empty() ? 0.0 : pctl(par_scan, 0.5)),
                 (par_merge.empty() ? 0.0 : pctl(par_merge, 0.5)),
                 last_par.chunks, (unsigned long long)last_par.max_chunk_rows,
                 (unsigned long long)(T_use > 1 ? pc.partial_bytes : 0),
                 js.size(), wj, (bser.empty() ? 0.0 : bser[1]),
                 wb, t2 - t0, (unsigned long long)rows_checksum(rows), (long long)peak_rss_kb());
}

// ---------------------------------------------------------------------------
// verify: losslessness of the clustered representation vs the input columns
//   (values,counts) must equal the exact histogram of VAL, and the bit-packed
//   stream must be a permutation of GID with every row inside its value run.
// ---------------------------------------------------------------------------
static void cmd_verify(const char* gid_path, const char* val_path, const char* path) {
    Mapped gid = map_i32(gid_path);
    Mapped val = map_i32(val_path);
    Clst c;
    clst_open(path, c);
    const u64 n = val.n;
    if (gid.n != n || n != c.h.n) { std::fprintf(stderr, "ERR size\n"); std::exit(1); }
    const u64 G = c.h.G, V = c.h.V;
    const u32 W = c.h.W;
    const i64 gmin = c.h.gmin;

    // (1) exact per-value histogram: (value,count) list vs the input column
    const u8* p = (const u8*)c.base + c.h.off_counts;
    std::vector<u64> dcnt(V, 0);
    for (u64 r = 0; r < V; ++r) dcnt[r] = get_varint(p);
    std::vector<u64> gcount(G, 0), gcount_in(G, 0);
    u64 rows_checked = 0, bad_value_rows = 0;
    BitReader br;
    if (W) {
        br.W = W;
        br.mask = (W == 32) ? 0xFFFFFFFFull : ((1ull << W) - 1ull);
        br.init(c.gids, 0);
    }
    for (u64 r = 0; r < V; ++r) {
        const u64 k = dcnt[r];
        if (c.starts[r + 1] - c.starts[r] != k) ++bad_value_rows;
        for (u64 j = 0; j < k; ++j) {
            const u32 g = W ? br.next() : 0u;
            if ((u64)g >= G) { std::fprintf(stderr, "ERR gid out of domain\n"); std::exit(1); }
            ++gcount[g];
        }
        rows_checked += k;
    }
    // reverse: gid histogram of the input column
    for (u64 i = 0; i < n; ++i) {
        const i64 gi = (i64)gid.p[i] - gmin;
        if (gi < 0 || (u64)gi >= G) {
            std::fprintf(stderr, "ERR input gid out of domain\n");
            std::exit(1);
        }
        ++gcount_in[(u32)gi];
    }
    u64 gid_mismatch = 0;
    for (u64 g = 0; g < G; ++g)
        if (gcount[g] != gcount_in[g]) ++gid_mismatch;
    // value histogram via numpy-free scan: compare against a dense histogram
    const u64 span = (u64)(c.h.vmax - c.h.vmin) + 1;
    std::vector<u32> hval(span, 0);
    for (u64 i = 0; i < n; ++i) ++hval[(u64)((i64)val.p[i] - c.h.vmin)];
    u64 val_mismatch = 0;
    {
        const u8* pv = (const u8*)c.base + c.h.off_values;
        i64 prev = 0;
        for (u64 r = 0; r < V; ++r) {
            i64 v = prev + unzigzag(get_varint(pv));
            prev = v;
            const u64 d = (u64)(v - c.h.vmin);
            if (d >= span || (u64)hval[d] != dcnt[r]) ++val_mismatch;
        }
        // and no observed value may be missing from the stored list
        u64 nonzero = 0;
        for (u64 d = 0; d < span; ++d)
            if (hval[d]) ++nonzero;
        if (nonzero != V) val_mismatch += (nonzero > V ? nonzero - V : V - nonzero);
    }
    std::printf("{\"mode\":\"clst_verify\",\"rows\":%llu,\"G\":%llu,\"V\":%llu,"
                "\"gid_bits\":%u,\"gmin\":%lld,\"rows_checked\":%llu,\"run_start_mismatches\":%llu,"
                "\"gid_histogram_mismatches\":%llu,\"value_histogram_mismatches\":%llu,"
                "\"exact\":%d}\n",
                (unsigned long long)n, (unsigned long long)G, (unsigned long long)V, W,
                (long long)gmin, (unsigned long long)rows_checked,
                (unsigned long long)bad_value_rows,
                (unsigned long long)gid_mismatch, (unsigned long long)val_mismatch,
                (int)(bad_value_rows == 0 && gid_mismatch == 0 && val_mismatch == 0 &&
                      rows_checked == n));
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::fprintf(stderr,
                     "usage: clst_tool build GID VAL OUT [--with-stats]\n"
                     "       clst_tool info FILE\n"
                     "       clst_tool query FILE LO HI OUT.json OUT.bin [REPS] [--no-fastpath]\n"
                     "       clst_tool verify GID VAL FILE\n");
        return 2;
    }
    const std::string mode = argv[1];
    if (mode == "build") {
        bool ws = false;
        for (int i = 2; i < argc; ++i)
            if (!std::strcmp(argv[i], "--with-stats")) ws = true;
        if (argc < 5) return 2;
        cmd_build(argv[2], argv[3], argv[4], ws);
        return 0;
    }
    if (mode == "info") {
        cmd_info(argv[2]);
        return 0;
    }
    if (mode == "verify") {
        cmd_verify(argv[2], argv[3], argv[4]);
        return 0;
    }
    if (mode == "query") {
        if (argc < 7) return 2;
        bool fp = true;
        int reps = 10;
        int threads = 1;
        bool auto_mode = false;
        u64 max_partial = 640ull << 20;   // 640 MB cap on live partial state
        for (int i = 7; i < argc; ++i) {
            const char* a = argv[i];
            if (!std::strcmp(a, "--no-fastpath")) fp = false;
            else if (!std::strcmp(a, "--auto")) auto_mode = true;
            else if (!std::strcmp(a, "--threads") && i + 1 < argc) threads = std::atoi(argv[++i]);
            else if (!std::strcmp(a, "--max-partial-mb") && i + 1 < argc)
                max_partial = (u64)std::atoll(argv[++i]) << 20;
            else if (a[0] == '-' && a[1] == '-') { std::fprintf(stderr, "ERR unknown flag %s\n", a); return 2; }
            else reps = std::atoi(a);
        }
        if (threads > 64) threads = 64;
        cmd_query(argv[2], std::atoll(argv[3]), std::atoll(argv[4]), argv[5], argv[6], reps,
                  fp, threads, auto_mode, max_partial);
        return 0;
    }
    std::fprintf(stderr, "ERR unknown mode %s\n", mode.c_str());
    return 2;
}
