// tiers_tool.cpp -- structures the router picks when the dense ECUBE02 cube does
// not fit its O(G*V) space budget (group-cardinality tiers of the tiers stream).
//
//   stats_build  GID VAL OUT             materialized per-group stats, 32 B/group
//   stats_emit   STATS OUT.json OUT.bin REPS
//                                        engine-resident fixed-group query, then
//                                        JSON and binary result writers (timed
//                                        per format, never mixed)
//   bsum_build   GID VAL OUT [BLOCK]     exact per-block min/max summaries
//   bsum_query   BSUM GID VAL LO HI OUT.json OUT.bin REPS
//                                        pruned exact scan fallback for a value
//                                        range, dense per-group state
//
// The MSTAT01 / TIERBS01 files are single-threaded; one process = one open
// structure, so the reader is exactly the engine-resident protocol used for the
// ECUBE02 binary (open + warmup + REP timed executions, median and P95).
// Single-thread by construction: every timing run is pinned (nice -n 5 taskset -c 96).
#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <string>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

using u64 = uint64_t;
using i64 = int64_t;
using u32 = uint32_t;
using i32 = int32_t;

static constexpr const char* MMAGIC = "MSTAT01\n";   // 8 bytes, same as scale stream
static constexpr const char* BMAGIC = "TIERBS01";    // 8 bytes
static constexpr const char* XMAGIC = "TIERSB01";    // 8 bytes, binary result format
static constexpr u64 DEFAULT_BLOCK = 1024;

static double now_ms() {
    using clk = std::chrono::steady_clock;
    return std::chrono::duration<double, std::milli>(clk::now().time_since_epoch()).count();
}
static i64 peak_rss_kb() {
    struct rusage ru;
    getrusage(RUSAGE_SELF, &ru);
    return (i64)ru.ru_maxrss;
}

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

struct MHeader { char magic[8]; u64 n, ngroups, has_null, null_group; };
struct MRec { i64 sum, cnt, mn, mx; };
struct BHeader { char magic[8]; u64 n, block, nblocks; i64 vmin, vmax; };
struct BlockMM { i64 mn, mx; };
struct Row { i64 g, sum, cnt, mn, mx; };

struct FileBuf {
    std::vector<char> b;
    void put(const void* p, size_t n) { b.insert(b.end(), (const char*)p, (const char*)p + n); }
};
static void put_u64(FileBuf& f, u64 v) { f.put(&v, 8); }
static void put_i64(FileBuf& f, i64 v) { f.put(&v, 8); }

// JSON writer, byte-compatible with the ECUBE02 result format:
// {"rows":[["<group>","<sum>","<count>","<min>","<max>"],...]}
static void json_rows(const std::vector<Row>& rows, std::string& out, bool numeric_names) {
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
        if (numeric_names) {
            auto w = std::to_chars(buf, buf + sizeof(buf), (long long)r.g);
            out.append(buf, w.ptr);
        } else {
            out += 'g';
            auto w = std::to_chars(buf, buf + sizeof(buf), (long long)r.g);
            out.append(buf, w.ptr);
        }
        out += '"';
        auto app = [&](long long v) {
            out += ",\"";
            auto w = std::to_chars(buf, buf + sizeof(buf), v);
            out.append(buf, w.ptr);
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

// Binary result format: magic + u64 nrows + nrows * (i64 g, sum, cnt, mn, mx)
static void bin_rows(const std::vector<Row>& rows, std::vector<char>& out) {
    out.clear();
    out.reserve(16 + rows.size() * 40);
    out.insert(out.end(), XMAGIC, XMAGIC + 8);
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
static double pct(std::vector<double>& v, double p) {
    std::sort(v.begin(), v.end());
    return v[std::min(v.size() - 1, (size_t)(p * v.size()))];
}
static std::string fmtd(double v) { char b[32]; std::snprintf(b, sizeof b, "%.4f", v); return b; }

// ---------------------------------------------------------------------------
// materialized per-group stats: 32 B / group
// ---------------------------------------------------------------------------
static void cmd_stats_build(const char* gid_path, const char* val_path, const char* out_path) {
    double t0 = now_ms();
    Mapped gid = map_i32(gid_path);
    Mapped val = map_i32(val_path);
    if (gid.n != val.n) { std::fprintf(stderr, "ERR size mismatch\n"); std::exit(1); }
    const u64 n = gid.n;
    u64 G = 0;
    for (u64 i = 0; i < n; ++i) {
        i32 g = gid.p[i];
        if (g < 0) { std::fprintf(stderr, "ERR negative gid\n"); std::exit(1); }
        if (u64(g) + 1 > G) G = u64(g) + 1;
    }
    double t1 = now_ms();
    std::vector<MRec> R(G, MRec{0, 0, INT64_MAX, INT64_MIN});
    for (u64 i = 0; i < n; ++i) {
        MRec& r = R[(u64)gid.p[i]];
        i64 v = val.p[i];
        if (!r.cnt) { r.mn = r.mx = v; }
        else { if (v < r.mn) r.mn = v; if (v > r.mx) r.mx = v; }
        r.sum += v;
        r.cnt += 1;
    }
    double t2 = now_ms();
    MHeader h{};
    std::memcpy(h.magic, MMAGIC, 8);
    h.n = n;
    h.ngroups = G;
    h.has_null = 0;
    h.null_group = 0;
    FileBuf out;
    out.put(&h, sizeof(h));
    out.put(R.data(), R.size() * sizeof(MRec));
    write_file(out_path, out.b.data(), out.b.size());
    double t3 = now_ms();
    u64 tot_cnt = 0;
    i64 tot_sum = 0;
    for (const MRec& r : R) { tot_cnt += (u64)r.cnt; tot_sum += r.sum; }
    std::fprintf(stderr,
                 "{\"mode\":\"stats_build\",\"build_ms\":%.3f,\"domain_ms\":%.3f,"
                 "\"accum_ms\":%.3f,\"write_ms\":%.3f,\"rows\":%llu,\"groups\":%llu,"
                 "\"bytes\":%llu,\"bytes_per_group\":%zu,\"total_cnt\":%llu,"
                 "\"total_sum\":%lld,\"peak_rss_kb\":%lld}\n",
                 t3 - t0, t1 - t0, t2 - t1, t3 - t2, (unsigned long long)n,
                 (unsigned long long)G, (unsigned long long)out.b.size(), sizeof(MRec),
                 (unsigned long long)tot_cnt, (long long)tot_sum, (long long)peak_rss_kb());
}

static void load_stats(const char* path, MHeader& h, const MRec*& R, void*& base, size_t& bytes) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) { std::perror(path); std::exit(1); }
    struct stat st {};
    fstat(fd, &st);
    bytes = (size_t)st.st_size;
    base = mmap(nullptr, bytes, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (base == MAP_FAILED) { std::perror("mmap"); std::exit(1); }
    std::memcpy(&h, base, sizeof(h));
    if (std::memcmp(h.magic, MMAGIC, 8)) { std::fprintf(stderr, "ERR stats magic\n"); std::exit(1); }
    R = reinterpret_cast<const MRec*>((const char*)base + sizeof(MHeader));
}

static void cmd_stats_emit(const char* stats_path, const char* out_json, const char* out_bin,
                           int reps) {
    double t0 = now_ms();
    MHeader h{};
    const MRec* R = nullptr;
    void* base = nullptr;
    size_t bytes = 0;
    load_stats(stats_path, h, R, base, bytes);
    const u64 G = h.ngroups;
    double t1 = now_ms();
    std::vector<Row> rows;
    auto select_rows = [&]() {
        rows.clear();
        rows.reserve(G);
        for (u64 g = 0; g < G; ++g) {
            const MRec& r = R[g];
            if (!r.cnt) continue;
            rows.push_back(Row{(i64)g, r.sum, r.cnt, r.mn, r.mx});
        }
    };
    select_rows();
    std::vector<double> sel;
    sel.reserve(reps);
    for (int i = 0; i < reps; ++i) {
        double s = now_ms();
        select_rows();
        sel.push_back(now_ms() - s);
    }
    std::string js;
    std::vector<double> jser;
    jser.reserve(reps);
    for (int i = 0; i < reps; ++i) {
        double s = now_ms();
        json_rows(rows, js, true);
        jser.push_back(now_ms() - s);
    }
    std::vector<char> bs;
    std::vector<double> bser;
    bser.reserve(reps);
    for (int i = 0; i < reps; ++i) {
        double s = now_ms();
        bin_rows(rows, bs);
        bser.push_back(now_ms() - s);
    }
    // file writes, one measured write per format (page-cached target dir)
    double w0 = now_ms();
    write_file(out_json, js.data(), js.size());
    double wj = now_ms() - w0;
    w0 = now_ms();
    write_file(out_bin, bs.data(), bs.size());
    double wb = now_ms() - w0;
    double t2 = now_ms();
    const double json_ser = pct(jser, 0.5), json_p95 = pct(jser, 0.95);
    const double bin_ser = pct(bser, 0.5), bin_p95 = pct(bser, 0.95);
    const double sel_med = pct(sel, 0.5), sel_p95 = pct(sel, 0.95);
    std::fprintf(stderr,
                 "{\"mode\":\"stats_emit\",\"reps\":%d,\"groups\":%llu,\"groups_nonempty\":%zu,"
                 "\"open_ms\":%.3f,\"select_median_ms\":%.4f,\"select_p95_ms\":%.4f,"
                 "\"json_median_ms\":%.4f,\"json_p95_ms\":%.4f,\"bin_median_ms\":%.4f,"
                 "\"bin_p95_ms\":%.4f,\"json_write_ms\":%.4f,\"bin_write_ms\":%.4f,"
                 "\"json_bytes\":%zu,\"bin_bytes\":%zu,\"complete_json_ms\":%.4f,"
                 "\"complete_bin_ms\":%.4f,\"wall_ms\":%.3f,\"checksum\":%llu,"
                 "\"peak_rss_kb\":%lld}\n",
                 reps, (unsigned long long)G, rows.size(), t1 - t0, sel_med, sel_p95, json_ser,
                 json_p95, bin_ser, bin_p95, wj, wb, js.size(), bs.size(),
                 sel_med + json_ser + wj, sel_med + bin_ser + wb, t2 - t0, rows_checksum(rows),
                 (long long)peak_rss_kb());
}

// ---------------------------------------------------------------------------
// block min/max summaries + exact pruned-scan range fallback
// ---------------------------------------------------------------------------
static void cmd_bsum_build(const char* gid_path, const char* val_path, const char* out_path,
                           u64 block) {
    double t0 = now_ms();
    Mapped gid = map_i32(gid_path);
    Mapped val = map_i32(val_path);
    if (gid.n != val.n) { std::fprintf(stderr, "ERR size\n"); std::exit(1); }
    const u64 n = gid.n;
    if (block < 1) block = 1;
    const u64 nblocks = (n + block - 1) / block;
    std::vector<BlockMM> B(nblocks, BlockMM{INT64_MAX, INT64_MIN});
    double t1 = now_ms();
    for (u64 b = 0; b < nblocks; ++b) {
        const u64 i0 = b * block, i1 = std::min(n, i0 + block);
        i64 mn = INT64_MAX, mx = INT64_MIN;
        for (u64 i = i0; i < i1; ++i) {
            i64 v = val.p[i];
            if (v < mn) mn = v;
            if (v > mx) mx = v;
        }
        B[b] = BlockMM{mn, mx};
    }
    double t2 = now_ms();
    BHeader h{};
    std::memcpy(h.magic, BMAGIC, 8);
    h.n = n;
    h.block = block;
    h.nblocks = nblocks;
    h.vmin = B.empty() ? 0 : B[0].mn;
    h.vmax = B.empty() ? 0 : B[0].mx;
    for (const BlockMM& b : B) {
        h.vmin = std::min(h.vmin, b.mn);
        h.vmax = std::max(h.vmax, b.mx);
    }
    FileBuf out;
    out.put(&h, sizeof(h));
    out.put(B.data(), B.size() * sizeof(BlockMM));
    write_file(out_path, out.b.data(), out.b.size());
    double t3 = now_ms();
    std::fprintf(stderr,
                 "{\"mode\":\"bsum_build\",\"build_ms\":%.3f,\"scan_ms\":%.3f,"
                 "\"write_ms\":%.3f,\"rows\":%llu,\"block\":%llu,\"blocks\":%llu,"
                 "\"bytes\":%llu,\"vmin\":%lld,\"vmax\":%lld,\"peak_rss_kb\":%lld}\n",
                 t3 - t0, t2 - t1, t3 - t2, (unsigned long long)n, (unsigned long long)block,
                 (unsigned long long)nblocks, (unsigned long long)out.b.size(),
                 (long long)h.vmin, (long long)h.vmax, (long long)peak_rss_kb());
}

static void cmd_bsum_query(const char* bpath, const char* gid_path, const char* val_path,
                           i64 lo, i64 hi, const char* out_json, const char* out_bin, int reps) {
    double t0 = now_ms();
    int fd = open(bpath, O_RDONLY);
    if (fd < 0) { std::perror(bpath); std::exit(1); }
    struct stat st {};
    fstat(fd, &st);
    const size_t bbytes = (size_t)st.st_size;
    void* bbase = mmap(nullptr, bbytes, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    BHeader h{};
    std::memcpy(&h, bbase, sizeof(h));
    if (std::memcmp(h.magic, BMAGIC, 8)) { std::fprintf(stderr, "ERR bsum magic\n"); std::exit(1); }
    const BlockMM* B = reinterpret_cast<const BlockMM*>((const char*)bbase + sizeof(BHeader));
    Mapped gid = map_i32(gid_path);
    Mapped val = map_i32(val_path);
    if (gid.n != val.n || gid.n != h.n) { std::fprintf(stderr, "ERR size\n"); std::exit(1); }
    const u64 n = gid.n;
    u64 G = 0;
    for (u64 i = 0; i < n; ++i) {
        i32 g = gid.p[i];
        if (u64(g) + 1 > G) G = u64(g) + 1;
    }
    const double t1 = now_ms();
    std::vector<i64> sum(G, 0), cnt(G, 0), mn(G, INT64_MAX), mx(G, INT64_MIN);
    auto run = [&](u64& ncand) {
        std::memset(sum.data(), 0, G * sizeof(i64));
        std::memset(cnt.data(), 0, G * sizeof(i64));
        std::fill(mn.begin(), mn.end(), INT64_MAX);
        std::fill(mx.begin(), mx.end(), INT64_MIN);
        ncand = 0;
        for (u64 b = 0; b < h.nblocks; ++b) {
            if (B[b].mx < lo || B[b].mn > hi) continue;
            ++ncand;
            const u64 i0 = b * h.block, i1 = std::min(n, i0 + h.block);
            for (u64 i = i0; i < i1; ++i) {
                i64 v = val.p[i];
                if (v < lo || v > hi) continue;
                u64 g = (u64)gid.p[i];
                if (!cnt[g]) { mn[g] = mx[g] = v; }
                else { if (v < mn[g]) mn[g] = v; if (v > mx[g]) mx[g] = v; }
                sum[g] += v;
                cnt[g] += 1;
            }
        }
    };
    u64 ncand = 0;
    run(ncand);  // warmup
    std::vector<double> q;
    q.reserve(reps);
    u64 chk = 0;
    for (int i = 0; i < reps; ++i) {
        double s = now_ms();
        run(ncand);
        q.push_back(now_ms() - s);
        u64 c = 0;
        for (u64 g = 0; g < G; ++g)
            c = (c ^ (u64)sum[g] ^ (u64)cnt[g] ^ (u64)mn[g] ^ (u64)mx[g]) * 1099511628211ULL;
        chk += c;
    }
    std::vector<Row> rows;
    rows.reserve(G);
    for (u64 g = 0; g < G; ++g)
        if (cnt[g]) rows.push_back(Row{(i64)g, sum[g], cnt[g], mn[g], mx[g]});
    std::string js;
    std::vector<char> bs;
    double s0 = now_ms();
    json_rows(rows, js, true);
    double jser = now_ms() - s0;
    s0 = now_ms();
    bin_rows(rows, bs);
    double bser = now_ms() - s0;
    s0 = now_ms();
    write_file(out_json, js.data(), js.size());
    double wj = now_ms() - s0;
    s0 = now_ms();
    write_file(out_bin, bs.data(), bs.size());
    double wb = now_ms() - s0;
    const double t2 = now_ms();
    const double qmed = pct(q, 0.5), qp95 = pct(q, 0.95);
    u64 total_cnt = 0;
    i64 total_sum = 0, gmn = INT64_MAX, gmx = INT64_MIN;
    for (const Row& r : rows) {
        total_cnt += (u64)r.cnt;
        total_sum += r.sum;
        gmn = std::min(gmn, r.mn);
        gmx = std::max(gmx, r.mx);
    }
    std::fprintf(stderr,
                 "{\"mode\":\"bsum_query\",\"reps\":%d,\"range_lo\":%lld,\"range_hi\":%lld,"
                 "\"query_median_ms\":%.4f,\"query_p95_ms\":%.4f,\"setup_ms\":%.3f,"
                 "\"candidate_blocks\":%llu,\"total_blocks\":%llu,\"candidate_fraction\":%.6f,"
                 "\"rows\":%zu,\"groups\":%llu,\"json_median_ms\":%.4f,\"bin_median_ms\":%.4f,"
                 "\"json_write_ms\":%.4f,\"bin_write_ms\":%.4f,\"json_bytes\":%zu,"
                 "\"bin_bytes\":%zu,\"complete_json_ms\":%.4f,\"complete_bin_ms\":%.4f,"
                 "\"total_cnt\":%llu,\"total_sum\":%lld,\"min_mn\":%lld,\"max_mx\":%lld,"
                 "\"checksum\":%llu,\"peak_rss_kb\":%lld}\n",
                 reps, (long long)lo, (long long)hi, qmed, qp95, t1 - t0,
                 (unsigned long long)ncand, (unsigned long long)h.nblocks,
                 h.nblocks ? double(ncand) / double(h.nblocks) : 0.0, rows.size(),
                 (unsigned long long)G, jser, bser, wj, wb, js.size(), bs.size(),
                 qmed + jser + wj, qmed + bser + wb, (unsigned long long)total_cnt,
                 (long long)total_sum, (long long)gmn, (long long)gmx, chk,
                 (long long)peak_rss_kb());
    (void)t2;
}


// ---------------------------------------------------------------------------
// complete_bench: the full complete-result path per repetition (select rows +
// serialize + write the file), JSON and binary timed in separate processes.
// usage: complete_bench STATS OUT MODE REPS      MODE = json | bin
// ---------------------------------------------------------------------------
static void cmd_complete_bench(const char* stats_path, const char* out_path, const char* mode,
                               int reps) {
    double t0 = now_ms();
    MHeader h{};
    const MRec* R = nullptr;
    void* base = nullptr;
    size_t bytes = 0;
    load_stats(stats_path, h, R, base, bytes);
    const double open_ms = now_ms() - t0;
    const bool is_json = std::strcmp(mode, "json") == 0;
    const u64 G = h.ngroups;
    std::vector<Row> rows;
    auto select_rows = [&]() {
        rows.clear();
        rows.reserve(G);
        for (u64 g = 0; g < G; ++g) {
            const MRec& r = R[g];
            if (!r.cnt) continue;
            rows.push_back(Row{(i64)g, r.sum, r.cnt, r.mn, r.mx});
        }
    };
    select_rows();
    std::string js;
    std::vector<char> bs;
    std::vector<double> sel((size_t)reps), ser((size_t)reps), tot((size_t)reps);
    double tw = 0;
    for (int i = 0; i < reps; ++i) {
        double a = now_ms();
        select_rows();
        double b = now_ms();
        if (is_json) json_rows(rows, js, true); else bin_rows(rows, bs);
        double c = now_ms();
        write_file(out_path, is_json ? (const void*)js.data() : (const void*)bs.data(),
                   is_json ? js.size() : bs.size());
        double d = now_ms();
        sel[i] = b - a; ser[i] = c - b; tot[i] = d - a; tw = d - a;
    }
    const u64 out_bytes = is_json ? js.size() : bs.size();
    const double t1 = now_ms();
    std::fprintf(stderr,
                 "{\"mode\":\"complete_bench\",\"format\":\"%s\",\"reps\":%d,\"groups\":%llu,"
                 "\"rows\":%zu,\"open_ms\":%.3f,\"select_median_ms\":%.4f,"
                 "\"serialize_median_ms\":%.4f,\"serialize_p95_ms\":%.4f,"
                 "\"complete_median_ms\":%.4f,\"complete_p95_ms\":%.4f,"
                 "\"complete_min_ms\":%.4f,\"complete_max_ms\":%.4f,\"out_bytes\":%llu,"
                 "\"checksum\":%llu,\"peak_rss_kb\":%lld}\n",
                 is_json ? "json" : "binary", reps, (unsigned long long)G, rows.size(),
                 open_ms, pct(sel, 0.5), pct(ser, 0.5), pct(ser, 0.95), pct(tot, 0.5),
                 pct(tot, 0.95), *std::min_element(tot.begin(), tot.end()),
                 *std::max_element(tot.begin(), tot.end()), (unsigned long long)out_bytes,
                 rows_checksum(rows), (long long)peak_rss_kb());
    (void)t1; (void)tw;
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::fprintf(stderr,
                     "usage:\n"
                     "  tiers_tool stats_build GID VAL OUT\n"
                     "  tiers_tool stats_emit STATS OUT.json OUT.bin REPS\n"
                     "  tiers_tool bsum_build GID VAL OUT [BLOCK]\n"
                     "  tiers_tool bsum_query BSUM GID VAL LO HI OUT.json OUT.bin REPS\n"
                     "  tiers_tool complete_bench STATS OUT MODE(json|bin) REPS\n");
        return 2;
    }
    std::string m = argv[1];
    if (m == "stats_build" && argc >= 5) {
        cmd_stats_build(argv[2], argv[3], argv[4]);
        return 0;
    }
    if (m == "stats_emit" && argc >= 6) {
        cmd_stats_emit(argv[2], argv[3], argv[4], std::stoi(argv[5]));
        return 0;
    }
    if (m == "bsum_build" && argc >= 5) {
        cmd_bsum_build(argv[2], argv[3], argv[4], argc >= 6 ? std::strtoull(argv[5], nullptr, 10) : DEFAULT_BLOCK);
        return 0;
    }
    if (m == "bsum_query" && argc >= 10) {
        cmd_bsum_query(argv[2], argv[3], argv[4], std::stoll(argv[5]), std::stoll(argv[6]),
                       argv[7], argv[8], std::stoi(argv[9]));
        return 0;
    }
    if (m == "complete_bench" && argc >= 6) {
        cmd_complete_bench(argv[2], argv[3], argv[4], std::stoi(argv[5]));
        return 0;
    }
    std::fprintf(stderr, "ERR bad mode/args\n");
    return 2;
}
