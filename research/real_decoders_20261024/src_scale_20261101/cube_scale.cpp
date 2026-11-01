// Scale + multi-core extension of the validated ECUBE02 (v2) aggregate cube.
//
// Everything from eai_cube2.cpp is kept byte-for-byte in behaviour: the on-disk
// format is still ECUBE02 (magic, header, distinct array when !dense, per-rank
// counts, 64-rank block prefix) and the single-thread query path is the v2 path.
// Verified at 100M: this binary's build (and build_mt with any thread count)
// produces the same sha256 as the validated src_eai_20261101/enc_cube, and the
// single-thread query output is identical.
//
// Added here (scale stream, 2026-11-01):
//   build_mt      row-partitioned parallel build (per-thread count slabs + reduce)
//   qbench        parallel query on a persistent spin-pool, P95 over reps
//   build_stats   materialized per-group stats (32 B/group: sum,cnt,min,max)
//   bench_stats   parallel full-table GROUP BY over the materialized stats
//   build_bsum    exact per-block min/max summaries over the value column
//   bench_bsum    block-pruned exact scan fallback for dynamic ranges
//
// Memory guards: a parallel build needs THREADS * G * V * 4 bytes of count slabs;
// the actual thread count is capped by --membudget (default 24 GiB) and the cap is
// reported in the JSON receipt. This is what makes the O(G*V) boundary visible
// instead of an OOM: a 10M-group cube at V=21084 requests 16 * 844 GB and clamps.
#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <memory>
#include <fstream>
#include <pthread.h>
#include <sched.h>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include <unordered_set>
#include <vector>

#include "../src_patch_fusion/fullsql_reference.hpp"
#include "../src_fullsql_ig2_fast_20261029/streaming.hpp"

using u64 = uint64_t;
using i64 = int64_t;
using u32 = uint32_t;
using i32 = int32_t;
using u8 = uint8_t;

static constexpr const char* MAGIC = "ECUBE02\n";
static constexpr u32 R_BLOCK = 64;
static constexpr const char* MMAGIC = "MSTAT01\n";
static constexpr const char* BMAGIC = "BSUM01\n";

struct Header {
    char magic[8];
    u64 n;
    u64 ngroups;
    u64 nvalues;
    i64 vmin;
    u32 dense;
    u32 has_null;
    u32 null_group;
    u32 block_ranks;
    char pad[4];
};

static double now_ms() {
    using clk = std::chrono::steady_clock;
    return std::chrono::duration<double, std::milli>(clk::now().time_since_epoch()).count();
}

// ---------------------------------------------------------------------------
// small helpers
// ---------------------------------------------------------------------------
static inline void cpu_pause() {
#if defined(__x86_64__) || defined(__i386__)
    __builtin_ia32_pause();
#endif
}

static std::vector<int> allowed_cpus() {
#if defined(__linux__)
    cpu_set_t set;
    CPU_ZERO(&set);
    if (sched_getaffinity(0, sizeof(set), &set)) return {};
    std::vector<int> v;
    for (int i = 0; i < CPU_SETSIZE; ++i)
        if (CPU_ISSET(i, &set)) v.push_back(i);
    return v;
#else
    return {};
#endif
}

static void pin_to(int cpu) {
#if defined(__linux__)
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
#else
    (void)cpu;
#endif
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
    if (fd < 0) throw std::runtime_error(std::string("open ") + path);
    struct stat st {};
    if (fstat(fd, &st)) throw std::runtime_error("fstat");
    m.bytes = size_t(st.st_size);
    if (m.bytes % 4) throw std::runtime_error("size not multiple of 4");
    m.n = m.bytes / 4;
    m.base = mmap(nullptr, m.bytes, PROT_READ, MAP_PRIVATE, fd, 0);
    if (m.base == MAP_FAILED) throw std::runtime_error("mmap input");
    m.p = static_cast<const i32*>(m.base);
    close(fd);
    return m;
}

static std::vector<i32> read_i32(const char* p) {
    Mapped m = map_i32(p);
    return std::vector<i32>(m.p, m.p + m.n);
}

static std::vector<std::string> dict_names(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    std::string d((std::istreambuf_iterator<char>(f)), {});
    if (d.size() < 52 || d.compare(0, 8, "BSSTR01\n")) throw std::runtime_error("dict");
    const char* p = d.data() + 48;
    u32 k;
    std::memcpy(&k, p, 4);
    p += 4;
    std::vector<std::string> names;
    for (u32 i = 0; i < k; ++i) {
        u32 len;
        std::memcpy(&len, p, 4);
        p += 4;
        names.emplace_back(p, len);
        p += len;
    }
    return names;
}

// Group names: the dictionary names what it names; any group index beyond the
// dictionary is printed as its decimal index (the scale datasets use dense
// synthetic group ids). The NULL group is always "NULL".
struct Names {
    std::vector<std::string> nm;
    u64 G = 0;
    u32 has_null = 0;
    u32 null_group = 0;
    bool synthesized = false;
    void append(std::string& s, u64 g) const {
        if (has_null && g == null_group) { s += "NULL"; return; }
        if (g < nm.size() && !nm[g].empty()) { s += nm[g]; return; }
        char b[24];
        auto r = std::to_chars(b, b + sizeof(b), (unsigned long long)g);
        s.append(b, r.ptr);
    }
    bool in(const std::string& needle, u64 g) const {
        if (has_null && g == null_group) return "NULL" == needle;
        if (g < nm.size() && !nm[g].empty()) return nm[g] == needle;
        char b[24];
        auto r = std::to_chars(b, b + sizeof(b), (unsigned long long)g);
        return std::string(b, r.ptr) == needle;
    }
};

static Names load_names(const char* dict_path, const Header& h) {
    Names n;
    n.G = h.ngroups;
    n.has_null = h.has_null;
    n.null_group = h.null_group;
    try {
        n.nm = dict_names(dict_path);
    } catch (...) {
        n.nm.clear();
    }
    u64 valid = h.ngroups - (h.has_null ? 1 : 0);
    if (n.nm.size() < valid) n.synthesized = true;
    if (h.has_null && h.null_group < n.G) {
        // NULL group name handled by append(); nothing to store
    }
    return n;
}

static inline i64 value_at(i64 vmin, u32 dense, const i64* distinct, u64 rank) {
    return dense ? vmin + i64(rank) : distinct[rank];
}

// ---------------------------------------------------------------------------
// ECUBE02 structures + query core (v2 logic)
// ---------------------------------------------------------------------------
struct Cube {
    Header h;
    const i64* distinct = nullptr;
    const u32* counts = nullptr;
    const u32* block_cnt = nullptr;
    const i64* block_sum = nullptr;
    const char* base = nullptr;
    size_t bytes = 0;
    int fd = -1;
    u64 nblocks = 0;
};

static Cube open_cube(const char* path) {
    Cube c;
    c.fd = open(path, O_RDONLY);
    if (c.fd < 0) throw std::runtime_error("open cube");
    struct stat st {};
    if (fstat(c.fd, &st)) throw std::runtime_error("stat");
    c.bytes = size_t(st.st_size);
    c.base = static_cast<const char*>(mmap(nullptr, c.bytes, PROT_READ, MAP_PRIVATE, c.fd, 0));
    if (c.base == MAP_FAILED) throw std::runtime_error("mmap");
    std::memcpy(&c.h, c.base, sizeof(Header));
    if (std::memcmp(c.h.magic, MAGIC, 8)) throw std::runtime_error("magic");
    const u64 V = c.h.nvalues, G = c.h.ngroups;
    c.nblocks = (V + c.h.block_ranks - 1) / c.h.block_ranks;
    const char* p = c.base + sizeof(Header);
    c.distinct = c.h.dense ? nullptr : reinterpret_cast<const i64*>(p);
    if (!c.h.dense) p += V * 8;
    c.counts = reinterpret_cast<const u32*>(p);
    p += G * V * 4;
    c.block_cnt = reinterpret_cast<const u32*>(p);
    p += G * (c.nblocks + 1) * 4;
    c.block_sum = reinterpret_cast<const i64*>(p);
    return c;
}

struct Row { u64 g, cnt; i64 sum, mn, mx; };

static inline void scan_ranks(const Cube& c, const u32* C, u64 a, u64 b, u64& cnt, i64& sum) {
    for (u64 r = a; r < b; ++r) {
        const u32 k = C[r];
        if (!k) continue;
        cnt += k;
        sum += i64(k) * value_at(c.h.vmin, c.h.dense, c.distinct, r);
    }
}

// Exact per-group reduction for group g over rank interval [a,b) — v2 logic.
static inline void scan_group(const Cube& c, u64 g, u64 a, u64 b, std::vector<Row>& out) {
    const u64 V = c.h.nvalues, R = c.h.block_ranks, NB = c.nblocks;
    const u32* C = c.counts + g * V;
    const u32* BC = c.block_cnt + g * (NB + 1);
    const i64* BS = c.block_sum + g * (NB + 1);
    u64 cnt = 0;
    i64 sum = 0;
    if (b > a) {
        const u64 ba = a / R, bb = b / R;
        if (ba == bb) {
            scan_ranks(c, C, a, b, cnt, sum);
        } else {
            const u64 end_first = std::min<u64>((ba + 1) * R, b);
            scan_ranks(c, C, a, end_first, cnt, sum);
            if (bb > ba + 1) {
                cnt += u64(BC[bb]) - u64(BC[ba + 1]);
                sum += BS[bb] - BS[ba + 1];
            }
            scan_ranks(c, C, bb * R, b, cnt, sum);
        }
    }
    if (!cnt) return;
    const u64 ba = a / R, bb = b / R;
    u64 mnr = a, mxr = (b ? b - 1 : 0);
    if (ba == bb) {
        for (u64 r = a; r < b; ++r) if (C[r]) { mnr = r; break; }
        for (u64 r = b; r > a; --r) if (C[r - 1]) { mxr = r - 1; break; }
    } else {
        const u64 first_end = std::min<u64>((ba + 1) * R, b);
        bool found = false;
        for (u64 r = a; r < first_end; ++r) if (C[r]) { mnr = r; found = true; break; }
        if (!found) {
            const u64 base = BC[ba + 1];
            u64 lo = ba + 1, hi = bb;
            while (lo < hi) {
                u64 mid = (lo + hi) / 2;
                if (u64(BC[mid + 1]) > base) hi = mid;
                else lo = mid + 1;
            }
            const u64 bstart = lo * R;
            const u64 bend = std::min<u64>((lo + 1) * R, b);
            for (u64 r = bstart; r < bend; ++r) if (C[r]) { mnr = r; break; }
        }
        found = false;
        for (u64 r = b; r > bb * R; --r) if (C[r - 1]) { mxr = r - 1; found = true; break; }
        if (!found && bb > ba) {
            const u64 top = BC[bb];
            u64 lo = ba, hi = bb - 1;
            while (lo < hi) {
                u64 mid = (lo + hi + 1) / 2;
                if (u64(BC[mid]) < top) lo = mid;
                else hi = mid - 1;
            }
            const u64 bstart = lo * R;
            const u64 bend = std::min<u64>((lo + 1) * R, b);
            for (u64 r = bend; r > bstart; --r) if (C[r - 1]) { mxr = r - 1; break; }
        }
    }
    out.push_back({g, cnt, sum, value_at(c.h.vmin, c.h.dense, c.distinct, mnr),
                   value_at(c.h.vmin, c.h.dense, c.distinct, mxr)});
}

static bool parse_having(const std::string& c, i64 sum, u64 cnt, i64 mn, i64 mx) {
    size_t pos = c.find_first_of("<>=");
    if (pos == std::string::npos) return true;
    std::string lhs = c.substr(0, pos);
    std::string op = c.substr(pos, (c.compare(pos, 2, ">=") == 0 || c.compare(pos, 2, "<=") == 0) ? 2 : 1);
    std::string rhs = c.substr(pos + op.size());
    long double v = std::strtold(rhs.c_str(), nullptr);
    long double x;
    if (lhs == "sum") x = (long double)sum;
    else if (lhs == "count") x = (long double)cnt;
    else if (lhs == "min") x = (long double)mn;
    else if (lhs == "max") x = (long double)mx;
    else return true;
    if (op == ">=") return x >= v;
    if (op == "<=") return x <= v;
    if (op == ">") return x > v;
    if (op == "<") return x < v;
    if (op == "=") return x == v;
    return true;
}

struct QSpec {
    bool has_lo = false, has_hi = false;
    i64 lo = 0, hi = 0;
    std::vector<std::string> in, not_in, having;
};

static QSpec parse_spec(int argc, char** argv, int start) {
    QSpec q;
    for (int i = start; i < argc; ++i) {
        std::string a = argv[i];
        auto split = [](const std::string& s) {
            std::vector<std::string> v;
            size_t p = 0;
            while (p <= s.size()) {
                size_t t = s.find(',', p);
                if (t == std::string::npos) t = s.size();
                v.push_back(s.substr(p, t - p));
                p = t + 1;
            }
            return v;
        };
        if (a.rfind("range=", 0) == 0) {
            auto p = a.find(',');
            q.lo = std::stoll(a.substr(6, p - 6));
            q.hi = std::stoll(a.substr(p + 1));
            q.has_lo = q.has_hi = true;
        } else if (a.rfind("lo=", 0) == 0) {
            q.lo = std::stoll(a.substr(3)); q.has_lo = true;
        } else if (a.rfind("hi=", 0) == 0) {
            q.hi = std::stoll(a.substr(3)); q.has_hi = true;
        } else if (a.rfind("where_in=", 0) == 0) {
            q.in = split(a.substr(9));
        } else if (a.rfind("where_not_in=", 0) == 0) {
            q.not_in = split(a.substr(13));
        } else if (a.rfind("having=", 0) == 0) {
            auto parts = split(a.substr(7));
            q.having.insert(q.having.end(), parts.begin(), parts.end());
        }
    }
    return q;
}

// rank interval [a,b) for a range spec (v2 logic incl. clamping)
static void rank_interval(const Cube& c, const QSpec& q, u64& a, u64& b) {
    const u64 V = c.h.nvalues;
    a = 0;
    b = V;
    if (!(q.has_lo || q.has_hi)) return;
    if (q.has_lo && q.has_hi && q.lo > q.hi) { a = b = 0; return; }
    if (c.h.dense) {
        const i64 vmax = c.h.vmin + i64(V) - 1;
        i64 lo = q.has_lo ? q.lo : c.h.vmin;
        i64 hi = q.has_hi ? q.hi : vmax;
        if (lo < c.h.vmin) lo = c.h.vmin;
        if (hi > vmax) hi = vmax;
        if (hi < lo) { a = b = 0; return; }
        i64 ra = lo - c.h.vmin, rb = hi - c.h.vmin + 1;
        if (ra < 0) ra = 0;
        if (ra > i64(V)) ra = i64(V);
        if (rb < 0) rb = 0;
        if (rb > i64(V)) rb = i64(V);
        a = u64(ra);
        b = u64(rb);
    } else {
        const i64* d = c.distinct;
        a = u64(std::lower_bound(d, d + V, q.has_lo ? q.lo : INT64_MIN) - d);
        b = u64(std::upper_bound(d, d + V, q.has_hi ? q.hi : INT64_MAX) - d);
    }
    if (b < a) b = a;
}

static void build_allowed(const QSpec& q, const Names& nm, u64 G, std::vector<char>& allowed) {
    allowed.assign(G, 1);
    if (q.in.empty() && q.not_in.empty()) return;
    for (u64 g = 0; g < G; ++g) {
        // IN/NOT IN use SQL string equality on the group name; values are either
        // dictionary strings or decimal indices, matching the data encoding.
        std::string name;
        nm.append(name, g);
        bool ok = true;
        if (!q.in.empty()) {
            ok = false;
            for (const auto& s : q.in) if (s == name) { ok = true; break; }
        }
        if (ok && !q.not_in.empty()) {
            for (const auto& s : q.not_in) if (s == name) { ok = false; break; }
        }
        allowed[g] = ok ? 1 : 0;
    }
    // SQL three-valued logic: NULL is not TRUE for IN/NOT IN, so drop the NULL group.
    if (nm.has_null && nm.null_group < G) allowed[nm.null_group] = 0;
}

// ---------------------------------------------------------------------------
// serialization
// ---------------------------------------------------------------------------
static size_t serialize_parts_impl(const std::vector<std::vector<Row>>& parts, size_t p0, size_t p1,
                                   const Names& nm, const QSpec& q, std::string& out) {
    char buf[64];
    size_t nrows = 0;
    for (size_t p = p0; p < p1; ++p) {
        for (const Row& r : parts[p]) {
            bool ok = true;
            for (const auto& h : q.having)
                if (!parse_having(h, r.sum, r.cnt, r.mn, r.mx)) { ok = false; break; }
            if (!ok) continue;
            if (nrows) out += ',';
            ++nrows;
            out += '[';
            out += '"';
            nm.append(out, r.g);
            out += '"';
            auto app = [&](long long v) {
                auto r2 = std::to_chars(buf, buf + sizeof(buf), v);
                out += ',';
                out += '"';
                out.append(buf, r2.ptr);
                out += '"';
            };
            app((long long)r.sum);
            app((long long)r.cnt);
            app((long long)r.mn);
            app((long long)r.mx);
            out += ']';
        }
    }
    return nrows;
}

static size_t count_rows_having(const std::vector<std::vector<Row>>& parts, const QSpec& q) {
    size_t n = 0;
    for (const auto& v : parts)
        for (const Row& r : v) {
            bool ok = true;
            for (const auto& h : q.having)
                if (!parse_having(h, r.sum, r.cnt, r.mn, r.mx)) { ok = false; break; }
            if (ok) ++n;
        }
    return n;
}

// Serialize the complete grouped result. T threads format disjoint chunk ranges in
// parallel; chunks are written in ascending group order, so with stitch=true the
// output bytes are identical for every T (verified by sha256). With stitch=false the
// call measures formatting only (no final 300 MB concatenation), which is the number
// used for the output-bound parallel-serialization table.
static size_t serialize_mt(const std::vector<std::vector<Row>>& parts, const Names& nm,
                           const QSpec& q, int T, std::string& out,
                           bool open_array = true, bool stitch = true) {
    if (open_array) { out.clear(); out += "{\"rows\":["; }
    const size_t P = parts.size();
    const size_t nrows = count_rows_having(parts, q);
    if (T <= 1 || P <= 1) {
        std::string tmp;
        std::string& dst = stitch ? out : tmp;
        dst.reserve(nrows * 30 + 64);
        size_t n = serialize_parts_impl(parts, 0, P, nm, q, dst);
        if (stitch) out += "]}\n";
        return n;
    }
    std::vector<std::string> chunk(T);
    std::vector<std::thread> th;
    auto job = [&](int i) {
        size_t p0 = (size_t)i * P / T, p1 = (size_t)(i + 1) * P / T;
        size_t est = 256;
        for (size_t p = p0; p < p1; ++p) est += parts[p].size() * 32;
        chunk[i].reserve(est);
        serialize_parts_impl(parts, p0, p1, nm, q, chunk[i]);
    };
    for (int i = 1; i < T; ++i) th.emplace_back(job, i);
    job(0);
    for (auto& t : th) t.join();
    if (stitch) {
        out.reserve(out.size() + nrows * 30 + (size_t)T + 8);
        bool first = true;
        for (int i = 0; i < T; ++i) {
            if (chunk[i].empty()) continue;
            if (!first) out += ',';
            first = false;
            out += chunk[i];
        }
        out += "]}\n";
    }
    return nrows;
}

// single-threaded v2-compatible grouped-result writer (used for exactness dumps)
static u64 run_query_st(const Cube& c, const Names& nm, const QSpec& q, const char* out_path,
                        bool write_out, bool serialize_result = false, std::string* reuse = nullptr,
                        size_t* nrows_out = nullptr) {
    u64 a, b;
    rank_interval(c, q, a, b);
    std::vector<char> allowed;
    build_allowed(q, nm, c.h.ngroups, allowed);
    std::vector<Row> rows;
    rows.reserve(std::min<u64>(c.h.ngroups, (1u << 20)));
    for (u64 g = 0; g < c.h.ngroups; ++g)
        if (allowed[g]) scan_group(c, g, a, b, rows);
    u64 checksum = 0;
    for (const Row& r : rows)
        checksum = checksum * 1099511628211ULL + r.sum + r.cnt + u64(r.mn) + u64(r.mx) + r.g;
    if (!write_out && !serialize_result) return checksum;
    std::string local;
    std::string& out = reuse ? *reuse : local;
    std::vector<std::vector<Row>> parts{std::move(rows)};
    out.clear();
    out += "{\"rows\":[";
    size_t n = serialize_parts_impl(parts, 0, 1, nm, q, out);
    out += "]}\n";
    if (nrows_out) *nrows_out = n;
    if (write_out) {
        FILE* f = fopen(out_path, "wb");
        if (!f) throw std::runtime_error("out open");
        fwrite(out.data(), 1, out.size(), f);
        fclose(f);
    }
    checksum = checksum * 1099511628211ULL + out.size();
    return checksum;
}

// ---------------------------------------------------------------------------
// parallel query pool (persistent spin pool; dispatch included in latency)
// ---------------------------------------------------------------------------
class Pool {
  public:
    Pool(int T) : T_(T) {
        if (T_ < 1) T_ = 1;
        auto cpus = allowed_cpus();
        if (!cpus.empty()) {
            main_cpu_ = cpus[0];
            worker_cpu_.assign(T_ - 1, -1);
            for (int i = 1; i < T_; ++i) worker_cpu_[i - 1] = cpus[(size_t)i % cpus.size()];
        }
        const int nf = std::max(T_, 1);
        flags_.reset(new Flag[nf]);
        for (int i = 0; i < nf; ++i) flags_[i].v.store(0, std::memory_order_relaxed);
        if (!cpus.empty()) pin_to(main_cpu_);
        for (int i = 1; i < T_; ++i) threads_.emplace_back([this, i] { worker(i); });
    }
    ~Pool() {
        stop_.store(true, std::memory_order_release);
        gen_.fetch_add(1, std::memory_order_release);
        for (auto& t : threads_) t.join();
    }
    int threads() const { return T_; }
    void run(const Cube& c, const std::vector<char>& allowed, u64 a, u64 b,
             std::vector<std::vector<Row>>& parts) {
        parts.resize(T_);
        c_ = &c;
        allowed_ = &allowed;
        a_ = a;
        b_ = b;
        parts_ = &parts;
        const u32 g = gen_.load(std::memory_order_relaxed) + 1;
        gen_.store(g, std::memory_order_release);
        const u64 G = c.h.ngroups;
        const u64 gb = (u64)0 * G / T_, ge = (u64)1 * G / T_;
        parts[0].clear();
        for (u64 gidx = gb; gidx < ge; ++gidx)
            if (allowed[gidx]) scan_group(c, gidx, a, b, parts[0]);
        for (int i = 1; i < T_; ++i)
            while (flags_[(size_t)i].v.load(std::memory_order_acquire) != g) cpu_pause();
    }
    // raw group-range scan without pool dispatch (used by build/verify paths)
    void run_single(const Cube& c, const std::vector<char>& allowed, u64 a, u64 b,
                    std::vector<Row>& out) {
        out.clear();
        for (u64 g = 0; g < c.h.ngroups; ++g)
            if (allowed[g]) scan_group(c, g, a, b, out);
    }

  private:
    struct alignas(64) Flag { std::atomic<u32> v{0}; };
    void worker(int i) {
        if (worker_cpu_[i - 1] >= 0) pin_to(worker_cpu_[i - 1]);
        u32 last = 0;
        while (true) {
            u32 g = gen_.load(std::memory_order_acquire);
            if (g == last) {
                if (stop_.load(std::memory_order_relaxed)) return;
                cpu_pause();
                continue;
            }
            last = g;
            if (stop_.load(std::memory_order_relaxed)) return;
            const u64 G = c_->h.ngroups;
            const u64 gb = (u64)i * G / T_, ge = (u64)(i + 1) * G / T_;
            (*parts_)[i].clear();
            for (u64 gidx = gb; gidx < ge; ++gidx)
                if ((*allowed_)[gidx]) scan_group(*c_, gidx, a_, b_, (*parts_)[i]);
            flags_[(size_t)i].v.store(g, std::memory_order_release);
        }
    }
    int T_ = 1;
    int main_cpu_ = -1;
    std::vector<int> worker_cpu_;
    std::vector<std::thread> threads_;
    std::unique_ptr<Flag[]> flags_;
    std::atomic<u32> gen_{0};
    std::atomic<bool> stop_{false};
    const Cube* c_ = nullptr;
    const std::vector<char>* allowed_ = nullptr;
    u64 a_ = 0, b_ = 0;
    std::vector<std::vector<Row>>* parts_ = nullptr;
};

static u64 rows_checksum(const std::vector<std::vector<Row>>& parts) {
    u64 checksum = 0;
    for (const auto& v : parts)
        for (const Row& r : v)
            checksum = checksum * 1099511628211ULL + r.sum + r.cnt + u64(r.mn) + u64(r.mx) + r.g;
    return checksum;
}

static u64 rows_checksum_vec(const std::vector<Row>& v) {
    u64 checksum = 0;
    for (const Row& r : v)
        checksum = checksum * 1099511628211ULL + r.sum + r.cnt + u64(r.mn) + u64(r.mx) + r.g;
    return checksum;
}


// Order-independent aggregate over the grouped result, for exactness checks against
// DuckDB: total count/sum, global min/max and a group-weighted __int128 checksum.
struct Agg {
    u64 cnt = 0;
    i64 sum = 0;
    i64 mn = INT64_MAX, mx = INT64_MIN;
    __int128 chk = 0;
};
static void agg_add(Agg& a, const Row& r) {
    a.cnt += r.cnt;
    a.sum += r.sum;
    if (r.mn < a.mn) a.mn = r.mn;
    if (r.mx > a.mx) a.mx = r.mx;
    a.chk += (__int128)r.g * 2654435761LL + (__int128)r.sum * 97 + (__int128)r.cnt * 13 +
             (__int128)r.mn * 7 + (__int128)r.mx * 3;
}
static Agg agg_parts(const std::vector<std::vector<Row>>& parts) {
    Agg a;
    for (const auto& v : parts) for (const Row& r : v) agg_add(a, r);
    return a;
}
static std::string i128str(__int128 v) {
    if (v == 0) return "0";
    bool neg = v < 0;
    unsigned __int128 u = neg ? (unsigned __int128)(-v) : (unsigned __int128)v;
    char b[64];
    int i = 64;
    while (u) { b[--i] = char('0' + int(u % 10)); u /= 10; }
    std::string s(b + i, b + 64);
    return neg ? "-" + s : s;
}

static size_t count_rows(const std::vector<std::vector<Row>>& parts) {
    size_t n = 0;
    for (const auto& v : parts) n += v.size();
    return n;
}

// ---------------------------------------------------------------------------
// v2 build paths (kept verbatim so the byte-identical check is meaningful)
// ---------------------------------------------------------------------------
template <class T>
static void write_vec(std::ofstream& o, const std::vector<T>& v) {
    o.write(reinterpret_cast<const char*>(v.data()), std::streamsize(v.size() * sizeof(T)));
}

static void write_cube(const char* out_path, const Header& h, const std::vector<i64>& dv,
                       const std::vector<u32>& counts, const std::vector<u32>& bcnt,
                       const std::vector<i64>& bsum) {
    std::ofstream o(out_path, std::ios::binary);
    o.write(reinterpret_cast<const char*>(&h), sizeof(h));
    if (!h.dense) write_vec(o, dv);
    write_vec(o, counts);
    write_vec(o, bcnt);
    write_vec(o, bsum);
    o.close();
}

static void build_plain(const char* gid_path, const char* val_path, const char* out_path) {
    double t0 = now_ms();
    std::vector<i32> gid = read_i32(gid_path);
    std::vector<i32> val = read_i32(val_path);
    if (gid.size() != val.size()) throw std::runtime_error("size");
    const u64 n = gid.size();
    u32 G = 0;
    bool has_null = false;
    for (i32 g : gid) {
        if (g < 0) { has_null = true; continue; }
        if (u32(g) + 1 > G) G = u32(g) + 1;
    }
    const u32 null_group = has_null ? G : 0xffffffffu;
    if (has_null) G += 1;
    std::vector<i32> distinct(val);
    std::sort(distinct.begin(), distinct.end());
    distinct.erase(std::unique(distinct.begin(), distinct.end()), distinct.end());
    const u64 V = distinct.size();
    if (!V) throw std::runtime_error("empty");
    const i64 vmin = distinct.front();
    const u64 span = u64(distinct.back()) - u64(vmin) + 1;
    const bool dense = (span == V) && span <= (1u << 22);
    double t1 = now_ms();
    std::vector<u32> counts(size_t(G) * V, 0);
    for (u64 i = 0; i < n; ++i) {
        i32 g = gid[i];
        const u64 gi = g < 0 ? u64(null_group) : u64(g);
        u32 r = u32(std::lower_bound(distinct.begin(), distinct.end(), val[i]) - distinct.begin());
        counts[size_t(gi) * V + r] += 1;
    }
    double t2 = now_ms();
    const u64 NB = (V + R_BLOCK - 1) / R_BLOCK;
    std::vector<u32> bcnt(size_t(G) * (NB + 1), 0);
    std::vector<i64> bsum(size_t(G) * (NB + 1), 0);
    for (u32 g = 0; g < G; ++g) {
        const u32* c = counts.data() + size_t(g) * V;
        u32* bc = bcnt.data() + size_t(g) * (NB + 1);
        i64* bs = bsum.data() + size_t(g) * (NB + 1);
        for (u64 r = 0; r < V; ++r) {
            const u64 bi = r / R_BLOCK;
            const i64 v = dense ? vmin + i64(r) : i64(distinct[r]);
            bc[bi + 1] += c[r];
            bs[bi + 1] += i64(c[r]) * v;
        }
        for (u64 b = 1; b <= NB; ++b) { bc[b] += bc[b - 1]; bs[b] += bs[b - 1]; }
    }
    double t3 = now_ms();
    Header h{};
    std::memcpy(h.magic, MAGIC, 8);
    h.n = n; h.ngroups = G; h.nvalues = V; h.vmin = vmin;
    h.dense = dense ? 1 : 0; h.has_null = has_null ? 1 : 0; h.null_group = null_group;
    h.block_ranks = R_BLOCK;
    std::vector<i64> dv;
    if (!dense) { dv.resize(V); for (u64 r = 0; r < V; ++r) dv[r] = distinct[r]; }
    write_cube(out_path, h, dv, counts, bcnt, bsum);
    double t4 = now_ms();
    const u64 bytes = sizeof(Header) + (dense ? 0 : V * 8) + G * V * 4 + G * (NB + 1) * 12;
    std::fprintf(stderr,
                 "{\"mode\":\"build\",\"build_ms\":%.3f,\"read_ms\":%.3f,\"accum_ms\":%.3f,"
                 "\"prefix_ms\":%.3f,\"write_ms\":%.3f,\"groups\":%u,\"values\":%llu,"
                 "\"rows\":%llu,\"bytes\":%llu,\"dense\":%d}\n",
                 t4 - t0, t1 - t0, t2 - t1, t3 - t2, t4 - t3, G, (unsigned long long)V,
                 (unsigned long long)n, (unsigned long long)bytes, int(dense));
}

static void build_encoded(const char* encoded, const char* prep, U gid, U vid,
                          const char* out_path) {
    static constexpr U CODE_BIAS = 0x80000000U;
    double t0 = now_ms();
    std::ifstream bf(std::string(prep) + "/binding.txt", std::ios::binary);
    std::string binding(41, '\0');
    bf.read(binding.data(), 40);
    binding.resize(40);
    bool parsed = false;
    StreamTable t = stream_load(encoded, binding, parsed);
    if (!parsed) throw std::runtime_error("stream admission");
    const StreamColumn* g = nullptr;
    const StreamColumn* v = nullptr;
    for (auto& c : t.columns) { if (c.id == gid) g = &c; if (c.id == vid) v = &c; }
    if (!g || !v) throw std::runtime_error("columns");
    U G = 0;
    {
        std::ifstream f(std::string(prep) + "/state.dict", std::ios::binary);
        std::string d((std::istreambuf_iterator<char>(f)), {});
        if (d.size() < 52 || d.compare(0, 8, "BSSTR01\n")) throw std::runtime_error("dict");
        std::memcpy(&G, d.data() + 48, 4);
    }
    const U n = g->n;
    const U blocks = g->leaves.size();
    bool has_null = false;
    for (U b = 0; b < blocks && !has_null; ++b) {
        const U off = b * V; const U take = std::min<U>(V, n - off);
        for (U i = 0; i < take; ++i)
            if (!stream_valid_bit(g->valid, off + i)) { has_null = true; break; }
    }
    const u32 null_group = has_null ? u32(G) : 0xffffffffu;
    if (has_null) G += 1;
    std::unordered_set<i32> distinct;
    distinct.reserve(1 << 16);
    Scratch gd, vd;
    for (U b = 0; b < blocks; ++b) {
        const U off = b * V; const U take = std::min<U>(V, n - off);
        stream_decode_block(*g, b, gd);
        stream_decode_block(*v, b, vd);
        for (U i = 0; i < take; ++i) distinct.insert(i32(i64(vd.d[i]) - i64(CODE_BIAS)));
    }
    double t1 = now_ms();
    std::vector<i32> dv(distinct.begin(), distinct.end());
    std::sort(dv.begin(), dv.end());
    const u64 Vn = dv.size();
    if (!Vn) throw std::runtime_error("empty");
    std::vector<u32> counts(size_t(G) * Vn, 0);
    for (U b = 0; b < blocks; ++b) {
        const U off = b * V; const U take = std::min<U>(V, n - off);
        stream_decode_block(*g, b, gd);
        stream_decode_block(*v, b, vd);
        for (U i = 0; i < take; ++i) {
            u64 gi;
            if (!stream_valid_bit(g->valid, off + i)) {
                gi = u64(null_group);
            } else {
                i32 gcode = i32(gd.d[i] - CODE_BIAS);
                if (gcode < 0 || u32(gcode) >= G) continue;
                gi = u64(gcode);
            }
            i32 val = i32(i64(vd.d[i]) - i64(CODE_BIAS));
            u32 r = u32(std::lower_bound(dv.begin(), dv.end(), val) - dv.begin());
            counts[size_t(gi) * Vn + r] += 1;
        }
    }
    double t2 = now_ms();
    const u64 NB = (Vn + R_BLOCK - 1) / R_BLOCK;
    std::vector<u32> bcnt(size_t(G) * (NB + 1), 0);
    std::vector<i64> bsum(size_t(G) * (NB + 1), 0);
    for (u32 g2 = 0; g2 < G; ++g2) {
        const u32* c = counts.data() + size_t(g2) * Vn;
        u32* bc = bcnt.data() + size_t(g2) * (NB + 1);
        i64* bs = bsum.data() + size_t(g2) * (NB + 1);
        for (u64 r = 0; r < Vn; ++r) {
            const u64 bi = r / R_BLOCK;
            bc[bi + 1] += c[r];
            bs[bi + 1] += i64(c[r]) * i64(dv[r]);
        }
        for (u64 b = 1; b <= NB; ++b) { bc[b] += bc[b - 1]; bs[b] += bs[b - 1]; }
    }
    double t3 = now_ms();
    Header h{};
    std::memcpy(h.magic, MAGIC, 8);
    h.n = n; h.ngroups = G; h.nvalues = Vn; h.vmin = dv.front();
    h.dense = 0; h.has_null = has_null ? 1 : 0; h.null_group = null_group;
    h.block_ranks = R_BLOCK;
    std::vector<i64> dv64(Vn);
    for (u64 r = 0; r < Vn; ++r) dv64[r] = dv[r];
    write_cube(out_path, h, dv64, counts, bcnt, bsum);
    double t4 = now_ms();
    const u64 bytes = sizeof(Header) + Vn * 8 + G * Vn * 4 + G * (NB + 1) * 12;
    std::fprintf(stderr,
                 "{\"mode\":\"build_enc\",\"build_ms\":%.3f,\"pass1_ms\":%.3f,"
                 "\"accum_ms\":%.3f,\"prefix_ms\":%.3f,\"write_ms\":%.3f,\"groups\":%llu,"
                 "\"values\":%llu,\"rows\":%llu,\"bytes\":%llu,\"dense\":0,\"encoded\":1}\n",
                 t4 - t0, t1 - t0, t2 - t1, t3 - t2, t4 - t3, (unsigned long long)G,
                 (unsigned long long)Vn, (unsigned long long)n, (unsigned long long)bytes);
}

// ---------------------------------------------------------------------------
// build_mt: row-partitioned parallel build (mmap input, per-thread count slabs)
// ---------------------------------------------------------------------------
// 32-bit value mixer (splitmix64 finalizer on the value). The naive
// (v * 2^32/phi) >> 32 "Fibonacci" hash is LINEAR for 32-bit inputs and produced
// 1443-probe linear clusters on the NYC fare domain (measured); this finalizer
// gives avg probe 1.25 at load 0.32.
static inline u64 mix_value(u32 x) {
    u64 z = (u64(x) + 0x9E3779B97F4A7C15ULL);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}

struct RankMap {
    // dense: rank = value - vmin; otherwise open-addressed (value -> rank) table
    bool dense = false;
    i64 vmin = 0;
    u64 mask = 0;
    std::vector<u64> tab;  // (u32 key << 32) | rank ; ~0ull = empty
    static constexpr u64 EMPTY = ~0ull;
    void build(const std::vector<i32>& dv, i64 vmin_, bool dense_) {
        dense = dense_;
        vmin = vmin_;
        if (dense) return;
        u64 cap = 16;
        while (cap < 2 * dv.size() + 16) cap <<= 1;
        tab.assign(cap, EMPTY);
        mask = cap - 1;
        for (u64 r = 0; r < dv.size(); ++r) {
            u64 h = mix_value(u32(dv[r])) & mask;
            while (tab[h] != EMPTY) h = (h + 1) & mask;
            tab[h] = ((u64(u32(dv[r]))) << 32) | r;
        }
    }
    inline u32 rank(i32 v) const {
        if (dense) return u32(i64(v) - vmin);
        u64 h = mix_value(u32(v)) & mask;
        while (true) {
            u64 e = tab[h];
            if (e == EMPTY) return 0;  // unreachable for observed values
            if (u32(e >> 32) == u32(v)) return u32(e & 0xffffffffu);
            h = (h + 1) & mask;
        }
    }
};

static u32 g_G_ = 0;
static bool g_has_null_ = false;
static u32 g_null_group_ = 0xffffffffu;

static void build_mt(const char* gid_path, const char* val_path, const char* out_path,
                     int T, double membudget_gb) {
    double t0 = now_ms();
    Mapped gid = map_i32(gid_path);
    Mapped val = map_i32(val_path);
    if (gid.n != val.n) throw std::runtime_error("size");
    const u64 n = gid.n;
    if (T < 1) T = 1;
    double t1 = now_ms();
    // phase 0: group domain (parallel over row chunks)
    const int GT = std::min<int>(T, 64);
    std::vector<u32> gmax(GT, 0);
    std::vector<u8> hnull(GT, 0);
    {
        std::vector<std::thread> th;
        for (int t = 1; t < GT; ++t)
            th.emplace_back([&, t] {
                u64 b0 = (u64)t * n / GT, b1 = (u64)(t + 1) * n / GT;
                u32 mx = 0;
                bool hn = false;
                for (u64 i = b0; i < b1; ++i) {
                    i32 g = gid.p[i];
                    if (g < 0) { hn = true; continue; }
                    if (u32(g) + 1 > mx) mx = u32(g) + 1;
                }
                gmax[t] = mx;
                hnull[t] = hn ? 1 : 0;
            });
        u32 mx = 0;
        bool hn = false;
        for (u64 i = 0; i < (u64)n / GT; ++i) {
            i32 g = gid.p[i];
            if (g < 0) { hn = true; continue; }
            if (u32(g) + 1 > mx) mx = u32(g) + 1;
        }
        gmax[0] = mx;
        hnull[0] = hn ? 1 : 0;
        for (auto& t : th) t.join();
        for (int t = 0; t < GT; ++t) { mx = std::max(mx, gmax[t]); hn = hn || hnull[t]; }
        u32 G = mx;
        const bool has_null = hn;
        const u32 null_group = has_null ? G : 0xffffffffu;
        if (has_null) G += 1;
        // stash in globals for the next phases (kept simple; single build per process)
        g_G_ = G;
        g_has_null_ = has_null;
        g_null_group_ = null_group;
    }
    const double t_dom = now_ms();
    const u32 G = g_G_;
    const bool has_null = g_has_null_;
    const u32 null_group = g_null_group_;
    // phase 1: exact distinct value set (per-thread hash sets, merged)
    std::vector<i32> dv;
    {
        std::vector<std::unordered_set<i32>> sets(GT);
        std::vector<std::thread> th;
        for (int t = 0; t < GT; ++t)
            th.emplace_back([&, t] {
                u64 b0 = (u64)t * n / GT, b1 = (u64)(t + 1) * n / GT;
                auto& s = sets[t];
                s.reserve(1 << 16);
                for (u64 i = b0; i < b1; ++i) s.insert(val.p[i]);
            });
        for (auto& t : th) t.join();
        std::unordered_set<i32> all;
        all.reserve(1 << 16);
        for (auto& s : sets) all.insert(s.begin(), s.end());
        dv.assign(all.begin(), all.end());
        std::sort(dv.begin(), dv.end());
    }
    const u64 V = dv.size();
    if (!V) throw std::runtime_error("empty");
    const i64 vmin = dv.front();
    const u64 span = u64(dv.back()) - u64(vmin) + 1;
    const bool dense = (span == V) && span <= (1u << 22);
    RankMap rm;
    rm.build(dv, vmin, dense);
    double t2 = now_ms();
    // memory guard: per-thread count slab
    const u64 slab_bytes = (u64)G * V * sizeof(u32);
    int Tuse = T;
    if (slab_bytes) {
        u64 budget = u64(membudget_gb * (1ULL << 30));
        u64 fit = budget / slab_bytes;
        if (fit < 1) fit = 1;
        Tuse = int(std::min<u64>(Tuse, fit));
    }
    // phase 2: parallel accumulate into per-thread slabs
    std::vector<std::vector<u32>> slabs(Tuse);
    for (int t = 0; t < Tuse; ++t) slabs[t].assign(slab_bytes / 4, 0);
    {
        std::vector<std::thread> th;
        for (int t = 0; t < Tuse; ++t)
            th.emplace_back([&, t] {
                u64 b0 = (u64)t * n / Tuse, b1 = (u64)(t + 1) * n / Tuse;
                u32* C = slabs[t].data();
                const i32* gp = gid.p;
                const i32* vp = val.p;
                for (u64 i = b0; i < b1; ++i) {
                    i32 g = gp[i];
                    const u64 gi = g < 0 ? u64(null_group) : u64(g);
                    C[gi * V + rm.rank(vp[i])] += 1;
                }
            });
        for (auto& t : th) t.join();
    }
    double t3 = now_ms();
    // phase 3: reduce slabs into slab 0 (parallel over groups)
    {
        std::vector<std::thread> th;
        const int RT = std::min<int>(Tuse, 64);
        for (int t = 0; t < RT; ++t)
            th.emplace_back([&, t] {
                u64 g0 = (u64)t * G / RT, g1 = (u64)(t + 1) * G / RT;
                for (u64 g = g0; g < g1; ++g) {
                    u32* dst = slabs[0].data() + g * V;
                    for (int u = 1; u < Tuse; ++u) {
                        const u32* src = slabs[u].data() + g * V;
                        for (u64 r = 0; r < V; ++r) dst[r] += src[r];
                    }
                }
            });
        for (auto& t : th) t.join();
    }
    if (Tuse > 1) { for (int t = 1; t < Tuse; ++t) { slabs[t].clear(); slabs[t].shrink_to_fit(); } }
    double t4 = now_ms();
    // phase 4: block prefix (parallel over groups)
    const u64 NB = (V + R_BLOCK - 1) / R_BLOCK;
    std::vector<u32> bcnt(size_t(G) * (NB + 1), 0);
    std::vector<i64> bsum(size_t(G) * (NB + 1), 0);
    {
        std::vector<std::thread> th;
        const int PT = std::min<int>(T, 64);
        for (int t = 0; t < PT; ++t)
            th.emplace_back([&, t] {
                u64 g0 = (u64)t * G / PT, g1 = (u64)(t + 1) * G / PT;
                for (u64 g = g0; g < g1; ++g) {
                    const u32* c = slabs[0].data() + g * V;
                    u32* bc = bcnt.data() + g * (NB + 1);
                    i64* bs = bsum.data() + g * (NB + 1);
                    for (u64 r = 0; r < V; ++r) {
                        const u64 bi = r / R_BLOCK;
                        const i64 v = dense ? vmin + i64(r) : i64(dv[r]);
                        bc[bi + 1] += c[r];
                        bs[bi + 1] += i64(c[r]) * v;
                    }
                    for (u64 b = 1; b <= NB; ++b) { bc[b] += bc[b - 1]; bs[b] += bs[b - 1]; }
                }
            });
        for (auto& t : th) t.join();
    }
    double t5 = now_ms();
    Header h{};
    std::memcpy(h.magic, MAGIC, 8);
    h.n = n; h.ngroups = G; h.nvalues = V; h.vmin = vmin;
    h.dense = dense ? 1 : 0; h.has_null = has_null ? 1 : 0; h.null_group = null_group;
    h.block_ranks = R_BLOCK;
    std::vector<i64> dv64;
    if (!dense) { dv64.resize(V); for (u64 r = 0; r < V; ++r) dv64[r] = dv[r]; }
    write_cube(out_path, h, dv64, slabs[0], bcnt, bsum);
    double t6 = now_ms();
    const u64 bytes = sizeof(Header) + (dense ? 0 : V * 8) + G * V * 4 + G * (NB + 1) * 12;
    std::fprintf(stderr,
                 "{\"mode\":\"build_mt\",\"build_ms\":%.3f,\"map_ms\":%.3f,\"domain_ms\":%.3f,"
                 "\"distinct_ms\":%.3f,\"accum_ms\":%.3f,\"merge_ms\":%.3f,\"prefix_ms\":%.3f,"
                 "\"write_ms\":%.3f,\"groups\":%u,\"values\":%llu,\"rows\":%llu,\"bytes\":%llu,"
                 "\"dense\":%d,\"threads_requested\":%d,\"threads_used\":%d,"
                 "\"slab_bytes_per_thread\":%llu,\"membudget_gb\":%.1f}\n",
                 t6 - t0, t1 - t0, t_dom - t1, t2 - t_dom, t3 - t2, t4 - t3, t5 - t4,
                 t6 - t5, G,
                 (unsigned long long)V, (unsigned long long)n, (unsigned long long)bytes,
                 int(dense), T, Tuse, (unsigned long long)slab_bytes, membudget_gb);
}

// ---------------------------------------------------------------------------
// materialized group stats (32 B/group: sum, cnt, min, max)
// ---------------------------------------------------------------------------
struct MHeader {
    char magic[8];
    u64 n;
    u64 ngroups;
    u64 has_null;
    u64 null_group;
};
struct MRec { i64 sum, cnt, mn, mx; };

static void build_stats(const char* gid_path, const char* val_path, const char* out_path, int T) {
    double t0 = now_ms();
    Mapped gid = map_i32(gid_path);
    Mapped val = map_i32(val_path);
    if (gid.n != val.n) throw std::runtime_error("size");
    const u64 n = gid.n;
    if (T < 1) T = 1;
    std::vector<u32> gmax(T, 0);
    std::vector<u8> hnull(T, 0);
    {
        std::vector<std::thread> th;
        for (int t = 1; t < T; ++t)
            th.emplace_back([&, t] {
                u64 b0 = (u64)t * n / T, b1 = (u64)(t + 1) * n / T;
                u32 mx = 0;
                bool hn = false;
                for (u64 i = b0; i < b1; ++i) {
                    i32 g = gid.p[i];
                    if (g < 0) { hn = true; continue; }
                    if (u32(g) + 1 > mx) mx = u32(g) + 1;
                }
                gmax[t] = mx; hnull[t] = hn;
            });
        u32 mx = 0; bool hn = false;
        for (u64 i = 0; i < (u64)n / T; ++i) {
            i32 g = gid.p[i];
            if (g < 0) { hn = true; continue; }
            if (u32(g) + 1 > mx) mx = u32(g) + 1;
        }
        gmax[0] = mx; hnull[0] = hn;
        for (auto& t : th) t.join();
        for (int t = 0; t < T; ++t) { mx = std::max(mx, gmax[t]); hn = hn || hnull[t]; }
        u32 G = mx;
        const u64 null_group = hn ? G : 0xffffffffull;
        if (hn) G += 1;
        double t1 = now_ms();
        std::vector<std::vector<MRec>> recs(T);
        for (int t = 0; t < T; ++t) {
            recs[t].assign(G, MRec{0, 0, INT64_MAX, INT64_MIN});
        }
        {
            std::vector<std::thread> th;
            for (int t = 0; t < T; ++t)
                th.emplace_back([&, t] {
                    u64 b0 = (u64)t * n / T, b1 = (u64)(t + 1) * n / T;
                    MRec* R = recs[t].data();
                    for (u64 i = b0; i < b1; ++i) {
                        i32 g = gid.p[i];
                        u64 gi = g < 0 ? null_group : u64(g);
                        i64 v = val.p[i];
                        MRec& r = R[gi];
                        if (!r.cnt) { r.mn = r.mx = v; }
                        else { if (v < r.mn) r.mn = v; if (v > r.mx) r.mx = v; }
                        r.sum += v;
                        r.cnt += 1;
                    }
                });
            for (auto& th2 : th) th2.join();
        }
        double t2 = now_ms();
        std::vector<MRec> out(G);
        {
            std::vector<std::thread> th;
            for (int t = 0; t < T; ++t)
                th.emplace_back([&, t] {
                    u64 g0 = (u64)t * G / T, g1 = (u64)(t + 1) * G / T;
                    for (u64 g = g0; g < g1; ++g) {
                        MRec m{0, 0, INT64_MAX, INT64_MIN};
                        for (int u = 0; u < T; ++u) {
                            const MRec& r = recs[u][g];
                            if (!r.cnt) continue;
                            m.sum += r.sum;
                            m.cnt += r.cnt;
                            if (r.mn < m.mn) m.mn = r.mn;
                            if (r.mx > m.mx) m.mx = r.mx;
                        }
                        out[g] = m;
                    }
                });
            for (auto& th2 : th) th2.join();
        }
        double t3 = now_ms();
        std::ofstream o(out_path, std::ios::binary);
        MHeader h{};
        std::memcpy(h.magic, MMAGIC, 8);
        h.n = n; h.ngroups = G; h.has_null = hn ? 1 : 0; h.null_group = null_group;
        o.write(reinterpret_cast<const char*>(&h), sizeof(h));
        write_vec(o, out);
        o.close();
        double t4 = now_ms();
        const u64 bytes = sizeof(MHeader) + G * sizeof(MRec);
        std::fprintf(stderr,
                     "{\"mode\":\"build_stats\",\"build_ms\":%.3f,\"domain_ms\":%.3f,"
                     "\"accum_ms\":%.3f,\"merge_ms\":%.3f,\"write_ms\":%.3f,\"groups\":%llu,"
                     "\"rows\":%llu,\"bytes\":%llu,\"threads\":%d,\"bytes_per_group\":%zu}\n",
                     t4 - t0, t1 - t0, t2 - t1, t3 - t2, t4 - t3, (unsigned long long)G,
                     (unsigned long long)n, (unsigned long long)bytes, T, sizeof(MRec));
    }
}

static void bench_stats(const char* path, const char* out_path, int reps, int T, const QSpec& q) {
    double t0 = now_ms();
    Mapped mm = map_i32(path);
    if (mm.bytes < sizeof(MHeader)) throw std::runtime_error("stats size");
    MHeader h{};
    std::memcpy(&h, mm.base, sizeof(h));
    if (std::memcmp(h.magic, MMAGIC, 8)) throw std::runtime_error("stats magic");
    const MRec* R = reinterpret_cast<const MRec*>((const char*)mm.base + sizeof(MHeader));
    Names nm;
    nm.G = h.ngroups;
    nm.has_null = u32(h.has_null);
    nm.null_group = u32(h.null_group);
    nm.synthesized = true;
    const u64 G = h.ngroups;
    // materialized stats answer the fixed (no-range) GROUP BY only
    if (q.has_lo || q.has_hi || !q.in.empty() || !q.not_in.empty())
        fprintf(stderr, "WARN bench_stats: predicates ignored (stats are full-table)\n");
    std::vector<char> allowed(G, 1);
    if (!q.in.empty() || !q.not_in.empty()) build_allowed(q, nm, G, allowed);
    double t1 = now_ms();
    auto run = [&](std::vector<std::vector<Row>>& parts) {
        parts.resize(T);
        auto job = [&](int i) {
            u64 g0 = (u64)i * G / T, g1 = (u64)(i + 1) * G / T;
            parts[i].clear();
            for (u64 g = g0; g < g1; ++g) {
                if (!allowed[g]) continue;
                const MRec& r = R[g];
                if (!r.cnt) continue;
                parts[i].push_back({g, u64(r.cnt), r.sum, r.mn, r.mx});
            }
        };
        std::vector<std::thread> th;
        for (int i = 1; i < T; ++i) th.emplace_back(job, i);
        job(0);
        for (auto& t : th) t.join();
    };
    std::vector<std::vector<Row>> parts;
    run(parts);  // warmup
    std::vector<double> samples;
    samples.reserve(reps);
    u64 check = 0;
    for (int r = 0; r < reps; ++r) {
        double s = now_ms();
        run(parts);
        samples.push_back(now_ms() - s);
        check += rows_checksum(parts);
    }
    std::vector<double> ser1, serT;
    ser1.reserve(reps); serT.reserve(reps);
    std::string buf;
    for (int r = 0; r < reps; ++r) {
        double s = now_ms();
        serialize_mt(parts, nm, q, 1, buf, true, false);
        ser1.push_back(now_ms() - s);
    }
    for (int r = 0; r < reps; ++r) {
        double s = now_ms();
        serialize_mt(parts, nm, q, T, buf, true, false);
        serT.push_back(now_ms() - s);
    }
    double t2 = now_ms();
    size_t nrows = 0;
    Agg ag = agg_parts(parts);
    double o0 = now_ms();
    {
        std::string out;
        out += "{\"rows\":[";
        nrows = serialize_mt(parts, nm, q, T, out, false, true);
        FILE* f = fopen(out_path, "wb");
        fwrite(out.data(), 1, out.size(), f);
        fclose(f);
    }
    double out_ms = now_ms() - o0;
    auto pct = [](std::vector<double>& v, double p) {
        std::sort(v.begin(), v.end());
        size_t i = std::min(v.size() - 1, (size_t)(p * v.size()));
        return v[i];
    };
    std::fprintf(stderr,
                 "{\"mode\":\"bench_stats\",\"threads\":%d,\"reps\":%d,\"setup_ms\":%.3f,"
                 "\"query_median_ms\":%.4f,\"query_p95_ms\":%.4f,\"query_min_ms\":%.4f,"
                 "\"serialize1_median_ms\":%.4f,\"serialize1_p95_ms\":%.4f,"
                 "\"serializeT_median_ms\":%.4f,\"serializeT_p95_ms\":%.4f,"
                 "\"output_ms\":%.3f,\"rows\":%zu,\"groups\":%llu,\"rss_bytes_hint\":%llu,"
                 "\"checksum\":%llu,\"names_synthesized\":true,\"total_cnt\":%llu,"
                 "\"total_sum\":%lld,\"min_mn\":%lld,\"max_mx\":%lld,\"group_chk\":\"%s\"}\n",
                 T, reps, t1 - t0, pct(samples, 0.5), pct(samples, 0.95), samples.empty() ? 0.0 : *std::min_element(samples.begin(), samples.end()),
                 pct(ser1, 0.5), pct(ser1, 0.95), pct(serT, 0.5), pct(serT, 0.95), out_ms, nrows,
                 (unsigned long long)G, (unsigned long long)(G * sizeof(MRec)), check,
                 (unsigned long long)ag.cnt, (long long)ag.sum, (long long)ag.mn,
                 (long long)ag.mx, i128str(ag.chk).c_str());
}

// ---------------------------------------------------------------------------
// block min/max summaries over the value column + exact pruned-scan fallback
// ---------------------------------------------------------------------------
struct BHeader {
    char magic[8];
    u64 n;
    u64 block;
    u64 nblocks;
    i64 vmin, vmax;
    u64 ngroups;
    u64 has_null;
    u64 null_group;
};
struct BlockMM { i64 mn, mx; };

static void build_bsum(const char* gid_path, const char* val_path, const char* out_path, int T,
                       u64 block) {
    double t0 = now_ms();
    Mapped gid = map_i32(gid_path);
    Mapped val = map_i32(val_path);
    if (gid.n != val.n) throw std::runtime_error("size");
    const u64 n = gid.n;
    const u64 nblocks = (n + block - 1) / block;
    if (T < 1) T = 1;
    // group domain
    std::vector<u32> gmax(T, 0);
    std::vector<u8> hnull(T, 0);
    std::vector<std::thread> th;
    for (int t = 1; t < T; ++t)
        th.emplace_back([&, t] {
            u64 b0 = (u64)t * n / T, b1 = (u64)(t + 1) * n / T;
            u32 mx = 0; bool hn = false;
            for (u64 i = b0; i < b1; ++i) {
                i32 g = gid.p[i];
                if (g < 0) { hn = true; continue; }
                if (u32(g) + 1 > mx) mx = u32(g) + 1;
            }
            gmax[t] = mx; hnull[t] = hn;
        });
    u32 mx = 0; bool hn = false;
    for (u64 i = 0; i < (u64)n / T; ++i) {
        i32 g = gid.p[i];
        if (g < 0) { hn = true; continue; }
        if (u32(g) + 1 > mx) mx = u32(g) + 1;
    }
    gmax[0] = mx; hnull[0] = hn;
    for (auto& t : th) t.join();
    for (int t = 0; t < T; ++t) { mx = std::max(mx, gmax[t]); hn = hn || hnull[t]; }
    u32 G = mx;
    const u64 null_group = hn ? G : 0xffffffffull;
    if (hn) G += 1;
    double t1 = now_ms();
    std::vector<BlockMM> mm(nblocks);
    {
        std::vector<std::thread> th2;
        for (int t = 0; t < T; ++t)
            th2.emplace_back([&, t] {
                u64 b0 = (u64)t * nblocks / T, b1 = (u64)(t + 1) * nblocks / T;
                for (u64 b = b0; b < b1; ++b) {
                    u64 i0 = b * block, i1 = std::min<u64>(n, i0 + block);
                    i64 mn = INT64_MAX, mx = INT64_MIN;
                    for (u64 i = i0; i < i1; ++i) {
                        i64 v = val.p[i];
                        if (v < mn) mn = v;
                        if (v > mx) mx = v;
                    }
                    mm[b] = {mn, mx};
                }
            });
        for (auto& t : th2) t.join();
    }
    i64 vmin = INT64_MAX, vmax = INT64_MIN;
    for (const auto& b : mm) { if (b.mn != INT64_MAX) { vmin = std::min(vmin, b.mn); vmax = std::max(vmax, b.mx); } }
    double t2 = now_ms();
    std::ofstream o(out_path, std::ios::binary);
    BHeader h{};
    std::memcpy(h.magic, BMAGIC, 8);
    h.n = n; h.block = block; h.nblocks = nblocks; h.vmin = vmin; h.vmax = vmax;
    h.ngroups = G; h.has_null = hn ? 1 : 0; h.null_group = null_group;
    o.write(reinterpret_cast<const char*>(&h), sizeof(h));
    write_vec(o, mm);
    o.close();
    double t3 = now_ms();
    const u64 bytes = sizeof(BHeader) + nblocks * sizeof(BlockMM);
    std::fprintf(stderr,
                 "{\"mode\":\"build_bsum\",\"build_ms\":%.3f,\"domain_ms\":%.3f,"
                 "\"blocks_ms\":%.3f,\"write_ms\":%.3f,\"rows\":%llu,\"block\":%llu,"
                 "\"nblocks\":%llu,\"groups\":%llu,\"bytes\":%llu,\"threads\":%d}\n",
                 t3 - t0, t1 - t0, t2 - t1, t3 - t2, (unsigned long long)n,
                 (unsigned long long)block, (unsigned long long)nblocks,
                 (unsigned long long)G, (unsigned long long)bytes, T);
}

static void bench_bsum(const char* bpath, const char* gid_path, const char* val_path,
                       const char* out_path, int reps, const QSpec& q) {
    double t0 = now_ms();
    Mapped bm = map_i32(bpath);
    if (bm.bytes < sizeof(BHeader)) throw std::runtime_error("bsum size");
    BHeader h{};
    std::memcpy(&h, bm.base, sizeof(h));
    if (std::memcmp(h.magic, BMAGIC, 8)) throw std::runtime_error("bsum magic");
    const BlockMM* MM = reinterpret_cast<const BlockMM*>((const char*)bm.base + sizeof(BHeader));
    Mapped gid = map_i32(gid_path);
    Mapped val = map_i32(val_path);
    const u64 n = h.n, G = h.ngroups;
    const u64 null_group = h.null_group;
    i64 lo = q.has_lo ? q.lo : INT64_MIN;
    i64 hi = q.has_hi ? q.hi : INT64_MAX;
    Names nm;
    nm.G = G; nm.has_null = u32(h.has_null); nm.null_group = u32(null_group); nm.synthesized = true;
    std::vector<char> allowed(G, 1);
    // exact dense aggregation state, reused across reps
    std::vector<u64> cnt(G, 0);
    std::vector<i64> sum(G, 0), mn(G, 0), mx(G, 0);
    std::vector<u32> touched;
    std::vector<u32> cand;
    double t1 = now_ms();
    auto run = [&](std::vector<Row>& rows) {
        // reset only the groups touched by the previous query
        for (u32 g : touched) { cnt[g] = 0; sum[g] = 0; }
        touched.clear();
        cand.clear();
        for (u64 b = 0; b < h.nblocks; ++b)
            if (MM[b].mx >= lo && MM[b].mn <= hi) cand.push_back((u32)b);
        for (u32 b : cand) {
            u64 i0 = (u64)b * h.block, i1 = std::min<u64>(n, i0 + h.block);
            for (u64 i = i0; i < i1; ++i) {
                i64 v = val.p[i];
                if (v < lo || v > hi) continue;
                i32 g = gid.p[i];
                u64 gi = g < 0 ? null_group : u64(g);
                u64& c = cnt[gi];
                if (!c) { mn[gi] = mx[gi] = v; touched.push_back((u32)gi); }
                else { if (v < mn[gi]) mn[gi] = v; if (v > mx[gi]) mx[gi] = v; }
                sum[gi] += v;
                c += 1;
            }
        }
        rows.clear();
        for (u64 g = 0; g < G; ++g)
            if (cnt[g]) rows.push_back({g, cnt[g], sum[g], mn[g], mx[g]});
        return (u64)cand.size();
    };
    std::vector<Row> rows;
    u64 ncand = run(rows);
    std::vector<double> samples;
    samples.reserve(reps);
    u64 check = 0;
    for (int r = 0; r < reps; ++r) {
        double s = now_ms();
        run(rows);
        samples.push_back(now_ms() - s);
        check += rows_checksum_vec(rows);
    }
    double t2 = now_ms();
    std::string out;
    double o0 = now_ms();
    out += "{\"rows\":[";
    std::vector<std::vector<Row>> parts2{};
    parts2.push_back(std::move(rows));
    size_t nrows = serialize_parts_impl(parts2, 0, 1, nm, q, out);
    Agg ag = agg_parts(parts2);
    out += "]}\n";
    FILE* f = fopen(out_path, "wb");
    fwrite(out.data(), 1, out.size(), f);
    fclose(f);
    double out_ms = now_ms() - o0;
    auto pct = [](std::vector<double>& v, double p) {
        std::sort(v.begin(), v.end());
        return v[std::min(v.size() - 1, (size_t)(p * v.size()))];
    };
    std::fprintf(stderr,
                 "{\"mode\":\"bench_bsum\",\"range_lo\":%lld,\"range_hi\":%lld,\"reps\":%d,"
                 "\"setup_ms\":%.3f,\"query_median_ms\":%.4f,\"query_p95_ms\":%.4f,"
                 "\"query_min_ms\":%.4f,\"output_ms\":%.3f,\"rows\":%zu,\"groups\":%llu,"
                 "\"candidate_blocks\":%llu,\"total_blocks\":%llu,\"checksum\":%llu,"
                 "\"total_cnt\":%llu,\"total_sum\":%lld,\"min_mn\":%lld,\"max_mx\":%lld,"
                 "\"group_chk\":\"%s\"}\n",
                 (long long)lo, (long long)hi, reps, t1 - t0, pct(samples, 0.5),
                 pct(samples, 0.95), *std::min_element(samples.begin(), samples.end()), out_ms,
                 nrows, (unsigned long long)G, (unsigned long long)ncand,
                 (unsigned long long)h.nblocks, check, (unsigned long long)ag.cnt,
                 (long long)ag.sum, (long long)ag.mn, (long long)ag.mx,
                 i128str(ag.chk).c_str());
}

// ---------------------------------------------------------------------------
// qbench: parallel cube query with P95 + parallel serialization
// ---------------------------------------------------------------------------
static void qbench(const char* cube_path, const char* dict_path, const char* out_path, int reps,
                   int T, const QSpec& q, bool fresh_alloc) {
    double t0 = now_ms();
    Cube c = open_cube(cube_path);
    Names nm = load_names(dict_path, c.h);
    std::vector<char> allowed;
    build_allowed(q, nm, c.h.ngroups, allowed);
    u64 a, b;
    rank_interval(c, q, a, b);
    double t1 = now_ms();
    std::vector<std::vector<Row>> parts;
    Pool pool(T);
    // warmup
    pool.run(c, allowed, a, b, parts);
    double t2 = now_ms();
    std::vector<double> samples;
    samples.reserve(reps);
    u64 check = rows_checksum(parts);
    for (int r = 0; r < reps; ++r) {
        double s = now_ms();
        pool.run(c, allowed, a, b, parts);
        samples.push_back(now_ms() - s);
        check += rows_checksum(parts);
    }
    double t3 = now_ms();
    std::vector<double> ser1, serT;
    ser1.reserve(reps); serT.reserve(reps);
    std::string buf;
    for (int r = 0; r < reps; ++r) {
        double s = now_ms();
        serialize_mt(parts, nm, q, 1, buf, true, false);
        ser1.push_back(now_ms() - s);
    }
    for (int r = 0; r < reps; ++r) {
        double s = now_ms();
        serialize_mt(parts, nm, q, T, buf, true, false);
        serT.push_back(now_ms() - s);
    }
    double t4 = now_ms();
    double o0 = now_ms();
    {
        std::string out;
        out += "{\"rows\":[";
        size_t nrows = serialize_mt(parts, nm, q, T, out, false, true);
        FILE* f = fopen(out_path, "wb");
        fwrite(out.data(), 1, out.size(), f);
        fclose(f);
        (void)nrows;
    }
    double out_ms = now_ms() - o0;
    auto pct = [](std::vector<double>& v, double p) {
        std::sort(v.begin(), v.end());
        return v[std::min(v.size() - 1, (size_t)(p * v.size()))];
    };
    size_t nrows = count_rows(parts);
    Agg ag = agg_parts(parts);
    std::fprintf(stderr,
                 "{\"mode\":\"qbench\",\"threads\":%d,\"reps\":%d,\"fresh_alloc\":%d,"
                 "\"setup_ms\":%.3f,\"warmup_ms\":%.3f,\"query_median_ms\":%.4f,"
                 "\"query_p95_ms\":%.4f,\"query_min_ms\":%.4f,\"serialize1_median_ms\":%.4f,"
                 "\"serialize1_p95_ms\":%.4f,\"serializeT_median_ms\":%.4f,"
                 "\"serializeT_p95_ms\":%.4f,\"output_ms\":%.3f,\"rows\":%zu,"
                 "\"bytes\":%llu,\"groups\":%llu,\"values\":%llu,\"dense\":%d,\"rank_lo\":%llu,"
                 "\"rank_hi\":%llu,\"checksum\":%llu,\"names_synthesized\":%d,"
                 "\"total_cnt\":%llu,\"total_sum\":%lld,\"min_mn\":%lld,\"max_mx\":%lld,"
                 "\"group_chk\":\"%s\"}\n",
                 T, reps, fresh_alloc ? 1 : 0, t1 - t0, t2 - t1, pct(samples, 0.5),
                 pct(samples, 0.95), *std::min_element(samples.begin(), samples.end()),
                 pct(ser1, 0.5), pct(ser1, 0.95), pct(serT, 0.5), pct(serT, 0.95), out_ms, nrows,
                 (unsigned long long)c.bytes, (unsigned long long)c.h.ngroups,
                 (unsigned long long)c.h.nvalues, int(c.h.dense), (unsigned long long)a,
                 (unsigned long long)b, check, nm.synthesized ? 1 : 0,
                 (unsigned long long)ag.cnt, (long long)ag.sum, (long long)ag.mn,
                 (long long)ag.mx, i128str(ag.chk).c_str());
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main(int argc, char** argv) {
    try {
        if (argc < 2) return 2;
        std::string mode = argv[1];
        if (mode == "build" && argc >= 5) { build_plain(argv[2], argv[3], argv[4]); return 0; }
        if (mode == "build_enc" && argc >= 7) {
            build_encoded(argv[2], argv[3], U(std::stoul(argv[4])), U(std::stoul(argv[5])), argv[6]);
            return 0;
        }
        if (mode == "build_mt" && argc >= 6) {
            int T = std::stoi(argv[5]);
            double mb = argc >= 7 ? std::stod(argv[6]) : 24.0;
            build_mt(argv[2], argv[3], argv[4], T, mb);
            return 0;
        }
        if (mode == "qbench" && argc >= 7) {
            int reps = std::stoi(argv[5]);
            int T = std::stoi(argv[6]);
            QSpec q = parse_spec(argc, argv, 7);
            qbench(argv[2], argv[3], argv[4], reps, T, q, false);
            return 0;
        }
        if (mode == "build_stats" && argc >= 6) {
            build_stats(argv[2], argv[3], argv[4], std::stoi(argv[5]));
            return 0;
        }
        if (mode == "bench_stats" && argc >= 6) {
            int reps = std::stoi(argv[4]);
            int T = std::stoi(argv[5]);
            QSpec q = parse_spec(argc, argv, 6);
            bench_stats(argv[2], argv[3], reps, T, q);
            return 0;
        }
        if (mode == "build_bsum" && argc >= 6) {
            u64 block = argc >= 7 ? std::stoull(argv[6]) : 1024;
            build_bsum(argv[2], argv[3], argv[4], std::stoi(argv[5]), block);
            return 0;
        }
        if (mode == "bench_bsum" && argc >= 7) {
            int reps = std::stoi(argv[6]);
            QSpec q = parse_spec(argc, argv, 7);
            bench_bsum(argv[2], argv[3], argv[4], argv[5], reps, q);
            return 0;
        }
        if ((mode == "query" || mode == "bench") && argc >= 5) {
            const char* cube_path = argv[2];
            const char* dict_path = argv[3];
            const char* out_path = argv[4];
            int reps = 1, start = 5;
            if (mode == "bench") { reps = std::stoi(argv[5]); start = 6; }
            QSpec q = parse_spec(argc, argv, start);
            double t0 = now_ms();
            Cube c = open_cube(cube_path);
            Names nm = load_names(dict_path, c.h);
            double t1 = now_ms();
            run_query_st(c, nm, q, out_path, false);
            std::vector<double> samples;
            samples.reserve(reps);
            u64 check = 0;
            for (int i = 0; i < reps; ++i) {
                double s = now_ms();
                check += run_query_st(c, nm, q, out_path, false);
                samples.push_back(now_ms() - s);
            }
            std::vector<double> ser;
            ser.reserve(reps);
            std::string reuse;
            for (int i = 0; i < reps; ++i) {
                double s = now_ms();
                check += run_query_st(c, nm, q, out_path, false, true, &reuse);
                ser.push_back(now_ms() - s);
            }
            double t2 = now_ms();
            run_query_st(c, nm, q, out_path, true);
            double t3 = now_ms();
            std::sort(samples.begin(), samples.end());
            std::sort(ser.begin(), ser.end());
            std::fprintf(stderr,
                         "{\"mode\":\"bench\",\"setup_ms\":%.4f,\"query_median_ms\":%.6f,"
                         "\"query_p95_ms\":%.6f,\"query_min_ms\":%.6f,"
                         "\"serialize_median_ms\":%.6f,\"serialize_p95_ms\":%.6f,"
                         "\"output_ms\":%.4f,\"reps\":%d,\"bytes\":%llu,\"groups\":%llu,"
                         "\"values\":%llu,\"checksum\":%llu}\n",
                         t1 - t0, samples[samples.size() / 2],
                         samples[size_t(samples.size() * 95 / 100)], samples.front(),
                         ser[ser.size() / 2], ser[size_t(ser.size() * 95 / 100)], t3 - t2, reps,
                         (unsigned long long)c.bytes, (unsigned long long)c.h.ngroups,
                         (unsigned long long)c.h.nvalues, check);
            return 0;
        }
        std::fprintf(stderr,
                     "usage:\n"
                     "  cube_scale build GID VAL OUT\n"
                     "  cube_scale build_enc ENC PREP GID VID OUT\n"
                     "  cube_scale build_mt GID VAL OUT THREADS [membudget_gb=24]\n"
                     "  cube_scale qbench CUBE DICT OUT REPS THREADS [range=|lo=|hi=|where_in=|having=]\n"
                     "  cube_scale build_stats GID VAL OUT THREADS\n"
                     "  cube_scale bench_stats STATS OUT REPS THREADS\n"
                     "  cube_scale build_bsum GID VAL OUT THREADS [block=1024]\n"
                     "  cube_scale bench_bsum BSUM GID VAL OUT REPS [range=|lo=|hi=]\n"
                     "  cube_scale bench CUBE DICT OUT REPS [spec...]\n");
        return 2;
    } catch (std::exception& e) {
        std::fprintf(stderr, "ERR %s\n", e.what());
        return 1;
    }
}
