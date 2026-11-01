// EAI: exact encoded-domain aggregate cube (ECUBE01).
//
// One structure answers, exactly, the query class
//   SELECT group, SUM(v), COUNT(*), MIN(v), MAX(v) FROM C
//   WHERE v BETWEEN lo AND hi [AND group IN (...)] [HAVING ...] GROUP BY group
//
// Structure: for each group g and each observed value rank r, store
//   count[g][r]  and prefix-sum(P) ; sum[g][r] prefix sums.
// Values are ranked over the *observed distinct set* (size V), not over the
// numeric domain, so space is O(G*V), independent of the row count n.
//
// Exactness: the range predicate maps to a half-open rank interval [a,b) by
// binary search on the distinct-value array. Sum/count are two prefix
// differences. MIN/MAX over the filtered value column are the first/last rank
// in [a,b) with a non-zero count, found by binary search over the count prefix,
// so MIN/MAX are derived from exact statistics and never from raw scans.
//
// Build is a single pass over the columns. Query is O(G log V + G) after a
// O(log V) rank mapping.
//
// ---------------------------------------------------------------------------
// SPECS workstream copy (2026-11-01, teammate `specs`).
// Verbatim copy of src_eai_20261101/enc_cube.cpp (sha256 pinned in the receipt)
// with exactly two behavioural fixes, each marked "SPEC-RUNNER FIX" below:
//   FIX 1  build_plain dense mode now requires a contiguous observed domain.
//   FIX 2  NULL group keys are dropped when a group-domain predicate (IN or
//          NOT IN) is present, matching SQL three-valued logic.
//   FIX 3  dense-mode range bounds are clamped before subtraction, so ranges
//          touching INT64_MIN/INT64_MAX cannot overflow (empty-result bug).
// Everything else (CLI, file format, query semantics, timings) is unchanged.
// ---------------------------------------------------------------------------
#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <unordered_set>
#include <vector>

#include "../src_patch_fusion/fullsql_reference.hpp"
#include "../src_fullsql_ig2_fast_20261029/streaming.hpp"

using u64 = uint64_t;
using i64 = int64_t;
using u32 = uint32_t;
using i32 = int32_t;

static constexpr const char* MAGIC = "ECUBE01\n";
struct Header {
    char magic[8];
    u64 n;        // input rows
    u64 ngroups;  // G
    u64 nvalues;  // V (observed distinct values)
    i64 vmin;     // numeric min (dense mode base)
    u32 dense;    // 1: value = vmin + rank, 0: distinct[] gives the value
    u32 nmeasures;  // one sum prefix per measure
    u32 has_null;   // 1 when a NULL group key exists (kept as its own group)
    u32 null_group;  // index of the NULL group when has_null
    char pad[4];
};

static double now_ms() {
    using clk = std::chrono::steady_clock;
    return std::chrono::duration<double, std::milli>(clk::now().time_since_epoch()).count();
}

static std::vector<i32> read_i32(const char* p) {
    std::ifstream f(p, std::ios::binary);
    f.seekg(0, std::ios::end);
    size_t n = size_t(f.tellg()) / 4;
    f.seekg(0);
    std::vector<i32> v(n);
    f.read(reinterpret_cast<char*>(v.data()), std::streamsize(n) * 4);
    return v;
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

static inline i64 value_at(i64 vmin, u32 dense, const i64* distinct, u64 rank) {
    return dense ? vmin + i64(rank) : distinct[rank];
}

// ---------------------------------------------------------------------------
// build: plain columns
// ---------------------------------------------------------------------------
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

    // observed distinct values (NULL payloads are not part of the value domain)
    std::vector<i32> distinct(val);
    std::sort(distinct.begin(), distinct.end());
    distinct.erase(std::unique(distinct.begin(), distinct.end()), distinct.end());
    const u64 V = distinct.size();
    if (!V) throw std::runtime_error("empty");
    const i64 vmin = distinct.front();
    // dense mode addresses a value by (vmin + rank), which is only exact when
    // every integer in [vmin, vmax] is observed.  The original condition only
    // checked that the span was small relative to V, so a gappy observed set
    // (e.g. {0, 8}: span 9 <= 2*8) selected dense mode and then mapped range
    // bounds to wrong ranks (and MIN/MAX to wrong values).
    // SPEC-RUNNER FIX 1 (2026-11-01): require span == V (contiguity).
    const i64 span_i = i64(distinct.back()) - vmin + 1;
    const bool dense = span_i > 0 && u64(span_i) == V &&
                       u64(span_i) <= 4u * 1024u * 1024u && u64(span_i) <= V * 8;
    double t1 = now_ms();

    // rank lookup: L2-resident binary search over the distinct array
    auto rank_of = [&](i32 v) -> u32 {
        return u32(std::lower_bound(distinct.begin(), distinct.end(), v) - distinct.begin());
    };

    const u64 S = V + 1;
    std::vector<u64> counts(size_t(G) * S, 0);
    std::vector<i64> sums(size_t(G) * S, 0);
    for (u64 i = 0; i < n; ++i) {
        i32 g = gid[i];
        const u64 gi = g < 0 ? u64(null_group) : u64(g);
        u32 r = rank_of(val[i]);
        u64& c = counts[size_t(gi) * S + r + 1];
        c += 1;
        sums[size_t(gi) * S + r + 1] += val[i];
    }
    double t2 = now_ms();
    for (u32 g = 0; g < G; ++g) {
        u64* c = counts.data() + size_t(g) * S;
        i64* s = sums.data() + size_t(g) * S;
        for (u64 r = 1; r < S; ++r) {
            c[r] += c[r - 1];
            s[r] += s[r - 1];
        }
    }
    double t3 = now_ms();

    Header h{};
    std::memcpy(h.magic, MAGIC, 8);
    h.n = n;
    h.ngroups = G;
    h.nvalues = V;
    h.vmin = vmin;
    h.dense = dense ? 1 : 0;
    h.nmeasures = 1;
    h.has_null = has_null ? 1 : 0;
    h.null_group = has_null ? null_group : 0;
    std::ofstream o(out_path, std::ios::binary);
    o.write(reinterpret_cast<const char*>(&h), sizeof(h));
    if (!dense) {
        std::vector<i64> dv(V);
        for (u64 r = 0; r < V; ++r) dv[r] = distinct[r];
        o.write(reinterpret_cast<const char*>(dv.data()), std::streamsize(V * 8));
    }
    o.write(reinterpret_cast<const char*>(counts.data()), std::streamsize(counts.size() * 8));
    o.write(reinterpret_cast<const char*>(sums.data()), std::streamsize(sums.size() * 8));
    o.close();
    double t4 = now_ms();
    std::fprintf(stderr,
                 "{\"build_ms\":%.3f,\"read_ms\":%.3f,\"accum_ms\":%.3f,\"prefix_ms\":%.3f,"
                 "\"write_ms\":%.3f,\"groups\":%u,\"values\":%llu,\"rows\":%llu,"
                 "\"bytes\":%llu,\"dense\":%d}\n",
                 t4 - t0, t1 - t0, t2 - t1, t3 - t2, t4 - t3, G, (unsigned long long)V,
                 (unsigned long long)n,
                 (unsigned long long)(sizeof(Header) + (dense ? 0 : V * 8) + counts.size() * 8 +
                                      sums.size() * 8),
                 int(dense));
}


// ---------------------------------------------------------------------------
// build: directly from the fused encoded column tree (no row materialization)
// ---------------------------------------------------------------------------
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
    // number of dictionary codes for the group column
    U G = 0;
    {
        std::ifstream f(std::string(prep) + "/state.dict", std::ios::binary);
        std::string d((std::istreambuf_iterator<char>(f)), {});
        if (d.size() < 52 || d.compare(0, 8, "BSSTR01\n")) throw std::runtime_error("dict");
        std::memcpy(&G, d.data() + 48, 4);
    }
    const U n = g->n;
    const U blocks = g->leaves.size();
    // pass 0: does the group column contain invalid (NULL) keys?
    bool has_null = false;
    for (U b = 0; b < blocks && !has_null; ++b) {
        const U off = b * V; const U take = std::min<U>(V, n - off);
        for (U i = 0; i < take; ++i) {
            if (!stream_valid_bit(g->valid, off + i)) { has_null = true; break; }
        }
    }
    const u32 null_group = has_null ? u32(G) : 0xffffffffu;
    if (has_null) G += 1;
    // pass 1: observed distinct payload values
    std::unordered_set<i32> distinct;
    distinct.reserve(1 << 16);
    Scratch gd, vd;
    for (U b = 0; b < blocks; ++b) {
        const U off = b * V;
        const U take = std::min<U>(V, n - off);
        stream_decode_block(*g, b, gd);
        stream_decode_block(*v, b, vd);
        for (U i = 0; i < take; ++i) {
            distinct.insert(i32(i64(vd.d[i]) - i64(CODE_BIAS)));
        }
    }
    double t1 = now_ms();
    std::vector<i32> dv(distinct.begin(), distinct.end());
    std::sort(dv.begin(), dv.end());
    const u64 Vn = dv.size();
    if (!Vn) throw std::runtime_error("empty");
    const u64 S = Vn + 1;
    const bool dense = false;  // encoded build always uses the observed distinct set
    std::vector<u64> counts(size_t(G) * S, 0);
    std::vector<i64> sums(size_t(G) * S, 0);
    // pass 2: accumulate
    for (U b = 0; b < blocks; ++b) {
        const U off = b * V;
        const U take = std::min<U>(V, n - off);
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
            counts[size_t(gi) * S + r + 1] += 1;
            sums[size_t(gi) * S + r + 1] += val;
        }
    }
    double t2 = now_ms();
    for (U gg = 0; gg < G; ++gg) {
        u64* c = counts.data() + size_t(gg) * S;
        i64* s2 = sums.data() + size_t(gg) * S;
        for (u64 r = 1; r < S; ++r) { c[r] += c[r - 1]; s2[r] += s2[r - 1]; }
    }
    double t3 = now_ms();
    Header h{};
    std::memcpy(h.magic, MAGIC, 8);
    h.n = n; h.ngroups = G; h.nvalues = Vn; h.vmin = dv.front();
    h.dense = 0; h.nmeasures = 1;
    h.has_null = has_null ? 1 : 0;
    h.null_group = has_null ? null_group : 0;
    std::ofstream o(out_path, std::ios::binary);
    o.write(reinterpret_cast<const char*>(&h), sizeof(h));
    std::vector<i64> dv64(Vn);
    for (u64 r = 0; r < Vn; ++r) dv64[r] = dv[r];
    o.write(reinterpret_cast<const char*>(dv64.data()), std::streamsize(Vn * 8));
    o.write(reinterpret_cast<const char*>(counts.data()), std::streamsize(counts.size() * 8));
    o.write(reinterpret_cast<const char*>(sums.data()), std::streamsize(sums.size() * 8));
    o.close();
    double t4 = now_ms();
    std::fprintf(stderr,
                 "{\"build_ms\":%.3f,\"pass1_ms\":%.3f,\"accum_ms\":%.3f,\"prefix_ms\":%.3f,"
                 "\"write_ms\":%.3f,\"groups\":%llu,\"values\":%llu,\"rows\":%llu,"
                 "\"bytes\":%llu,\"dense\":0,\"encoded\":1}\n",
                 t4 - t0, t1 - t0, t2 - t1, t3 - t2, t4 - t3,
                 (unsigned long long)G, (unsigned long long)Vn, (unsigned long long)n,
                 (unsigned long long)(sizeof(Header) + Vn * 8 + counts.size() * 8 + sums.size() * 8));
}

// ---------------------------------------------------------------------------
// query
// ---------------------------------------------------------------------------
struct Cube {
    Header h;
    const i64* distinct = nullptr;
    const u64* counts = nullptr;  // G*(V+1)
    const i64* sums = nullptr;    // G*(V+1)
    const char* base = nullptr;
    size_t bytes = 0;
    int fd = -1;
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
    const char* p = c.base + sizeof(Header);
    c.distinct = c.h.dense ? nullptr : reinterpret_cast<const i64*>(p);
    if (!c.h.dense) p += c.h.nvalues * 8;
    c.counts = reinterpret_cast<const u64*>(p);
    p += c.h.ngroups * (c.h.nvalues + 1) * 8;
    c.sums = reinterpret_cast<const i64*>(p);
    return c;
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

static u64 run_query(const Cube& c, const std::vector<std::string>& names, const QSpec& q,
                     const char* out_path, bool write_out, bool serialize_result = false,
                     std::string* reuse = nullptr) {
    const u64 V = c.h.nvalues;
    const u64 G = c.h.ngroups;
    const u64 S = V + 1;
    u64 a = 0, b = V;
    if (q.has_lo || q.has_hi) {
        if (q.has_lo && q.has_hi && q.lo > q.hi) {
            a = b = 0;
        } else if (c.h.dense) {
            // Value domain is contiguous, so rank r <-> value vmin+r.
            // Clamp the bounds *before* subtracting: lo - vmin and
            // hi - vmin + 1 overflow for lo=INT64_MIN / hi=INT64_MAX, which
            // silently produced an empty result for ranges that span the
            // whole int64 domain.  vmin is an i32 value and V is bounded by
            // the dense-mode cap, so `last` cannot overflow.
            // SPEC-RUNNER FIX 3 (2026-11-01).
            const i64 last = c.h.vmin + i64(V) - 1;
            i64 ra, rb;
            if (!q.has_lo || q.lo <= c.h.vmin) ra = 0;
            else if (q.lo > last) ra = i64(V);
            else ra = q.lo - c.h.vmin;
            if (!q.has_hi || q.hi >= last) rb = i64(V);
            else if (q.hi < c.h.vmin) rb = 0;
            else rb = q.hi - c.h.vmin + 1;
            a = u64(ra);
            b = u64(rb);
        } else {
            const i64* d = c.distinct;
            a = u64(std::lower_bound(d, d + V, q.has_lo ? q.lo : INT64_MIN) - d);
            b = u64(std::upper_bound(d, d + V, q.has_hi ? q.hi : INT64_MAX) - d);
        }
        if (b < a) b = a;
    }
    std::vector<char> allowed(G, 1);
    for (u64 g = 0; g < G; ++g) {
        const std::string& nm = names[g];
        bool ok = true;
        if (!q.in.empty()) ok = std::find(q.in.begin(), q.in.end(), nm) != q.in.end();
        if (ok && !q.not_in.empty()) ok = std::find(q.not_in.begin(), q.not_in.end(), nm) == q.not_in.end();
        allowed[g] = ok ? 1 : 0;
    }
    // SQL three-valued logic: a NULL group key is never TRUE for `g IN (...)`
    // and never TRUE for `g NOT IN (...)`.  The original code kept the NULL
    // group for NOT IN (its rendered name "NULL" is not in the exclusion list),
    // which added a row that SQL does not return.
    // SPEC-RUNNER FIX 2 (2026-11-01): drop the NULL group whenever a group-domain
    // predicate is present.
    if (c.h.has_null && (!q.in.empty() || !q.not_in.empty())) allowed[c.h.null_group] = 0;
    // materialize exact per-group results
    struct Row { u64 g, cnt; i64 sum, mn, mx; };
    std::vector<Row> rows;
    rows.reserve(G);
    for (u64 g = 0; g < G; ++g) {
        if (!allowed[g]) continue;
        const u64* cp = c.counts + g * S;
        const i64* sp = c.sums + g * S;
        u64 cnt = cp[b] - cp[a];
        if (!cnt) continue;
        i64 sum = sp[b] - sp[a];
        // first non-empty rank in [a,b): smallest j>a with cp[j] > cp[a]
        u64 mnr, mxr;
        {
            u64 lo = a + 1, hi = b;
            u64 base = cp[a];
            while (lo < hi) {
                u64 mid = (lo + hi) / 2;
                if (cp[mid] > base) hi = mid;
                else lo = mid + 1;
            }
            mnr = lo - 1;
        }
        {
            // largest r<b with count>0: smallest j in [a,b] with cp[j]==cp[b]
            u64 lo = a, hi = b;
            u64 top = cp[b];
            while (lo < hi) {
                u64 mid = (lo + hi) / 2;
                if (cp[mid] >= top) hi = mid;
                else lo = mid + 1;
            }
            mxr = lo - 1;
        }
        i64 mn = value_at(c.h.vmin, c.h.dense, c.distinct, mnr);
        i64 mx = value_at(c.h.vmin, c.h.dense, c.distinct, mxr);
        rows.push_back({g, cnt, sum, mn, mx});
    }
    u64 checksum = 0;
    for (const Row& r : rows) checksum = checksum * 1099511628211ULL + r.sum + r.cnt + u64(r.mn) + u64(r.mx) + r.g;
    if (!write_out && !serialize_result) return checksum;
    std::string local;
    std::string& out = reuse ? *reuse : local;
    out.clear();
    out.reserve(64 * 1024);
    out += "{\"rows\":[";
    bool first = true;
    char buf[64];
    for (const Row& r : rows) {
        bool ok = true;
        for (const auto& h : q.having)
            if (!parse_having(h, r.sum, r.cnt, r.mn, r.mx)) { ok = false; break; }
        if (!ok) continue;
        if (!first) out += ',';
        first = false;
        out += '[';
        out += '"';
        out += names[r.g];
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
    out += "]}\n";
    if (write_out) {
        FILE* f = fopen(out_path, "wb");
        fwrite(out.data(), 1, out.size(), f);
        fclose(f);
    }
    checksum = checksum * 1099511628211ULL + out.size();
    return checksum;
}

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
            q.lo = std::stoll(a.substr(3));
            q.has_lo = true;
        } else if (a.rfind("hi=", 0) == 0) {
            q.hi = std::stoll(a.substr(3));
            q.has_hi = true;
        } else if (a.rfind("where_in=", 0) == 0) {
            q.in = split(a.substr(9));
        } else if (a.rfind("where_not_in=", 0) == 0) {
            q.not_in = split(a.substr(13));
        } else if (a.rfind("having=", 0) == 0) {
            q.having = split(a.substr(7));
        }
    }
    return q;
}

int main(int argc, char** argv) {
    try {
        if (argc < 2) return 2;
        std::string mode = argv[1];
        if (mode == "build" && argc >= 5) {
            build_plain(argv[2], argv[3], argv[4]);
            return 0;
        }
        if (mode == "build_enc" && argc >= 7) {
            build_encoded(argv[2], argv[3], U(std::stoul(argv[4])), U(std::stoul(argv[5])),
                          argv[6]);
            return 0;
        }
        if ((mode == "query" || mode == "bench") && argc >= 5) {
            const char* cube_path = argv[2];
            const char* dict_path = argv[3];
            const char* out_path = argv[4];
            int reps = 1;
            int start = 5;
            if (mode == "bench") {
                reps = std::stoi(argv[5]);
                start = 6;
            }
            QSpec q = parse_spec(argc, argv, start);
            double t0 = now_ms();
            Cube c = open_cube(cube_path);
            std::vector<std::string> names = dict_names(dict_path);
            if (names.size() + (c.h.has_null ? 1 : 0) < c.h.ngroups)
                throw std::runtime_error("dict short");
            names.resize(c.h.ngroups);
            if (c.h.has_null && c.h.null_group < names.size()) names[c.h.null_group] = "NULL";
            double t1 = now_ms();
            // warm the touched pages so per-query numbers are not page faults
            run_query(c, names, q, out_path, false);
            std::vector<double> samples;
            samples.reserve(reps);
            u64 check = 0;
            for (int i = 0; i < reps; ++i) {
                double s = now_ms();
                check += run_query(c, names, q, out_path, false);
                samples.push_back(now_ms() - s);
            }
            std::vector<double> ser_samples;
            ser_samples.reserve(reps);
            std::string reuse;
            for (int i = 0; i < reps; ++i) {
                double s = now_ms();
                check += run_query(c, names, q, out_path, false, true, &reuse);
                ser_samples.push_back(now_ms() - s);
            }
            double t2 = now_ms();
            run_query(c, names, q, out_path, true);
            double t3 = now_ms();
            std::sort(samples.begin(), samples.end());
            double med = samples[samples.size() / 2];
            double p95 = samples[size_t(samples.size() * 95 / 100)];
            std::sort(ser_samples.begin(), ser_samples.end());
            double ser_med = ser_samples[ser_samples.size() / 2];
            double ser_p95 = ser_samples[size_t(ser_samples.size() * 95 / 100)];
            std::fprintf(stderr,
                         "{\"setup_ms\":%.4f,\"query_ms\":%.4f,\"query_median_ms\":%.4f,"
                         "\"query_p95_ms\":%.4f,\"query_min_ms\":%.4f,\"output_ms\":%.4f,"
                         "\"stored_ms\":%.4f,\"reps\":%d,\"bytes\":%llu,\"groups\":%llu,"
                         "\"values\":%llu,\"checksum\":%llu}\n",
                         t1 - t0, samples.empty() ? 0.0 : med, med, p95,
                         samples.empty() ? 0.0 : samples.front(), ser_med, ser_p95, t3 - t2,
                         t3 - t1, reps,
                         (unsigned long long)c.bytes, (unsigned long long)c.h.ngroups,
                         (unsigned long long)c.h.nvalues, (unsigned long long)check);
            return 0;
        }
        std::fprintf(stderr,
                     "usage:\n  enc_cube build GID VALUES OUT\n"
                     "  enc_cube query CUBE DICT OUT [range=lo,hi|lo=|hi=] [where_in=a,b] ...\n"
                     "  enc_cube bench CUBE DICT OUT REPS [spec...]\n");
        return 2;
    } catch (std::exception& e) {
        std::fprintf(stderr, "ERR %s\n", e.what());
        return 1;
    }
}
