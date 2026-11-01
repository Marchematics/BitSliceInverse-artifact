// EAI v3: exact encoded-domain aggregate cube with NULL-measure support (ECUBE03).
//
// Same structure as ECUBE02 (per-rank counts plus a 64-rank (count,sum) block
// prefix, value-implicit sums), extended with an exact per-group NULL-measure
// count. Rows whose value payload is NULL contribute neither to SUM nor to
// MIN/MAX and are absent from the value domain, but they do contribute to
// COUNT(*) when the query has no range predicate on the value column (a range
// predicate is NULL -> not TRUE, so those rows are excluded, exactly as in SQL).

//
// For every group g and observed value rank r we keep ONLY the exact
// count[g][r]; a rank's value is known from the distinct-value array, so its SUM
// contribution is value[r]*count[r]. Every R=64 ranks we store a prefix of
// (count,sum), so a range answer is two block-prefix differences for the interior
// plus an exact scan of at most 2R rank counts at the two boundaries. This drops
// the per-rank sum prefix (the dominant term of v1) while staying exact: SUM is a
// sum of exact products. Space is ~4.2 bytes per (group,rank) vs 12 in v1.
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

static constexpr const char* MAGIC = "ECUBE03\n";
static constexpr u32 R_BLOCK = 64;

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
    u32 null_measure;  // 1 when the value column carries NULL payloads
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

struct Cube {
    Header h;
    const i64* distinct = nullptr;
    const u32* counts = nullptr;
    const u32* null_cnt = nullptr;  // G entries: rows whose value payload is NULL
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
    c.null_cnt = reinterpret_cast<const u32*>(p);
    p += G * 4;
    c.block_cnt = reinterpret_cast<const u32*>(p);
    p += G * (c.nblocks + 1) * 4;
    c.block_sum = reinterpret_cast<const i64*>(p);
    return c;
}

struct Writer {
    std::ofstream o;
    explicit Writer(const char* path) : o(path, std::ios::binary) {}
    template <class T> void put(const std::vector<T>& v) {
        o.write(reinterpret_cast<const char*>(v.data()), std::streamsize(v.size() * sizeof(T)));
    }
};

static void write_cube(const char* out_path, const Header& h, const std::vector<i64>& dv,
                       const std::vector<u32>& counts, const std::vector<u32>& nullcnt,
                       const std::vector<u32>& bcnt, const std::vector<i64>& bsum) {
    Writer w(out_path);
    w.o.write(reinterpret_cast<const char*>(&h), sizeof(h));
    if (!h.dense) w.put(dv);
    w.put(counts);
    w.put(nullcnt);
    w.put(bcnt);
    w.put(bsum);
    w.o.close();
}

static void build_plain(const char* gid_path, const char* val_path, const char* out_path,
                        const char* valid_path) {
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
    // Dense addressing (value = vmin + rank) is only exact for a contiguous
    // observed domain; gappy domains must use the explicit distinct array.
    const u64 span = u64(distinct.back()) - u64(vmin) + 1;
    const bool dense = (span == V) && span <= (1u << 22);
    double t1 = now_ms();
    std::vector<u32> counts(size_t(G) * V, 0);
    std::vector<u32> nullcnt(G, 0);
    std::vector<uint64_t> valid_words;
    if (valid_path && *valid_path) {
        std::ifstream vf(valid_path, std::ios::binary);
        vf.seekg(0, std::ios::end);
        size_t vb = size_t(vf.tellg());
        vf.seekg(0);
        valid_words.resize((vb + 7) / 8);
        vf.read(reinterpret_cast<char*>(valid_words.data()), std::streamsize(valid_words.size() * 8));
    }
    for (u64 i = 0; i < n; ++i) {
        i32 g = gid[i];
        const u64 gi = g < 0 ? u64(null_group) : u64(g);
        if (!valid_words.empty() && !((valid_words[i >> 6] >> (i & 63)) & 1ULL)) {
            nullcnt[gi] += 1;
            continue;
        }
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
    h.null_measure = valid_words.empty() ? 0 : 1;
    std::vector<i64> dv;
    if (!dense) { dv.resize(V); for (u64 r = 0; r < V; ++r) dv[r] = distinct[r]; }
    write_cube(out_path, h, dv, counts, nullcnt, bcnt, bsum);
    double t4 = now_ms();
    const u64 bytes = sizeof(Header) + (dense ? 0 : V * 8) + G * V * 4 + G * 4 + G * (NB + 1) * 12;
    std::fprintf(stderr,
                 "{\"build_ms\":%.3f,\"read_ms\":%.3f,\"accum_ms\":%.3f,\"prefix_ms\":%.3f,"
                 "\"write_ms\":%.3f,\"groups\":%u,\"values\":%llu,\"rows\":%llu,"
                 "\"bytes\":%llu,\"dense\":%d,\"v2\":1}\n",
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
    const bool v_nullable = (v->valid != nullptr);
    bool has_null_measure = false;
    if (v_nullable) {
        for (U b = 0; b < blocks && !has_null_measure; ++b) {
            const U off = b * V; const U take = std::min<U>(V, n - off);
            for (U i = 0; i < take; ++i)
                if (!stream_valid_bit(v->valid, off + i)) { has_null_measure = true; break; }
        }
    }
    Scratch gd, vd;
    for (U b = 0; b < blocks; ++b) {
        const U off = b * V; const U take = std::min<U>(V, n - off);
        stream_decode_block(*g, b, gd);
        stream_decode_block(*v, b, vd);
        for (U i = 0; i < take; ++i) {
            if (v_nullable && !stream_valid_bit(v->valid, off + i)) continue;
            distinct.insert(i32(i64(vd.d[i]) - i64(CODE_BIAS)));
        }
    }
    double t1 = now_ms();
    std::vector<i32> dv(distinct.begin(), distinct.end());
    std::sort(dv.begin(), dv.end());
    const u64 Vn = dv.size();
    if (!Vn) throw std::runtime_error("empty");
    std::vector<u32> counts(size_t(G) * Vn, 0);
    std::vector<u32> nullcnt(G, 0);
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
            if (v_nullable && !stream_valid_bit(v->valid, off + i)) {
                nullcnt[gi] += 1;
                continue;
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
    h.null_measure = has_null_measure ? 1 : 0;
    std::vector<i64> dv64(Vn);
    for (u64 r = 0; r < Vn; ++r) dv64[r] = dv[r];
    write_cube(out_path, h, dv64, counts, nullcnt, bcnt, bsum);
    double t4 = now_ms();
    const u64 bytes = sizeof(Header) + Vn * 8 + G * Vn * 4 + G * 4 + G * (NB + 1) * 12;
    std::fprintf(stderr,
                 "{\"build_ms\":%.3f,\"pass1_ms\":%.3f,\"accum_ms\":%.3f,\"prefix_ms\":%.3f,"
                 "\"write_ms\":%.3f,\"groups\":%llu,\"values\":%llu,\"rows\":%llu,"
                 "\"bytes\":%llu,\"dense\":0,\"encoded\":1,\"v2\":1}\n",
                 t4 - t0, t1 - t0, t2 - t1, t3 - t2, t4 - t3, (unsigned long long)G,
                 (unsigned long long)Vn, (unsigned long long)n, (unsigned long long)bytes);
}

struct QSpec {
    bool has_lo = false, has_hi = false;
    i64 lo = 0, hi = 0;
    std::vector<std::string> in, not_in, having;
};

static bool parse_having(const std::string& c, i64 sum, u64 cnt, i64 mn, i64 mx,
                         bool val_null) {
    size_t pos = c.find_first_of("<>=");
    if (pos == std::string::npos) return true;
    std::string lhs = c.substr(0, pos);
    std::string op = c.substr(pos, (c.compare(pos, 2, ">=") == 0 || c.compare(pos, 2, "<=") == 0) ? 2 : 1);
    std::string rhs = c.substr(pos + op.size());
    long double v = std::strtold(rhs.c_str(), nullptr);
    long double x;
    if (lhs == "sum") { if (val_null) return false; x = (long double)sum; }
    else if (lhs == "count") x = (long double)cnt;
    else if (lhs == "min") { if (val_null) return false; x = (long double)mn; }
    else if (lhs == "max") { if (val_null) return false; x = (long double)mx; }
    else return true;
    if (op == ">=") return x >= v;
    if (op == "<=") return x <= v;
    if (op == ">") return x > v;
    if (op == "<") return x < v;
    if (op == "=") return x == v;
    return true;
}

struct Row { u64 g, cnt; i64 sum, mn, mx; bool val_null; };

static inline void scan_ranks(const Cube& c, const u32* C, u64 a, u64 b, u64& cnt, i64& sum) {
    for (u64 r = a; r < b; ++r) {
        const u32 k = C[r];
        if (!k) continue;
        cnt += k;
        sum += i64(k) * value_at(c.h.vmin, c.h.dense, c.distinct, r);
    }
}

static u64 run_query(const Cube& c, const std::vector<std::string>& names, const QSpec& q,
                     const char* out_path, bool write_out, bool serialize_result = false,
                     std::string* reuse = nullptr) {
    const u64 V = c.h.nvalues, G = c.h.ngroups, R = c.h.block_ranks;
    u64 a = 0, b = V;
    if (q.has_lo || q.has_hi) {
        if (q.has_lo && q.has_hi && q.lo > q.hi) {
            a = b = 0;
        } else if (c.h.dense) {
            const i64 vmax = c.h.vmin + i64(V) - 1;
            i64 lo = q.has_lo ? q.lo : c.h.vmin;
            i64 hi = q.has_hi ? q.hi : vmax;
            if (lo < c.h.vmin) lo = c.h.vmin;  // clamp first: no overflow below
            if (hi > vmax) hi = vmax;           // clamp first: no overflow above
            i64 ra = lo - c.h.vmin, rb = hi - c.h.vmin + 1;
            if (hi < lo) { ra = 0; rb = 0; }
            if (ra < 0) ra = 0;
            if (ra > i64(V)) ra = i64(V);
            if (rb < 0) rb = 0;
            if (rb > i64(V)) rb = i64(V);
            a = u64(ra); b = u64(rb);
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
    // NULL is not TRUE for any IN/NOT IN predicate, so SQL drops the NULL group.
    if (c.h.has_null && c.h.null_group < G && (!q.in.empty() || !q.not_in.empty()))
        allowed[c.h.null_group] = 0;
    const u64 NB = c.nblocks;
    const u64 ba = a / R, bb = b / R;
    std::vector<Row> rows;
    rows.reserve(G);
    for (u64 g = 0; g < G; ++g) {
        if (!allowed[g]) continue;
        const u32* C = c.counts + g * V;
        const u32* BC = c.block_cnt + g * (NB + 1);
        const i64* BS = c.block_sum + g * (NB + 1);
        u64 cnt = 0;
        i64 sum = 0;
        if (b > a) {
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
        const bool has_range = q.has_lo || q.has_hi;
        const u64 nulls = (c.h.null_measure && !has_range) ? u64(c.null_cnt[g]) : 0;
        const u64 total = cnt + nulls;
        if (!total) continue;
        if (!cnt) {
            rows.push_back({g, total, 0, 0, 0, true});
            continue;
        }
        // MIN/MAX: first/last rank in [a,b) with count>0. Block-prefix
        // predicates must be monotone: min uses "count up to block j exceeds the
        // count before block ba", max uses "count before block j is below the
        // total before block bb", with the two boundary blocks scanned directly.
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
        rows.push_back({g, total, sum, value_at(c.h.vmin, c.h.dense, c.distinct, mnr),
                        value_at(c.h.vmin, c.h.dense, c.distinct, mxr), false});
    }
    u64 checksum = 0;
    for (const Row& r : rows)
        checksum = checksum * 1099511628211ULL + r.sum + r.cnt + u64(r.mn) + u64(r.mx) + r.g;
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
            if (!parse_having(h, r.sum, r.cnt, r.mn, r.mx, r.val_null)) { ok = false; break; }
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
        auto app_or_null = [&](long long v, bool is_null) {
            if (is_null) { out += ",\"None\""; return; }
            app(v);
        };
        app_or_null((long long)r.sum, r.val_null);
        app((long long)r.cnt);
        app_or_null((long long)r.mn, r.val_null);
        app_or_null((long long)r.mx, r.val_null);
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

int main(int argc, char** argv) {
    try {
        if (argc < 2) return 2;
        std::string mode = argv[1];
        if (mode == "build" && argc >= 5) {
            build_plain(argv[2], argv[3], argv[4], argc >= 6 ? argv[5] : nullptr);
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
            int reps = 1, start = 5;
            if (mode == "bench") { reps = std::stoi(argv[5]); start = 6; }
            QSpec q = parse_spec(argc, argv, start);
            double t0 = now_ms();
            Cube c = open_cube(cube_path);
            std::vector<std::string> names = dict_names(dict_path);
            if (names.size() + (c.h.has_null ? 1 : 0) < c.h.ngroups) throw std::runtime_error("dict short");
            names.resize(c.h.ngroups);
            if (c.h.has_null && c.h.null_group < names.size()) names[c.h.null_group] = "NULL";
            double t1 = now_ms();
            run_query(c, names, q, out_path, false);
            std::vector<double> samples;
            samples.reserve(reps);
            u64 check = 0;
            for (int i = 0; i < reps; ++i) {
                double s = now_ms();
                check += run_query(c, names, q, out_path, false);
                samples.push_back(now_ms() - s);
            }
            std::vector<double> ser;
            ser.reserve(reps);
            std::string reuse;
            for (int i = 0; i < reps; ++i) {
                double s = now_ms();
                check += run_query(c, names, q, out_path, false, true, &reuse);
                ser.push_back(now_ms() - s);
            }
            double t2 = now_ms();
            run_query(c, names, q, out_path, true);
            double t3 = now_ms();
            std::sort(samples.begin(), samples.end());
            std::sort(ser.begin(), ser.end());
            double med = samples[samples.size() / 2];
            double p95 = samples[size_t(samples.size() * 95 / 100)];
            double smed = ser[ser.size() / 2];
            double sp95 = ser[size_t(ser.size() * 95 / 100)];
            std::fprintf(stderr,
                         "{\"setup_ms\":%.4f,\"query_median_ms\":%.6f,\"query_p95_ms\":%.6f,"
                         "\"query_min_ms\":%.6f,\"serialize_median_ms\":%.6f,"
                         "\"serialize_p95_ms\":%.6f,\"output_ms\":%.4f,\"reps\":%d,"
                         "\"bytes\":%llu,\"groups\":%llu,\"values\":%llu,\"checksum\":%llu}\n",
                         t1 - t0, med, p95, samples.front(), smed, sp95, t3 - t2, reps,
                         (unsigned long long)c.bytes, (unsigned long long)c.h.ngroups,
                         (unsigned long long)c.h.nvalues, (unsigned long long)check);
            return 0;
        }
        std::fprintf(stderr,
                     "usage:\n  enc_cube build GID VALUES OUT [VALIDITY_BITS]\n"
                     "  enc_cube build_enc ENC PREP GID VID OUT\n"
                     "  enc_cube query CUBE DICT OUT [range=|lo=|hi=] [where_in=] [having=]\n"
                     "  enc_cube bench CUBE DICT OUT REPS [spec...]\n");
        return 2;
    } catch (std::exception& e) {
        std::fprintf(stderr, "ERR %s\n", e.what());
        return 1;
    }
}
