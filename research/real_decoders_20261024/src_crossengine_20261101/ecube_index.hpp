// Exact aggregate cube (ECUBE02 pinned / ECUBE03 with NULL-measure) reader.
//
// Single source of truth for the *query* semantics of the pinned cube. Shared by
//   * ecube_duck.cpp      -> DuckDB 2.0.0 C API table function + engine harness
//   * ecube_panel.cpp     -> engine-resident warm panel driver
//   * ecube_fmt_build.cpp -> cross-format builder (verifies its own output)
// The reduction is a verbatim port of run_query() in
// src_eai_20261101/eai_cube2.cpp (ECUBE02) with the ECUBE03 NULL-measure
// extension from src_eai_20261101/eai_cube3.cpp, so the JSON produced here is
// byte-comparable with `enc_cube query` output.
#pragma once

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
#include <vector>

namespace ecube {

using u64 = uint64_t;
using i64 = int64_t;
using u32 = uint32_t;
using i32 = int32_t;

static constexpr const char* MAGIC2 = "ECUBE02\n";
static constexpr const char* MAGIC3 = "ECUBE03\n";
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
    u32 null_measure;  // ECUBE03 only; ECUBE02 pack = 0
    char pad[4];
};

inline double now_ms() {
    using clk = std::chrono::steady_clock;
    return std::chrono::duration<double, std::milli>(clk::now().time_since_epoch()).count();
}

inline std::vector<std::string> dict_names(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    if (!f) throw std::runtime_error("dict open " + path);
    std::string d((std::istreambuf_iterator<char>(f)), {});
    if (d.size() < 52 || d.compare(0, 8, "BSSTR01\n")) throw std::runtime_error("dict magic");
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

inline i64 value_at(i64 vmin, u32 dense, const i64* distinct, u64 rank) {
    return dense ? vmin + i64(rank) : distinct[rank];
}

struct Cube {
    Header h{};
    u32 format = 0;  // 2 or 3
    const i64* distinct = nullptr;
    const u32* counts = nullptr;
    const u32* null_cnt = nullptr;
    const u32* block_cnt = nullptr;
    const i64* block_sum = nullptr;
    const char* base = nullptr;
    size_t bytes = 0;
    int fd = -1;
    u64 nblocks = 0;
    std::vector<std::string> names;

    ~Cube() { close_index(); }
    Cube() = default;
    Cube(const Cube&) = delete;
    Cube& operator=(const Cube&) = delete;

    void close_index() {
        if (base && base != MAP_FAILED) munmap(const_cast<char*>(base), bytes);
        base = nullptr;
        if (fd >= 0) { ::close(fd); fd = -1; }
    }
};

inline void open_cube(const char* path, Cube& c) {
    c.fd = ::open(path, O_RDONLY);
    if (c.fd < 0) throw std::runtime_error(std::string("open cube ") + path);
    struct stat st {};
    if (fstat(c.fd, &st)) throw std::runtime_error("stat");
    c.bytes = size_t(st.st_size);
    c.base = static_cast<const char*>(mmap(nullptr, c.bytes, PROT_READ, MAP_PRIVATE, c.fd, 0));
    if (c.base == MAP_FAILED) { c.base = nullptr; throw std::runtime_error("mmap"); }
    if (c.bytes < sizeof(Header)) throw std::runtime_error("short cube");
    std::memcpy(&c.h, c.base, sizeof(Header));
    if (!std::memcmp(c.h.magic, MAGIC2, 8)) c.format = 2;
    else if (!std::memcmp(c.h.magic, MAGIC3, 8)) c.format = 3;
    else throw std::runtime_error("cube magic");
    if (c.format == 2) c.h.null_measure = 0;
    const u64 V = c.h.nvalues, G = c.h.ngroups;
    c.nblocks = (V + c.h.block_ranks - 1) / c.h.block_ranks;
    const char* p = c.base + sizeof(Header);
    c.distinct = c.h.dense ? nullptr : reinterpret_cast<const i64*>(p);
    if (!c.h.dense) p += V * 8;
    c.counts = reinterpret_cast<const u32*>(p);
    p += G * V * 4;
    c.null_cnt = nullptr;
    if (c.format >= 3) { c.null_cnt = reinterpret_cast<const u32*>(p); p += G * 4; }
    c.block_cnt = reinterpret_cast<const u32*>(p);
    p += G * (c.nblocks + 1) * 4;
    c.block_sum = reinterpret_cast<const i64*>(p);
    if (size_t(p - c.base) + G * (c.nblocks + 1) * 8 != c.bytes)
        throw std::runtime_error("cube size mismatch (format drift?)");
}

// Load an index plus its dictionary once. `load_ms` is the cost of open+dict.
struct LoadedIndex {
    Cube cube;
    double load_ms = 0;
};

inline void load_index(const std::string& cube_path, const std::string& dict_path,
                       LoadedIndex& out) {
    double t0 = now_ms();
    open_cube(cube_path.c_str(), out.cube);
    out.cube.names = dict_names(dict_path);
    if (out.cube.names.size() + (out.cube.h.has_null ? 1 : 0) < out.cube.h.ngroups)
        throw std::runtime_error("dict short");
    out.cube.names.resize(out.cube.h.ngroups);
    if (out.cube.h.has_null && out.cube.h.null_group < out.cube.names.size())
        out.cube.names[out.cube.h.null_group] = "NULL";
    out.load_ms = now_ms() - t0;
}

struct QSpec {
    bool has_lo = false, has_hi = false;
    i64 lo = 0, hi = 0;
    std::vector<std::string> in, not_in, having;
};

inline std::vector<std::string> split_commas(const std::string& s) {
    std::vector<std::string> v;
    size_t p = 0;
    while (p <= s.size()) {
        size_t t = s.find(',', p);
        if (t == std::string::npos) t = s.size();
        v.push_back(s.substr(p, t - p));
        p = t + 1;
    }
    return v;
}

// Parse one CLI-style token ("range=a,b", "lo=", "hi=", "where_in=",
// "where_not_in=", "having=") and merge it into q. Returns false if unknown.
inline bool add_spec_token(const std::string& a, QSpec& q) {
    if (a.rfind("range=", 0) == 0) {
        auto p = a.find(',');
        if (p == std::string::npos) return false;
        q.lo = std::stoll(a.substr(6, p - 6));
        q.hi = std::stoll(a.substr(p + 1));
        q.has_lo = q.has_hi = true;
    } else if (a.rfind("lo=", 0) == 0) {
        q.lo = std::stoll(a.substr(3)); q.has_lo = true;
    } else if (a.rfind("hi=", 0) == 0) {
        q.hi = std::stoll(a.substr(3)); q.has_hi = true;
    } else if (a.rfind("where_in=", 0) == 0) {
        q.in = split_commas(a.substr(9));
    } else if (a.rfind("where_not_in=", 0) == 0) {
        q.not_in = split_commas(a.substr(13));
    } else if (a.rfind("having=", 0) == 0) {
        auto parts = split_commas(a.substr(7));
        q.having.insert(q.having.end(), parts.begin(), parts.end());
    } else if (a.empty()) {
        return true;
    } else {
        return false;
    }
    return true;
}

// "; "-separated form used as a single SQL VARCHAR argument:
//   "hi=-720;where_in=1,2;having=sum>=1e10"
inline QSpec parse_spec_string(const std::string& s) {
    QSpec q;
    std::vector<std::string> toks;
    size_t p = 0;
    while (p <= s.size()) {
        size_t t = s.find(';', p);
        if (t == std::string::npos) t = s.size();
        std::string tok = s.substr(p, t - p);
        // trim
        while (!tok.empty() && (tok.front() == ' ' || tok.front() == '\t')) tok.erase(tok.begin());
        while (!tok.empty() && (tok.back() == ' ' || tok.back() == '\t')) tok.pop_back();
        if (!tok.empty()) toks.push_back(tok);
        p = t + 1;
    }
    for (const auto& t : toks)
        if (!add_spec_token(t, q)) throw std::runtime_error("bad spec token: " + t);
    return q;
}

inline bool parse_having(const std::string& c, i64 sum, u64 cnt, i64 mn, i64 mx,
                         bool val_null) {
    size_t pos = c.find_first_of("<>=");
    if (pos == std::string::npos) return true;
    std::string lhs = c.substr(0, pos);
    std::string op = c.substr(pos, (c.compare(pos, 2, ">=") == 0 || c.compare(pos, 2, "<=") == 0) ? 2 : 1);
    std::string rhs = c.substr(pos + op.size());
    long double v = std::strtold(rhs.c_str(), nullptr);
    long double x;
    if (lhs == "sum") {
        if (val_null) return false;  // NULL measure -> comparison is unknown
        x = (long double)sum;
    } else if (lhs == "count") {
        x = (long double)cnt;
    } else if (lhs == "min") {
        if (val_null) return false;
        x = (long double)mn;
    } else if (lhs == "max") {
        if (val_null) return false;
        x = (long double)mx;
    } else {
        return true;
    }
    if (op == ">=") return x >= v;
    if (op == "<=") return x <= v;
    if (op == ">") return x > v;
    if (op == "<") return x < v;
    if (op == "=") return x == v;
    return true;
}

struct Row { u64 g, cnt; i64 sum, mn, mx; bool val_null; };

inline void scan_ranks(const Cube& c, const u32* C, u64 a, u64 b, u64& cnt, i64& sum) {
    for (u64 r = a; r < b; ++r) {
        const u32 k = C[r];
        if (!k) continue;
        cnt += k;
        sum += i64(k) * value_at(c.h.vmin, c.h.dense, c.distinct, r);
    }
}

// Exact reduction: identical arithmetic and identical MIN/MAX monotone
// block-prefix search to the pinned `enc_cube query` path.
inline void reduce(const Cube& c, const QSpec& q, std::vector<Row>& rows) {
    const u64 V = c.h.nvalues, G = c.h.ngroups, R = c.h.block_ranks;
    u64 a = 0, b = V;
    if (q.has_lo || q.has_hi) {
        if (q.has_lo && q.has_hi && q.lo > q.hi) {
            a = b = 0;
        } else if (c.h.dense) {
            const i64 vmax = c.h.vmin + i64(V) - 1;
            i64 lo = q.has_lo ? q.lo : c.h.vmin;
            i64 hi = q.has_hi ? q.hi : vmax;
            if (lo < c.h.vmin) lo = c.h.vmin;
            if (hi > vmax) hi = vmax;
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
        const std::string& nm = c.names[g];
        bool ok = true;
        if (!q.in.empty()) ok = std::find(q.in.begin(), q.in.end(), nm) != q.in.end();
        if (ok && !q.not_in.empty())
            ok = std::find(q.not_in.begin(), q.not_in.end(), nm) == q.not_in.end();
        allowed[g] = ok ? 1 : 0;
    }
    // NULL is not TRUE for any IN/NOT IN predicate, so SQL drops the NULL group.
    if (c.h.has_null && c.h.null_group < G && (!q.in.empty() || !q.not_in.empty()))
        allowed[c.h.null_group] = 0;
    const u64 NB = c.nblocks;
    const u64 ba = a / R, bb = b / R;
    const bool has_range = q.has_lo || q.has_hi;
    rows.clear();
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
        const u64 nulls = (c.h.null_measure && c.null_cnt && !has_range) ? u64(c.null_cnt[g]) : 0;
        const u64 total = cnt + nulls;
        if (!total) continue;
        if (!cnt) {
            rows.push_back({g, total, 0, 0, 0, true});
            continue;
        }
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
}

inline u64 checksum(const std::vector<Row>& rows) {
    u64 ck = 0;
    for (const Row& r : rows)
        ck = ck * 1099511628211ULL + r.sum + r.cnt + u64(r.mn) + u64(r.mx) + r.g;
    return ck;
}

// Same JSON envelope as `enc_cube query` (HAVING applied here), so callers can
// assert byte equality against the native CLI output.
inline std::string to_json(const Cube& c, const std::vector<Row>& rows, const QSpec& q) {
    std::string out;
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
        out += "[\"";
        out += c.names[r.g];
        out += '"';
        auto app = [&](long long v) {
            auto r2 = std::to_chars(buf, buf + sizeof(buf), v);
            out += ",\"";
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
    return out;
}

}  // namespace ecube
