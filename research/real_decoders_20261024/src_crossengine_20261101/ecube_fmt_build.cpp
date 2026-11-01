// Cross-format ECUBE02/ECUBE03 builder.
//
// The pinned builder (src_eai_20261101/eai_cube2.cpp) has two input paths:
//   build_plain  <- two raw int32 column files (.i32)
//   build_enc    <- the fused encoded column tree (decode pass)
// and the program note claims the two produce byte-identical indexes. This file
// adds the third and fourth input paths needed for the cross-*format* claim:
// Parquet and Arrow IPC. The decode is done by pyarrow in dump_arrays.py (one
// decoded pass over the file, zero copy into int32 buffers), and the cube
// construction below is the pinned builder's accumulate / prefix / write code
// moved here verbatim, so the only difference between the four indexes is where
// the two int32 columns came from.
//
// Inputs (all produced by dump_arrays.py):
//   gid.i32           int32 group code, -1 = NULL group member
//   val.i32           int32 value
//   valid.bitmap | -  optional uint64 bitmap: 1 = value payload present
//                     (only meaningful for ECUBE03 / null_measure)
//
// Usage:
//   ecube_fmt_build build <gid.i32> <val.i32> <valid.bitmap|-> <out> <2|3>
//   ecube_fmt_build check <cube> <dict>          # header + sha-free self check
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>

#include "ecube_index.hpp"

using ecube::u32;
using ecube::u64;
using ecube::i32;
using ecube::i64;

namespace {

static constexpr u32 R_BLOCK = 64;

std::vector<i32> read_i32(const char* p) {
    std::ifstream f(p, std::ios::binary);
    if (!f) throw std::runtime_error(std::string("read ") + p);
    f.seekg(0, std::ios::end);
    size_t n = size_t(f.tellg()) / 4;
    f.seekg(0);
    std::vector<i32> v(n);
    f.read(reinterpret_cast<char*>(v.data()), std::streamsize(n) * 4);
    return v;
}

std::vector<uint64_t> read_bitmap(const char* p) {
    if (!p || !*p || std::strcmp(p, "-") == 0) return {};
    std::ifstream f(p, std::ios::binary);
    if (!f) throw std::runtime_error(std::string("read bitmap ") + p);
    f.seekg(0, std::ios::end);
    size_t bytes = size_t(f.tellg());
    f.seekg(0);
    std::vector<uint64_t> v((bytes + 7) / 8, 0);
    f.read(reinterpret_cast<char*>(v.data()), std::streamsize(v.size() * 8));
    return v;
}

template <class T>
void put(std::ofstream& o, const std::vector<T>& v) {
    o.write(reinterpret_cast<const char*>(v.data()), std::streamsize(v.size() * sizeof(T)));
}

int build(const char* gid_path, const char* val_path, const char* valid_path,
          const char* out_path, int format) {
    const double t0 = ecube::now_ms();
    std::vector<i32> gid = read_i32(gid_path);
    std::vector<i32> val = read_i32(val_path);
    if (gid.size() != val.size()) throw std::runtime_error("column length mismatch");
    const u64 n = gid.size();
    std::vector<uint64_t> vw = read_bitmap(valid_path);
    if (!vw.empty() && (vw.size() * 64) < n) throw std::runtime_error("bitmap short");
    const double t_read = ecube::now_ms();

    // ---- pinned builder semantics (build_plain) ------------------------------
    u32 G = 0;
    bool has_null = false;
    for (i32 g : gid) {
        if (g < 0) { has_null = true; continue; }
        if (u32(g) + 1 > G) G = u32(g) + 1;
    }
    const u32 null_group = has_null ? G : 0xffffffffu;
    if (has_null) G += 1;
    // Distinct value set. ECUBE03 excludes rows whose value payload is NULL from
    // the value domain; ECUBE02 has no NULL measure so this is a no-op there.
    std::vector<i32> distinct;
    distinct.reserve(1 << 16);
    for (u64 i = 0; i < n; ++i) {
        if (!vw.empty() && !((vw[i >> 6] >> (i & 63)) & 1ULL)) continue;
        distinct.push_back(val[i]);
    }
    std::sort(distinct.begin(), distinct.end());
    distinct.erase(std::unique(distinct.begin(), distinct.end()), distinct.end());
    const u64 V = distinct.size();
    if (!V) throw std::runtime_error("empty value domain");
    const i64 vmin = distinct.front();
    // The pinned build_enc path always writes dense = 0; match that exactly so
    // the four formats are byte-comparable with the encoded-tree index.
    const bool dense = false;
    const double t_distinct = ecube::now_ms();

    std::vector<u32> counts(size_t(G) * V, 0);
    std::vector<u32> nullcnt(G, 0);
    for (u64 i = 0; i < n; ++i) {
        const i32 g = gid[i];
        const u64 gi = g < 0 ? u64(null_group) : u64(g);
        if (gi >= G) continue;
        if (!vw.empty() && !((vw[i >> 6] >> (i & 63)) & 1ULL)) {
            nullcnt[gi] += 1;
            continue;
        }
        const u32 r = u32(std::lower_bound(distinct.begin(), distinct.end(), val[i]) - distinct.begin());
        counts[size_t(gi) * V + r] += 1;
    }
    const double t_accum = ecube::now_ms();

    const u64 NB = (V + R_BLOCK - 1) / R_BLOCK;
    std::vector<u32> bcnt(size_t(G) * (NB + 1), 0);
    std::vector<i64> bsum(size_t(G) * (NB + 1), 0);
    for (u32 g = 0; g < G; ++g) {
        const u32* c = counts.data() + size_t(g) * V;
        u32* bc = bcnt.data() + size_t(g) * (NB + 1);
        i64* bs = bsum.data() + size_t(g) * (NB + 1);
        for (u64 r = 0; r < V; ++r) {
            const u64 bi = r / R_BLOCK;
            const i64 v = i64(distinct[r]);
            bc[bi + 1] += c[r];
            bs[bi + 1] += i64(c[r]) * v;
        }
        for (u64 b = 1; b <= NB; ++b) { bc[b] += bc[b - 1]; bs[b] += bs[b - 1]; }
    }
    const double t_prefix = ecube::now_ms();

    ecube::Header h{};
    std::memcpy(h.magic, format == 3 ? ecube::MAGIC3 : ecube::MAGIC2, 8);
    h.n = n;
    h.ngroups = G;
    h.nvalues = V;
    h.vmin = vmin;
    h.dense = dense ? 1 : 0;
    h.has_null = has_null ? 1 : 0;
    h.null_group = null_group;
    h.block_ranks = R_BLOCK;
    h.null_measure = (format == 3 && !vw.empty()) ? 1 : 0;
    std::vector<i64> dv;
    if (!dense) {
        dv.resize(V);
        for (u64 r = 0; r < V; ++r) dv[r] = distinct[r];
    }
    std::ofstream o(out_path, std::ios::binary);
    if (!o) throw std::runtime_error("open out");
    o.write(reinterpret_cast<const char*>(&h), sizeof(h));
    if (!h.dense) put(o, dv);
    put(o, counts);
    if (format == 3) put(o, nullcnt);
    put(o, bcnt);
    put(o, bsum);
    o.close();
    const double t_write = ecube::now_ms();
    const u64 bytes = sizeof(ecube::Header) + (dense ? 0 : V * 8) + u64(G) * V * 4 +
                      (format == 3 ? u64(G) * 4 : 0) + u64(G) * (NB + 1) * 12;
    std::printf("{\"build_ms\":%.3f,\"read_ms\":%.3f,\"distinct_ms\":%.3f,"
                "\"accum_ms\":%.3f,\"prefix_ms\":%.3f,\"write_ms\":%.3f,\"format\":%d,"
                "\"rows\":%llu,\"groups\":%u,\"values\":%llu,\"bytes\":%llu,"
                "\"has_null_group\":%d,\"null_measure\":%u}\n",
                t_write - t0, t_read - t0, t_distinct - t_read, t_accum - t_distinct,
                t_prefix - t_accum, t_write - t_prefix, format, (unsigned long long)n, G,
                (unsigned long long)V, (unsigned long long)bytes, int(has_null),
                h.null_measure);
    return 0;
}

int check(const char* cube, const char* dict) {
    ecube::LoadedIndex li;
    ecube::load_index(cube, dict, li);
    std::printf("{\"format\":%u,\"rows\":%llu,\"groups\":%llu,\"values\":%llu,\"vmin\":%lld,"
                "\"dense\":%u,\"has_null\":%u,\"null_group\":%u,\"block_ranks\":%u,"
                "\"null_measure\":%u,\"bytes\":%llu,\"index_load_ms\":%.6f,"
                "\"null_group_name\":\"%s\"}\n",
                li.cube.format, (unsigned long long)li.cube.h.n, 
                (unsigned long long)li.cube.h.ngroups, (unsigned long long)li.cube.h.nvalues,
                (long long)li.cube.h.vmin, li.cube.h.dense, li.cube.h.has_null,
                li.cube.h.null_group, li.cube.h.block_ranks, li.cube.h.null_measure,
                (unsigned long long)li.cube.bytes, li.load_ms,
                li.cube.h.has_null && li.cube.h.null_group < li.cube.names.size()
                    ? li.cube.names[li.cube.h.null_group].c_str() : "");
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    try {
        if (argc < 2) {
            std::fprintf(stderr,
                         "usage:\n  ecube_fmt_build build <gid.i32> <val.i32> "
                         "<valid.bitmap|-> <out> <2|3>\n"
                         "  ecube_fmt_build check <cube> <dict>\n");
            return 2;
        }
        const std::string mode = argv[1];
        if (mode == "build" && argc >= 7)
            return build(argv[2], argv[3], argv[4], argv[5], std::stoi(argv[6]));
        if (mode == "check" && argc >= 4) return check(argv[2], argv[3]);
        std::fprintf(stderr, "bad mode/args\n");
        return 2;
    } catch (std::exception& e) {
        std::fprintf(stderr, "ERR %s\n", e.what());
        return 1;
    }
}
