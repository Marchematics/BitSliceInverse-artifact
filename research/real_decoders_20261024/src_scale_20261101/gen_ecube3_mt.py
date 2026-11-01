#!/usr/bin/env python3
"""Generate src_scale_20261101/ecube3_mt.cpp from the FROZEN ECUBE03 source.

Provenance: input is ../src_eai_20261101/eai_cube3.cpp at the frozen revision
sha256 c93a4530f7b21d5e1a1f174374f5fdd49783a20ad32638b370baabf3e86562f0
(commit 3131521). Do NOT regenerate from eai_cube3_parity.cpp: that revision
differs only in the encoded builder's `h.dense` assignment (contiguity rule) but
is the slower-query variant the Lead rejected for the artifact.

The generated file = frozen source verbatim + (1) parallel-build helpers,
(2) build_enc_mt (block-parallel decode, per-thread rank-count + NULL-measure
slabs, deterministic integer merge), (3) build_plain_mt (same for plain columns
with optional validity bitmap), (4) main() branches for the two new modes.
The frozen single-thread build paths and the whole query path are untouched:
the query-regression check measures that against the frozen binary.

usage: python3 gen_ecube3_mt.py [--check]
       --check only verifies that the existing ecube3_mt.cpp matches the output.
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import hashlib
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
SRC = os.path.join(ROOT, "src_eai_20261101", "eai_cube3.cpp")
DST = os.path.join(HERE, "ecube3_mt.cpp")
FROZEN_SHA = "c93a4530f7b21d5e1a1f174374f5fdd49783a20ad32638b370baabf3e86562f0"


def sha256_text(t):
    return hashlib.sha256(t.encode()).hexdigest()


def main():
    src = open(SRC).read()
    got = hashlib.sha256(src.encode()).hexdigest()
    if got != FROZEN_SHA:
        raise SystemExit("input is not the frozen source: %s != %s" % (got, FROZEN_SHA))

    # ---- 1. includes ----
    s = src.replace('''#include <unordered_set>
#include <vector>''', '''#include <atomic>
#include <memory>
#include <pthread.h>
#include <sched.h>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>''')

    # ---- 2. helpers (rank map + mmap reader) ----
    anchor = '''static inline i64 value_at(i64 vmin, u32 dense, const i64* distinct, u64 rank) {
    return dense ? vmin + i64(rank) : distinct[rank];
}
'''
    helper = anchor + '''
// ---------------------------------------------------------------------------
// Parallel build support (scale stream). The on-disk ECUBE03 layout and every
// single-thread build path above are kept verbatim: the *_mt builders must
// reproduce build_encoded/build_plain byte-for-byte. Determinism argument:
// counts and NULL-measure counts are u32 integer additions, commutative and
// associative, so merging per-thread slabs in any order gives the same array;
// the distinct-value set is sorted before use; the prefix, header and write
// order are single-thread identical.
// ---------------------------------------------------------------------------
static inline u64 mix_value(u32 x) {
    u64 z = (u64(x) + 0x9E3779B97F4A7C15ULL);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}

struct RankMap3 {
    bool dense = false;
    i64 vmin = 0;
    u64 mask = 0;
    std::vector<u64> tab;  // (u32 value << 32) | rank ; ~0ull = empty
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

struct MapI32 {
    const i32* p = nullptr;
    u64 n = 0;
    void* base = nullptr;
    size_t bytes = 0;
};

static MapI32 map_i32(const char* path) {
    MapI32 m;
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
'''
    assert anchor in s
    s = s.replace(anchor, helper, 1)

    # ---- 3. the two parallel builders, inserted before QSpec ----
    anchor2 = '''struct QSpec {
    bool has_lo = false, has_hi = false;'''
    funcs = '''// ---------------------------------------------------------------------------
// build_enc_mt: block-partitioned parallel build from the frozen encoded tree.
// Mirrors build_encoded exactly (same G/NULL derivation, same distinct set, same
// NULL-measure ordering, same dense=0 for the encoded path, same write order).
// ---------------------------------------------------------------------------
static void build_enc_mt(const char* encoded, const char* prep, U gid, U vid,
                         const char* out_path, int T, double membudget_gb) {
    static constexpr U CODE_BIAS = 0x80000000U;
    if (T < 1) T = 1;
    double t0 = now_ms();
    std::ifstream bf(std::string(prep) + "/binding.txt", std::ios::binary);
    std::string binding(41, '\\0');
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
        if (d.size() < 52 || d.compare(0, 8, "BSSTR01\\n")) throw std::runtime_error("dict");
        std::memcpy(&G, d.data() + 48, 4);
    }
    const U n = g->n;
    const U blocks = g->leaves.size();
    const bool v_nullable = (v->valid != nullptr);
    double t_load = now_ms();
    // phase 0: group-validity / value-validity domain flags, block-parallel
    bool has_null = false, has_null_measure = false;
    {
        const int PT = (int)std::min<U>(T, (U)64);
        std::vector<char> hn(PT, 0), hnm(PT, 0);
        std::vector<std::thread> th;
        for (int p = 1; p < PT; ++p)
            th.emplace_back([&, p] {
                U b0 = (U)p * blocks / PT, b1 = (U)(p + 1) * blocks / PT;
                bool a = false, b = false;
                for (U blk = b0; blk < b1; ++blk) {
                    const U off = blk * V; const U take = std::min<U>(V, n - off);
                    for (U i = 0; i < take; ++i) {
                        if (!stream_valid_bit(g->valid, off + i)) a = true;
                        if (v_nullable && !stream_valid_bit(v->valid, off + i)) b = true;
                        if (a && (!v_nullable || b)) break;
                    }
                    if (a && (!v_nullable || b)) break;
                }
                hn[p] = a ? 1 : 0;
                hnm[p] = b ? 1 : 0;
            });
        bool a = false, b = false;
        for (U blk = 0; blk < (U)blocks / PT; ++blk) {
            const U off = blk * V; const U take = std::min<U>(V, n - off);
            for (U i = 0; i < take; ++i) {
                if (!stream_valid_bit(g->valid, off + i)) a = true;
                if (v_nullable && !stream_valid_bit(v->valid, off + i)) b = true;
            }
        }
        hn[0] = a ? 1 : 0;
        hnm[0] = b ? 1 : 0;
        for (auto& x : th) x.join();
        for (int p = 0; p < PT; ++p) { has_null |= hn[p]; has_null_measure |= hnm[p]; }
    }
    const u32 null_group = has_null ? u32(G) : 0xffffffffu;
    if (has_null) G += 1;
    double t_domain = now_ms();
    // phase 1: exact distinct value set (per-thread hash sets over block ranges)
    std::vector<i32> dv;
    {
        const int DT = (int)std::min<U>(T, (U)64);
        std::vector<std::unordered_set<i32>> sets(DT);
        std::vector<std::thread> th;
        for (int p = 0; p < DT; ++p)
            th.emplace_back([&, p] {
                U b0 = (U)p * blocks / DT, b1 = (U)(p + 1) * blocks / DT;
                Scratch gd, vd;
                auto& s = sets[p];
                s.reserve(1 << 16);
                for (U blk = b0; blk < b1; ++blk) {
                    const U off = blk * V; const U take = std::min<U>(V, n - off);
                    stream_decode_block(*g, blk, gd);
                    stream_decode_block(*v, blk, vd);
                    for (U i = 0; i < take; ++i) {
                        if (v_nullable && !stream_valid_bit(v->valid, off + i)) continue;
                        s.insert(i32(i64(vd.d[i]) - i64(CODE_BIAS)));
                    }
                }
            });
        for (auto& x : th) x.join();
        std::unordered_set<i32> all;
        all.reserve(1 << 16);
        for (auto& s : sets) all.insert(s.begin(), s.end());
        dv.assign(all.begin(), all.end());
        std::sort(dv.begin(), dv.end());
    }
    const u64 Vn = dv.size();
    if (!Vn) throw std::runtime_error("empty");
    const i64 vmin = dv.front();
    // The frozen build_encoded sets h.dense = 0 unconditionally (only the plain
    // builder applies the contiguity rule); mirror that exactly.
    const bool dense = false;
    RankMap3 rm;
    rm.build(dv, vmin, dense);
    double t_distinct = now_ms();
    // memory guard on per-thread slabs
    const u64 slab_bytes = (u64)G * Vn * sizeof(u32) + (u64)G * sizeof(u32);
    int Tuse = T;
    if (slab_bytes) {
        u64 budget = u64(membudget_gb * (1ULL << 30));
        u64 fit = budget / slab_bytes;
        if (fit < 1) fit = 1;
        Tuse = int(std::min<u64>((u64)Tuse, fit));
    }
    // phase 2: parallel accumulate into per-thread (counts, null_measure) slabs
    std::vector<std::vector<u32>> slabs(Tuse), nulls(Tuse);
    for (int p = 0; p < Tuse; ++p) slabs[p].assign((size_t)G * Vn, 0);
    for (int p = 0; p < Tuse; ++p) nulls[p].assign((size_t)G, 0);
    {
        std::vector<std::thread> th;
        for (int p = 0; p < Tuse; ++p)
            th.emplace_back([&, p] {
                U b0 = (U)p * blocks / Tuse, b1 = (U)(p + 1) * blocks / Tuse;
                Scratch gd, vd;
                u32* C = slabs[p].data();
                u32* NC = nulls[p].data();
                for (U blk = b0; blk < b1; ++blk) {
                    const U off = blk * V; const U take = std::min<U>(V, n - off);
                    stream_decode_block(*g, blk, gd);
                    stream_decode_block(*v, blk, vd);
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
                            NC[gi] += 1;
                            continue;
                        }
                        i32 val = i32(i64(vd.d[i]) - i64(CODE_BIAS));
                        C[gi * Vn + rm.rank(val)] += 1;
                    }
                }
            });
        for (auto& x : th) x.join();
    }
    double t_accum = now_ms();
    // phase 3: deterministic integer merge (parallel over groups)
    {
        std::vector<std::thread> th;
        const int MT = (int)std::min<int>(Tuse, 64);
        for (int p = 0; p < MT; ++p)
            th.emplace_back([&, p] {
                u64 g0 = (u64)p * G / MT, g1 = (u64)(p + 1) * G / MT;
                for (u64 gg = g0; gg < g1; ++gg) {
                    u32* dst = slabs[0].data() + gg * Vn;
                    for (int u = 1; u < Tuse; ++u) {
                        const u32* src = slabs[u].data() + gg * Vn;
                        for (u64 r = 0; r < Vn; ++r) dst[r] += src[r];
                    }
                    u32 nc = nulls[0][gg];
                    for (int u = 1; u < Tuse; ++u) nc += nulls[u][gg];
                    nulls[0][gg] = nc;
                }
            });
        for (auto& x : th) x.join();
    }
    if (Tuse > 1) {
        for (int p = 1; p < Tuse; ++p) { slabs[p].clear(); slabs[p].shrink_to_fit();
                                         nulls[p].clear(); nulls[p].shrink_to_fit(); }
    }
    double t_merge = now_ms();
    // phase 4: block prefix (parallel over groups), same integer arithmetic
    const u64 NB = (Vn + R_BLOCK - 1) / R_BLOCK;
    std::vector<u32> bcnt(size_t(G) * (NB + 1), 0);
    std::vector<i64> bsum(size_t(G) * (NB + 1), 0);
    {
        std::vector<std::thread> th;
        const int PT = (int)std::min<int>(T, 64);
        for (int p = 0; p < PT; ++p)
            th.emplace_back([&, p] {
                u64 g0 = (u64)p * G / PT, g1 = (u64)(p + 1) * G / PT;
                for (u64 gg = g0; gg < g1; ++gg) {
                    const u32* c = slabs[0].data() + gg * Vn;
                    u32* bc = bcnt.data() + gg * (NB + 1);
                    i64* bs = bsum.data() + gg * (NB + 1);
                    for (u64 r = 0; r < Vn; ++r) {
                        const u64 bi = r / R_BLOCK;
                        bc[bi + 1] += c[r];
                        bs[bi + 1] += i64(c[r]) * i64(dv[r]);
                    }
                    for (u64 b = 1; b <= NB; ++b) { bc[b] += bc[b - 1]; bs[b] += bs[b - 1]; }
                }
            });
        for (auto& x : th) x.join();
    }
    double t_prefix = now_ms();
    Header h{};
    std::memcpy(h.magic, MAGIC, 8);
    h.n = n; h.ngroups = G; h.nvalues = Vn; h.vmin = dv.front();
    h.dense = dense ? 1 : 0;
    h.has_null = has_null ? 1 : 0; h.null_group = null_group;
    h.block_ranks = R_BLOCK;
    h.null_measure = has_null_measure ? 1 : 0;
    std::vector<i64> dv64(Vn);
    for (u64 r = 0; r < Vn; ++r) dv64[r] = dv[r];
    write_cube(out_path, h, dv64, slabs[0], nulls[0], bcnt, bsum);
    double t_write = now_ms();
    const u64 bytes = sizeof(Header) + (dense ? 0 : Vn * 8) + G * Vn * 4 + G * 4 +
                      G * (NB + 1) * 12;
    std::fprintf(stderr,
                 "{\\"mode\\":\\"build_enc_mt\\",\\"build_ms\\":%.3f,\\"load_ms\\":%.3f,"
                 "\\"domain_ms\\":%.3f,\\"distinct_ms\\":%.3f,\\"accum_ms\\":%.3f,"
                 "\\"merge_ms\\":%.3f,\\"prefix_ms\\":%.3f,\\"write_ms\\":%.3f,"
                 "\\"groups\\":%llu,\\"values\\":%llu,\\"rows\\":%llu,\\"bytes\\":%llu,"
                 "\\"dense\\":%d,\\"null_measure\\":%d,\\"threads_requested\\":%d,"
                 "\\"threads_used\\":%d,\\"slab_bytes_per_thread\\":%llu,\\"encoded\\":1,"
                 "\\"v3_frozen\\":1}\\n",
                 t_write - t0, t_load - t0, t_domain - t_load, t_distinct - t_domain,
                 t_accum - t_distinct, t_merge - t_accum, t_prefix - t_merge,
                 t_write - t_prefix, (unsigned long long)G, (unsigned long long)Vn,
                 (unsigned long long)n, (unsigned long long)bytes, int(dense),
                 int(h.null_measure), T, Tuse, (unsigned long long)slab_bytes);
}

// ---------------------------------------------------------------------------
// build_plain_mt: parallel plain-column build (ECUBE03), optional validity
// bitmap for the NULL-measure path. Mirrors the frozen build_plain exactly: the
// distinct set is taken over ALL value payloads (validity is only applied to
// counts), the same contiguity/dense rule, and NULL rows go to nullcnt.
// ---------------------------------------------------------------------------
static void build_plain_mt(const char* gid_path, const char* val_path, const char* out_path,
                           const char* valid_path, int T, double membudget_gb) {
    if (T < 1) T = 1;
    double t0 = now_ms();
    MapI32 g = map_i32(gid_path);
    MapI32 v = map_i32(val_path);
    if (g.n != v.n) throw std::runtime_error("size");
    const u64 n = g.n;
    double t_map = now_ms();
    // group domain, parallel over row chunks
    u32 G = 0;
    bool has_null = false;
    {
        const int DT = (int)std::min<u64>((u64)T, (u64)64);
        std::vector<u32> gmax(DT, 0);
        std::vector<char> hn(DT, 0);
        std::vector<std::thread> th;
        for (int p = 1; p < DT; ++p)
            th.emplace_back([&, p] {
                u64 b0 = (u64)p * n / DT, b1 = (u64)(p + 1) * n / DT;
                u32 mx = 0; bool a = false;
                for (u64 i = b0; i < b1; ++i) {
                    i32 gg = g.p[i];
                    if (gg < 0) { a = true; continue; }
                    if (u32(gg) + 1 > mx) mx = u32(gg) + 1;
                }
                gmax[p] = mx; hn[p] = a ? 1 : 0;
            });
        u32 mx = 0; bool a = false;
        for (u64 i = 0; i < n / DT; ++i) {
            i32 gg = g.p[i];
            if (gg < 0) { a = true; continue; }
            if (u32(gg) + 1 > mx) mx = u32(gg) + 1;
        }
        gmax[0] = mx; hn[0] = a ? 1 : 0;
        for (auto& x : th) x.join();
        for (int p = 0; p < DT; ++p) { G = std::max(G, gmax[p]); has_null |= hn[p] != 0; }
    }
    const u32 null_group = has_null ? G : 0xffffffffu;
    if (has_null) G += 1;
    // distinct over ALL payloads (mirrors the frozen build_plain)
    std::vector<i32> dv;
    {
        const int DT = (int)std::min<u64>((u64)T, (u64)64);
        std::vector<std::unordered_set<i32>> sets(DT);
        std::vector<std::thread> th;
        for (int p = 0; p < DT; ++p)
            th.emplace_back([&, p] {
                u64 b0 = (u64)p * n / DT, b1 = (u64)(p + 1) * n / DT;
                auto& s = sets[p];
                s.reserve(1 << 16);
                for (u64 i = b0; i < b1; ++i) s.insert(v.p[i]);
            });
        for (auto& x : th) x.join();
        std::unordered_set<i32> all;
        all.reserve(1 << 16);
        for (auto& s : sets) all.insert(s.begin(), s.end());
        dv.assign(all.begin(), all.end());
        std::sort(dv.begin(), dv.end());
    }
    const u64 Vn = dv.size();
    if (!Vn) throw std::runtime_error("empty");
    const i64 vmin = dv.front();
    const u64 span = u64(dv.back()) - u64(dv.front()) + 1;
    const bool dense = (span == Vn) && span <= (1u << 22);
    RankMap3 rm;
    rm.build(dv, vmin, dense);
    std::vector<uint64_t> valid_words;
    if (valid_path && *valid_path) {
        std::ifstream vf(valid_path, std::ios::binary);
        vf.seekg(0, std::ios::end);
        size_t vb = size_t(vf.tellg());
        vf.seekg(0);
        valid_words.resize((vb + 7) / 8);
        vf.read(reinterpret_cast<char*>(valid_words.data()),
                std::streamsize(valid_words.size() * 8));
    }
    double t_distinct = now_ms();
    const u64 slab_bytes = (u64)G * Vn * sizeof(u32) + (u64)G * sizeof(u32);
    int Tuse = T;
    if (slab_bytes) {
        u64 budget = u64(membudget_gb * (1ULL << 30));
        u64 fit = budget / slab_bytes;
        if (fit < 1) fit = 1;
        Tuse = int(std::min<u64>((u64)Tuse, fit));
    }
    std::vector<std::vector<u32>> slabs(Tuse), nulls(Tuse);
    for (int p = 0; p < Tuse; ++p) slabs[p].assign((size_t)G * Vn, 0);
    for (int p = 0; p < Tuse; ++p) nulls[p].assign((size_t)G, 0);
    {
        std::vector<std::thread> th;
        for (int p = 0; p < Tuse; ++p)
            th.emplace_back([&, p] {
                u64 b0 = (u64)p * n / Tuse, b1 = (u64)(p + 1) * n / Tuse;
                u32* C = slabs[p].data();
                u32* NC = nulls[p].data();
                for (u64 i = b0; i < b1; ++i) {
                    i32 gg = g.p[i];
                    const u64 gi = gg < 0 ? u64(null_group) : u64(gg);
                    if (!valid_words.empty() &&
                        !((valid_words[i >> 6] >> (i & 63)) & 1ULL)) {
                        NC[gi] += 1;
                        continue;
                    }
                    C[gi * Vn + rm.rank(v.p[i])] += 1;
                }
            });
        for (auto& x : th) x.join();
    }
    double t_accum = now_ms();
    {
        std::vector<std::thread> th;
        const int MT = (int)std::min<int>(Tuse, 64);
        for (int p = 0; p < MT; ++p)
            th.emplace_back([&, p] {
                u64 g0 = (u64)p * G / MT, g1 = (u64)(p + 1) * G / MT;
                for (u64 gg = g0; gg < g1; ++gg) {
                    u32* dst = slabs[0].data() + gg * Vn;
                    for (int u = 1; u < Tuse; ++u) {
                        const u32* src = slabs[u].data() + gg * Vn;
                        for (u64 r = 0; r < Vn; ++r) dst[r] += src[r];
                    }
                    u32 nc = nulls[0][gg];
                    for (int u = 1; u < Tuse; ++u) nc += nulls[u][gg];
                    nulls[0][gg] = nc;
                }
            });
        for (auto& x : th) x.join();
    }
    if (Tuse > 1) {
        for (int p = 1; p < Tuse; ++p) { slabs[p].clear(); slabs[p].shrink_to_fit();
                                         nulls[p].clear(); nulls[p].shrink_to_fit(); }
    }
    double t_merge = now_ms();
    const u64 NB = (Vn + R_BLOCK - 1) / R_BLOCK;
    std::vector<u32> bcnt(size_t(G) * (NB + 1), 0);
    std::vector<i64> bsum(size_t(G) * (NB + 1), 0);
    {
        std::vector<std::thread> th;
        const int PT = (int)std::min<int>(T, 64);
        for (int p = 0; p < PT; ++p)
            th.emplace_back([&, p] {
                u64 g0 = (u64)p * G / PT, g1 = (u64)(p + 1) * G / PT;
                for (u64 gg = g0; gg < g1; ++gg) {
                    const u32* c = slabs[0].data() + gg * Vn;
                    u32* bc = bcnt.data() + gg * (NB + 1);
                    i64* bs = bsum.data() + gg * (NB + 1);
                    for (u64 r = 0; r < Vn; ++r) {
                        const u64 bi = r / R_BLOCK;
                        const i64 vv = dense ? vmin + i64(r) : i64(dv[r]);
                        bc[bi + 1] += c[r];
                        bs[bi + 1] += i64(c[r]) * vv;
                    }
                    for (u64 b = 1; b <= NB; ++b) { bc[b] += bc[b - 1]; bs[b] += bs[b - 1]; }
                }
            });
        for (auto& x : th) x.join();
    }
    double t_prefix = now_ms();
    Header h{};
    std::memcpy(h.magic, MAGIC, 8);
    h.n = n; h.ngroups = G; h.nvalues = Vn; h.vmin = vmin;
    h.dense = dense ? 1 : 0;
    h.has_null = has_null ? 1 : 0; h.null_group = null_group;
    h.block_ranks = R_BLOCK;
    h.null_measure = valid_words.empty() ? 0 : 1;
    std::vector<i64> dv64(Vn);
    for (u64 r = 0; r < Vn; ++r) dv64[r] = dv[r];
    write_cube(out_path, h, dv64, slabs[0], nulls[0], bcnt, bsum);
    double t_write = now_ms();
    const u64 bytes = sizeof(Header) + (dense ? 0 : Vn * 8) + G * Vn * 4 + G * 4 +
                      G * (NB + 1) * 12;
    std::fprintf(stderr,
                 "{\\"mode\\":\\"build_plain_mt\\",\\"build_ms\\":%.3f,\\"map_ms\\":%.3f,"
                 "\\"distinct_ms\\":%.3f,\\"accum_ms\\":%.3f,\\"merge_ms\\":%.3f,"
                 "\\"prefix_ms\\":%.3f,\\"write_ms\\":%.3f,\\"groups\\":%u,"
                 "\\"values\\":%llu,\\"rows\\":%llu,\\"bytes\\":%llu,\\"dense\\":%d,"
                 "\\"null_measure\\":%d,\\"threads_requested\\":%d,\\"threads_used\\":%d,"
                 "\\"slab_bytes_per_thread\\":%llu,\\"v3_frozen\\":1}\\n",
                 t_write - t0, t_map - t0, t_distinct - t_map, t_accum - t_distinct,
                 t_merge - t_accum, t_prefix - t_merge, t_write - t_prefix, G,
                 (unsigned long long)Vn, (unsigned long long)n, (unsigned long long)bytes,
                 int(dense), int(h.null_measure), T, Tuse,
                 (unsigned long long)slab_bytes);
}

struct QSpec {
    bool has_lo = false, has_hi = false;'''
    assert anchor2 in s
    s = s.replace(anchor2, funcs, 1)

    # ---- 4. main branches ----
    anchor3 = '''        if (mode == "build_enc" && argc >= 7) {
            build_encoded(argv[2], argv[3], U(std::stoul(argv[4])), U(std::stoul(argv[5])),
                          argv[6]);
            return 0;
        }'''
    newmain = anchor3 + '''
        if (mode == "build_enc_mt" && argc >= 8) {
            build_enc_mt(argv[2], argv[3], U(std::stoul(argv[4])), U(std::stoul(argv[5])),
                         argv[6], std::stoi(argv[7]),
                         argc >= 9 ? std::stod(argv[8]) : 24.0);
            return 0;
        }
        if (mode == "build_plain_mt" && argc >= 7) {
            const char* vp = (std::string(argv[6]) == "-") ? nullptr : argv[6];
            build_plain_mt(argv[2], argv[3], argv[4], vp, std::stoi(argv[5]),
                           argc >= 8 ? std::stod(argv[7]) : 24.0);
            return 0;
        }'''
    assert anchor3 in s
    s = s.replace(anchor3, newmain, 1)

    # ---- 5. usage text ----
    old_usage = '''                     "usage:\\n  enc_cube build GID VALUES OUT [VALIDITY_BITS]\\n"'''
    new_usage = '''                     "usage:\\n  ecube3_mt build GID VALUES OUT [VALIDITY_BITS]\\n"
                     "  ecube3_mt build_plain_mt GID VALUES OUT THREADS [VALIDITY_BITS|-] [membudget_gb]\\n"
                     "  ecube3_mt build_enc ENC PREP GID VID OUT\\n"
                     "  ecube3_mt build_enc_mt ENC PREP GID VID OUT THREADS [membudget_gb]\\n"'''
    assert s.count(old_usage) == 1
    s = s.replace(old_usage, new_usage, 1)

    banner = ("// GENERATED by src_scale_20261101/gen_ecube3_mt.py from the FROZEN ECUBE03\n"
              "// source (src_eai_20261101/eai_cube3.cpp, sha256\n"
              "// c93a4530f7b21d5e1a1f174374f5fdd49783a20ad32638b370baabf3e86562f0, commit 3131521).\n"
              "// The frozen single-thread build paths and the complete query path are byte-for-byte\n"
              "// untouched; the parallel builders are additive. Output info field: v3_frozen.\n")
    s = banner + s
    if "--check" in sys.argv:
        cur = open(DST).read()
        if cur != s:
            raise SystemExit("ecube3_mt.cpp is stale; regenerate")
        print("ecube3_mt.cpp matches the generator")
        return
    open(DST, "w").write(s)
    print("wrote", DST, "sha256", sha256_text(s)[:16])


if __name__ == "__main__":
    main()
