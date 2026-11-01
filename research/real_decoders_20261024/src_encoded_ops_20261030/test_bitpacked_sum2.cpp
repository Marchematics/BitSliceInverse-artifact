#include "../src_patch_fusion/fullsql_reference.hpp"
#include "../src_fullsql_ig2_fast_20261029/streaming.hpp"
#include <chrono>
#include <cstdio>

static inline U extract_value(const StreamLeaf& leaf, U p) {
    const U i = p % 32, k = p / 32;
    U v = leaf.base;
    for (U b = 0; b < leaf.bw; ++b) {
        const U off = k * leaf.bw + b;
        const U word = leaf.packed[i + 32 * (off / 32)];
        v += U((word >> (off % 32)) & 1u) << b;
    }
    return v;
}

int main(int argc, char** argv) {
    std::ifstream bf(std::string(argv[2]) + "/binding.txt", std::ios::binary);
    std::string binding(std::istreambuf_iterator<char>(bf), {});
    binding = binding.substr(0, 40);
    bool parsed = false;
    StreamTable t = stream_load(argv[1], binding, parsed);
    if (!parsed) { std::printf("not parsed\n"); return 1; }
    const StreamColumn* col = nullptr;
    for (auto& c : t.columns) if (c.id == 13) col = &c;
    if (!col) return 1;
    // correctness on first leaf
    {
        auto& leaf = col->leaves[0];
        Scratch s; stream_decode_block(*col, 0, s);
        for (U p = 0; p < 8; ++p) std::printf("pos %u decoded=%u extracted=%u %s\n", p, s.d[p], extract_value(leaf, p),
                                             s.d[p] == extract_value(leaf, p) ? "OK" : "BAD");
        __int128 direct = 0; for (U i = 0; i < V; ++i) direct += s.d[i];
        __int128 extracted = 0; for (U p = 0; p < V; ++p) extracted += extract_value(leaf, p);
        std::printf("leaf0 direct=%lld extracted=%lld %s\n", (long long)direct, (long long)extracted,
                    direct == extracted ? "MATCH" : "DIFF");
    }
    // timing over all leaves
    const int reps = 2000;
    Scratch s;
    auto t0 = std::chrono::steady_clock::now();
    __int128 acc = 0;
    for (int r = 0; r < reps; ++r)
        for (U b = 0; b < col->leaves.size(); ++b) {
            stream_decode_block(*col, b, s);
            __int128 sum = 0; for (U i = 0; i < V; ++i) sum += s.d[i];
            acc += sum;
        }
    auto t1 = std::chrono::steady_clock::now();
    for (int r = 0; r < reps; ++r)
        for (U b = 0; b < col->leaves.size(); ++b) {
            auto& leaf = col->leaves[b];
            __int128 sum = 0; for (U p = 0; p < V; ++p) sum += extract_value(leaf, p);
            acc += sum;
        }
    auto t2 = std::chrono::steady_clock::now();
    const double decode_ms = std::chrono::duration<double, std::milli>(t1 - t0).count() / reps;
    const double extract_ms = std::chrono::duration<double, std::milli>(t2 - t1).count() / reps;
    std::printf("decode=%.4f ms extract=%.4f ms ratio=%.2f acc=%lld\n", decode_ms, extract_ms, decode_ms / extract_ms, (long long)acc);
    return 0;
}
