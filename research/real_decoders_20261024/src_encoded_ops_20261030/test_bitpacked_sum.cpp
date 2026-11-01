#include "../src_patch_fusion/fullsql_reference.hpp"
#include "../src_fullsql_ig2_fast_20261029/streaming.hpp"
#include <cstdio>
#include <cstring>

static int popcount64(uint64_t x) { return __builtin_popcountll(x); }

int main(int argc, char** argv) {
    std::string encoded = argv[1], prep = argv[2];
    std::ifstream bf(prep + "/binding.txt", std::ios::binary);
    std::string binding(std::istreambuf_iterator<char>(bf), {});
    binding = binding.substr(0, 40);
    bool parsed = false;
    StreamTable t = stream_load(encoded, binding, parsed);
    if (!parsed) { std::printf("not parsed\n"); return 1; }
    for (auto& c : t.columns) {
        if (c.id != 13) continue;
        std::printf("column %u bw/leaves %zu\n", c.id, c.leaves.size());
        for (size_t li = 0; li < std::min<size_t>(c.leaves.size(), 8); ++li) {
            auto& leaf = c.leaves[li];
            Scratch s;
            stream_decode_block(c, U(li), s);
            __int128 direct = 0;
            for (U i = 0; i < V; ++i) direct += s.d[i];
            __int128 plane = (__int128)V * leaf.base;
            const U words_per_plane = 32;
            if (leaf.bw) {
                for (U b = 0; b < leaf.bw; ++b) {
                    uint64_t ones = 0;
                    for (U w = 0; w < words_per_plane; ++w) ones += popcount64(leaf.packed[b * words_per_plane + w]);
                    plane += (__int128(1) << b) * ones;
                }
            }
            // patches: direct sum already includes overwritten values; adjust plane by deltas.
            __int128 patched = plane;
            for (U j = 0; j < leaf.npatch; ++j) {
                const U pos = leaf.pos[j];
                U orig = leaf.base;
                for (U b = 0; b < leaf.bw; ++b) {
                    const U word = leaf.packed[b * words_per_plane + pos / 32];
                    orig += U((word >> (pos % 32)) & 1u) << b;
                }
                patched += __int128(leaf.vals[j]) - __int128(orig);
            }
            for (U pos = 0; pos < 4; ++pos) {
                U h4 = leaf.base;
                for (U b = 0; b < leaf.bw; ++b) {
                    const U word = leaf.packed[(pos / 32) * leaf.bw + b];
                    h4 += U((word >> (pos % 32)) & 1u) << b;
                }
                std::printf("  pos %u decoded=%u h4=%u\n", pos, s.d[pos], h4);
            }
            std::printf("leaf %zu bw=%u base=%u npatch=%u direct=%lld plane=%lld patched=%lld %s\n", li, leaf.bw, leaf.base,
                        leaf.npatch, (long long)direct, (long long)plane, (long long)patched,
                        direct == patched ? "MATCH" : "DIFF");
        }
    }
    return 0;
}
