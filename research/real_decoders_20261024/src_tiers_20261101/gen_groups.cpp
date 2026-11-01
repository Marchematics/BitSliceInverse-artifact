// gen_groups.cpp -- deterministic synthetic group keys for the cardinality tiers.
//
// gid[i] = splitmix64(i) % G   (i = 0-based row id), written as little-endian i32.
// splitmix64 is the standard finalizer (Steele/Lea/Flood), identical in the tiers
// driver's reference SQL side because the reference reads the *same file* bytes.
//
// usage: gen_groups N G OUT
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <vector>

using u64 = uint64_t;
using i32 = int32_t;

static inline u64 splitmix64(u64 x) {
    u64 z = x + 0x9E3779B97F4A7C15ULL;
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}

int main(int argc, char** argv) {
    if (argc < 4) {
        std::fprintf(stderr, "usage: %s N G OUT\n", argv[0]);
        return 2;
    }
    const u64 n = std::strtoull(argv[1], nullptr, 10);
    const u64 G = std::strtoull(argv[2], nullptr, 10);
    if (G == 0) return 2;
    FILE* f = std::fopen(argv[3], "wb");
    if (!f) { std::perror("fopen"); return 1; }
    const size_t CH = 1 << 20;
    std::vector<i32> buf(CH);
    for (u64 off = 0; off < n; off += CH) {
        const size_t take = (size_t)std::min<u64>(CH, n - off);
        for (size_t i = 0; i < take; ++i) buf[i] = (i32)(splitmix64(off + i) % G);
        if (std::fwrite(buf.data(), sizeof(i32), take, f) != take) {
            std::perror("fwrite");
            return 1;
        }
    }
    std::fclose(f);
    std::printf("{\"rows\":%llu,\"G\":%llu,\"out\":\"%s\",\"gid\":\"splitmix64(rowid)%%G\"}\n",
                (unsigned long long)n, (unsigned long long)G, argv[3]);
    return 0;
}
