#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

struct Map { const int32_t* p; size_t n; };
static Map map_i32(const char* path) {
    int fd = open(path, O_RDONLY); if (fd < 0) { perror("open"); exit(1); }
    struct stat st{}; if (fstat(fd, &st)) exit(1);
    void* m = mmap(nullptr, size_t(st.st_size), PROT_READ, MAP_PRIVATE, fd, 0); close(fd);
    if (m == MAP_FAILED) exit(1);
    return {static_cast<const int32_t*>(m), size_t(st.st_size) / 4};
}
int main(int argc, char** argv) {
    const char* pp = argv[1]; const char* fp = argv[2]; const char* out = argv[3];
    const std::string op = argc > 4 ? argv[4] : "all";
    auto t0 = std::chrono::steady_clock::now();
    Map P = map_i32(pp), F = map_i32(fp);
    if (P.n != F.n) return 1;
    const size_t n = P.n;
    int64_t sum[16] = {0}; uint64_t cnt[16] = {0}; int32_t mn[16], mx[16];
    for (int i = 0; i < 16; ++i) { mn[i] = INT32_MAX; mx[i] = INT32_MIN; }
    auto t1 = std::chrono::steady_clock::now();
    if (op == "sum") {
        for (size_t i = 0; i < n; ++i) { int g = P.p[i]; if (g < 0) continue; if (g > 15) g = 15; sum[g] += F.p[i]; ++cnt[g]; }
    } else if (op == "count") {
        for (size_t i = 0; i < n; ++i) { int g = P.p[i]; if (g < 0) continue; if (g > 15) g = 15; ++cnt[g]; }
    } else if (op == "min") {
        for (size_t i = 0; i < n; ++i) { int g = P.p[i]; if (g < 0) continue; if (g > 15) g = 15; int32_t v = F.p[i]; if (v < mn[g]) mn[g] = v; ++cnt[g]; }
    } else if (op == "max") {
        for (size_t i = 0; i < n; ++i) { int g = P.p[i]; if (g < 0) continue; if (g > 15) g = 15; int32_t v = F.p[i]; if (v > mx[g]) mx[g] = v; ++cnt[g]; }
    } else {
        for (size_t i = 0; i < n; ++i) { int g = P.p[i]; if (g < 0) continue; if (g > 15) g = 15; int32_t v = F.p[i]; sum[g] += v; ++cnt[g]; if (v < mn[g]) mn[g] = v; if (v > mx[g]) mx[g] = v; }
    }
    auto t2 = std::chrono::steady_clock::now();
    FILE* o = fopen(out, "w");
    fprintf(o, "{\"rows\":[");
    bool first = true;
    for (int g = 0; g < 16; ++g) if (cnt[g]) {
        if (!first) fprintf(o, ","); first = false;
        fprintf(o, "[\"%d\",\"%lld\",\"%llu\",\"%d\",\"%d\"]", g, (long long)sum[g],
                (unsigned long long)cnt[g], mn[g], mx[g]);
    }
    fprintf(o, "]}\n"); fclose(o);
    auto t3 = std::chrono::steady_clock::now();
    auto ms = [](auto a, auto b) { return std::chrono::duration<double, std::milli>(b - a).count(); };
    fprintf(stdout, "{\"map_ms\":%.4f,\"query_ms\":%.4f,\"output_ms\":%.4f,\"stored_ms\":%.4f}\n",
            ms(t0,t1), ms(t1,t2), ms(t2,t3), ms(t0,t3));
    return 0;
}
