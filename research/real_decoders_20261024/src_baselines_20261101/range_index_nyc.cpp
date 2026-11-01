// Independent sorted row-ID range index baseline (CUBIT/RABIT-like) for the NYC query:
//   SELECT passenger_count, SUM(fare_cents), COUNT(*), MIN(fare_cents), MAX(fare_cents)
//   FROM nyc WHERE fare_cents BETWEEN lo AND hi GROUP BY 1
//
// Build: one uint32 row-id array sorted by fare_cents (4 B/row); the fare column itself
//        is the existing input column.  Query: binary-search the sorted row-id array,
//        sweep the qualifying row ids, gather passenger_count and accumulate the four
//        aggregates for the 11 groups.  Output is the complete SQL aggregate result.
//
// g++ -O3 -march=native -std=c++17 -o range_index_nyc range_index_nyc.cpp
//
//   range_index_nyc build fare.i32 passenger.i32 out.idx
//   range_index_nyc query out.idx fare.i32 passenger.i32 lo hi reps
//
// NULL passenger_count is encoded as -1 in passenger.i32 and maps to SQL NULL (group 10).
#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

static const int NG = 11;  // groups 0..9 + NULL

static double now_ms() {
    using namespace std::chrono;
    return duration<double, std::milli>(steady_clock::now().time_since_epoch()).count();
}

static int32_t* map_i32(const char* path, size_t* n) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) { perror(path); exit(2); }
    struct stat st {};
    fstat(fd, &st);
    void* p = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (p == MAP_FAILED) { perror("mmap"); exit(2); }
    *n = st.st_size / sizeof(int32_t);
    return (int32_t*)p;
}

struct Acc {
    long long sum[NG];
    long long cnt[NG];
    int32_t mn[NG];
    int32_t mx[NG];
    void reset() {
        for (int g = 0; g < NG; g++) { sum[g] = 0; cnt[g] = 0; mn[g] = INT32_MAX; mx[g] = INT32_MIN; }
    }
};

static void print_rows_tail(const Acc& a) {
    printf("\"rows\":[");
    bool first = true;
    for (int g = 0; g < NG; g++) {
        if (!a.cnt[g]) continue;
        if (!first) printf(",");
        first = false;
        const char* key = (g == 10) ? "null" : nullptr;
        char kb[8]; snprintf(kb, sizeof(kb), "%d", g);
        printf("[%s,%lld,%lld,%d,%d]", key ? key : kb, a.sum[g], a.cnt[g], a.mn[g], a.mx[g]);
    }
    printf("]}\n");
}

int main(int argc, char** argv) {
    if (argc < 3) { fprintf(stderr, "build|query ...\n"); return 2; }
    std::string mode = argv[1];

    if (mode == "build") {
        if (argc < 5) { fprintf(stderr, "build fare.i32 passenger.i32 out.idx\n"); return 2; }
        size_t nf, np_;
        int32_t* fare = map_i32(argv[2], &nf);
        map_i32(argv[3], &np_);  // validate size only
        if (nf != np_) { fprintf(stderr, "size mismatch %zu %zu\n", nf, np_); return 2; }
        size_t n = nf;
        double t0 = now_ms();
        // packed (fare-offset)<<32 | rowid  => uint64 sort is branch-free and fast
        std::vector<uint64_t> pk(n);
        const int64_t kOff = 1LL << 31;
        for (size_t i = 0; i < n; i++)
            pk[i] = ((uint64_t)((int64_t)fare[i] + kOff) << 32) | (uint32_t)i;
        std::sort(pk.begin(), pk.end());
        std::vector<uint32_t> ids(n);
        for (size_t i = 0; i < n; i++) ids[i] = (uint32_t)(pk[i] & 0xffffffffULL);
        std::vector<uint64_t>().swap(pk);
        double t1 = now_ms();
        FILE* f = fopen(argv[4], "wb");
        if (!f) { perror(argv[4]); return 2; }
        fwrite("RIDXNYC ", 1, 8, f);
        uint64_t nn = n;
        fwrite(&nn, 8, 1, f);
        fwrite(ids.data(), sizeof(uint32_t), n, f);
        fclose(f);
        double t2 = now_ms();
        struct stat st {};
        stat(argv[4], &st);
        printf("{\"build_ms\":%.3f,\"write_ms\":%.3f,\"total_ms\":%.3f,\"rows\":%zu,"
               "\"index_bytes\":%lld}\n", t1 - t0, t2 - t1, t2 - t0, n, (long long)st.st_size);
        return 0;
    }

    if (mode == "query") {
        if (argc < 8) { fprintf(stderr, "query idx fare passenger lo hi reps\n"); return 2; }
        const char* idxp = argv[2];
        int64_t lo = atoll(argv[5]), hi = atoll(argv[6]);
        int reps = atoi(argv[7]);
        size_t nf, np_;
        int32_t* fare = map_i32(argv[3], &nf);
        int32_t* pass = map_i32(argv[4], &np_);
        int fd = open(idxp, O_RDONLY);
        struct stat st {};
        fstat(fd, &st);
        const char* base = (const char*)mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        close(fd);
        uint64_t n;
        memcpy(&n, base + 8, 8);
        const uint32_t* ids = (const uint32_t*)(base + 16);

        auto lower = [&](int64_t x) {
            size_t a = 0, b = n;
            while (a < b) { size_t m = (a + b) / 2; if (fare[ids[m]] < x) a = m + 1; else b = m; }
            return a;
        };
        auto upper = [&](int64_t x) {
            size_t a = 0, b = n;
            while (a < b) { size_t m = (a + b) / 2; if (fare[ids[m]] <= x) a = m + 1; else b = m; }
            return a;
        };

        std::vector<double> ts;
        Acc acc;
        size_t range_n = 0;
        for (int r = 0; r < reps; r++) {
            double t0 = now_ms();
            size_t a = (lo == INT64_MIN) ? 0 : lower(lo);
            size_t b = (hi == INT64_MAX) ? n : upper(hi);
            range_n = b - a;
            acc.reset();
            for (size_t i = a; i < b; i++) {
                uint32_t id = ids[i];
                int g = pass[id];
                if (g < 0) g = 10;
                int32_t v = fare[id];
                acc.sum[g] += v;
                acc.cnt[g] += 1;
                if (v < acc.mn[g]) acc.mn[g] = v;
                if (v > acc.mx[g]) acc.mx[g] = v;
            }
            ts.push_back(now_ms() - t0);
        }
        std::sort(ts.begin(), ts.end());
        double med = ts[ts.size() / 2];
        printf("{\"reps\":%d,\"range_rows\":%zu,\"median_ms\":%.4f,\"min_ms\":%.4f,"
               "\"total_ms\":%.4f,", reps, range_n, med, ts.front(),
               [&]{ double s = 0; for (double t : ts) s += t; return s; }());
        print_rows_tail(acc);
        return 0;
    }
    fprintf(stderr, "unknown mode %s\n", mode.c_str());
    return 2;
}
