// Scale-data generators for the ECUBE02 scale campaign (scale stream, 2026-11-01).
//
// Two deterministic generators, both writing plain little-endian int32 columns
// (no compression, no metadata) so the cube build path reads exactly the same
// bytes that DuckDB is fed through parquet:
//
//   gen_scale replica   GID100 VAL100 OUTGID OUTVAL [replicas=10] [offset=10]
//       out_gid[r*n+i] = (gid[i] < 0) ? -1 : gid[i] + r*offset      (NULL preserved)
//       out_val[r*n+i] = val[i]
//       n = rows of the 100M source columns.  replica 0 is the original 100M data
//       (for offset=0) / shifted by r*offset.  1B rows = 10 replicas.
//
//   gen_scale hashgroups GID100 VAL100 OUTGID OUTVAL [ngroups] [valmode] [nrows]
//       out_gid[i] = splitmix64(i) % ngroups
//       valmode raw      : out_val[i] = val[i]                      (V=21084)
//       valmode bucket64 : out_val[i] in 0..63, equal-frequency buckets of val
//       valmode rng64    : out_val[i] = splitmix64(i^SALT) % 64
//
// Determinism: splitmix64 (Steele/Lea/Flood) and the quantile bucketing are
// fully specified here, so a rebuild of the data reproduces the same bytes.
// Every run prints one JSON line on stderr with the exact parameters + wall time.
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

using u64 = uint64_t;
using i64 = int64_t;
using u32 = uint32_t;
using i32 = int32_t;

static double now_ms() {
    using clk = std::chrono::steady_clock;
    return std::chrono::duration<double, std::milli>(clk::now().time_since_epoch()).count();
}

static inline u64 splitmix64(u64 x) {
    x += 0x9E3779B97F4A7C15ULL;
    x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
    x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
    return x ^ (x >> 31);
}

// read-only mmap of an i32 column (page-cache resident after first touch)
struct Mapped {
    const i32* p = nullptr;
    size_t n = 0;
    void* base = nullptr;
    size_t bytes = 0;
    int fd = -1;
};

static Mapped map_i32(const char* path) {
    Mapped m;
    m.fd = open(path, O_RDONLY);
    if (m.fd < 0) { perror(path); exit(2); }
    struct stat st {};
    if (fstat(m.fd, &st)) { perror("fstat"); exit(2); }
    m.bytes = size_t(st.st_size);
    if (m.bytes % 4) { fprintf(stderr, "ERR %s size not multiple of 4\n", path); exit(2); }
    m.n = m.bytes / 4;
    m.base = mmap(nullptr, m.bytes, PROT_READ, MAP_PRIVATE, m.fd, 0);
    if (m.base == MAP_FAILED) { perror("mmap"); exit(2); }
    m.p = static_cast<const i32*>(m.base);
    return m;
}

static std::vector<i32> read_i32_vec(const char* path) {
    Mapped m = map_i32(path);
    std::vector<i32> v(m.p, m.p + m.n);
    return v;
}

// buffered sequential writer for a large output column
struct OutCol {
    FILE* f = nullptr;
    std::vector<i32> buf;
    size_t cap = 0;
    explicit OutCol(const char* path, size_t cap_rows) {
        f = fopen(path, "wb");
        if (!f) { perror(path); exit(2); }
        cap = cap_rows;
        buf.clear();
        buf.reserve(cap);
        if (setvbuf(f, nullptr, _IOFBF, 8u << 20)) {}
    }
    void flush() {
        if (buf.empty()) return;
        if (fwrite(buf.data(), 4, buf.size(), f) != buf.size()) { perror("fwrite"); exit(2); }
        buf.clear();
    }
    void put(i32 v) {
        if (buf.size() == cap) flush();
        buf.push_back(v);
    }
    void close() {
        flush();
        fclose(f);
    }
};

static void emit(const char* json) { fprintf(stderr, "%s\n", json); }

int main(int argc, char** argv) {
    if (argc < 2) {
        fprintf(stderr,
                "usage:\n  gen_scale replica GID100 VAL100 OUTGID OUTVAL [reps=10] [offset=10]\n"
                "  gen_scale hashgroups GID100 VAL100 OUTGID OUTVAL [ngroups] [raw|bucket64|rng64] [nrows]\n");
        return 2;
    }
    std::string mode = argv[1];
    try {
        if (mode == "replica") {
            if (argc < 6) throw std::runtime_error("args");
            const char* in_gid = argv[2];
            const char* in_val = argv[3];
            const char* out_gid = argv[4];
            const char* out_val = argv[5];
            u64 reps = argc > 6 ? std::stoull(argv[6]) : 10;
            i64 offset = argc > 7 ? std::stoll(argv[7]) : 10;
            u64 per_rows = argc > 8 ? std::stoull(argv[8]) : 0;
            double t0 = now_ms();
            Mapped g = map_i32(in_gid);
            Mapped v = map_i32(in_val);
            if (g.n != v.n) throw std::runtime_error("size");
            const u64 n = (per_rows && per_rows < g.n) ? per_rows : g.n;
            if (g.n != 100000000ULL)
                fprintf(stderr, "WARN source rows=%llu (expected 100M)\n", (unsigned long long)g.n);
            double t1 = now_ms();
            const size_t CHUNK = 16u << 20;  // rows per write chunk
            OutCol og(out_gid, CHUNK);
            OutCol ov(out_val, CHUNK);
            for (u64 r = 0; r < reps; ++r) {
                for (u64 off = 0; off < n; off += CHUNK) {
                    u64 take = std::min<u64>(CHUNK, n - off);
                    for (u64 i = 0; i < take; ++i) {
                        i32 gi = g.p[off + i];
                        i32 go = gi < 0 ? -1 : i32(i64(gi) + i64(r) * offset);
                        og.put(go);
                        ov.put(v.p[off + i]);
                    }
                }
            }
            og.close();
            ov.close();
            double t2 = now_ms();
            struct stat sg {}, sv {};
            stat(out_gid, &sg);
            stat(out_val, &sv);
            char js[512];
            snprintf(js, sizeof(js),
                     "{\"gen\":\"replica\",\"in_gid\":\"%s\",\"in_val\":\"%s\","
                     "\"out_gid\":\"%s\",\"out_val\":\"%s\",\"src_rows\":%llu,\"replicas\":%llu,"
                     "\"offset\":%lld,\"per_replica_rows\":%llu,\"rows\":%llu,"
                     "\"gid_bytes\":%lld,\"val_bytes\":%lld,"
                     "\"map_ms\":%.1f,\"write_ms\":%.1f,\"wall_ms\":%.1f}",
                     in_gid, in_val, out_gid, out_val, (unsigned long long)n,
                     (unsigned long long)reps, (long long)offset,
                     (unsigned long long)n, (unsigned long long)(n * reps),
                     (long long)sg.st_size,
                     (long long)sv.st_size, t1 - t0, t2 - t1, t2 - t0);
            emit(js);
            return 0;
        }
        if (mode == "stratified") {
            // Stratified replica: G=101 and V stay EXACTLY the source domain at any n.
            // per-replica sample = systematic (stride) rows over the whole 100M source
            // (all groups present), plus one row per distinct value that the stride
            // sample missed, so the observed value set is always the full 21,084.
            // Output rows = reps * (per + missing) per replica block.
            if (argc < 6) throw std::runtime_error("args");
            const char* in_gid = argv[2];
            const char* in_val = argv[3];
            const char* out_gid = argv[4];
            const char* out_val = argv[5];
            u64 reps = argc > 6 ? std::stoull(argv[6]) : 10;
            i64 offset = argc > 7 ? std::stoll(argv[7]) : 10;
            u64 per = argc > 8 ? std::stoull(argv[8]) : 10000000ULL;
            double t0 = now_ms();
            Mapped g = map_i32(in_gid);
            Mapped v = map_i32(in_val);
            if (g.n != v.n) throw std::runtime_error("size");
            const u64 n = g.n;
            if (per > n) per = n;
            double t1 = now_ms();
            // full distinct domain + first source row of every value
            std::unordered_map<i32, u32> first;
            first.reserve(1 << 16);
            for (u64 i = 0; i < n; ++i) {
                i32 val = v.p[i];
                if (first.find(val) == first.end()) first.emplace(val, u32(i));
            }
            // stride sample row indices (same sample for every replica)
            std::vector<u32> idx(per);
            for (u64 j = 0; j < per; ++j) idx[j] = u32((unsigned long long)j * n / per);
            std::unordered_set<i32> present;
            present.reserve(1 << 16);
            for (u64 j = 0; j < per; ++j) present.insert(v.p[idx[j]]);
            std::vector<i32> missing;
            for (const auto& kv : first)
                if (present.find(kv.first) == present.end()) missing.push_back(kv.first);
            std::sort(missing.begin(), missing.end());
            double t2 = now_ms();
            const size_t CHUNK = 16u << 20;
            OutCol og(out_gid, CHUNK);
            OutCol ov(out_val, CHUNK);
            for (u64 r = 0; r < reps; ++r) {
                for (u64 j = 0; j < per; ++j) {
                    u64 i = idx[j];
                    i32 gi = g.p[i];
                    og.put(gi < 0 ? -1 : i32(i64(gi) + i64(r) * offset));
                    ov.put(v.p[i]);
                }
                for (i32 mv : missing) {
                    u32 i = first[mv];
                    i32 gi = g.p[i];
                    og.put(gi < 0 ? -1 : i32(i64(gi) + i64(r) * offset));
                    ov.put(mv);
                }
            }
            og.close();
            ov.close();
            double t3 = now_ms();
            struct stat sg {}, sv {};
            stat(out_gid, &sg);
            stat(out_val, &sv);
            char js[1024];
            snprintf(js, sizeof(js),
                     "{\"gen\":\"stratified\",\"in_gid\":\"%s\",\"in_val\":\"%s\","
                     "\"out_gid\":\"%s\",\"out_val\":\"%s\",\"replicas\":%llu,"
                     "\"offset\":%lld,\"per_replica_rows\":%llu,\"distinct_values\":%llu,"
                     "\"missing_in_stride_sample\":%zu,\"rows\":%llu,\"gid_bytes\":%lld,"
                     "\"val_bytes\":%lld,\"domain_ms\":%.1f,\"write_ms\":%.1f,\"wall_ms\":%.1f}",
                     in_gid, in_val, out_gid, out_val, (unsigned long long)reps,
                     (long long)offset, (unsigned long long)per,
                     (unsigned long long)first.size(), missing.size(),
                     (unsigned long long)(reps * (per + missing.size())),
                     (long long)sg.st_size, (long long)sv.st_size, t2 - t1, t3 - t2, t3 - t0);
            emit(js);
            return 0;
        }
        if (mode == "hashgroups") {
            if (argc < 6) throw std::runtime_error("args");
            const char* in_gid = argv[2];
            const char* in_val = argv[3];
            const char* out_gid = argv[4];
            const char* out_val = argv[5];
            u64 ngroups = argc > 6 ? std::stoull(argv[6]) : 10000000ULL;
            std::string vm = argc > 7 ? argv[7] : "raw";
            u64 nrows = argc > 8 ? std::stoull(argv[8]) : 0;
            double t0 = now_ms();
            Mapped g = map_i32(in_gid);
            Mapped v = map_i32(in_val);
            if (g.n != v.n) throw std::runtime_error("size");
            const u64 n = nrows ? std::min<u64>(nrows, g.n) : g.n;
            double t1 = now_ms();
            std::vector<i64> thr;  // bucket64 thresholds (up to 63)
            if (vm == "bucket64") {
                std::vector<i32> s(v.p, v.p + n);
                std::sort(s.begin(), s.end());
                for (int k = 1; k < 64; ++k)
                    thr.push_back(i64(s[(size_t)((long double)k * n / 64.0L)]));
                s.clear();
                s.shrink_to_fit();
            } else if (vm != "raw" && vm != "rng64") {
                throw std::runtime_error("valmode");
            }
            double t2 = now_ms();
            const size_t CHUNK = 16u << 20;
            OutCol og(out_gid, CHUNK);
            OutCol ov(out_val, CHUNK);
            for (u64 i = 0; i < n; ++i) {
                og.put(i32(splitmix64(i) % ngroups));
                i32 val;
                if (vm == "raw") {
                    val = v.p[i];
                } else if (vm == "bucket64") {
                    i64 x = v.p[i];
                    val = i32(std::upper_bound(thr.begin(), thr.end(), x) - thr.begin());
                } else {  // rng64
                    val = i32(splitmix64(i ^ 0xA5A5A5A5A5A5A5A5ULL) % 64);
                }
                ov.put(val);
            }
            og.close();
            ov.close();
            double t3 = now_ms();
            char thrbuf[1024];
            size_t o = 0;
            o += (size_t)snprintf(thrbuf + o, sizeof(thrbuf) - o, "[");
            for (size_t k = 0; k < thr.size() && o < sizeof(thrbuf) - 32; ++k)
                o += (size_t)snprintf(thrbuf + o, sizeof(thrbuf) - o, "%s%lld", k ? "," : "",
                                      (long long)thr[k]);
            o += (size_t)snprintf(thrbuf + o, sizeof(thrbuf) - o, "]");
            struct stat sg {}, sv {};
            stat(out_gid, &sg);
            stat(out_val, &sv);
            char js[2048];
            snprintf(js, sizeof(js),
                     "{\"gen\":\"hashgroups\",\"in_gid\":\"%s\",\"in_val\":\"%s\","
                     "\"out_gid\":\"%s\",\"out_val\":\"%s\",\"rows\":%llu,\"ngroups\":%llu,"
                     "\"valmode\":\"%s\",\"gid_bytes\":%lld,\"val_bytes\":%lld,"
                     "\"map_ms\":%.1f,\"quantile_ms\":%.1f,\"write_ms\":%.1f,\"wall_ms\":%.1f,"
                     "\"bucket_thresholds\":%s}",
                     in_gid, in_val, out_gid, out_val, (unsigned long long)n,
                     (unsigned long long)ngroups, vm.c_str(), (long long)sg.st_size,
                     (long long)sv.st_size, t1 - t0, t2 - t1, t3 - t2, t3 - t0, thrbuf);
            emit(js);
            return 0;
        }
        throw std::runtime_error("mode");
    } catch (std::exception& e) {
        fprintf(stderr, "ERR %s\n", e.what());
        return 1;
    }
}
