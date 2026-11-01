// Materialized-summary baseline for the fixed-GROUP-BY query (classic textbook approach):
//   one 11-row summary table per predicate, precomputed by a single group-by pass.
// Query path = open the summary file, read 11 rows, emit the complete aggregate result.
// This is the "materialized group aggregate" baseline that BYPASSES scanning entirely;
// it cannot answer a predicate that was not materialized.
//
// g++ -O3 -std=c++17 -o mat_summary mat_summary.cpp
//   mat_summary summary.csv out.json reps
//
// summary.csv: one line per group "key,sum,count,min,max" (key = integer or "null")
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

struct Row { std::string k; long long s, c, mn, mx; };

static double now_us() {
    using namespace std::chrono;
    return duration<double, std::micro>(steady_clock::now().time_since_epoch()).count();
}

static std::vector<Row> read_csv(const char* path) {
    std::vector<Row> rows;
    FILE* f = fopen(path, "r");
    if (!f) { perror(path); exit(2); }
    char line[512];
    while (fgets(line, sizeof(line), f)) {
        Row r;
        char kb[64];
        if (sscanf(line, "%63[^,],%lld,%lld,%lld,%lld", kb, &r.s, &r.c, &r.mn, &r.mx) != 5) continue;
        r.k = kb;
        rows.push_back(r);
    }
    fclose(f);
    return rows;
}

static size_t format_rows(const std::vector<Row>& rows, char* out, size_t cap) {
    size_t off = 0;
    off += (size_t)snprintf(out + off, cap - off, "{\"rows\":[");
    for (size_t i = 0; i < rows.size(); i++) {
        const Row& r = rows[i];
        const char* key = r.k == "null" ? "null" : r.k.c_str();
        off += (size_t)snprintf(out + off, cap - off, "%s[%s,%lld,%lld,%lld,%lld]",
                                i ? "," : "", key, r.s, r.c, r.mn, r.mx);
    }
    off += (size_t)snprintf(out + off, cap - off, "]}\n");
    return off;
}

int main(int argc, char** argv) {
    if (argc < 4) { fprintf(stderr, "mat_summary summary.csv out.json reps\n"); return 2; }
    const char* in = argv[1];
    const char* out = argv[2];
    int reps = atoi(argv[3]);
    // in-process "stored" measurement, same protocol as the Lead's materialized_group
    double t0 = now_us();
    std::vector<Row> rows = read_csv(in);
    static char buf[1 << 16];
    size_t len = format_rows(rows, buf, sizeof(buf));
    FILE* o = fopen(out, "w");
    fwrite(buf, 1, len, o);
    fclose(o);
    double stored_us = now_us() - t0;
    // engine-resident measurement: summary already parsed, formatting only
    std::vector<double> ts;
    for (int r = 0; r < reps; r++) {
        double t = now_us();
        len = format_rows(rows, buf, sizeof(buf));
        ts.push_back(now_us() - t);
    }
    std::sort(ts.begin(), ts.end());
    double med = ts[ts.size() / 2];
    double tot = 0;
    for (double t : ts) tot += t;
    printf("{\"rows\":%zu,\"reps\":%d,\"format_median_us\":%.3f,\"format_min_us\":%.3f,"
           "\"stored_us\":%.3f,\"bytes_out\":%zu}\n",
           rows.size(), reps, med, ts.front(), stored_us, len);
    return 0;
}
