// Engine-free ECUBE02/ECUBE03 driver: the index's own access path, no SQL engine.
//
// This is the "warm in-process" tier of the cube protocol (one open index, many
// timed reductions) and the "cold per-request CLI" process floor. It links
// nothing but libstdc++, so its process-start cost is the honest lower bound on
// any cold request through *any* engine on the same host.
//
// Modes:
//   query <cube> <dict> <out.json> [spec]   one request; JSON on stderr timing
//   panel <cube> <dict> <panel.txt> <reps>  warm medians for the whole panel
//   load  <cube> <dict>                     index load only
#include <algorithm>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>

#include "ecube_index.hpp"

namespace {

struct Stats {
    std::vector<double> v;
    void sort() { std::sort(v.begin(), v.end()); }
    double median() const { return v.empty() ? 0 : v[v.size() / 2]; }
    double min() const { return v.empty() ? 0 : v.front(); }
    double p95() const { return v.empty() ? 0 : v[size_t(v.size() * 95 / 100)]; }
};

int mode_query(const char* cube, const char* dict, const char* out, const std::string& spec) {
    const double t_main = ecube::now_ms();
    ecube::LoadedIndex li;
    ecube::load_index(cube, dict, li);
    const double t_open = ecube::now_ms();
    auto q = ecube::parse_spec_string(spec);
    std::vector<ecube::Row> rows;
    ecube::reduce(li.cube, q, rows);
    const double t_red = ecube::now_ms();
    std::string json = ecube::to_json(li.cube, rows, q);
    const double t_done = ecube::now_ms();
    FILE* f = std::fopen(out, "wb");
    if (f) { std::fwrite(json.data(), 1, json.size(), f); std::fclose(f); }
    std::fprintf(stderr,
                 "{\"engine\":\"direct\",\"protocol\":\"cold_process\",\"format\":%u,"
                 "\"index_load_ms\":%.6f,\"reduce_ms\":%.6f,\"json_ms\":%.6f,"
                 "\"t_main_to_done_ms\":%.6f,\"json_bytes\":%zu,\"rows\":%zu,"
                 "\"index_bytes\":%zu,\"checksum\":%llu}\n",
                 li.cube.format, t_open - t_main, t_red - t_open, t_done - t_red, t_done - t_main,
                 json.size(), rows.size(), li.cube.bytes,
                 (unsigned long long)ecube::checksum(rows));
    return 0;
}

int mode_load(const char* cube, const char* dict) {
    ecube::LoadedIndex li;
    ecube::load_index(cube, dict, li);
    std::printf("{\"index_load_ms\":%.6f,\"format\":%u,\"groups\":%llu,\"values\":%llu,"
                "\"index_bytes\":%llu}\n",
                li.load_ms, li.cube.format, (unsigned long long)li.cube.h.ngroups,
                (unsigned long long)li.cube.h.nvalues, (unsigned long long)li.cube.bytes);
    return 0;
}

struct PanelCase { std::string name, spec; };

// panel.tsv: name <TAB> cube_spec <TAB> scan_where <TAB> scan_having
std::vector<PanelCase> read_panel(const char* path) {
    std::ifstream f(path);
    if (!f) throw std::runtime_error("panel open");
    std::vector<PanelCase> out;
    std::string line;
    while (std::getline(f, line)) {
        if (line.empty() || line[0] == '#') continue;
        std::vector<std::string> fld;
        size_t p = 0;
        while (p <= line.size()) {
            size_t z = line.find('\t', p);
            if (z == std::string::npos) z = line.size();
            fld.push_back(line.substr(p, z - p));
            p = z + 1;
        }
        while (fld.size() < 4) fld.push_back("");
        out.push_back({fld[0], fld[1]});
    }
    return out;
}

int mode_panel(const char* cube, const char* dict, const char* panel, int reps) {
    ecube::LoadedIndex li;
    ecube::load_index(cube, dict, li);
    auto cases = read_panel(panel);
    // one untimed warmup of every case, then reps per case
    std::vector<std::vector<ecube::Row>> sink(cases.size());
    for (size_t i = 0; i < cases.size(); ++i) {
        auto q = ecube::parse_spec_string(cases[i].spec);
        ecube::reduce(li.cube, q, sink[i]);
    }
    std::printf("{\"engine\":\"direct\",\"protocol\":\"warm_in_process\",\"format\":%u,"
                "\"index_load_ms\":%.6f,\"cases\":[",
                li.cube.format, li.load_ms);
    for (size_t i = 0; i < cases.size(); ++i) {
        auto q = ecube::parse_spec_string(cases[i].spec);
        std::vector<ecube::Row> rows;
        Stats s, sj;
        for (int r = 0; r < reps; ++r) {
            const double t = ecube::now_ms();
            ecube::reduce(li.cube, q, rows);
            const double t2 = ecube::now_ms();
            std::string js = ecube::to_json(li.cube, rows, q);
            const double t3 = ecube::now_ms();
            s.v.push_back(t2 - t);
            sj.v.push_back(t3 - t);
            if (!js.empty()) sink[i].swap(rows);
        }
        s.sort(); sj.sort();
        std::string js = ecube::to_json(li.cube, sink[i], q);
        if (i) std::printf(",");
        std::printf("{\"name\":\"%s\",\"spec\":\"%s\",\"median_ms\":%.6f,\"min_ms\":%.6f,"
                    "\"p95_ms\":%.6f,\"median_with_json_ms\":%.6f,\"json_bytes\":%zu,"
                    "\"reps\":%d,\"checksum\":%llu}",
                    cases[i].name.c_str(), cases[i].spec.c_str(), s.median(), s.min(), s.p95(),
                    sj.median(), js.size(), reps,
                    (unsigned long long)ecube::checksum(sink[i]));
    }
    std::printf("]}\n");
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    try {
        if (argc < 2) {
            std::fprintf(stderr,
                         "usage:\n  ecube_direct query <cube> <dict> <out.json> [spec]\n"
                         "  ecube_direct panel <cube> <dict> <panel.txt> <reps>\n"
                         "  ecube_direct load  <cube> <dict>\n");
            return 2;
        }
        const std::string mode = argv[1];
        if (mode == "query" && argc >= 5)
            return mode_query(argv[2], argv[3], argv[4], argc > 5 ? argv[5] : "");
        if (mode == "panel" && argc >= 5)
            return mode_panel(argv[2], argv[3], argv[4], std::stoi(argv[5]));
        if (mode == "load" && argc >= 4) return mode_load(argv[2], argv[3]);
        std::fprintf(stderr, "bad mode/args\n");
        return 2;
    } catch (std::exception& e) {
        std::fprintf(stderr, "ERR %s\n", e.what());
        return 1;
    }
}
