// Query-level pipeline driver for the FastLanes (PVLDB 2025) format on the IG/2 query.
// This is the "format + aggregation" baseline: read the .fls file through the official API,
// materialize the two referenced columns, evaluate the unchanged IG/2 aggregation and write
// the canonical typed result. It measures what a user of the format gets end to end, as
// opposed to the bare decode kernel in bench_fastlanes_i32.cpp.
#include "fastlanes.hpp"
#include "fls/json/nlohmann/json.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string_view>
#include <unordered_map>
#include <sstream>
#include <string>
#include <vector>

using namespace fastlanes;

struct SumRow {
    std::string state;
    int64_t sum;
};

static std::string read_file(const char* p) {
    std::ifstream f(p, std::ios::binary);
    if (!f) throw std::runtime_error("open");
    return std::string(std::istreambuf_iterator<char>(f), {});
}

static const int64_t* int_data(const col_pt& c) {
    if (auto p = std::get_if<up<col_i64>>(&c)) return p->get()->data.data();
    if (auto p = std::get_if<up<col_i32>>(&c)) {
        static thread_local std::vector<int64_t> tmp;
        auto& d = p->get()->data;
        tmp.assign(d.begin(), d.end());
        return tmp.data();
    }
    throw std::runtime_error("not an integer column");
}

static std::string_view sv_at(const col_pt& c, size_t i) {
    if (auto p = std::get_if<up<str_col_t>>(&c)) return p->get()->data[i];
    if (auto p = std::get_if<up<FLSStrColumn>>(&c)) {
        auto& col = *p->get();
        return std::string_view(reinterpret_cast<const char*>(col.str_p_arr[i]),
                                col.length_arr[i]);
    }
    throw std::runtime_error("not a string column");
}

static std::string result_json(const std::vector<SumRow>& rows, const nlohmann::json& oracle) {
    nlohmann::json out;
    out["columns"] = oracle["columns"];
    out["rows"] = nlohmann::json::array();
    for (const auto& r : rows) out["rows"].push_back({r.state, std::to_string(r.sum)});
    return out.dump();
}

int main(int argc, char** argv) {
    if (argc < 5) {
        std::cerr << "usage: bench_fastlanes_query_i32 CSV_DIR FLs REPS OUT.json [ORACLE.json]\n";
        return 2;
    }
    const std::string csv_dir = argv[1];
    const std::string fls_path = argv[2];
    const int reps = std::max(1, std::atoi(argv[3]));
    const char* out_path = argv[4];
    if (argc < 6) { std::cerr << "ORACLE.json required\n"; return 2; }
    const char* oracle_path = argv[5];

    std::vector<double> materialize_ms, aggregate_ms;
    size_t rows_total = 0;
    std::string payload;
    for (int r = 0; r < reps; ++r) {
        std::error_code ec;
        std::filesystem::remove(fls_path, ec);
        Connection writer;
        writer.read_csv(csv_dir);
        writer.to_fls(fls_path);  // encoding is outside the measured interval (persisted input)
    }
    for (int r = 0; r < reps; ++r) {
        auto t0 = std::chrono::steady_clock::now();
        Connection reader_conn;
        auto reader = reader_conn.reset().read_fls(fls_path);
        auto table = reader->materialize();
        auto t1 = std::chrono::steady_clock::now();
        std::unordered_map<std::string_view, int64_t> sums;
        sums.reserve(64);
        size_t n = 0;
        for (auto& rgp : table->m_rowgroups) {
            auto& rg = *rgp;
            const auto& scol = rg.internal_rowgroup[0];
            const auto& icol = rg.internal_rowgroup[1];
            const int64_t* ints = int_data(icol);
            for (size_t i = 0; i < rg.n_tup; ++i) {
                std::string_view state = sv_at(scol, i);
                if (state == "District of Columbia") continue;
                sums[state] += ints[i];
                ++n;
            }
        }
        std::vector<SumRow> rows;
        rows.reserve(sums.size());
        for (auto& kv : sums) rows.push_back({std::string(kv.first), kv.second});
        std::sort(rows.begin(), rows.end(),
                  [](const SumRow& a, const SumRow& b) { return a.state < b.state; });
        nlohmann::json oracle = nlohmann::json::parse(read_file(oracle_path));
        payload = result_json(rows, oracle);
        rows_total = rows.size();
        auto t2 = std::chrono::steady_clock::now();
        materialize_ms.push_back(std::chrono::duration<double, std::milli>(t1 - t0).count());
        aggregate_ms.push_back(std::chrono::duration<double, std::milli>(t2 - t1).count());
    }
    std::sort(materialize_ms.begin(), materialize_ms.end());
    std::sort(aggregate_ms.begin(), aggregate_ms.end());
    const double mat = materialize_ms[materialize_ms.size() / 2];
    const double agg = aggregate_ms[aggregate_ms.size() / 2];
    bool exact = true;
    if (oracle_path) {
        auto got = nlohmann::json::parse(payload);
        auto want = nlohmann::json::parse(read_file(oracle_path));
        exact = (got == want);
    }
    std::ofstream out(out_path);
    out << "{\"codec\":\"fastlanes_query\",\"values\":65536,\"rows\":" << rows_total
        << ",\"materialize_median_ms\":" << mat
        << ",\"aggregate_output_median_ms\":" << agg
        << ",\"stored_interval_median_ms\":" << (mat + agg)
        << ",\"decoded_exact\":" << (exact ? "true" : "false") << "}\n";
    std::cout << "fastlanes_query materialize_ms=" << mat << " aggregate_ms=" << agg
              << " exact=" << (exact ? 1 : 0) << std::endl;
    return exact ? 0 : 1;
}
