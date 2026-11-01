// Kernel-level file-format comparison driver for FastLanes (PVLDB 2025).
// Input: a directory containing one single-column CSV (values separated by '|') and a
// schema.json describing that column. The driver times writing the FastLanes file
// (encode) and reading it back (decode/materialize), validates the decoded values and
// reports bytes/value and ns/value. This is a format/kernel comparison, not SQL.
#include "fastlanes.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

using namespace fastlanes;

static std::vector<int32_t> read_i32(const char* path, size_t& n) {
    std::ifstream f(path, std::ios::binary);
    if (!f) throw std::runtime_error("input open");
    f.seekg(0, std::ios::end);
    auto bytes = static_cast<size_t>(f.tellg());
    f.seekg(0, std::ios::beg);
    n = bytes / sizeof(int32_t);
    std::vector<int32_t> data(n);
    f.read(reinterpret_cast<char*>(data.data()), static_cast<std::streamsize>(bytes));
    if (!f) throw std::runtime_error("input read");
    return data;
}

int main(int argc, char** argv) {
    if (argc != 6) {
        std::cerr << "usage: bench_fastlanes_i32 CSV_DIR OUT.fls REPS OUT.json INPUT.bin\n";
        return 2;
    }
    const std::string csv_dir = argv[1];
    const std::string fls_path = argv[2];
    const int reps = std::atoi(argv[3]);
    const char* out_path = argv[4];
    size_t n = 0;
    auto expected = read_i32(argv[5], n);
    std::vector<double> encode_ms, decode_ms;
    uint64_t file_bytes = 0;
    bool ok = true;
    for (int r = 0; r < reps; ++r) {
        Connection writer;
        writer.read_csv(csv_dir);
        std::error_code ec;
        std::filesystem::remove(fls_path, ec);  // to_fls refuses an existing target
        auto t0 = std::chrono::steady_clock::now();
        writer.to_fls(fls_path);
        auto t1 = std::chrono::steady_clock::now();
        Connection reader_conn;
        auto t2 = std::chrono::steady_clock::now();
        auto reader = reader_conn.reset().read_fls(fls_path);
        auto table = reader->materialize();
        auto t3 = std::chrono::steady_clock::now();
        encode_ms.push_back(std::chrono::duration<double, std::milli>(t1 - t0).count());
        decode_ms.push_back(std::chrono::duration<double, std::milli>(t3 - t2).count());
        file_bytes = static_cast<uint64_t>(std::filesystem::file_size(fls_path));
        if (r == 0) {
            const std::string csv_out = fls_path + ".decoded.csv";
            reader->to_csv(csv_out);
            std::ifstream f(csv_out);
            std::string line;
            size_t i = 0;
            while (std::getline(f, line) && i < n) {
                std::string field = line.substr(0, line.find('|'));
                int32_t value = static_cast<int32_t>(std::stol(field));
                if (value != expected[i]) { ok = false; break; }
                ++i;
            }
            if (i != n) ok = false;
        }
    }
    std::sort(encode_ms.begin(), encode_ms.end());
    std::sort(decode_ms.begin(), decode_ms.end());
    const double enc = encode_ms[encode_ms.size() / 2];
    const double dec = decode_ms[decode_ms.size() / 2];
    std::ofstream out(out_path);
    out << "{\"codec\":\"fastlanes\",\"kind\":\"kernel\",\"column\":\"int32\","
        << "\"values\":" << n << ",\"reps\":" << reps
        << ",\"fls_bytes\":" << file_bytes
        << ",\"bytes_per_value\":" << (double(file_bytes) / double(n))
        << ",\"encode_median_ms\":" << enc
        << ",\"decode_median_ms\":" << dec
        << ",\"encode_ns_per_value\":" << (enc * 1e6 / double(n))
        << ",\"decode_ns_per_value\":" << (dec * 1e6 / double(n))
        << ",\"decoded_exact\":" << (ok ? "true" : "false") << "}\n";
    std::cout << "{\"status\":\"done\",\"decoded_exact\":" << (ok ? "true" : "false")
              << ",\"bytes_per_value\":" << (double(file_bytes) / double(n))
              << ",\"decode_ns_per_value\":" << (dec * 1e6 / double(n)) << "}\n";
    return ok ? 0 : 1;
}
