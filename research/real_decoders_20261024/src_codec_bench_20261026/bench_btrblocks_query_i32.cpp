// Query-level pipeline driver for BtrBlocks (SIGMOD 2023) on the IG/2 query.
// Compresses the two referenced columns once with the BtrBlocks Relation/Chunk API, then
// times decompression + aggregation + canonical output over repetitions. This is the
// "format + aggregation" baseline, not a bare codec kernel.
#include "btrblocks.hpp"
#include "common/Log.hpp"
#include "storage/Relation.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

using namespace btrblocks;

static std::vector<std::pair<std::string, int32_t>> read_csv(const char* path) {
    std::ifstream f(path);
    if (!f) throw std::runtime_error("csv open");
    std::vector<std::pair<std::string, int32_t>> rows;
    std::string line;
    while (std::getline(f, line)) {
        auto pos = line.rfind('|');
        if (pos == std::string::npos) throw std::runtime_error("csv shape");
        rows.emplace_back(line.substr(0, pos), std::stoi(line.substr(pos + 1)));
    }
    return rows;
}

int main(int argc, char** argv) {
    if (argc < 4) {
        std::cerr << "usage: bench_btrblocks_query_i32 CSV REPS OUT.json\n";
        return 2;
    }
    const int reps = std::max(1, std::atoi(argv[2]));
    BtrBlocksConfig::configure([&](BtrBlocksConfig& config) {
        config.strings.max_cascade_depth = 3;
        config.integers.max_cascade_depth = 3;
    });
    Log::set_level(Log::level::warn);
    auto rows = read_csv(argv[1]);
    const size_t n = rows.size();
    // Vector<string_view> is a file-mapped format: count, slots, then bytes.
    const std::string string_path = "/tmp/btrblocks_query_strings.bin";
    std::vector<std::pair<uint64_t, uint64_t>> slots;
    std::string bytes;
    slots.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        slots.emplace_back(rows[i].first.size(), 8 + n * 16 + bytes.size());
        bytes += rows[i].first;
    }
    {
        std::ofstream sf(string_path, std::ios::binary | std::ios::trunc);
        const uint64_t count = n;
        sf.write(reinterpret_cast<const char*>(&count), sizeof(count));
        for (auto [size, offset] : slots) {
            sf.write(reinterpret_cast<const char*>(&size), sizeof(size));
            sf.write(reinterpret_cast<const char*>(&offset), sizeof(offset));
        }
        sf.write(bytes.data(), bytes.size());
    }
    Vector<str> states(string_path.c_str());
    Vector<INTEGER> values(n);
    for (size_t i = 0; i < n; ++i) values.data[i] = rows[i].second;
    Relation relation;
    relation.addColumn(Column("State", Column::Data(std::move(states))));
    relation.addColumn(Column("POPESTIMATE2014", Column::Data(std::move(values))));
    Range range(0, relation.tuple_count);
    Chunk input = relation.getChunk({range}, 0);
    Datablock compressor(relation);
    std::unique_ptr<uint8_t[]> output(new uint8_t[input.tuple_count * 16 + 4096]);
    auto stats = compressor.compress(input, output);

    std::vector<double> total_ms;
    std::string best_payload;
    size_t groups = 0;
    for (int r = 0; r < reps; ++r) {
        auto t0 = std::chrono::steady_clock::now();
        Chunk decompressed = compressor.decompress(output);
        std::unordered_map<std::string_view, int64_t> sums;
        sums.reserve(64);
        for (size_t i = 0; i < n; ++i) {
            str state = decompressed(0, i);
            if (state == "District of Columbia") continue;
            sums[state] += decompressed.get<int32_t>(1, i);
        }
        std::vector<std::pair<std::string, int64_t>> out_rows;
        for (auto& kv : sums) out_rows.emplace_back(std::string(kv.first), kv.second);
        std::sort(out_rows.begin(), out_rows.end());
        groups = out_rows.size();
        std::string payload = "{\"rows\":[";
        for (size_t i = 0; i < out_rows.size(); ++i) {
            if (i) payload += ',';
            payload += "[\"" + out_rows[i].first + "\",\"" + std::to_string(out_rows[i].second) + "\"]";
        }
        payload += "]}";
        best_payload = std::move(payload);
        auto t1 = std::chrono::steady_clock::now();
        total_ms.push_back(std::chrono::duration<double, std::milli>(t1 - t0).count());
    }
    std::sort(total_ms.begin(), total_ms.end());
    const double med = total_ms[total_ms.size() / 2];
    std::ofstream out(argv[3]);
    out << "{\"codec\":\"btrblocks_query\",\"values\":65536,\"rows\":" << groups
        << ",\"compressed_bytes\":" << stats.total_data_size
        << ",\"stored_interval_median_ms\":" << med << "}\n";
    std::ofstream payload_out(std::string(argv[3]) + ".payload.json");
    payload_out << best_payload << "\n";
    std::cout << "btrblocks_query stored_interval_ms=" << med << " rows=" << groups << std::endl;
    return 0;
}
