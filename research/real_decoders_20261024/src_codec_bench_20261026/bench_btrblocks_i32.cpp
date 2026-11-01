// Kernel-level codec comparison driver for BtrBlocks (SIGMOD 2023).
// Input: a flat little-endian int32 binary. The driver compresses one INTEGER column
// with BtrBlocks' Relation/Chunk/Datablock API, decompresses it, validates every value and
// reports bytes/value and encode/decode nanoseconds per value. Format/kernel comparison
// only, not a SQL query.
#include "btrblocks.hpp"
#include "common/Log.hpp"
#include "storage/Relation.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <vector>

using namespace btrblocks;

static std::vector<int32_t> read_i32(const char* path) {
    std::ifstream f(path, std::ios::binary);
    if (!f) throw std::runtime_error("input open");
    f.seekg(0, std::ios::end);
    const auto bytes = static_cast<size_t>(f.tellg());
    f.seekg(0, std::ios::beg);
    std::vector<int32_t> data(bytes / sizeof(int32_t));
    f.read(reinterpret_cast<char*>(data.data()), static_cast<std::streamsize>(bytes));
    if (!f) throw std::runtime_error("input read");
    return data;
}

int main(int argc, char** argv) {
    if (argc < 4) {
        std::cerr << "usage: bench_btrblocks_i32 INPUT.bin REPS OUT.json [MAX_DEPTH]\n";
        return 2;
    }
    const char* input_path = argv[1];
    const int reps = std::max(1, std::atoi(argv[2]));
    const char* out_path = argv[3];
    const int depth = argc > 4 ? std::atoi(argv[4]) : 3;

    BtrBlocksConfig::configure([&](BtrBlocksConfig& config) {
        config.integers.max_cascade_depth = depth;
    });
    Log::set_level(Log::level::warn);

    auto values = read_i32(input_path);
    const size_t n = values.size();
    Relation relation;
    Vector<INTEGER> column(n);
    std::memcpy(column.data, values.data(), n * sizeof(int32_t));
    relation.addColumn(Column("ints", Column::Data(std::move(column))));
    Range range(0, relation.tuple_count);
    Chunk input = relation.getChunk({range}, 0);
    Datablock compressor(relation);

    std::vector<double> encode_ms, decode_ms;
    uint64_t compressed_bytes = 0;
    bool exact = true;
    for (int r = 0; r < reps; ++r) {
        std::unique_ptr<uint8_t[]> output(
            new uint8_t[input.tuple_count * sizeof(int32_t) * 2 + 4096]);
        auto t0 = std::chrono::steady_clock::now();
        auto stats = compressor.compress(input, output);
        auto t1 = std::chrono::steady_clock::now();
        Chunk decompressed = compressor.decompress(output);
        auto t2 = std::chrono::steady_clock::now();
        encode_ms.push_back(std::chrono::duration<double, std::milli>(t1 - t0).count());
        decode_ms.push_back(std::chrono::duration<double, std::milli>(t2 - t1).count());
        compressed_bytes = stats.total_data_size;
        if (!decompressed.columns || decompressed.tuple_count != n) {
            exact = false;
        } else {
            const auto* got = reinterpret_cast<const int32_t*>(decompressed.columns[0].get());
            for (size_t i = 0; i < n; ++i) {
                if (got[i] != values[i]) { exact = false; break; }
            }
        }
    }
    std::sort(encode_ms.begin(), encode_ms.end());
    std::sort(decode_ms.begin(), decode_ms.end());
    const double enc = encode_ms[encode_ms.size() / 2];
    const double dec = decode_ms[decode_ms.size() / 2];
    const double bpv = double(compressed_bytes) / double(n);
    const double enc_ns = enc * 1e6 / double(n);
    const double dec_ns = dec * 1e6 / double(n);
    std::ofstream out(out_path);
    out << "{\"codec\":\"btrblocks\",\"values\":" << n
        << ",\"compressed_bytes\":" << compressed_bytes
        << ",\"bytes_per_value\":" << bpv
        << ",\"encode_median_ms\":" << enc
        << ",\"decode_median_ms\":" << dec
        << ",\"encode_ns_per_value\":" << enc_ns
        << ",\"decode_ns_per_value\":" << dec_ns
        << ",\"decoded_exact\":" << (exact ? "true" : "false")
        << ",\"max_cascade_depth\":" << depth << "}\n";
    std::cout << "codec=btrblocks values=" << n << " bytes_per_value=" << bpv
              << " encode_ns=" << enc_ns << " decode_ns=" << dec_ns
              << " exact=" << (exact ? 1 : 0) << std::endl;
    if (!exact) return 1;
    return 0;
}
