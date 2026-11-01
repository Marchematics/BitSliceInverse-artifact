// Hand-written C++ baseline on plain (decoded) columns for the IG/2 query.
// This is the fairness control: the best straightforward C++ over decoded values,
// with no encoder/format overhead. It reads a flat binary (count, length-prefixed
// State strings, int64 POPESTIMATE2014 values), aggregates and writes the canonical
// typed result. Compare its stored interval with the encoded exact-query path.
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fstream>
#include <iostream>
#include <string>
#include <string_view>
#include <unistd.h>
#include <unordered_map>
#include <vector>

struct Row {
    std::string state;
    int64_t sum;
};

struct MappedFile {
    const char* data;
    size_t size;
    ~MappedFile() { if (data) munmap(const_cast<char*>(data), size); }
};

static MappedFile map_file(const char* path) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) throw std::runtime_error("open");
    struct stat sb;
    if (fstat(fd, &sb) != 0) throw std::runtime_error("fstat");
    void* p = mmap(nullptr, static_cast<size_t>(sb.st_size), PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (p == MAP_FAILED) throw std::runtime_error("mmap");
    return {static_cast<const char*>(p), static_cast<size_t>(sb.st_size)};
}

int main(int argc, char** argv) {
    if (argc < 4) {
        std::cerr << "usage: bench_plain_cpp_query_i32 PLAIN.bin REPS OUT.json\n";
        return 2;
    }
    const int reps = std::max(1, std::atoi(argv[2]));
    std::vector<double> read_ms, agg_ms;
    std::string payload;
    size_t groups = 0;
    for (int r = 0; r < reps; ++r) {
        auto t0 = std::chrono::steady_clock::now();
        auto data = map_file(argv[1]);
        auto t1 = std::chrono::steady_clock::now();
        const char* p = data.data;
        uint64_t n = *reinterpret_cast<const uint64_t*>(p);
        p += sizeof(uint64_t);
        std::vector<uint32_t> lens(n);
        std::vector<std::string_view> states(n);
        for (size_t i = 0; i < n; ++i) {
            uint32_t len = *reinterpret_cast<const uint32_t*>(p);
            p += sizeof(uint32_t);
            lens[i] = len;
            states[i] = std::string_view(p, len);
            p += len;
        }
        const int64_t* values = reinterpret_cast<const int64_t*>(p);
        std::vector<Row> rows;
        rows.reserve(64);
        size_t i = 0;
        while (i < n) {
            size_t j = i + 1;
            while (j < n && states[j] == states[i]) ++j;
            if (states[i] != "District of Columbia") {
                int64_t sum = 0;
                for (size_t k = i; k < j; ++k) sum += values[k];
                rows.push_back({std::string(states[i]), sum});
            }
            i = j;
        }
        std::sort(rows.begin(), rows.end(), [](const Row& a, const Row& b) {
            return a.state < b.state;
        });
        std::string out = "{\"rows\":[";
        for (size_t i = 0; i < rows.size(); ++i) {
            if (i) out += ',';
            out += "[\"" + rows[i].state + "\",\"" + std::to_string(rows[i].sum) + "\"]";
        }
        out += "]}";
        payload = std::move(out);
        groups = rows.size();
        auto t2 = std::chrono::steady_clock::now();
        read_ms.push_back(std::chrono::duration<double, std::milli>(t1 - t0).count());
        agg_ms.push_back(std::chrono::duration<double, std::milli>(t2 - t1).count());
    }
    std::sort(read_ms.begin(), read_ms.end());
    std::sort(agg_ms.begin(), agg_ms.end());
    const double rd = read_ms[read_ms.size() / 2];
    const double ag = agg_ms[agg_ms.size() / 2];
    std::ofstream out(argv[3]);
    out << "{\"codec\":\"plain_cpp\",\"values\":65536,\"rows\":" << groups
        << ",\"read_median_ms\":" << rd << ",\"aggregate_output_median_ms\":" << ag
        << ",\"stored_interval_median_ms\":" << (rd + ag) << "}\n";
    std::ofstream(std::string(argv[3]) + ".payload.json") << payload << "\n";
    std::cout << "plain_cpp read_ms=" << rd << " aggregate_ms=" << ag << std::endl;
    return 0;
}
