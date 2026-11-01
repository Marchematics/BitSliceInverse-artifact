// Sorted row-ID range index baseline (CUBIT/RABIT-like), NULL-group aware.
//
// This is the repository's own strongest internal baseline for the dynamic
// multi-predicate class (`src_encoded_ops_20261030/range_index.cpp`): one sorted
// row-id permutation per predicate column; a query binary-searches each column,
// sets the matching row bits, ANDs the three bitmaps and scans the survivors in
// row order to aggregate.  Cost is O(n) per query.
//
// Local change over the repository version: rows whose group id is negative (a
// NULL group key, MEC/ECUBE02 convention) are aggregated into their own bucket
// and reported as "NULL" instead of indexing the group array out of bounds.
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <numeric>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

static std::vector<int64_t> read_i64(const char* p) {
  std::ifstream f(p, std::ios::binary);
  f.seekg(0, std::ios::end);
  size_t n = f.tellg() / 8;
  f.seekg(0);
  std::vector<int64_t> v(n);
  f.read((char*)v.data(), n * 8);
  return v;
}

int main(int argc, char** argv) {
  if (argc < 3) return 2;
  const std::string mode = argv[1];
  if (mode == "build") {
    if (argc < 7) {
      fprintf(stderr, "build g v c1 c2 out\n");
      return 2;
    }
    auto t0 = std::chrono::steady_clock::now();
    std::vector<std::vector<int64_t>> a = {read_i64(argv[2]), read_i64(argv[3]), read_i64(argv[4]),
                                           read_i64(argv[5])};
    size_t n = a[0].size();
    std::ofstream o(argv[6], std::ios::binary);
    o.write("RIDX01  ", 8);
    o.write((char*)&n, 8);
    o.write((char*)&n, 8);
    for (int c = 1; c < 4; c++) {
      std::vector<int32_t> ids(n);
      std::iota(ids.begin(), ids.end(), 0);
      std::sort(ids.begin(), ids.end(), [&](int32_t x, int32_t y) {
        return a[c][x] < a[c][y] || (a[c][x] == a[c][y] && x < y);
      });
      o.write((char*)ids.data(), (std::streamsize)n * 4);
    }
    o.close();
    auto t1 = std::chrono::steady_clock::now();
    fprintf(stderr, "{\"build_ms\":%.3f}\n",
            std::chrono::duration<double, std::milli>(t1 - t0).count());
    return 0;
  }
  if (argc < 14) {
    fprintf(stderr, "query idx g v c1 c2 out p_lo p_hi d_lo d_hi q_lo q_hi\n");
    return 2;
  }
  auto t0 = std::chrono::steady_clock::now();
  int fd = open(argv[2], O_RDONLY);
  struct stat st {};
  fstat(fd, &st);
  const char* base = (const char*)mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
  close(fd);
  uint64_t n;
  memcpy(&n, base + 8, 8);
  const int32_t* idx = (const int32_t*)(base + 24);
  const int64_t* A[4];
  for (int k = 0; k < 4; k++) {
    int f = open(argv[3 + k], O_RDONLY);
    struct stat s {};
    fstat(f, &s);
    A[k] = (const int64_t*)mmap(nullptr, s.st_size, PROT_READ, MAP_PRIVATE, f, 0);
    close(f);
  }
  int64_t lo[3] = {std::stoll(argv[8]), std::stoll(argv[10]), std::stoll(argv[12])};
  int64_t hi[3] = {std::stoll(argv[9]), std::stoll(argv[11]), std::stoll(argv[13])};
  uint64_t words = (n + 63) / 64;
  std::vector<uint64_t> mask(words, ~0ULL), col(words, 0);
  for (int c = 0; c < 3; c++) {
    const int32_t* ids = idx + uint64_t(c) * n;
    const int64_t* v = A[c + 1];
    auto lower = [&](int64_t x) {
      return std::lower_bound(ids, ids + n, x, [&](int32_t id, int64_t val) { return v[id] < val; });
    };
    auto upper = [&](int64_t x) {
      return std::upper_bound(ids, ids + n, x, [&](int64_t val, int32_t id) { return val < v[id]; });
    };
    int32_t const* l = lower(lo[c]);
    int32_t const* r = upper(hi[c]);
    std::fill(col.begin(), col.end(), 0);
    for (auto it = l; it < r; ++it) {
      uint32_t id = *it;
      col[id >> 6] |= 1ULL << (id & 63);
    }
    for (uint64_t w = 0; w < words; w++) mask[w] &= col[w];
  }
  constexpr int NULL_BUCKET = 128;  // one past the real group ids
  int64_t sum[129] = {0};
  uint64_t cnt[129] = {0};
  int64_t mn[129], mx[129];
  for (int i = 0; i < 129; i++) { mn[i] = INT64_MAX; mx[i] = INT64_MIN; }
  for (uint64_t i = 0; i < n; i++) {
    if (!((mask[i >> 6] >> (i & 63)) & 1ULL)) continue;
    int64_t gi = A[0][i];
    if (gi < 0) gi = NULL_BUCKET;  // NULL group bucket
    if (gi > NULL_BUCKET) continue;
    sum[gi] += A[1][i];
    cnt[gi]++;
    if (A[1][i] < mn[gi]) mn[gi] = A[1][i];
    if (A[1][i] > mx[gi]) mx[gi] = A[1][i];
  }
  auto t1 = std::chrono::steady_clock::now();
  FILE* f = fopen(argv[7], "w");
  fprintf(f, "{\"rows\":[");
  bool first = true;
  for (int g = 0; g <= NULL_BUCKET; g++) {
    if (!cnt[g]) continue;
    if (!first) fprintf(f, ",");
    first = false;
    if (g == NULL_BUCKET)
      fprintf(f, "[\"NULL\",\"%lld\",\"%llu\",\"%lld\",\"%lld\"]", (long long)sum[g],
              (unsigned long long)cnt[g], (long long)mn[g], (long long)mx[g]);
    else
      fprintf(f, "[\"%d\",\"%lld\",\"%llu\",\"%lld\",\"%lld\"]", g, (long long)sum[g],
              (unsigned long long)cnt[g], (long long)mn[g], (long long)mx[g]);
  }
  fprintf(f, "]}\n");
  fclose(f);
  auto t2 = std::chrono::steady_clock::now();
  fprintf(stderr, "{\"query_ms\":%.4f,\"output_ms\":%.4f,\"stored_ms\":%.4f}\n",
          std::chrono::duration<double, std::milli>(t1 - t0).count(),
          std::chrono::duration<double, std::milli>(t2 - t1).count(),
          std::chrono::duration<double, std::milli>(t2 - t0).count());
  return 0;
}
