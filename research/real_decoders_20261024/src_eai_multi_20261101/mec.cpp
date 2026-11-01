// MEC02 — exact multi-column conjunction cube for
//   SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v)
//   FROM T WHERE v BETWEEN vlo AND vhi
//            AND c1 BETWEEN a_lo AND a_hi
//            AND c2 BETWEEN b_lo AND b_hi
//            [AND g IN (...) | AND g NOT IN (...)]
//   GROUP BY g
//
// Groups: dense ascending values; a NULL group key (negative group column value)
// becomes its own group at index G-1, exactly like ECUBE02 (has_null/null_group).
// SQL NULL is excluded from both `IN` and `NOT IN` and MEC does the same.
//
// PHYSICAL DESIGN (see MULTICOL_CUBE_20261101.md for the exactness argument):
//
//  1. Rows are ordered by (group, c1, c2, v): the narrow columns form a dense
//     G x D1 x D2 grid of *runs*, each contiguous and v-sorted.  A rectangle over
//     (c1,c2) selects whole runs, so those two predicates cost zero residual work.
//
//  2. v is cut into fixed-width value blocks of width W (value-addressed, so
//     b = (v - vmin)/W is one integer division).  Per run, cumulative over blocks:
//         cumc  [b][g][c] : # rows of run (g,c) with v-block < b
//         cumsum[b][g][c] : SUM(v) of those rows
//     cumc is uint16 when every run fits (fast default) and uint32 otherwise, so a
//     run with more than 65,535 rows is supported; the width is in the header and
//     selected at query time.
//
//  3. Per run and block, non-cumulative dense planes
//         first_off[b][g][c], last_off[b][g][c] : uint16 in-block offset of the
//     first / last row of that block in that run, so MIN/MAX over whole blocks
//     reads two *contiguous* planes instead of chasing random per-row offsets.
//
//  4. Boundary residual: the at most two v-blocks the interval partially covers are
//     decoded from a uint16 per-row in-block offset array.  Offsets inside a run
//     are non-decreasing, so the matching rows are one contiguous slice.
//
// File layout (little endian, sections 64-byte aligned):
//   Header | gvals[G0] | c1vals[D1] | c2vals[D2] | runbase[R+1] |
//   cumc[(BP+1)*R] | cumsum[(BP+1)*R] | first[(BP+1)*R] | last[(BP+1)*R] | offs[n]
// with R = G*D1*D2, G = G0 + (has_null ? 1 : 0).
//
// Modes:
//   mec plan   <g.i64> <v.i64> <c1.i64> <c2.i64> <budget_bytes>
//   mec build  <g.i64> <v.i64> <c1.i64> <c2.i64> <out> [W] [budget_bytes]
//   mec info   <index>
//   mec query  <index> vlo vhi a_lo a_hi b_lo b_hi [sc] [gin=a,b] [gnotin=a,b]
//   mec bench  <index> <reps> <cases.txt>
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <functional>
#include <thread>
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

// W candidates are powers of two <= 2^16 so the per-row in-block offset is uint16.
static constexpr uint32_t W_CANDIDATES[] = {512, 1024, 2048, 4096, 8192, 16384, 32768, 65536};
static constexpr uint32_t MAX_GROUPS = 64;

struct Header {
  char magic[8];  // "MEC02\0\0\0"
  uint64_t n;
  int64_t vmin, vmax;
  uint32_t G;            // total groups, including the NULL group when present
  uint32_t G0;           // non-NULL group values
  uint32_t D1, D2;
  uint32_t cells;        // D1*D2
  uint32_t W, BP;
  uint32_t count_width;  // 2 or 4 bytes per cumc entry
  uint32_t has_null;     // 1 when a NULL group key exists
  uint32_t null_group;   // index of the NULL group (G-1) when has_null
  uint32_t pad0;         // mode (bit 0) | rank_block_size << 1   (0 = value mode)
  uint64_t off_g, off_a, off_b, off_runbase, off_cumc, off_cumsum, off_first, off_last, off_offs;
};

static inline size_t align64(size_t x) { return (x + 63u) & ~size_t(63u); }
static inline double ms_between(std::chrono::steady_clock::time_point a,
                                std::chrono::steady_clock::time_point b) {
  return std::chrono::duration<double, std::milli>(b - a).count();
}

// ---- threading (MEC03) ----------------------------------------------------
// Spawn-once workers for the benchmark loop; parallel_ranges() for the build.
// Every merge is over disjoint integer partials, so results are identical to the
// single-threaded path for any thread count (and the index is byte-identical).
static uint32_t parse_threads(int argc, char** argv) {
  uint32_t t = 1;
  for (int i = 0; i < argc; i++) {
    const std::string a = argv[i];
    if (a.rfind("--threads=", 0) == 0) t = (uint32_t)std::max(1, atoi(a.c_str() + 10));
  }
  return t;
}

template <typename F>
static void parallel_ranges(uint32_t n_items, uint32_t threads, F&& fn) {
  if (n_items == 0) return;
  if (threads > n_items) threads = n_items;
  auto bounds = [&](uint32_t t, uint32_t& a, uint32_t& b) {
    a = (uint32_t)((uint64_t)n_items * t / threads);
    b = (uint32_t)((uint64_t)n_items * (t + 1) / threads);
  };
  if (threads <= 1) {
    uint32_t a, b;
    bounds(0, a, b);
    fn(0u, a, b);
    return;
  }
  std::vector<std::thread> ts;
  ts.reserve(threads - 1);
  for (uint32_t t = 1; t < threads; t++)
    ts.emplace_back([&, t]() {
      uint32_t a, b;
      bounds(t, a, b);
      fn(t, a, b);
    });
  uint32_t a, b;
  bounds(0, a, b);
  fn(0u, a, b);
  for (auto& th : ts) th.join();
}

struct ThreadPool {
  uint32_t n = 0;
  std::vector<std::thread> workers;
  std::atomic<uint32_t> epoch{0};
  std::atomic<uint32_t> done{0};
  std::atomic<bool> quit{false};
  std::function<void(uint32_t)> job;

  void start(uint32_t n_) {
    stop();
    n = n_;
    quit.store(false);
    epoch.store(0);
    done.store(0);
    workers.reserve(n > 0 ? n - 1 : 0);
    for (uint32_t t = 1; t < n; t++)
      workers.emplace_back([this, t]() {
        uint32_t seen = 0;
        for (;;) {
          while (epoch.load(std::memory_order_acquire) == seen) {
            if (quit.load(std::memory_order_relaxed)) return;
            std::this_thread::yield();
          }
          seen = epoch.load(std::memory_order_acquire);
          job(t);
          done.fetch_add(1, std::memory_order_acq_rel);
          parked.fetch_add(1, std::memory_order_acq_rel);
        }
      });
  }
  std::atomic<uint32_t> parked{0};
  void run() {
    if (n <= 1) {
      job(0);
      return;
    }
    done.store(0);
    parked.store(0);
    epoch.fetch_add(1, std::memory_order_acq_rel);
    job(0);
    while (done.load(std::memory_order_acquire) < n - 1) std::this_thread::yield();
    // every worker has published its part AND is back in the spin loop before the
    // caller may reassign/tear down this job.
    while (parked.load(std::memory_order_acquire) < n - 1) std::this_thread::yield();
  }
  void stop() {
    if (workers.empty()) return;
    quit.store(true);
    epoch.fetch_add(1, std::memory_order_acq_rel);
    for (auto& w : workers) w.join();
    workers.clear();
    n = 0;
  }
  ~ThreadPool() { stop(); }
};

struct Map {
  const char* base = nullptr;
  size_t size = 0;
};
static bool map_file(const char* path, Map& m) {
  int fd = open(path, O_RDONLY);
  if (fd < 0) return false;
  struct stat st {};
  if (fstat(fd, &st) != 0) { close(fd); return false; }
  void* p = mmap(nullptr, (size_t)st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
  close(fd);
  if (p == MAP_FAILED) return false;
  m.base = (const char*)p;
  m.size = (size_t)st.st_size;
  return true;
}
static bool map_i64(const char* path, const int64_t*& p, size_t& n) {
  Map m;
  if (!map_file(path, m)) return false;
  p = (const int64_t*)m.base;
  n = m.size / 8;
  return true;
}

static std::vector<int64_t> distinct_sorted(const int64_t* a, size_t n) {
  std::vector<int64_t> v(a, a + n);
  std::sort(v.begin(), v.end());
  v.erase(std::unique(v.begin(), v.end()), v.end());
  return v;
}

struct ValueIndex {
  std::vector<int64_t> vals;
  int64_t mn = 0;
  std::vector<int32_t> tab;
  bool dense = false;
  void init(const int64_t* a, size_t n, uint32_t threads = 1) {
    int64_t lo = a[0], hi = a[0];
    if (threads > 1 && n > (1u << 20)) {
      std::vector<int64_t> los(threads, INT64_MAX), his(threads, INT64_MIN);
      parallel_ranges((uint32_t)n, threads, [&](uint32_t t, uint32_t b_, uint32_t e_) {
        int64_t l = INT64_MAX, h = INT64_MIN;
        for (uint32_t i = b_; i < e_; i++) {
          if (a[i] < l) l = a[i];
          if (a[i] > h) h = a[i];
        }
        los[t] = l;
        his[t] = h;
      });
      lo = *std::min_element(los.begin(), los.end());
      hi = *std::max_element(his.begin(), his.end());
    } else {
      for (size_t i = 1; i < n; i++) {
        if (a[i] < lo) lo = a[i];
        if (a[i] > hi) hi = a[i];
      }
    }
    const int64_t span = hi - lo + 1;
    if (span > 0 && span <= (int64_t)16 * 1024 * 1024) {
      dense = true;
      mn = lo;
      // presence marking is the O(n) part; the span walks are small (|span| <= 16M)
      std::vector<uint8_t> present((size_t)span, 0);
      if (threads > 1 && n > (1u << 20)) {
        parallel_ranges((uint32_t)n, threads, [&](uint32_t, uint32_t b_, uint32_t e_) {
          for (uint32_t i = b_; i < e_; i++) present[(size_t)(a[i] - lo)] = 1;
        });
      } else {
        for (size_t i = 0; i < n; i++) present[(size_t)(a[i] - lo)] = 1;
      }
      vals.clear();
      for (int64_t o = 0; o < span; o++)
        if (present[(size_t)o]) vals.push_back(lo + o);
      tab.assign((size_t)span, -1);
      parallel_ranges((uint32_t)vals.size(), threads, [&](uint32_t, uint32_t b_, uint32_t e_) {
        for (uint32_t k = b_; k < e_; k++) tab[(size_t)(vals[k] - lo)] = (int32_t)k;
      });
      return;
    }
    vals = distinct_sorted(a, n);
    mn = vals.empty() ? 0 : vals.front();
  }
  inline int32_t idx(int64_t v) const {
    if (dense) {
      const int64_t o = v - mn;
      if (o < 0 || (size_t)o >= tab.size()) return -1;
      return tab[(size_t)o];
    }
    auto it = std::lower_bound(vals.begin(), vals.end(), v);
    if (it == vals.end() || *it != v) return -1;
    return (int32_t)(it - vals.begin());
  }
};

static void radix_u32(uint32_t* a, uint32_t* tmp, size_t n, uint32_t maxv) {
  if (n < 32) {
    for (size_t i = 1; i < n; i++) {
      uint32_t x = a[i];
      size_t j = i;
      while (j > 0 && a[j - 1] > x) { a[j] = a[j - 1]; j--; }
      a[j] = x;
    }
    return;
  }
  int passes = 1;
  while (passes < 4 && (maxv >> (8 * passes)) != 0) passes++;
  for (int p = 0; p < passes; p++) {
    const int sh = 8 * p;
    size_t cnt[256] = {0};
    for (size_t i = 0; i < n; i++) cnt[(a[i] >> sh) & 0xFF]++;
    size_t sum = 0;
    for (int i = 0; i < 256; i++) {
      size_t c = cnt[i];
      cnt[i] = sum;
      sum += c;
    }
    for (size_t i = 0; i < n; i++) tmp[cnt[(a[i] >> sh) & 0xFF]++] = a[i];
    memcpy(a, tmp, n * sizeof(uint32_t));
  }
}

// ---------------------------------------------------------------- W fitting --
struct PlanInput {
  uint64_t n = 0;
  uint64_t runs = 0;
  uint64_t vspan = 1;
  uint32_t count_width = 2;
};
struct PlanRow {
  uint32_t w;
  uint64_t bp, planes_bytes, offs_bytes, total_bytes;
  double est_residual_rows;
};
static PlanRow plan_row(const PlanInput& in, uint32_t w) {
  PlanRow r;
  r.w = w;
  r.bp = in.vspan / w + 1;
  const uint64_t entries = (r.bp + 1) * in.runs;
  r.planes_bytes = entries * (uint64_t)(in.count_width + 8 + 2 + 2);
  r.offs_bytes = in.n * 2;
  r.total_bytes = r.planes_bytes + r.offs_bytes + 8192;
  r.est_residual_rows = in.vspan ? (double)in.n * (double)w / (double)in.vspan : 0.0;
  return r;
}
// Smallest W whose predicted footprint fits the budget: max residual resolution
// inside the storage budget.  Residual rows are monotone increasing in W.
static uint32_t fit_w(const PlanInput& in, uint64_t budget) {
  for (uint32_t w : W_CANDIDATES)
    if (plan_row(in, w).total_bytes <= budget) return w;
  return W_CANDIDATES[sizeof(W_CANDIDATES) / sizeof(W_CANDIDATES[0]) - 1];
}

// ------------------------------------------------------------ shared shaping --
struct Shape {
  size_t n = 0;
  int64_t vmin = 0, vmax = 0;
  ValueIndex gvi, ai, bi;
  bool has_null = false;
  uint32_t G0 = 0, G = 0, null_group = 0xFFFFFFFFu;
  uint32_t D1 = 0, D2 = 0, cells = 0, R = 0, count_width = 2;
  uint64_t vspan = 1, rows_per_run = 0, max_run_rows = 0;
  uint32_t mode = 0;   // 0 = value blocks, 1 = equi-depth (rank) blocks
  uint32_t RB = 64;    // rank block size (rank mode only)
  double max_value_block_share = 0;  // max share of rows in one value block
  std::vector<uint32_t> runhist;  // prefix-summed run starts, size R+1
};

static int shape_from_columns(const char* gpath, const char* vpath, const char* a1path,
                              const char* a2path, Shape& s, uint32_t threads = 1) {
  const int64_t *ga, *va, *aa, *ba;
  size_t ng, nv, na, nb;
  if (!map_i64(gpath, ga, ng) || !map_i64(vpath, va, nv) || !map_i64(a1path, aa, na) ||
      !map_i64(a2path, ba, nb)) {
    fprintf(stderr, "{\"error\":\"cannot map input columns\"}\n");
    return 1;
  }
  if (ng != nv || ng != na || ng != nb) {
    fprintf(stderr, "{\"error\":\"column length mismatch\"}\n");
    return 1;
  }
  if (ng == 0 || ng > 0xFFFFFFFFull) {
    fprintf(stderr, "{\"error\":\"empty input or >4B rows\"}\n");
    return 1;
  }
  s.n = ng;
  const uint32_t T = threads;
  {
    std::vector<int64_t> vmn(T, INT64_MAX), vmx(T, INT64_MIN);
    std::vector<uint32_t> nnull(T, 0), npos(T, 0);
    parallel_ranges((uint32_t)ng, T, [&](uint32_t t, uint32_t a, uint32_t b) {
      int64_t l = INT64_MAX, h = INT64_MIN;
      uint32_t nn = 0, np = 0;
      for (uint32_t i = a; i < b; i++) {
        if (va[i] < l) l = va[i];
        if (va[i] > h) h = va[i];
        if (ga[i] < 0) nn++;
        else np++;
      }
      vmn[t] = l; vmx[t] = h; nnull[t] = nn; npos[t] = np;
    });
    s.vmin = *std::min_element(vmn.begin(), vmn.end());
    s.vmax = *std::max_element(vmx.begin(), vmx.end());
    uint32_t any_null = 0;
    for (uint32_t x : nnull) any_null += x;
    s.has_null = any_null > 0;
    std::vector<uint32_t> off(T + 1, 0);
    for (uint32_t t = 0; t < T; t++) off[t + 1] = off[t] + npos[t];
    std::vector<int64_t> gpos(off[T] + 1);
    parallel_ranges((uint32_t)ng, T, [&](uint32_t t, uint32_t a, uint32_t b) {
      uint32_t w = off[t];
      for (uint32_t i = a; i < b; i++)
        if (ga[i] >= 0) gpos[w++] = ga[i];
    });
    if (!gpos.empty()) s.gvi.init(gpos.data(), gpos.size(), T);
  }
  s.G0 = (uint32_t)s.gvi.vals.size();
  s.G = s.G0 + (s.has_null ? 1u : 0u);
  s.null_group = s.has_null ? s.G - 1 : 0xFFFFFFFFu;
  s.ai.init(aa, ng, T);
  s.bi.init(ba, ng, T);
  s.D1 = (uint32_t)s.ai.vals.size();
  s.D2 = (uint32_t)s.bi.vals.size();
  s.cells = s.D1 * s.D2;
  s.R = s.G * s.cells;
  s.vspan = (uint64_t)(s.vmax - s.vmin) + 1;
  s.rows_per_run = s.R ? s.n / s.R : s.n;
  // run histogram: the uint16 count plane is only safe when the *largest* run
  // fits (a skewed dataset can have a 8 M-row run even with a 5 k-row average).
  s.runhist.assign((size_t)s.R + 1, 0);
  if (T > 1 && (uint64_t)T * s.R * 4 <= (256ull << 20)) {
    std::vector<std::vector<uint32_t>> h(T, std::vector<uint32_t>(s.R, 0));
    parallel_ranges((uint32_t)ng, T, [&](uint32_t t, uint32_t a, uint32_t b) {
      auto& hh = h[t];
      for (uint32_t i = a; i < b; i++) {
        const uint32_t gi = (ga[i] < 0) ? s.null_group : (uint32_t)s.gvi.idx(ga[i]);
        const uint32_t r = gi * s.cells + (uint32_t)s.ai.idx(aa[i]) * s.D2 +
                           (uint32_t)s.bi.idx(ba[i]);
        hh[r]++;
      }
    });
    parallel_ranges(s.R, T, [&](uint32_t, uint32_t a, uint32_t b) {
      for (uint32_t r = a; r < b; r++) {
        uint32_t v = 0;
        for (uint32_t t = 0; t < T; t++) v += h[t][r];
        s.runhist[(size_t)r + 1] = v;
      }
    });
  } else {
    for (size_t i = 0; i < ng; i++) {
      const uint32_t gi = (ga[i] < 0) ? s.null_group : (uint32_t)s.gvi.idx(ga[i]);
      const uint32_t r = gi * s.cells + (uint32_t)s.ai.idx(aa[i]) * s.D2 +
                         (uint32_t)s.bi.idx(ba[i]);
      s.runhist[(size_t)r + 1]++;
    }
  }
  for (uint32_t r = 0; r < s.R; r++) s.runhist[(size_t)r + 1] += s.runhist[r];
  for (uint32_t r = 0; r < s.R; r++)
    s.max_run_rows = std::max<uint64_t>(s.max_run_rows,
                                        s.runhist[(size_t)r + 1] - s.runhist[r]);
  s.count_width = (s.max_run_rows <= 65535) ? 2u : 4u;
  // Skew probe: the worst-case residual of value-mode is ~2 * (rows in the
  // fullest value block), so a single over-full block (NYC fares: 95% of the
  // rows sit inside a $655 window) makes value blocks unusable.  Sample the
  // column and compare the fullest block at a moderate width against the
  // uniform expectation; switch to equi-depth blocks when it is far above.
  {
    const uint32_t wprobe = (uint32_t)std::max<uint64_t>(1, s.vspan / 1024);
    const size_t nprobe = (size_t)(s.vspan / wprobe) + 2;
    const size_t stride = std::max<size_t>(1, ng / 200000);
    const size_t nitems = (ng + stride - 1) / stride;
    std::vector<std::vector<uint32_t>> pr(T, std::vector<uint32_t>(nprobe, 0));
    std::vector<uint64_t> taken(T, 0);
    parallel_ranges((uint32_t)nitems, T, [&](uint32_t t, uint32_t a, uint32_t b) {
      auto& pp = pr[t];
      uint64_t cnt = 0;
      for (uint32_t k = a; k < b; k++) {
        const size_t i = (size_t)k * stride;
        pp[(size_t)((uint64_t)(va[i] - s.vmin) / wprobe)]++;
        cnt++;
      }
      taken[t] = cnt;
    });
    std::vector<uint32_t> probe(nprobe, 0);
    for (uint32_t t = 0; t < T; t++)
      for (size_t j = 0; j < nprobe; j++) probe[j] += pr[t][j];
    uint64_t take = 0;
    for (uint64_t c : taken) take += c;
    uint32_t mx = 1;
    for (uint32_t c : probe) mx = std::max(mx, c);
    s.max_value_block_share = take ? (double)mx / (double)take : 0.0;
    // uniform share at this probe width is 1/1024 ~= 0.1%; >4% means heavy skew
    s.mode = (s.max_value_block_share > 0.04) ? 1u : 0u;
    s.RB = 64;
  }
  if (s.G == 0 || s.R == 0) {
    fprintf(stderr, "{\"error\":\"empty domains\"}\n");
    return 1;
  }
  if (s.G > MAX_GROUPS) {
    fprintf(stderr, "{\"error\":\"group count %u exceeds MAX_GROUPS %u\"}\n", s.G, MAX_GROUPS);
    return 1;
  }
  return 0;
}

// ---- equi-depth (rank) blocks: robust to heavy value skew ---------------------
// Value blocks are wonderful when the ordered column is not concentrated (residual
// ~n/BP rows), but ONE over-full value block (NYC fares: 95% of the rows inside a
// $655 window) makes the residual O(n).  Rank blocks bound the residual to
// 2*RB rows per run by construction.  The number of blocks per run is capped at
// BPr, so each run uses RB_r = ceil(len_r / BPr) rows per block; that keeps the
// plane array at (BPr+1)*R entries even when the run sizes are wildly unequal
// (NYC: one run holds 61 M of the 101 M rows).
static constexpr uint32_t BPR_CANDIDATES[] = {512, 1024, 2048, 4096, 8192, 16384, 32768};

static inline uint32_t rb_for(uint32_t len, uint32_t BPr) {
  return BPr ? (uint32_t)(((uint64_t)len + BPr - 1) / BPr) : 1u;
}
static uint64_t rank_bytes(const Shape& s, uint32_t BPr) {
  const uint64_t entries = (uint64_t)(BPr + 1) * s.R;
  return entries * (uint64_t)(s.count_width + 8 + 4 + 4 + 4) + (uint64_t)s.n * 2 + 8192;
}
static uint32_t fit_bp(const Shape& s, uint64_t budget, uint64_t* bytes_out) {
  for (uint32_t bp : BPR_CANDIDATES) {
    const uint64_t b = rank_bytes(s, bp);
    if (budget && b <= budget) {
      if (bytes_out) *bytes_out = b;
      return bp;
    }
  }
  // the unavoidable 2 B/row residual already exceeds the budget (a highly
  // compressible projected source such as NYC): go latency-first and report it
  const uint32_t bp = 4096;
  if (bytes_out) *bytes_out = rank_bytes(s, bp);
  return bp;
}

static int cmd_plan(int argc, char** argv) {
  if (argc < 7) {
    fprintf(stderr, "usage: mec plan <g.i64> <v.i64> <c1.i64> <c2.i64> <budget_bytes>\n");
    return 2;
  }
  Shape s;
  const uint32_t threads = parse_threads(argc, argv);
  if (shape_from_columns(argv[2], argv[3], argv[4], argv[5], s, threads)) return 1;
  const uint64_t budget = strtoull(argv[6], nullptr, 10);
  PlanInput in;
  in.n = s.n;
  in.runs = s.R;
  in.vspan = s.vspan;
  in.count_width = s.count_width;
  if (s.mode == 1) {
    printf("{\"mode\":\"rank\",\"rows\":%llu,\"G\":%u,\"has_null\":%s,\"D1\":%u,\"D2\":%u,"
           "\"runs\":%u,\"rows_per_run\":%llu,\"max_run_rows\":%llu,\"count_width\":%u,"
           "\"vspan\":%llu,\"budget_bytes\":%llu,\"skew_share\":%.4f,\"rule\":"
           "\"value blocks would put >4%% of the rows in one block, so equi-depth (rank) blocks "
           "are used: BPr = smallest max-blocks-per-run whose predicted footprint fits the "
           "budget (else 4096 latency-first), RB_r = ceil(len_r/BPr) per run\","
           "\"candidates\":[",
           (unsigned long long)s.n, s.G, s.has_null ? "true" : "false", s.D1, s.D2, s.R,
           (unsigned long long)s.rows_per_run, (unsigned long long)s.max_run_rows,
           s.count_width, (unsigned long long)s.vspan, (unsigned long long)budget,
           s.max_value_block_share);
    for (size_t i = 0; i < sizeof(BPR_CANDIDATES) / sizeof(BPR_CANDIDATES[0]); i++) {
      const uint32_t bp = BPR_CANDIDATES[i];
      const uint64_t b = rank_bytes(s, bp);
      printf("%s{\"BP\":%u,\"max_RB\":%u,\"total_bytes\":%llu,\"est_residual_rows\":%llu,"
             "\"fits\":%s}", i ? "," : "", bp, rb_for((uint32_t)s.max_run_rows, bp),
             (unsigned long long)b, 2ull * s.n / bp, b <= budget ? "true" : "false");
    }
    uint64_t pb = 0;
    const uint32_t pick = fit_bp(s, budget, &pb);
    printf("],\"picked_BP\":%u,\"picked_bytes\":%llu}\n", pick, (unsigned long long)pb);
    return 0;
  }
  printf("{\"mode\":\"value\",\"rows\":%llu,\"G\":%u,\"has_null\":%s,\"D1\":%u,\"D2\":%u,\"runs\":%u,"
         "\"rows_per_run\":%llu,\"count_width\":%u,\"vspan\":%llu,\"budget_bytes\":%llu,"
         "\"candidates\":[",
         (unsigned long long)s.n, s.G, s.has_null ? "true" : "false", s.D1, s.D2, s.R,
         (unsigned long long)s.rows_per_run, s.count_width, (unsigned long long)s.vspan,
         (unsigned long long)budget);
  for (size_t i = 0; i < sizeof(W_CANDIDATES) / sizeof(W_CANDIDATES[0]); i++) {
    PlanRow r = plan_row(in, W_CANDIDATES[i]);
    printf("%s{\"W\":%u,\"BP\":%llu,\"planes_bytes\":%llu,\"offs_bytes\":%llu,"
           "\"total_bytes\":%llu,\"est_residual_rows\":%.0f,\"fits\":%s}",
           i ? "," : "", r.w, (unsigned long long)r.bp, (unsigned long long)r.planes_bytes,
           (unsigned long long)r.offs_bytes, (unsigned long long)r.total_bytes,
           r.est_residual_rows, r.total_bytes <= budget ? "true" : "false");
  }
  const uint32_t pick = fit_w(in, budget);
  PlanRow pr = plan_row(in, pick);
  printf("],\"picked_W\":%u,\"picked_BP\":%llu,\"picked_bytes\":%llu,"
         "\"rule\":\"smallest W (power of two, <=65536) whose predicted footprint fits the "
         "budget; residual rows per query are monotone increasing in W\"}\n",
         pick, (unsigned long long)pr.bp, (unsigned long long)pr.total_bytes);
  return 0;
}

// --------------------------------------------------------------------- build --
static int cmd_build(int argc, char** argv) {
  if (argc < 7) {
    fprintf(stderr, "usage: mec build <g.i64> <v.i64> <c1.i64> <c2.i64> <out> [W] [budget]\n");
    return 2;
  }
  auto t0 = std::chrono::steady_clock::now();
  const uint32_t threads = parse_threads(argc, argv);
  Shape s;
  if (shape_from_columns(argv[2], argv[3], argv[4], argv[5], s, threads)) return 1;
  const int64_t *ga, *va, *aa, *ba;
  size_t ng, nv, na, nb;
  map_i64(argv[2], ga, ng);
  map_i64(argv[3], va, nv);
  map_i64(argv[4], aa, na);
  map_i64(argv[5], ba, nb);
  const size_t n = s.n;
  const uint32_t G = s.G, G0 = s.G0, D1 = s.D1, D2 = s.D2, cells = s.cells, R = s.R;
  const uint32_t count_width = s.count_width, null_group = s.null_group;
  const bool has_null = s.has_null;
  const int64_t vmin = s.vmin, vmax = s.vmax;
  const uint64_t vspan = s.vspan;
  auto t_scan = std::chrono::steady_clock::now();

  PlanInput pin;
  pin.n = n;
  pin.runs = R;
  pin.vspan = vspan;
  pin.count_width = count_width;
  uint64_t budget = (argc >= 9) ? strtoull(argv[8], nullptr, 10) : 0;
  uint32_t mode = s.mode;
  if (argc >= 10) {
    const std::string a = argv[9];
    if (a == "0") mode = 0;
    else if (a == "1") mode = 1;
  }
  uint32_t W;
  const char* wsource = "explicit";
  if (argc >= 8 && strtoull(argv[7], nullptr, 10) > 0) {
    W = (uint32_t)strtoull(argv[7], nullptr, 10);
    if (W < 512 || W > 65536) {
      fprintf(stderr, "{\"error\":\"W must be in [512,65536] (uint16 in-block offset)\"}\n");
      return 1;
    }
  } else if (budget > 0) {
    W = fit_w(pin, budget);
    wsource = "auto_fit";
  } else {
    W = 65536;
    wsource = "widest_default";
  }
  const uint32_t BP = (uint32_t)(vspan / W) + 1;

  // 1) counting sort rows by run id (histogram already built by shape_from_columns)
  std::vector<uint32_t> runbase(s.runhist);
  std::vector<uint32_t> vrun(n);
  const size_t maxlen = (size_t)std::max<uint64_t>(1, s.max_run_rows);
  const size_t scratch = (size_t)threads * R * 4 * 2;
  if (threads > 1 && scratch <= (256ull << 20)) {
    // two-phase deterministic scatter: per-thread run counts, per-run prefix, then
    // each thread writes its own row chunk into disjoint run ranges.
    std::vector<std::vector<uint32_t>> tcnt(threads, std::vector<uint32_t>(R, 0));
    parallel_ranges((uint32_t)n, threads, [&](uint32_t t, uint32_t a, uint32_t b) {
      auto& c = tcnt[t];
      for (uint32_t i = a; i < b; i++) {
        const uint32_t gi = (ga[i] < 0) ? null_group : (uint32_t)s.gvi.idx(ga[i]);
        const uint32_t r = gi * cells + (uint32_t)s.ai.idx(aa[i]) * D2 +
                           (uint32_t)s.bi.idx(ba[i]);
        c[r]++;
      }
    });
    std::vector<std::vector<uint32_t>> tcur(threads, std::vector<uint32_t>(R, 0));
    parallel_ranges(R, threads, [&](uint32_t, uint32_t a, uint32_t b) {
      for (uint32_t r = a; r < b; r++) {
        uint32_t acc = runbase[r];
        for (uint32_t tt = 0; tt < threads; tt++) {
          tcur[tt][r] = acc;
          acc += tcnt[tt][r];
        }
      }
    });
    parallel_ranges((uint32_t)n, threads, [&](uint32_t t, uint32_t a, uint32_t b) {
      auto& cur = tcur[t];
      for (uint32_t i = a; i < b; i++) {
        const uint32_t gi = (ga[i] < 0) ? null_group : (uint32_t)s.gvi.idx(ga[i]);
        const uint32_t r = gi * cells + (uint32_t)s.ai.idx(aa[i]) * D2 +
                           (uint32_t)s.bi.idx(ba[i]);
        vrun[cur[r]++] = (uint32_t)(va[i] - vmin);
      }
    });
  } else {
    std::vector<uint32_t> cursor(runbase.begin(), runbase.end() - 1);
    for (size_t i = 0; i < n; i++) {
      const uint32_t gi = (ga[i] < 0) ? null_group : (uint32_t)s.gvi.idx(ga[i]);
      const uint32_t r = gi * cells + (uint32_t)s.ai.idx(aa[i]) * D2 + (uint32_t)s.bi.idx(ba[i]);
      vrun[cursor[r]++] = (uint32_t)(va[i] - vmin);
    }
  }
  auto t_sort1 = std::chrono::steady_clock::now();

  // 2) sort each run's values (independent per run)
  if (threads > 1) {
    // size each thread's radix scratch by the largest run in ITS chunk, not by the
    // global max: one 61 M-row run would otherwise allocate 244 MB per thread.
    std::vector<std::vector<uint32_t>> tmps(threads);
    parallel_ranges(R, threads, [&](uint32_t t, uint32_t a, uint32_t b) {
      size_t m = 1;
      for (uint32_t r = a; r < b; r++)
        m = std::max(m, (size_t)(runbase[(size_t)r + 1] - runbase[r]));
      tmps[t].assign(m + 8, 0);
      for (uint32_t r = a; r < b; r++) {
        const size_t st = runbase[r], en = runbase[(size_t)r + 1];
        radix_u32(vrun.data() + st, tmps[t].data(), en - st, (uint32_t)vspan);
      }
      std::vector<uint32_t>().swap(tmps[t]);
    });
  } else {
    std::vector<uint32_t> tmp(maxlen + 8);
    for (uint32_t r = 0; r < R; r++) {
      const size_t st = runbase[r], en = runbase[(size_t)r + 1];
      radix_u32(vrun.data() + st, tmp.data(), en - st, (uint32_t)vspan);
    }
  }
  auto t_sort2 = std::chrono::steady_clock::now();

  // ---- equi-depth (rank) mode -------------------------------------------
  if (mode == 1) {
    uint64_t pred = 0;
    const uint32_t BPr = fit_bp(s, budget, &pred);
    const uint32_t Wv = 65536;
    std::vector<uint32_t> rb_of(R, 1);
    for (uint32_t r = 0; r < R; r++)
      rb_of[r] = std::max(1u, rb_for(runbase[(size_t)r + 1] - runbase[r], BPr));
    // pass 1: how many blocks does each run need (break on rank target and on a
    // >65535 value span, which is what keeps the uint16 in-block offset exact)
    std::vector<uint32_t> nb_of(R, 0);
    std::vector<uint32_t> nb_max(threads > 1 ? threads : 1, 0);
    parallel_ranges(R, threads, [&](uint32_t t, uint32_t a, uint32_t b) {
      uint32_t m = 0;
      for (uint32_t r = a; r < b; r++) {
        const uint32_t st = runbase[r], len = runbase[(size_t)r + 1] - st;
        const uint32_t* vr = vrun.data() + st;
        const uint32_t rb = rb_of[r];
        uint32_t nb = 0;
        size_t i = 0;
        while (i < (size_t)len) {
          const uint32_t vbase = vr[i];
          const size_t lim = std::min((size_t)len, i + rb);
          i++;
          while (i < lim && (uint64_t)vr[i] - (uint64_t)vbase <= 65535ULL) i++;
          nb++;
        }
        nb_of[r] = nb;
        if (nb > m) m = nb;
      }
      nb_max[t] = m;
    });
    uint32_t BPa = 1;
    for (uint32_t m : nb_max) BPa = std::max(BPa, m);
    const size_t planesr = (size_t)(BPa + 1) * R;
    std::vector<uint16_t> cumc16(count_width == 2 ? planesr : 0, 0);
    std::vector<uint32_t> cumc32(count_width == 4 ? planesr : 0, 0);
    std::vector<uint64_t> cumsum(planesr, 0);
    std::vector<uint32_t> firstp(planesr, 0), lastp(planesr, 0);
    std::vector<uint16_t> offs(n, 0);
    std::vector<uint32_t> bnd((size_t)(BPa + 1) * R, 0xFFFFFFFFu);   // run-major: block first v
    std::vector<uint32_t> bend((size_t)(BPa + 1) * R, 0xFFFFFFFFu);  // run-major: block last v
    parallel_ranges(R, threads, [&](uint32_t, uint32_t ra, uint32_t rb_end) {
    for (uint32_t r = ra; r < rb_end; r++) {
      const uint32_t g = r / cells, c = r % cells;
      const uint32_t st = runbase[r], len = runbase[(size_t)r + 1] - st;
      const uint32_t plane_off = g * cells + c;
      uint32_t* bk = bnd.data() + (size_t)r * (BPa + 1);
      uint32_t* be = bend.data() + (size_t)r * (BPa + 1);
      if (!len) {
        for (uint32_t bb = 0; bb <= BPa; bb++) {
          const size_t idx = (size_t)bb * R + plane_off;
          if (count_width == 2) cumc16[idx] = 0; else cumc32[idx] = 0;
          cumsum[idx] = 0;
          firstp[idx] = 0;
          lastp[idx] = 0;
        }
        continue;
      }
      const uint32_t* vr = vrun.data() + st;
      uint16_t* oc = offs.data() + st;
      for (uint32_t i = 0; i < len; i++) oc[i] = (uint16_t)(vr[i] & (Wv - 1));
      const uint32_t rb = rb_of[r];
      uint32_t b = 0, cnt = 0;
      uint64_t sum = 0;
      size_t i = 0;
      while (i < (size_t)len) {
        const size_t i0 = i;
        const uint32_t vbase = vr[i0];
        const size_t lim = std::min((size_t)len, i0 + rb);
        i++;
        while (i < lim && (uint64_t)vr[i] - (uint64_t)vbase <= 65535ULL) i++;
        const size_t i1 = i;
        const size_t idx = (size_t)b * R + plane_off;
        if (count_width == 2) cumc16[idx] = (uint16_t)cnt; else cumc32[idx] = cnt;
        cumsum[idx] = sum;
        firstp[idx] = vbase;
        lastp[idx] = vr[i1 - 1];
        bk[b] = vbase;
        be[b] = vr[i1 - 1];
        for (size_t k = i0; k < i1; k++) sum += (uint64_t)((int64_t)vrun[st + k] + vmin);
        cnt += (uint32_t)(i1 - i0);
        b++;
      }
      for (uint32_t bb = b; bb <= BPa; bb++) {
        const size_t idx = (size_t)bb * R + plane_off;
        if (count_width == 2) cumc16[idx] = (uint16_t)cnt; else cumc32[idx] = cnt;
        cumsum[idx] = sum;
      }
    }
    });
    Header hr;
    memset(&hr, 0, sizeof(hr));
    memcpy(hr.magic, "MEC02\0\0\0", 8);
    hr.n = n;
    hr.vmin = vmin;
    hr.vmax = vmax;
    hr.G = G;
    hr.G0 = G0;
    hr.D1 = D1;
    hr.D2 = D2;
    hr.cells = cells;
    hr.W = 0;
    hr.BP = BPa;
    hr.count_width = count_width;
    hr.has_null = has_null ? 1u : 0u;
    hr.null_group = null_group;
    hr.pad0 = 1u | (BPr << 1);
    size_t posr = sizeof(Header);
    hr.off_g = posr; posr = align64(posr + (size_t)G0 * 8);
    hr.off_a = posr; posr = align64(posr + (size_t)D1 * 8);
    hr.off_b = posr; posr = align64(posr + (size_t)D2 * 8);
    hr.off_runbase = posr; posr = align64(posr + ((size_t)R + 1) * 4);
    hr.off_cumc = posr; posr = align64(posr + planesr * count_width);
    hr.off_cumsum = posr; posr = align64(posr + planesr * 8);
    hr.off_first = posr; posr = align64(posr + planesr * 4);
    hr.off_last = posr; posr = align64(posr + planesr * 4);
    hr.off_offs = posr; posr = align64(posr + n * 2);
    const size_t off_bnd = posr;   // two run-major arrays: block first v, block last v
    posr += 2 * (size_t)(BPa + 1) * R * 4;
    FILE* fr = fopen(argv[6], "wb");
    if (!fr) { fprintf(stderr, "{\"error\":\"cannot create output\"}\n"); return 1; }
    auto padr = [&](size_t at) {
      long cur2 = ftell(fr);
      if (cur2 >= 0 && (size_t)cur2 < at) {
        static const char zz[64] = {0};
        fwrite(zz, 1, at - (size_t)cur2, fr);
      }
    };
    fwrite(&hr, sizeof(hr), 1, fr);
    padr(hr.off_g);
    fwrite(s.gvi.vals.data(), 8, G0, fr);
    padr(hr.off_a);
    fwrite(s.ai.vals.data(), 8, D1, fr);
    padr(hr.off_b);
    fwrite(s.bi.vals.data(), 8, D2, fr);
    padr(hr.off_runbase);
    fwrite(runbase.data(), 4, (size_t)R + 1, fr);
    padr(hr.off_cumc);
    if (count_width == 2) fwrite(cumc16.data(), 2, planesr, fr);
    else fwrite(cumc32.data(), 4, planesr, fr);
    padr(hr.off_cumsum);
    fwrite(cumsum.data(), 8, planesr, fr);
    padr(hr.off_first);
    fwrite(firstp.data(), 4, planesr, fr);
    padr(hr.off_last);
    fwrite(lastp.data(), 4, planesr, fr);
    padr(hr.off_offs);
    fwrite(offs.data(), 2, n, fr);
    padr(off_bnd);
    fwrite(bnd.data(), 4, (size_t)(BPa + 1) * R, fr);
    fwrite(bend.data(), 4, (size_t)(BPa + 1) * R, fr);
    fclose(fr);
    auto t1r = std::chrono::steady_clock::now();
    fprintf(stderr,
            "{\"build_ms\":%.3f,\"bytes\":%llu,\"rows\":%llu,\"G\":%u,\"G0\":%u,"
            "\"has_null\":%s,\"D1\":%u,\"D2\":%u,\"cells\":%u,\"runs\":%u,"
            "\"rows_per_run\":%llu,\"max_run_rows\":%llu,\"mode\":\"rank\",\"BP\":%u,"
            "\"BPcap\":%u,\"count_width\":%u,\"w_source\":\"rank_fit\","
            "\"predicted_bytes\":%llu,\"budget_bytes\":%llu,\"skew_share\":%.4f,"
            "\"threads\":%u}\n",
            ms_between(t0, t1r), (unsigned long long)posr, (unsigned long long)n, G, G0,
            has_null ? "true" : "false", D1, D2, cells, R,
            (unsigned long long)s.rows_per_run, (unsigned long long)s.max_run_rows, BPa, BPr,
            count_width, (unsigned long long)pred, (unsigned long long)budget,
            s.max_value_block_share, threads);
    return 0;
  }

  // 3) planes
  const size_t planes = (size_t)(BP + 1) * R;
  std::vector<uint16_t> cumc16(count_width == 2 ? planes : 0, 0);
  std::vector<uint32_t> cumc32(count_width == 4 ? planes : 0, 0);
  std::vector<uint64_t> cumsum(planes, 0);
  std::vector<uint16_t> firstoff(planes, 0), lastoff(planes, 0);
  std::vector<uint16_t> offs(n, 0);
  parallel_ranges(R, threads, [&](uint32_t, uint32_t ra, uint32_t rb_end) {
  std::vector<uint32_t> cb(BP, 0);
  std::vector<uint64_t> sb(BP, 0);
  for (uint32_t r = ra; r < rb_end; r++) {
    const uint32_t g = r / cells, c = r % cells;
    const size_t st = runbase[r], en = runbase[(size_t)r + 1];
    const size_t len = en - st;
    const uint32_t plane_off = g * cells + c;
    if (len) {
      const uint32_t* vr = vrun.data() + st;
      uint16_t* oc = offs.data() + st;
      for (size_t i = 0; i < len; i++) {
        const uint32_t v = vr[i];
        const uint32_t b = v / W;
        oc[i] = (uint16_t)(v - b * W);
        cb[b]++;
        sb[b] += (uint64_t)((int64_t)v + vmin);
        if (cb[b] == 1) firstoff[(size_t)b * R + plane_off] = oc[i];
        lastoff[(size_t)b * R + plane_off] = oc[i];
      }
      uint32_t cnt = 0;
      uint64_t sum = 0;
      for (uint32_t b = 0; b < BP; b++) {
        const size_t idx = (size_t)b * R + plane_off;
        if (count_width == 2) cumc16[idx] = (uint16_t)cnt; else cumc32[idx] = cnt;
        cumsum[idx] = sum;
        cnt += cb[b];
        sum += sb[b];
      }
      const size_t last = (size_t)BP * R + plane_off;
      if (count_width == 2) cumc16[last] = (uint16_t)cnt; else cumc32[last] = cnt;
      cumsum[last] = sum;
      for (uint32_t b = 0; b < BP; b++) { cb[b] = 0; sb[b] = 0; }
    } else {
      for (uint32_t b = 0; b <= BP; b++) {
        const size_t idx = (size_t)b * R + plane_off;
        if (count_width == 2) cumc16[idx] = 0; else cumc32[idx] = 0;
        cumsum[idx] = 0;
        firstoff[idx] = 0;
        lastoff[idx] = 0;
      }
    }
  }
  });
  auto t_grid = std::chrono::steady_clock::now();

  // 4) write
  Header h;
  memset(&h, 0, sizeof(h));
  memcpy(h.magic, "MEC02\0\0\0", 8);
  h.n = n;
  h.vmin = vmin;
  h.vmax = vmax;
  h.G = G;
  h.G0 = G0;
  h.D1 = D1;
  h.D2 = D2;
  h.cells = cells;
  h.W = W;
  h.BP = BP;
  h.count_width = count_width;
  h.has_null = has_null ? 1u : 0u;
  h.null_group = null_group;
  size_t pos = sizeof(Header);
  h.off_g = pos; pos = align64(pos + (size_t)G0 * 8);
  h.off_a = pos; pos = align64(pos + (size_t)D1 * 8);
  h.off_b = pos; pos = align64(pos + (size_t)D2 * 8);
  h.off_runbase = pos; pos = align64(pos + ((size_t)R + 1) * 4);
  h.off_cumc = pos; pos = align64(pos + planes * count_width);
  h.off_cumsum = pos; pos = align64(pos + planes * 8);
  h.off_first = pos; pos = align64(pos + planes * 2);
  h.off_last = pos; pos = align64(pos + planes * 2);
  h.off_offs = pos; pos += n * 2;
  const size_t total = pos;

  FILE* f = fopen(argv[6], "wb");
  if (!f) {
    fprintf(stderr, "{\"error\":\"cannot create output\"}\n");
    return 1;
  }
  auto pad_to = [&](size_t at) {
    long cur = ftell(f);
    if (cur >= 0 && (size_t)cur < at) {
      static const char z[64] = {0};
      fwrite(z, 1, at - (size_t)cur, f);
    }
  };
  fwrite(&h, sizeof(h), 1, f);
  pad_to(h.off_g);
  fwrite(s.gvi.vals.data(), 8, G0, f);
  pad_to(h.off_a);
  fwrite(s.ai.vals.data(), 8, D1, f);
  pad_to(h.off_b);
  fwrite(s.bi.vals.data(), 8, D2, f);
  pad_to(h.off_runbase);
  fwrite(runbase.data(), 4, (size_t)R + 1, f);
  pad_to(h.off_cumc);
  if (count_width == 2) fwrite(cumc16.data(), 2, planes, f);
  else fwrite(cumc32.data(), 4, planes, f);
  pad_to(h.off_cumsum);
  fwrite(cumsum.data(), 8, planes, f);
  pad_to(h.off_first);
  fwrite(firstoff.data(), 2, planes, f);
  pad_to(h.off_last);
  fwrite(lastoff.data(), 2, planes, f);
  pad_to(h.off_offs);
  fwrite(offs.data(), 2, n, f);
  fclose(f);
  auto t1 = std::chrono::steady_clock::now();
  fprintf(stderr,
          "{\"build_ms\":%.3f,\"bytes\":%llu,\"rows\":%llu,\"G\":%u,\"G0\":%u,\"has_null\":%s,"
          "\"D1\":%u,\"D2\":%u,\"cells\":%u,\"runs\":%u,\"rows_per_run\":%llu,"
          "\"max_run_rows\":%llu,\"W\":%u,"
          "\"BP\":%u,\"count_width\":%u,\"w_source\":\"%s\",\"budget_bytes\":%llu,"
          "\"phase_ms\":{\"shape\":%.3f,\"counting_sort\":%.3f,\"run_radix\":%.3f,"
          "\"grid\":%.3f,\"write\":%.3f},\"threads\":%u}\n",
          ms_between(t0, t1), (unsigned long long)total, (unsigned long long)n, G, G0,
          has_null ? "true" : "false", D1, D2, cells, R, (unsigned long long)s.rows_per_run,
          (unsigned long long)s.max_run_rows, W, BP, count_width, wsource,
          (unsigned long long)budget, ms_between(t0, t_scan), ms_between(t_scan, t_sort1),
          ms_between(t_sort1, t_sort2), ms_between(t_sort2, t_grid), ms_between(t_grid, t1),
          threads);
  return 0;
}

// ------------------------------------------------------------------- query --
struct QOut {
  uint64_t cnt[MAX_GROUPS];
  int64_t sum[MAX_GROUPS];
  int64_t mn[MAX_GROUPS];
  int64_t mx[MAX_GROUPS];
  uint8_t selected[MAX_GROUPS];
};

struct GroupFilter {
  std::vector<int64_t> in, notin;
  bool any() const { return !in.empty() || !notin.empty(); }
  bool selects(int64_t value) const {
    if (!in.empty()) return std::find(in.begin(), in.end(), value) != in.end();
    return std::find(notin.begin(), notin.end(), value) == notin.end();
  }
};

template <typename CT>
static inline uint32_t find_first_block(const CT* cumc, size_t R, size_t plane_off, uint32_t lo,
                                        uint32_t hi, uint32_t rowidx) {
  while (lo < hi) {
    const uint32_t mid = lo + (hi - lo) / 2;
    if ((uint32_t)cumc[(size_t)(mid + 1) * R + plane_off] > rowidx) hi = mid; else lo = mid + 1;
  }
  return lo;
}

// v-offset recovery inside a rank block: offs stores (v mod 65536); blocks are cut
// so that their span is < 65536, so the block's first v-offset pins the base.
static inline uint32_t rank_recover_v(uint32_t base_v, uint16_t off) {
  uint32_t v = (base_v & ~65535u) + (uint32_t)off;
  if (v < base_v) v += 65536u;
  return v;
}

template <typename CT, bool MINMAX>
static void mec_query_rank(const Map& idx, int64_t vlo, int64_t vhi, int64_t a_lo, int64_t a_hi,
                           int64_t b_lo, int64_t b_hi, const GroupFilter& gf, QOut& out,
                           uint32_t c_lo, uint32_t c_hi) {
  const Header& h = *(const Header*)idx.base;
  const int64_t* gv = (const int64_t*)(idx.base + h.off_g);
  const int64_t* av = (const int64_t*)(idx.base + h.off_a);
  const int64_t* bv = (const int64_t*)(idx.base + h.off_b);
  const uint32_t* runbase = (const uint32_t*)(idx.base + h.off_runbase);
  const CT* cumc = (const CT*)(idx.base + h.off_cumc);
  const uint64_t* cumsum = (const uint64_t*)(idx.base + h.off_cumsum);
  const uint32_t* firstp = (const uint32_t*)(idx.base + h.off_first);
  const uint32_t* lastp = (const uint32_t*)(idx.base + h.off_last);
  const uint16_t* offs = (const uint16_t*)(idx.base + h.off_offs);
  const uint32_t* bnd = (const uint32_t*)(idx.base + align64(h.off_offs + (size_t)h.n * 2));
  const uint32_t G = h.G, D2 = h.D2, cells = h.cells, BP = h.BP;
  const size_t R = (size_t)G * cells;
  const int64_t vmin = h.vmin;
  const bool has_null = h.has_null != 0;
  const uint32_t null_group = h.null_group;

  for (uint32_t g = 0; g < G; g++) {
    out.cnt[g] = 0;
    out.sum[g] = 0;
    out.mn[g] = INT64_MAX;
    out.mx[g] = INT64_MIN;
    out.selected[g] = 1;
    if (gf.any()) {
      if (has_null && g == null_group) out.selected[g] = 0;
      else out.selected[g] = gf.selects(gv[g]) ? 1 : 0;
    }
  }
  if (vlo > vhi || vhi < h.vmin || vlo > h.vmax) return;
  const uint32_t a0 = (uint32_t)(std::lower_bound(av, av + h.D1, a_lo) - av);
  const uint32_t a1 = (uint32_t)(std::upper_bound(av, av + h.D1, a_hi) - av);
  const uint32_t b0 = (uint32_t)(std::lower_bound(bv, bv + D2, b_lo) - bv);
  const uint32_t b1i = (uint32_t)(std::upper_bound(bv, bv + D2, b_hi) - bv);
  if (a0 >= a1 || b0 >= b1i) return;
  const uint32_t o_lo = (vlo <= vmin) ? 0u : (uint32_t)(vlo - vmin);
  const uint32_t o_hi =
      (vhi >= h.vmax) ? (uint32_t)(h.vmax - vmin) : (uint32_t)(vhi - vmin);

  for (uint32_t g = 0; g < G; g++) {
    if (!out.selected[g]) continue;
    const uint32_t gbase = g * cells;
    uint64_t cnt = 0;
    int64_t sum = 0, mn = INT64_MAX, mx = INT64_MIN;
    for (uint32_t ai = a0; ai < a1; ai++) {
      const uint32_t c0 = std::max(ai * D2 + b0, c_lo),
                       c1 = std::min(ai * D2 + b1i, c_hi);
      for (uint32_t c = c0; c < c1; c++) {
        const uint32_t run = gbase + c;
        const uint32_t st = runbase[run];
        const uint32_t len = runbase[(size_t)run + 1] - st;
        if (!len) continue;
        const uint32_t plane_off = gbase + c;
        const uint32_t* bk = bnd + (size_t)run * (BP + 1);
        const uint32_t* be = bk + (size_t)(BP + 1) * R;  // block last v-offsets
        // number of blocks of this run: first b with cumc[b] == len (monotone)
        uint32_t nb = 0, hn = BP;
        while (nb < hn) {
          const uint32_t mid = (nb + hn) >> 1;
          if ((uint32_t)cumc[(size_t)mid * R + plane_off] >= len) hn = mid; else nb = mid + 1;
        }
        // Blocks intersect the interval in the contiguous range [v0, v1):
        //   v0 = first block whose LAST row >= o_lo,  v1 = first block whose FIRST row > o_hi.
        // Of those, a block is fully inside iff first row >= o_lo (u0) and last row <= o_hi (u1);
        // ties can straddle block boundaries, so the whole range is [u0, u1) and the boundary
        // blocks are scanned individually.  Rows are v-sorted inside a block, so each scan is a
        // contiguous slice.
        uint32_t v0 = 0, h0 = nb;
        while (v0 < h0) {
          const uint32_t mid = (v0 + h0) >> 1;
          if ((uint64_t)be[mid] < (uint64_t)o_lo) v0 = mid + 1; else h0 = mid;
        }
        uint32_t u0 = 0; hn = nb;
        while (u0 < hn) {
          const uint32_t mid = (u0 + hn) >> 1;
          if ((uint64_t)bk[mid] < (uint64_t)o_lo) u0 = mid + 1; else hn = mid;
        }
        uint32_t u1 = 0; hn = nb;
        while (u1 < hn) {
          const uint32_t mid = (u1 + hn) >> 1;
          if ((uint64_t)be[mid] <= (uint64_t)o_hi) u1 = mid + 1; else hn = mid;
        }
        uint32_t v1 = 0; hn = nb;
        while (v1 < hn) {
          const uint32_t mid = (v1 + hn) >> 1;
          if ((uint64_t)bk[mid] <= (uint64_t)o_hi) v1 = mid + 1; else hn = mid;
        }
        if (v0 >= v1) continue;  // no block of this run intersects the interval
        uint64_t cw = 0;
        if (u1 > u0) {
          cw = (uint64_t)(uint32_t)cumc[(size_t)u1 * R + plane_off] -
               (uint64_t)(uint32_t)cumc[(size_t)u0 * R + plane_off];
          cnt += cw;
          sum += (int64_t)(cumsum[(size_t)u1 * R + plane_off] -
                           cumsum[(size_t)u0 * R + plane_off]);
        }
        if constexpr (MINMAX) {
          if (cw) {
            const int64_t p0 = vmin + (int64_t)firstp[(size_t)u0 * R + plane_off];
            const int64_t p1 = vmin + (int64_t)lastp[(size_t)(u1 - 1) * R + plane_off];
            if (p0 < mn) mn = p0;
            if (p1 > mx) mx = p1;
          }
        }
        for (uint32_t rb = v0; rb < v1; rb++) {
          if (rb >= u0 && rb < u1) {  // already accounted for in bulk
            rb = (u1 > 0) ? u1 - 1 : rb;
            continue;
          }
          const size_t i0 = (size_t)(uint32_t)cumc[(size_t)rb * R + plane_off];
          const size_t i1 = (size_t)(uint32_t)cumc[(size_t)(rb + 1) * R + plane_off];
          if (i1 <= i0) continue;
          const uint32_t base = bk[rb];
          size_t i = st + i0;
          const size_t iend = st + i1;
          while (i < iend && (uint64_t)rank_recover_v(base, offs[i]) < (uint64_t)o_lo) i++;
          for (; i < iend; i++) {
            const uint32_t v = rank_recover_v(base, offs[i]);
            if ((uint64_t)v > (uint64_t)o_hi) break;
            cnt++;
            sum += (int64_t)v + vmin;
            if constexpr (MINMAX) {
              const int64_t p = vmin + (int64_t)v;
              if (p < mn) mn = p;
              if (p > mx) mx = p;
            }
          }
        }
      }
    }
    out.cnt[g] = cnt;
    out.sum[g] = sum;
    out.mn[g] = mn;
    out.mx[g] = mx;
  }
}

template <typename CT, bool MINMAX>
static void mec_query_impl(const Map& idx, int64_t vlo, int64_t vhi, int64_t a_lo, int64_t a_hi,
                           int64_t b_lo, int64_t b_hi, const GroupFilter& gf, QOut& out,
                           uint32_t c_lo, uint32_t c_hi) {
  const Header& h = *(const Header*)idx.base;
  const int64_t* gv = (const int64_t*)(idx.base + h.off_g);
  const int64_t* av = (const int64_t*)(idx.base + h.off_a);
  const int64_t* bv = (const int64_t*)(idx.base + h.off_b);
  const uint32_t* runbase = (const uint32_t*)(idx.base + h.off_runbase);
  const CT* cumc = (const CT*)(idx.base + h.off_cumc);
  const uint64_t* cumsum = (const uint64_t*)(idx.base + h.off_cumsum);
  const uint16_t* firstoff = (const uint16_t*)(idx.base + h.off_first);
  const uint16_t* lastoff = (const uint16_t*)(idx.base + h.off_last);
  const uint16_t* offs = (const uint16_t*)(idx.base + h.off_offs);
  const uint32_t G = h.G, D2 = h.D2, cells = h.cells, W = h.W, BP = h.BP;
  const size_t R = (size_t)G * cells;
  const int64_t vmin = h.vmin;
  const bool has_null = h.has_null != 0;
  const uint32_t null_group = h.null_group;

  for (uint32_t g = 0; g < G; g++) {
    out.cnt[g] = 0;
    out.sum[g] = 0;
    out.mn[g] = INT64_MAX;
    out.mx[g] = INT64_MIN;
    out.selected[g] = 1;
    if (gf.any()) {
      if (has_null && g == null_group) out.selected[g] = 0;  // NULL is never IN / NOT IN
      else out.selected[g] = gf.selects(gv[g]) ? 1 : 0;
    }
  }
  if (vlo > vhi || vhi < h.vmin || vlo > h.vmax) return;

  const uint32_t a0 = (uint32_t)(std::lower_bound(av, av + h.D1, a_lo) - av);
  const uint32_t a1 = (uint32_t)(std::upper_bound(av, av + h.D1, a_hi) - av);
  const uint32_t b0 = (uint32_t)(std::lower_bound(bv, bv + D2, b_lo) - bv);
  const uint32_t b1i = (uint32_t)(std::upper_bound(bv, bv + D2, b_hi) - bv);
  if (a0 >= a1 || b0 >= b1i) return;

  int64_t ib1 = (vlo <= vmin) ? 0 : (vlo - vmin) / (int64_t)W;
  int64_t ib2 = (vhi >= h.vmax) ? (int64_t)(BP - 1) : (vhi - vmin) / (int64_t)W;
  if (ib1 < 0) ib1 = 0;
  if (ib1 > (int64_t)(BP - 1)) ib1 = (int64_t)(BP - 1);
  if (ib2 < 0) ib2 = 0;
  if (ib2 > (int64_t)(BP - 1)) ib2 = (int64_t)(BP - 1);
  const uint32_t vbl = (uint32_t)ib1, vbh = (uint32_t)ib2;
  const bool full_ok = (vbh > vbl + 1);

  const uint32_t bb_lo = full_ok ? vbl + 1 : 0;
  const uint32_t bb_hi = full_ok ? vbh - 1 : 0;

  for (uint32_t g = 0; g < G; g++) {
    if (!out.selected[g]) continue;
    const CT* C = cumc + (size_t)g * cells;
    const uint64_t* S = cumsum + (size_t)g * cells;
    const uint32_t gbase = g * cells;

    // ---- whole value blocks [vbl+1, vbh-1]: two plane reads per cell ------
    uint64_t cntW = 0;
    int64_t sumW = 0;
    if (full_ok) {
      const CT* ch = C + (size_t)vbh * R;
      const CT* cl = C + (size_t)(vbl + 1) * R;
      const uint64_t* sh = S + (size_t)vbh * R;
      const uint64_t* sl = S + (size_t)(vbl + 1) * R;
      for (uint32_t ai = a0; ai < a1; ai++) {
        const uint32_t c0 = std::max(ai * D2 + b0, c_lo),
                       c1 = std::min(ai * D2 + b1i, c_hi);
        for (uint32_t c = c0; c < c1; c++) cntW += (uint32_t)ch[c] - (uint32_t)cl[c];
        for (uint32_t c = c0; c < c1; c++) sumW += (int64_t)(sh[c] - sl[c]);
      }
    }

    // ---- boundary residual: at most two partially covered blocks ----------
    // The residual blocks are *outside* the whole-block range in price order, so
    // they also decide MIN/MAX whenever they contain a match (no search needed).
    uint64_t rcnt[2] = {0, 0};
    int64_t rsum[2] = {0, 0}, rmn[2] = {INT64_MAX, INT64_MAX}, rmx[2] = {INT64_MIN, INT64_MIN};
    const int nres = (vbl == vbh) ? 1 : 2;
    for (int ri = 0; ri < nres; ri++) {
      const uint32_t rb = (ri == 0) ? vbl : vbh;
      const int64_t blo = vmin + (int64_t)rb * (int64_t)W;
      const int64_t wlo = std::max(vlo, blo);
      const int64_t whi = std::min(vhi, blo + (int64_t)W - 1);
      if (wlo > whi) continue;
      const CT* c0p = C + (size_t)rb * R;
      const CT* c1p = C + (size_t)(rb + 1) * R;
      const uint64_t* s0p = S + (size_t)rb * R;
      const uint64_t* s1p = S + (size_t)(rb + 1) * R;
      const int64_t olo = wlo - blo, ohi = whi - blo;
      const bool whole = (olo <= 0 && ohi >= (int64_t)W - 1);
      for (uint32_t ai = a0; ai < a1; ai++) {
        const uint32_t c0 = std::max(ai * D2 + b0, c_lo),
                       c1 = std::min(ai * D2 + b1i, c_hi);
        for (uint32_t c = c0; c < c1; c++) {
          const uint32_t run = g * cells + c;
          const uint32_t s = runbase[run] + c0p[c];
          const uint32_t len = (uint32_t)(runbase[run] + c1p[c]) - s;
          if (len == 0) continue;
          if (whole) {
            rcnt[ri] += len;
            rsum[ri] += (int64_t)(s1p[c] - s0p[c]);
            if constexpr (MINMAX) {
              const size_t plane_off = (size_t)gbase + c;
              const int64_t v0 = blo + (int64_t)firstoff[(size_t)rb * R + plane_off];
              const int64_t v1 = blo + (int64_t)lastoff[(size_t)rb * R + plane_off];
              if (v0 < rmn[ri]) rmn[ri] = v0;
              if (v1 > rmx[ri]) rmx[ri] = v1;
            }
          } else {
            const uint16_t* o = offs + s;
            uint32_t i = 0;
            while (i < len && (int64_t)o[i] < olo) i++;
            uint32_t j = i;
            int64_t ss = 0;
            while (j < len && (int64_t)o[j] <= ohi) {
              ss += o[j];
              j++;
            }
            if (j > i) {
              const uint32_t k = j - i;
              rcnt[ri] += k;
              rsum[ri] += (int64_t)k * blo + ss;
              if constexpr (MINMAX) {
                const int64_t v0 = blo + o[i], v1 = blo + o[j - 1];
                if (v0 < rmn[ri]) rmn[ri] = v0;
                if (v1 > rmx[ri]) rmx[ri] = v1;
              }
            }
          }
        }
      }
    }

    const uint64_t cnt = cntW + rcnt[0] + rcnt[1];
    const int64_t sum = sumW + rsum[0] + rsum[1];
    int64_t mn = INT64_MAX, mx = INT64_MIN;
    if constexpr (MINMAX) {
      if (cnt) {
        // MIN: lowest block containing a match is either residual b1 or, when
        // that is empty, the first non-empty whole block (searched globally with
        // per-block count reductions), else residual b2.
        if (rcnt[0])
          mn = rmn[0];
        else if (cntW)
          mn = [&]() -> int64_t {
            // smallest block in [bb_lo,bb_hi] with a row: the predicate
            // "cumulative count from bb_lo > 0" is monotone in the block index
            const CT* base = C + (size_t)bb_lo * R;
            uint32_t lo = bb_lo, hi = bb_hi;
            while (lo < hi) {
              const uint32_t mid = lo + (hi - lo) / 2;
              uint64_t s = 0;
              const CT* p1 = C + (size_t)(mid + 1) * R;
              for (uint32_t ai = a0; ai < a1; ai++) {
                const uint32_t c0 = std::max(ai * D2 + b0, c_lo),
                       c1 = std::min(ai * D2 + b1i, c_hi);
                for (uint32_t c = c0; c < c1; c++) s += (uint32_t)p1[c] - (uint32_t)base[c];
              }
              if (s) hi = mid; else lo = mid + 1;
            }
            const CT* p0 = C + (size_t)lo * R;
            const CT* p1 = p0 + R;
            int64_t best = INT64_MAX;
            for (uint32_t ai = a0; ai < a1; ai++) {
              const uint32_t c0 = std::max(ai * D2 + b0, c_lo),
                       c1 = std::min(ai * D2 + b1i, c_hi);
              for (uint32_t c = c0; c < c1; c++) {
                if ((uint32_t)p1[c] == (uint32_t)p0[c]) continue;
                const int64_t p = vmin + (int64_t)lo * (int64_t)W +
                                  (int64_t)firstoff[(size_t)lo * R + gbase + c];
                if (p < best) best = p;
              }
            }
            return best;
          }();
        else
          mn = rmn[1];
        // MAX: mirror image, highest block first.
        if (nres == 2 && rcnt[1])
          mx = rmx[1];
        else if (cntW)
          mx = [&]() -> int64_t {
            // smallest block whose cumulative count from bb_lo already equals the
            // whole-block total; monotone, so binary searchable even when interior
            // blocks are empty (a plain "is this block non-empty" test is NOT
            // monotone and would silently land on an earlier block).
            const CT* base = C + (size_t)bb_lo * R;
            uint32_t lo = bb_lo, hi = bb_hi;
            while (lo < hi) {
              const uint32_t mid = lo + (hi - lo) / 2;
              uint64_t s = 0;
              const CT* p1 = C + (size_t)(mid + 1) * R;
              for (uint32_t ai = a0; ai < a1; ai++) {
                const uint32_t c0 = std::max(ai * D2 + b0, c_lo),
                       c1 = std::min(ai * D2 + b1i, c_hi);
                for (uint32_t c = c0; c < c1; c++) s += (uint32_t)p1[c] - (uint32_t)base[c];
              }
              if (s == cntW) hi = mid; else lo = mid + 1;
            }
            const CT* p0 = C + (size_t)lo * R;
            const CT* p1 = p0 + R;
            int64_t best = INT64_MIN;
            for (uint32_t ai = a0; ai < a1; ai++) {
              const uint32_t c0 = std::max(ai * D2 + b0, c_lo),
                       c1 = std::min(ai * D2 + b1i, c_hi);
              for (uint32_t c = c0; c < c1; c++) {
                if ((uint32_t)p1[c] == (uint32_t)p0[c]) continue;
                const int64_t p = vmin + (int64_t)lo * (int64_t)W +
                                  (int64_t)lastoff[(size_t)lo * R + gbase + c];
                if (p > best) best = p;
              }
            }
            return best;
          }();
        else
          mx = rmx[0];
      }
    }
    out.cnt[g] = cnt;
    out.sum[g] = sum;
    out.mn[g] = mn;
    out.mx[g] = mx;
  }
}

// One cell-range chunk of a query.  c_lo/c_hi clamp the cell (c1,c2) index space,
// which is how the query is partitioned across threads: every chunk is a disjoint
// set of runs, so the partial results are exact and the merge is order-independent.
static void mec_query_chunk(const Map& idx, int64_t vlo, int64_t vhi, int64_t a_lo,
                            int64_t a_hi, int64_t b_lo, int64_t b_hi, const GroupFilter& gf,
                            bool minmax, uint32_t c_lo, uint32_t c_hi, QOut& out) {
  const Header& h = *(const Header*)idx.base;
  const bool rank_mode = (h.pad0 & 1u) != 0;
  if (rank_mode) {
    if (h.count_width == 2) {
      if (minmax) mec_query_rank<uint16_t, true>(idx, vlo, vhi, a_lo, a_hi, b_lo, b_hi, gf, out, c_lo, c_hi);
      else mec_query_rank<uint16_t, false>(idx, vlo, vhi, a_lo, a_hi, b_lo, b_hi, gf, out, c_lo, c_hi);
    } else {
      if (minmax) mec_query_rank<uint32_t, true>(idx, vlo, vhi, a_lo, a_hi, b_lo, b_hi, gf, out, c_lo, c_hi);
      else mec_query_rank<uint32_t, false>(idx, vlo, vhi, a_lo, a_hi, b_lo, b_hi, gf, out, c_lo, c_hi);
    }
    return;
  }
  if (h.count_width == 2) {
    if (minmax) mec_query_impl<uint16_t, true>(idx, vlo, vhi, a_lo, a_hi, b_lo, b_hi, gf, out, c_lo, c_hi);
    else mec_query_impl<uint16_t, false>(idx, vlo, vhi, a_lo, a_hi, b_lo, b_hi, gf, out, c_lo, c_hi);
  } else {
    if (minmax) mec_query_impl<uint32_t, true>(idx, vlo, vhi, a_lo, a_hi, b_lo, b_hi, gf, out, c_lo, c_hi);
    else mec_query_impl<uint32_t, false>(idx, vlo, vhi, a_lo, a_hi, b_lo, b_hi, gf, out, c_lo, c_hi);
  }
}

static void merge_qout(const QOut* p, uint32_t n, QOut& o) {
  for (uint32_t g = 0; g < MAX_GROUPS; g++) {
    uint64_t c = 0;
    int64_t sm = 0, mn = INT64_MAX, mx = INT64_MIN;
    for (uint32_t t = 0; t < n; t++) {
      c += p[t].cnt[g];
      sm += p[t].sum[g];
      if (p[t].mn[g] < mn) mn = p[t].mn[g];
      if (p[t].mx[g] > mx) mx = p[t].mx[g];
    }
    o.cnt[g] = c;
    o.sum[g] = sm;
    o.mn[g] = mn;
    o.mx[g] = mx;
    o.selected[g] = p[0].selected[g];
  }
}

// threads <= 1 -> the plain single-threaded path with no extra allocation.
// parts must hold at least `threads` entries; pool, when given, must have been
// started with exactly `threads` workers (so per-query dispatch costs ~1 us).
static void mec_query_mt(const Map& idx, int64_t vlo, int64_t vhi, int64_t a_lo, int64_t a_hi,
                         int64_t b_lo, int64_t b_hi, const GroupFilter& gf, bool minmax,
                         uint32_t threads, QOut* parts, ThreadPool* pool, QOut& out) {
  const Header& h = *(const Header*)idx.base;
  const uint32_t cells = h.cells;
  uint32_t T = threads;
  if (T > cells) T = cells;
  if (T <= 1 || cells <= 1) {
    mec_query_chunk(idx, vlo, vhi, a_lo, a_hi, b_lo, b_hi, gf, minmax, 0, cells, out);
    return;
  }
  auto chunk = [&](uint32_t t, uint32_t& lo, uint32_t& hi) {
    lo = (uint32_t)((uint64_t)cells * t / T);
    hi = (uint32_t)((uint64_t)cells * (t + 1) / T);
  };
  if (pool && pool->n == T) {
    // capture everything BY VALUE: the stored job must own its state, otherwise a
    // worker that is still inside job() when this frame dies reads freed locals.
    pool->job = [=](uint32_t t) {
      const uint32_t lo = (uint32_t)((uint64_t)cells * t / T);
      const uint32_t hi = (uint32_t)((uint64_t)cells * (t + 1) / T);
      mec_query_chunk(idx, vlo, vhi, a_lo, a_hi, b_lo, b_hi, gf, minmax, lo, hi, parts[t]);
    };
    pool->run();
  } else {
    parallel_ranges(T, T, [&](uint32_t t, uint32_t, uint32_t) {
      uint32_t lo, hi;
      chunk(t, lo, hi);
      mec_query_chunk(idx, vlo, vhi, a_lo, a_hi, b_lo, b_hi, gf, minmax, lo, hi, parts[t]);
    });
  }
  merge_qout(parts, T, out);
}

static void print_rows(const Header& h, const Map& idx, const QOut& out, FILE* f,
                       bool with_minmax = true) {
  const int64_t* gv = (const int64_t*)(idx.base + h.off_g);
  fprintf(f, "[");
  bool first = true;
  for (uint32_t g = 0; g < h.G; g++) {
    if (out.cnt[g] == 0 || !out.selected[g]) continue;
    if (!first) fprintf(f, ",");
    first = false;
    const char* gtxt = (h.has_null && g == h.null_group) ? "\"NULL\"" : nullptr;
    char gbuf[32];
    if (!gtxt) { snprintf(gbuf, sizeof(gbuf), "%lld", (long long)gv[g]); gtxt = gbuf; }
    if (with_minmax)
      fprintf(f, "{\"g\":%s,\"sum\":%lld,\"count\":%llu,\"min\":%lld,\"max\":%lld}", gtxt,
              (long long)out.sum[g], (unsigned long long)out.cnt[g], (long long)out.mn[g],
              (long long)out.mx[g]);
    else
      fprintf(f, "{\"g\":%s,\"sum\":%lld,\"count\":%llu,\"min\":null,\"max\":null}", gtxt,
              (long long)out.sum[g], (unsigned long long)out.cnt[g]);
  }
  fprintf(f, "]");
}

// ------------------------------------------------------------------- modes --
static int cmd_info(int argc, char** argv) {
  if (argc < 3) return 2;
  Map idx;
  if (!map_file(argv[2], idx)) {
    fprintf(stderr, "{\"error\":\"cannot map index\"}\n");
    return 1;
  }
  const Header& h = *(const Header*)idx.base;
  if (memcmp(h.magic, "MEC02", 5) != 0) {
    fprintf(stderr, "{\"error\":\"bad magic\"}\n");
    return 1;
  }
  const bool rank = (h.pad0 & 1u) != 0;
  const size_t planes = (size_t)(h.BP + 1) * h.G * h.cells;
  const size_t pl_first = rank ? 4 : 2;
  printf(
      "{\"bytes\":%llu,\"mode\":\"%s\",\"RB\":%u,\"rows\":%llu,\"G\":%u,\"G0\":%u,"
      "\"has_null\":%s,\"null_group\":%u,\"D1\":%u,\"D2\":%u,\"cells\":%u,\"W\":%u,"
      "\"BP\":%u,\"count_width\":%u,\"vmin\":%lld,\"vmax\":%lld,"
      "\"sections\":{\"cumc\":%llu,\"cumsum\":%llu,\"first\":%llu,\"last\":%llu,"
      "\"offs\":%llu,\"header\":%llu}}\n",
      (unsigned long long)idx.size, rank ? "rank" : "value", h.pad0 >> 1,
      (unsigned long long)h.n, h.G, h.G0, h.has_null ? "true" : "false", h.null_group, h.D1,
      h.D2, h.cells, h.W, h.BP, h.count_width, (long long)h.vmin, (long long)h.vmax,
      (unsigned long long)(planes * h.count_width), (unsigned long long)(planes * 8),
      (unsigned long long)(planes * pl_first), (unsigned long long)(planes * pl_first),
      (unsigned long long)(h.n * 2), (unsigned long long)h.off_cumc);
  return 0;
}

static GroupFilter parse_filter(int argc, char** argv, int start) {
  GroupFilter gf;
  for (int i = start; i < argc; i++) {
    std::string a = argv[i];
    auto split = [](const std::string& s) {
      std::vector<int64_t> v;
      size_t p = 0;
      while (p < s.size()) {
        size_t q = s.find(',', p);
        if (q == std::string::npos) q = s.size();
        if (q > p) v.push_back(strtoll(s.substr(p, q - p).c_str(), nullptr, 10));
        p = q + 1;
      }
      return v;
    };
    if (a.rfind("gin=", 0) == 0) gf.in = split(a.substr(4));
    else if (a.rfind("gnotin=", 0) == 0) gf.notin = split(a.substr(7));
  }
  return gf;
}

static int cmd_query(int argc, char** argv) {
  if (argc < 9) {
    fprintf(stderr,
            "usage: mec query <index> vlo vhi a_lo a_hi b_lo b_hi [sc] [gin=a,b] [gnotin=a,b]\n");
    return 2;
  }
  auto t0 = std::chrono::steady_clock::now();
  Map idx;
  if (!map_file(argv[2], idx)) {
    fprintf(stderr, "{\"error\":\"cannot map index\"}\n");
    return 1;
  }
  auto tq = std::chrono::steady_clock::now();
  const Header& h = *(const Header*)idx.base;
  int64_t q[6];
  for (int i = 0; i < 6; i++) q[i] = strtoll(argv[3 + i], nullptr, 10);
  bool sc = false;
  for (int i = 9; i < argc; i++)
    if (std::string(argv[i]) == "sc") sc = true;
  GroupFilter gf = parse_filter(argc, argv, 9);
  const uint32_t threads = parse_threads(argc, argv);
  std::vector<QOut> parts(threads > 1 ? threads : 1);
  QOut out;
  mec_query_mt(idx, q[0], q[1], q[2], q[3], q[4], q[5], gf, !sc, threads, parts.data(), nullptr,
               out);
  auto te = std::chrono::steady_clock::now();
  print_rows(h, idx, out, stdout, !sc);
  printf("\n");
  fflush(stdout);
  auto tf = std::chrono::steady_clock::now();
  fprintf(stderr, "{\"query_ms\":%.4f,\"mmap_ms\":%.4f,\"stored_ms\":%.4f}\n", ms_between(t0, te),
          ms_between(t0, tq), ms_between(t0, tf));
  return 0;
}

struct CaseDef {
  std::string name;
  int64_t v[2], a[2], b[2];
};

static std::vector<CaseDef> load_cases(const char* path) {
  std::vector<CaseDef> out;
  FILE* f = fopen(path, "r");
  if (!f) {
    fprintf(stderr, "{\"error\":\"cannot open cases file\"}\n");
    exit(1);
  }
  char line[512];
  while (fgets(line, sizeof(line), f)) {
    char* p = line;
    while (*p == ' ' || *p == '\t') p++;
    if (*p == '#' || *p == '\n' || *p == 0) continue;
    CaseDef c;
    char nm[128];
    long long x[6];
    if (sscanf(p, "%127s %lld %lld %lld %lld %lld %lld", nm, &x[0], &x[1], &x[2], &x[3], &x[4],
               &x[5]) != 7)
      continue;
    c.name = nm;
    for (int i = 0; i < 2; i++) {
      c.v[i] = x[i];
      c.a[i] = x[2 + i];
      c.b[i] = x[4 + i];
    }
    out.push_back(c);
  }
  fclose(f);
  return out;
}

static int cmd_bench(int argc, char** argv) {
  if (argc < 5) {
    fprintf(stderr, "usage: mec bench <index> <reps> <cases.txt>\n");
    return 2;
  }
  Map idx;
  if (!map_file(argv[2], idx)) {
    fprintf(stderr, "{\"error\":\"cannot map index\"}\n");
    return 1;
  }
  const Header& h = *(const Header*)idx.base;
  if (memcmp(h.magic, "MEC02", 5) != 0) {
    fprintf(stderr, "{\"error\":\"bad magic\"}\n");
    return 1;
  }
  const int reps = atoi(argv[3]);
  auto cases = load_cases(argv[4]);
  GroupFilter none;
  const uint32_t threads = parse_threads(argc, argv);
  std::vector<QOut> parts(threads > 1 ? threads : 1);
  ThreadPool pool;
  if (threads > 1) pool.start(threads);
  printf("{\n");
  printf("  \"threads\": %u,\n", threads);
  printf("  \"index_bytes\": %llu,\n", (unsigned long long)idx.size);
  printf("  \"rows\": %llu,\n", (unsigned long long)h.n);
  printf("  \"reps\": %d,\n", reps);
  printf("  \"mode\": \"%s\",\n", (h.pad0 & 1u) ? "rank" : "value");
  printf("  \"RB\": %u,\n", h.pad0 >> 1);
  printf("  \"count_width\": %u,\n", h.count_width);
  printf("  \"has_null\": %s,\n", h.has_null ? "true" : "false");
  printf("  \"cases\": {\n");
  for (size_t ci = 0; ci < cases.size(); ci++) {
    const CaseDef& c = cases[ci];
    QOut out, out_sc;
    mec_query_mt(idx, c.v[0], c.v[1], c.a[0], c.a[1], c.b[0], c.b[1], none, true, threads,
                 parts.data(), threads > 1 ? &pool : nullptr, out);
    mec_query_mt(idx, c.v[0], c.v[1], c.a[0], c.a[1], c.b[0], c.b[1], none, false, threads,
                 parts.data(), threads > 1 ? &pool : nullptr, out_sc);
    auto time_it = [&](bool minmax) {
      std::vector<double> ts((size_t)reps);
      for (int r = 0; r < reps; r++) {
        auto t0 = std::chrono::steady_clock::now();
        mec_query_mt(idx, c.v[0], c.v[1], c.a[0], c.a[1], c.b[0], c.b[1], none, minmax,
                     threads, parts.data(), threads > 1 ? &pool : nullptr,
                     minmax ? out : out_sc);
        auto t1 = std::chrono::steady_clock::now();
        ts[(size_t)r] = std::chrono::duration<double, std::milli>(t1 - t0).count();
      }
      std::sort(ts.begin(), ts.end());
      const double p95 = ts[std::min(ts.size() - 1, (size_t)((double)ts.size() * 0.95))];
      const double p05 = ts[std::min(ts.size() - 1, (size_t)((double)ts.size() * 0.05))];
      double mean = 0;
      for (double x : ts) mean += x;
      mean /= (double)ts.size();
      return std::array<double, 6>{ts[ts.size() / 2], p05, p95, ts.front(), ts.back(), mean};
    };
    auto a_full = time_it(true);
    auto a_sc = time_it(false);
    printf("    \"%s\": {\"params\": [%lld,%lld,%lld,%lld,%lld,%lld], "
           "\"median_us\": %.4f, \"p05_us\": %.4f, \"p95_us\": %.4f, \"min_us\": %.4f, "
           "\"max_us\": %.4f, \"mean_us\": %.4f, "
           "\"sc\": {\"median_us\": %.4f, \"p05_us\": %.4f, \"p95_us\": %.4f, "
           "\"min_us\": %.4f, \"max_us\": %.4f, \"mean_us\": %.4f}, \"rows\": ",
           c.name.c_str(), (long long)c.v[0], (long long)c.v[1], (long long)c.a[0],
           (long long)c.a[1], (long long)c.b[0], (long long)c.b[1], a_full[0] * 1000.0,
           a_full[1] * 1000.0, a_full[2] * 1000.0, a_full[3] * 1000.0, a_full[4] * 1000.0,
           a_full[5] * 1000.0, a_sc[0] * 1000.0, a_sc[1] * 1000.0, a_sc[2] * 1000.0,
           a_sc[3] * 1000.0, a_sc[4] * 1000.0, a_sc[5] * 1000.0);
    print_rows(h, idx, out, stdout);
    printf("}%s\n", (ci + 1 < cases.size()) ? "," : "");
  }
  printf("  }\n}\n");
  pool.stop();
  return 0;
}

int main(int argc, char** argv) {
  if (argc < 2) {
    fprintf(stderr,
            "usage: mec {plan|build|info|query|bench} ...\n"
            "  plan  <g.i64> <v.i64> <c1.i64> <c2.i64> <budget_bytes>\n"
            "  build <g.i64> <v.i64> <c1.i64> <c2.i64> <out> [W] [budget_bytes]\n"
            "  info  <index>\n"
            "  query <index> vlo vhi a_lo a_hi b_lo b_hi [sc] [gin=a,b] [gnotin=a,b]\n"
            "  bench <index> <reps> <cases.txt>\n");
    return 2;
  }
  const std::string m = argv[1];
  if (m == "build") return cmd_build(argc, argv);
  if (m == "plan") return cmd_plan(argc, argv);
  if (m == "info") return cmd_info(argc, argv);
  if (m == "query") return cmd_query(argc, argv);
  if (m == "bench") return cmd_bench(argc, argv);
  fprintf(stderr, "{\"error\":\"unknown mode\"}\n");
  return 2;
}
