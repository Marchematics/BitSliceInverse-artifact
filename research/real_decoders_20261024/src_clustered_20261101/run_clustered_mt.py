#!/usr/bin/env python3
"""Clustered stream, multi-core series (2026-11-01).

Parallelises the CLST01 range scan (run-partitioned chunks, disjoint per-thread
accumulation, slice-parallel merge) and measures the thread-count series for the
two configurations where the scan dominates:

  A) 1B / G=101   (the 1B cell of CLUSTERED_20261101 section 10)
  B) 100M / G=1M  (the worst same-thread-16 cell of the 100M panel)

Cases: p0.1 / p1 / p10 / p50 (value cuts -7000 / -720 / 650 / 1350, i.e. the
measured selectivities 0.135 / 1.04 / 11.9 / 51.0 %), plus a 1-row needle case for
the dispatch floor.  For each case and each T in 1/4/8/16: 3 bench runs, each
after the project's pinned 0.5 s P-state warmup, 30 timed queries per run, median
of the 3 bench medians and P95.  Every T's full result is md5'd and compared with
the T=1 result (bit-identity check).  `--auto` gives the production choice of the
tool's cost model.  DuckDB 2.0 runs the same SQL/data at threads=1/4/8/16, one
process per thread count on the same core set.

Run on localhost from research/real_decoders_20261024:
    nice -n 5 taskset -c 128-143 python3 \
        src_clustered_20261101/run_clustered_mt.py
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import hashlib
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import run_clustered as rc                       # noqa: E402 (shared helpers)

REPO = rc.REPO
SCRATCH = rc.SCRATCH
RECEIPT = rc.RECEIPT
TOOL = rc.TOOL
BENCH_DUCK = rc.BENCH_DUCK
PY = rc.PY
CORE = rc.CORE
CORES16 = rc.CORES16
TIERS = rc.TIERS
GROUPS_DB = rc.GROUPS_DB
SCALE = os.path.join(REPO, "results/scale_20261101")
PARQUET_1B = os.path.join(SCALE, "nyc1b.parquet")
THREADS = (1, 4, 8, 16)
REPS = 30
BENCHES = 3
CASES = (("p0.1", -7000), ("p1", -720), ("p10", 650), ("p50", 1350))
I32MIN, I32MAX = -2**31, 2**31 - 1


def md5_file(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 22), b""):
            h.update(chunk)
    return h.hexdigest()


def our_bench(path, lo, hi, out_json, threads, reps=REPS, benches=BENCHES, extra=()):
    runs = []
    for _ in range(benches):
        rc.warm_core()
        _, stderr, _, _ = rc.sh([TOOL, "query", path, str(lo), str(hi), out_json,
                                 "/dev/null", str(reps), "--threads", str(threads)]
                                + list(extra), cores=CORES16)
        runs.append(rc.last_json(stderr))
    runs.sort(key=lambda j: j["query_median_ms"])
    med = runs[len(runs) // 2]
    out = dict(med)
    out["reps"] = reps
    out["bench_medians_ms"] = [r["query_median_ms"] for r in runs]
    out["bench_p95_ms"] = [r["query_p95_ms"] for r in runs]
    out["bench_spread"] = (max(out["bench_medians_ms"]) /
                           max(1e-12, min(out["bench_medians_ms"])))
    out["query_median_ms"] = med["query_median_ms"]
    out["query_p95_ms"] = med["query_p95_ms"]
    return out


def duck_series(mode, target, spec, reps_by_t):
    """one process per thread count, pinned: 1T one core, else the 16-core set"""
    spec_path = os.path.join(SCRATCH, f"mt_spec_{target}.json")
    json.dump(spec, open(spec_path, "w"), indent=1)
    out = {}
    for T in THREADS:
        outp = os.path.join(SCRATCH, f"mt_duck_{target}_{T}t.json")
        cores = CORE if T == 1 else CORES16
        cmd = [PY, BENCH_DUCK]
        if mode == "parquet":
            cmd += ["--parquet", PARQUET_1B]
        else:
            cmd += [GROUPS_DB]
        cmd += [str(T), str(reps_by_t[T]), spec_path, outp]
        if os.path.exists(outp) and os.environ.get("MT_REDUCK", "0") != "1":
            j = json.load(open(outp))
            out[T] = j
            print(f"  duckdb {target} T={T} (cached, effective {j['threads_effective']})",
                  flush=True)
            continue
        wall, _, so, _ = rc.sh(cmd, cores=cores)
        j = json.load(open(outp))
        j["wall_s"] = wall
        out[T] = j
        print(f"  duckdb {target} T={T} (effective {j['threads_effective']}, "
              f"{len(j['affinity'])} cores) wall={wall:.1f}s", flush=True)
    return out


def main():
    os.makedirs(RECEIPT, exist_ok=True)
    rec = {"stream": "clustered", "series": "multi-core", "date": "2026-11-01",
           "protocol": {
               "host": "localhost (AMD EPYC 9554, 2x64-core, SMT on); stream core set 128-143",
               "cores_note": "CPUs 128-143 are the SMT siblings of CPUs 0-15 (16 distinct "
                             "physical cores, all NUMA node 0); measured pure-compute scaling "
                             "on this set is 14.5x at 16 threads",
               "pin": f"nice -n 5 taskset -c {CORES16} for every bench (workers are pinned "
                      f"one per CPU of the inherited set); DuckDB 1T pinned to {CORE}",
               "ours": f"{BENCHES} bench runs x {REPS} queries, each run preceded by a 0.5 s "
                       f"pinned busy loop; median of the 3 bench medians; P95 from the median "
                       f"run; timed region = state clear + parallel scan + merge "
                       f"(row collection/serialization untimed)",
               "parallel_design": "matching rows partitioned into row-balanced run-aligned "
                                  "chunks; each thread accumulates into its own dense per-group "
                                  "state (disjoint, no atomics); partials merged over disjoint "
                                  "group slices; epoch tags make the state self-initialising "
                                  "(no O(G) clearing); memory cap 640 MB of live partials, "
                                  "above which the scan runs in waves merged after each wave",
               "duckdb": "2.0.0, threads set via the connection config, one process per thread "
                         "count, same SQL/data, forced-aggregation engine SQL",
               "bit_identity": "md5 of the complete JSON result compared across 1/4/8/16 threads",
               "targets": "16-vs-16 vs DuckDB: >=1x at p50, >=3x at p10, no regression vs the "
                          "1-thread CLST01 number at p0.1/p1",
           },
           "artifacts": {"clst_tool_sha256": rc.sha256(TOOL),
                         "clst_tool_cpp_sha256": rc.sha256(os.path.join(
                             REPO, "src_clustered_20261101/clst_tool.cpp"))},
           "configs": {}, "dispatch_floor": {}, "duckdb": {}}

    configs = [
        ("1B_G101", os.path.join(SCRATCH, "clst_1b_stats.clst"), "parquet"),
        ("100M_G1M", os.path.join(SCRATCH, "clst_g1000000_stats.clst"), "db"),
    ]
    # ---------------- DuckDB controls (same core set) ----------------
    spec_1b, spec_100m = [], []
    for name, cut in CASES:
        spec_1b.append({"label": name,
                        "engine_sql": f"SELECT COUNT(*) FROM (SELECT g, SUM(v), COUNT(*), "
                                      f"MIN(v), MAX(v) FROM groups WHERE v BETWEEN -2147483648 "
                                      f"AND {cut} GROUP BY 1)"})
        spec_100m.append({"label": name,
                          "engine_sql": f"SELECT COUNT(*) FROM (SELECT g1000000, SUM(v), "
                                        f"COUNT(*), MIN(v), MAX(v) FROM groups WHERE v BETWEEN "
                                        f"-2147483648 AND {cut} GROUP BY 1)"})
    print("=== DuckDB controls ===", flush=True)
    rec["duckdb"]["1B_G101"] = duck_series("parquet", "1b", spec_1b,
                                           {1: 2, 4: 3, 8: 3, 16: 3})
    rec["duckdb"]["100M_G1M"] = duck_series("db", "100m", spec_100m,
                                            {1: 3, 4: 3, 8: 3, 16: 3})

    # ---------------- our thread series ----------------
    for tag, path, _ in configs:
        print(f"=== {tag} ===", flush=True)
        # cuts relative to this config's row count, from its own run table
        h, vals_c, counts_c, starts_c = rc.load_clst_meta(path)
        cfg = {"file": path, "rows": h["n"], "G": h["G"], "V": h["V"],
               "gid_bits": h["W"], "gmin": h["gmin"], "file_bytes": h["file_bytes"],
               "cases": {}, "auto": {}}
        import bisect
        for name, cut in CASES:
            r0 = bisect.bisect_left(vals_c, -2**31)
            r1 = bisect.bisect_right(vals_c, cut)
            nrows = starts_c[r1] - starts_c[r0] if r1 > r0 else 0
            case = {"lo": I32MIN, "hi": cut, "matching_rows": nrows,
                    "selectivity": nrows / h["n"], "by_threads": {}}
            base_md5 = None
            for T in THREADS:
                outj = os.path.join(SCRATCH, f"mt_{tag}_{name}_t{T}.json")
                q = our_bench(path, I32MIN, cut, outj, T)
                md5 = md5_file(outj)
                if T == 1:
                    base_md5 = md5
                case["by_threads"][str(T)] = {
                    "median_ms": q["query_median_ms"], "p95_ms": q["query_p95_ms"],
                    "bench_medians_ms": q["bench_medians_ms"],
                    "bench_spread": q["bench_spread"], "reps": q["reps"],
                    "threads_used": q["threads_used"], "waves": q["waves"],
                    "chunks": q.get("chunks"), "max_chunk_rows": q.get("max_chunk_rows"),
                    "scan_ms": q["parallel_scan_ms"], "merge_ms": q["parallel_merge_ms"],
                    "partial_state_bytes": q["partial_state_bytes"],
                    "peak_rss_kb": q["peak_rss_kb"], "rows": q["rows"],
                    "result_md5": md5, "bit_identical_to_1t": (md5 == base_md5)}
                print(f"  [{tag}/{name}] T={T:2d} used={q['threads_used']:2d} "
                      f"med={q['query_median_ms']:9.4f}ms P95={q['query_p95_ms']:9.4f} "
                      f"scan={q['parallel_scan_ms']:9.4f} merge={q['parallel_merge_ms']:7.4f} "
                      f"rss={q['peak_rss_kb']//1024}MB md5ok={md5 == base_md5}", flush=True)
            b = case["by_threads"]
            case["speedup"] = {str(T): b["1"]["median_ms"] / b[str(T)]["median_ms"]
                               for T in THREADS}
            case["x_duck"] = {str(T): rec["duckdb"][tag][T]["results"][name]["median_ms"] /
                              b[str(T)]["median_ms"] for T in THREADS}
            # production (auto) choice, one bench
            outj = os.path.join(SCRATCH, f"mt_{tag}_{name}_auto.json")
            qa = our_bench(path, I32MIN, cut, outj, 16, reps=REPS, benches=1,
                           extra=["--auto"])
            case["auto"] = {"choose_threads": qa["threads_used"],
                            "median_ms": qa["query_median_ms"],
                            "x_duck_at_its_threads": None}
            d = rec["duckdb"][tag][int(qa["threads_used"])]["results"][name]["median_ms"]
            case["auto"]["x_duck_at_its_threads"] = d / qa["query_median_ms"]
            cfg["cases"][name] = case
            print(f"  [{tag}/{name}] auto: T={qa['threads_used']} "
                  f"med={qa['query_median_ms']:.4f}ms", flush=True)
        # dispatch floor: a 1-row needle at T=1 and T=16
        needle_lo = vals_c[0]
        for T in (1, 16):
            outj = os.path.join(SCRATCH, f"mt_{tag}_needle_t{T}.json")
            q = our_bench(path, needle_lo, needle_lo, outj, T, reps=200, benches=2)
            rec["dispatch_floor"].setdefault(tag, {})[str(T)] = {
                "median_ms": q["query_median_ms"], "p95_ms": q["query_p95_ms"],
                "reps": q["reps"], "threads_used": q["threads_used"],
                "waves": q["waves"],
                "scan_ms": q["parallel_scan_ms"], "merge_ms": q["parallel_merge_ms"]}
            print(f"  [{tag}/needle(1 row)] T={T} med={q['query_median_ms']*1000:.3f}us "
                  f"scan={q['parallel_scan_ms']*1000:.1f}us merge={q['parallel_merge_ms']*1000:.1f}us",
                  flush=True)
        rec["configs"][tag] = cfg

    json.dump(rec, open(os.path.join(SCRATCH, "clustered_mt_raw.json"), "w"), indent=1)

    # ---------------- summary / targets ----------------
    summ = {}
    for tag in ("1B_G101", "100M_G1M"):
        c = rec["configs"][tag]["cases"]
        summ[tag] = {
            "p50_16v16_x": c["p50"]["x_duck"]["16"],
            "p10_16v16_x": c["p10"]["x_duck"]["16"],
            "p1_16v16_x": c["p1"]["x_duck"]["16"],
            "p0.1_16v16_x": c["p0.1"]["x_duck"]["16"],
            "p50_speedup_16t": c["p50"]["speedup"]["16"],
            "p10_speedup_16t": c["p10"]["speedup"]["16"],
            "p1_speedup_16t": c["p1"]["speedup"]["16"],
            "p0.1_speedup_16t": c["p0.1"]["speedup"]["16"],
            "all_bit_identical": all(
                v["bit_identical_to_1t"] for k in c for v in c[k]["by_threads"].values()),
            "no_regression_p01_p1_16t": (c["p0.1"]["speedup"]["16"] >= 1.0 and
                                         c["p1"]["speedup"]["16"] >= 1.0),
            "clears_1x_p50_16v16": c["p50"]["x_duck"]["16"] >= 1.0,
            "clears_3x_p10_16v16": c["p10"]["x_duck"]["16"] >= 3.0,
        }
    rec["summary"] = summ
    json.dump(rec, open(os.path.join(RECEIPT, "clustered_mt.json"), "w"), indent=1)
    json.dump(rec, open(os.path.join(SCRATCH, "clustered_mt.json"), "w"), indent=1)
    print("wrote", os.path.join(RECEIPT, "clustered_mt.json"), flush=True)
    print(json.dumps(summ, indent=1))


if __name__ == "__main__":
    main()
