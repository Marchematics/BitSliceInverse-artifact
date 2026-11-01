#!/usr/bin/env python3
"""Clustered stream, 1B cell (follow-up, 2026-11-01).

Builds CLST01 on the existing 1B scale dataset (10 deterministic replicas of
NYC-100M, group key = passenger + 10*r, NULL group-key sentinel gid = -1, so the
group domain is [-1,99] = G=101, V=21,084; plain i32 columns
results/scale_20261101/{nyc1b_gid.i32,nyc1b_val.i32}, 4 GB each) and measures:

  1. build time + exact structure bytes (range-only and with the fixed-query
     per-group totals) and the footprint against the 10 x 251,814,420 B
     encoded-tree equivalent (2,518,144,200 B);
  2. query latency at 0.1/1/10/50/100 % selectivity, 30 reps, median + P95,
     single thread, after the project's pinned 0.5 s P-state warmup, 3 bench
     runs, median of the 3 medians; DuckDB 2.0 controls on the same data
     (1.32 GB parquet, same SQL) at threads=1 and threads=16, one process per
     thread count;
  3. the structural claim: ns per matching row at 100M and at 1B with G=101
     fixed (the 100M G=101 structure is built from the same value column with
     the tiers stream's synthetic key), i.e. does latency scale with the answer
     rather than with n;
  4. exactness on the 5 cases: full grouped result (101 rows, group/SUM/COUNT/
     MIN/MAX) element-wise vs DuckDB 2.0.0 on the 1B parquet, plus the
     replica-map analytic check (all 10 replicas must produce identical rows
     for the same passenger group) and the 1B losslessness verify.

Run on localhost from research/real_decoders_20261024:
    nice -n 5 taskset -c 128-143 python3 \
        src_clustered_20261101/run_clustered_1b.py
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import json
import os
import statistics
import struct
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import run_clustered as rc                     # noqa: E402  (shared helpers)
import duckdb                                  # noqa: E402

REPO = rc.REPO
SCRATCH = rc.SCRATCH
BIN = rc.BIN
RECEIPT = rc.RECEIPT
TIERS = rc.TIERS
TOOL = rc.TOOL
BENCH_DUCK = rc.BENCH_DUCK
PY = rc.PY
CORE = rc.CORE
CORES16 = rc.CORES16
SCALE = os.path.join(REPO, "results/scale_20261101")
GID1B = os.path.join(SCALE, "nyc1b_gid.i32")
VAL1B = os.path.join(SCALE, "nyc1b_val.i32")
PARQUET = os.path.join(SCALE, "nyc1b.parquet")
VAL100 = os.path.join(TIERS, "val_nyc100.i32")
GID100_G101 = os.path.join(SCRATCH, "gid_100m_g101.i32")
GEN_GROUPS = _artifact.expand("../../work/tiers/bin/gen_groups")
TREE_EQ = 10 * 251_814_420          # 10 x the NYC-100M encoded tree
N1B = 1_000_000_000
N100 = 100_000_000
TARGETS = (0.001, 0.01, 0.10, 0.50, 1.00)
I32MIN, I32MAX = -2**31, 2**31 - 1
OURS_REPS = 30            # required by the follow-up spec
DUCK_REPS_1T = 3          # 4.8-9.5 s per execution at 1B: 3 reps is the budget
DUCK_REPS_16T = 3


# ---------------------------------------------------------------------------
def bench_fixed_reps(path, lo, hi, out_json, reps=OURS_REPS, n_benches=3, extra=()):
    """n_benches independent runs, each after a 0.5 s pinned busy loop, fixed
    `reps` timed queries per run, median of the bench medians."""
    runs = []
    for _ in range(n_benches):
        rc.warm_core()
        _, stderr, _, _ = rc.sh([TOOL, "query", path, str(lo), str(hi), out_json,
                                 "/dev/null", str(reps)] + list(extra))
        runs.append(rc.last_json(stderr))
    runs.sort(key=lambda j: j["query_median_ms"])
    med = runs[len(runs) // 2]
    out = dict(med)
    out["reps"] = reps
    out["bench_medians_ms"] = [r["query_median_ms"] for r in runs]
    out["bench_p95_ms"] = [r["query_p95_ms"] for r in runs]
    out["bench_spread"] = (max(out["bench_medians_ms"]) /
                           max(1e-12, min(out["bench_medians_ms"])))
    return out


def norm_ours_1b(rows):
    """gid -1 is this dataset's SQL-NULL group-key sentinel -> 'NULL'."""
    out = []
    for r in rows:
        g = int(r[0])
        out.append(("NULL" if g == -1 else str(g), str(int(r[1])), str(int(r[2])),
                    str(int(r[3])), str(int(r[4]))))
    return out


def sql_range(lo, hi):
    base = "SELECT g AS g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM groups"
    if lo <= I32MIN and hi >= I32MAX:
        return base + " GROUP BY 1 ORDER BY 1"
    return base + f" WHERE v BETWEEN {lo} AND {hi} GROUP BY 1 ORDER BY 1"


def sql_range_engine(lo, hi):
    inner = "SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM groups"
    if lo <= I32MIN and hi >= I32MAX:
        return f"SELECT COUNT(*) FROM ({inner} GROUP BY 1)"
    return f"SELECT COUNT(*) FROM ({inner} WHERE v BETWEEN {lo} AND {hi} GROUP BY 1)"


def duck_baselines_parquet(spec):
    spec_path = os.path.join(SCRATCH, "duck_1b_spec.json")
    json.dump(spec, open(spec_path, "w"), indent=1)
    outs = {}
    for threads, cores, reps in ((1, CORE, DUCK_REPS_1T), (16, CORES16, DUCK_REPS_16T)):
        outp = os.path.join(SCRATCH, f"duck_1b_{threads}t.json")
        wall, _, so, _ = rc.sh([PY, BENCH_DUCK, "--parquet", PARQUET, str(threads),
                                str(reps), spec_path, outp], cores=cores)
        j = json.load(open(outp))
        j["wall_s"] = wall
        j["pin"] = f"nice -n 5 taskset -c {cores}"
        outs[threads] = j
        print(f"  duckdb(parquet) threads={threads}: effective={j['threads_effective']} "
              f"cores={len(j['affinity'] or [])} wall={wall:.1f}s", flush=True)
    return outs


def main():
    os.makedirs(RECEIPT, exist_ok=True)
    rec = {"stream": "clustered", "cell": "1B", "date": "2026-11-01",
           "protocol": {
               "host": "localhost (AMD EPYC 9554, no GPU)",
               "pin_tool": f"nice -n 5 taskset -c {CORE}",
               "pin_driver": f"nice -n 5 taskset -c {CORES16}",
               "dataset": "10 deterministic replicas of NYC-100M plain columns "
                          "(results/scale_20261101/nyc1b_{gid,val}.i32, 4 GB each), "
                          "gid = passenger + 10*r, NULL group-key sentinel -1, "
                          "G=101, V=21084",
               "ours": f"engine-resident; 30 reps per bench run, 3 bench runs each after a "
                       f"0.5 s pinned busy loop, median of the 3 medians; timed region = "
                       f"state reset + aggregation scan (row collection and serialization "
                       f"timed apart)",
               "duckdb": f"2.0.0 on the 1B parquet (1.32 GB zstd), read via a view; "
                         f"config threads=1 (one pinned core) and threads=16 (16 cores), "
                         f"one process per thread count; {DUCK_REPS_1T} reps at 1T and "
                         f"{DUCK_REPS_16T} at 16T because one execution is 4.8-9.5 s; "
                         f"engine SQL = SELECT COUNT(*) FROM (... GROUP BY ...)",
               "exactness": "full grouped result (101 rows) element-wise vs DuckDB 2.0.0 "
                            "on the 1B parquet, threads=1",
               "duckdb_version": duckdb.__version__,
               "tree_equivalent_note": "10 x 251,814,420 B = the encoded tree a 1B encoded "
                                       "source would have (SCALE_20261101 1.2)"},
           "artifacts": {
               "clst_tool_sha256": rc.sha256(TOOL),
               "clst_tool_cpp_sha256": rc.sha256(os.path.join(
                   REPO, "src_clustered_20261101/clst_tool.cpp")),
               "gid_1b": {"path": GID1B, "bytes": os.path.getsize(GID1B)},
               "val_1b": {"path": VAL1B, "bytes": os.path.getsize(VAL1B)},
               "parquet_1b": {"path": PARQUET, "bytes": os.path.getsize(PARQUET)},
               "tree_equivalent_bytes": TREE_EQ},
           "build": {}, "cuts": {}, "series_1b": {}, "series_100m_g101": {},
           "scale_check": {}, "exactness": {}, "replica_map": {}, "footprint": {}}

    # -------- build (1B, both variants) --------
    print("=== 1B build (sort + encode) ===", flush=True)
    for variant, flags in (("range", []), ("stats", ["--with-stats"])):
        path = os.path.join(SCRATCH, f"clst_1b_{variant}.clst")
        wall, stderr, _, rss = rc.sh([TOOL, "build", GID1B, VAL1B, path] + flags)
        b = rc.last_json(stderr)
        b["wall_s"] = wall
        b["peak_rss_kb"] = rss
        b["file_bytes"] = os.path.getsize(path)
        b["sha256"] = rc.sha256(path)
        b["ratio_to_tree_equivalent"] = b["file_bytes"] / TREE_EQ
        b["sum_sort_encode_ms"] = b["build_ms"]
        rec["build"][variant] = b
        print(f"  {variant}: {b['build_ms']:.1f}ms bytes={b['file_bytes']} "
              f"({b['ratio_to_tree_equivalent']:.5f}x tree-eq) "
              f"sort={b['scatter_ms']:.0f}ms pack={b['bitpack_ms']:.0f}ms "
              f"write={b['write_ms']:.0f}ms rss={rss/1024/1024:.1f}GB "
              f"sha={b['sha256'][:12]}", flush=True)
        if variant == "stats":
            _, _, so, _ = rc.sh([TOOL, "info", path])
            rec["build"]["info"] = json.loads(so.strip())
            wv, _, vout, vrss = rc.sh([TOOL, "verify", GID1B, VAL1B, path])
            rec["build"]["verify"] = json.loads(vout.strip())
            rec["build"]["verify"]["wall_s"] = wv
            rec["build"]["verify"]["peak_rss_kb"] = vrss
            print(f"  verify: {rec['build']['verify']['exact']}", flush=True)
    ST1B = os.path.join(SCRATCH, "clst_1b_stats.clst")

    # -------- 100M G=101 structure (same value column, same G) --------
    print("=== 100M G=101 structure (for the n-scaling comparison) ===", flush=True)
    if not os.path.exists(GID100_G101):
        rc.sh([GEN_GROUPS, str(N100), "101", GID100_G101])
    CL100 = os.path.join(SCRATCH, "clst_100m_g101.clst")
    wall, stderr, _, rss = rc.sh([TOOL, "build", GID100_G101, VAL100, CL100,
                                  "--with-stats"])
    b100 = rc.last_json(stderr)
    b100["wall_s"] = wall
    b100["file_bytes"] = os.path.getsize(CL100)
    b100["sha256"] = rc.sha256(CL100)
    b100["gid"] = "splitmix64(rowid) % 101 (tiers gen_groups), 101 groups"
    rec["build"]["g101_100m"] = b100
    print(f"  100M G=101: {b100['build_ms']:.1f}ms bytes={b100['file_bytes']} "
          f"sha={b100['sha256'][:12]}", flush=True)

    # -------- cuts (from the 1B run table) --------
    print("=== cuts ===", flush=True)
    h, vals, counts, starts = rc.load_clst_meta(ST1B)
    cuts = {}
    for s in TARGETS:
        if s >= 1.0:
            cut, rows = I32MAX, N1B
        else:
            cut, rows = rc.cut_for_rank(vals, counts, starts, int(round(s * N1B)))
        cuts[f"p{s*100:g}"] = {"target": s, "cut": cut, "rows": rows,
                               "actual_selectivity": rows / N1B}
        print(f"  p{s*100:g}: cut={cut} rows={rows} sel={rows/N1B:.6f}", flush=True)
    rec["cuts"] = {"value_cuts": cuts, "V": h["V"], "gmin": h["gmin"],
                   "vmin": h["vmin"], "vmax": h["vmax"], "gid_bits": h["W"]}

    # -------- DuckDB controls --------
    print("=== DuckDB 2.0 controls on the 1B parquet ===", flush=True)
    spec = [{"label": name, "engine_sql": sql_range_engine(I32MIN, c["cut"])}
            for name, c in cuts.items()]
    base = duck_baselines_parquet(spec)
    rec["duckdb_baseline"] = base
    con = duckdb.connect(":memory:", config={"threads": 1})
    con.execute(f"CREATE VIEW groups AS SELECT * FROM read_parquet('{PARQUET}')")

    # -------- our series: 1B and 100M G=101 --------
    for label, (path, nrows) in (("series_1b", (ST1B, N1B)),
                                 ("series_100m_g101", (CL100, N100))):
        print(f"=== {label} ===", flush=True)
        for name, c in cuts.items():
            lo, hi = I32MIN, c["cut"]
            outj = os.path.join(SCRATCH, f"ours1b_{label}_{name}.json")
            q = bench_fixed_reps(path, lo, hi, outj)
            ours_rows = json.load(open(outj))["rows"]
            rec[label][name] = {
                "lo": lo, "hi": hi, "target_selectivity": c["target"],
                "actual_selectivity": q["selectivity"], "scan_rows": q["scan_rows"],
                "rows": len(ours_rows), "fast_path": q["fast_path"],
                "ours_median_ms": q["query_median_ms"],
                "ours_p95_ms": q["query_p95_ms"],
                "bench_medians_ms": q["bench_medians_ms"],
                "bench_p95_ms": q["bench_p95_ms"],
                "bench_spread": q["bench_spread"], "reps": q["reps"],
                "ours_aggregate_ms": q["aggregate_median_ms"],
                "ours_reset_ms": q["reset_median_ms"],
                "ns_per_matching_row": (q["query_median_ms"] * 1e6 / q["scan_rows"]
                                        if q["scan_rows"] else None),
                "exact_vs_duckdb": None,
            }
            if label == "series_1b":
                d1 = base[1]["results"][name]
                d16 = base[16]["results"][name]
                rec[label][name].update({
                    "duck1_median_ms": d1["median_ms"], "duck1_min_ms": d1["min_ms"],
                    "duck1_max_ms": d1["max_ms"], "duck1_reps": d1["reps"],
                    "duck16_median_ms": d16["median_ms"],
                    "x_duck1": d1["median_ms"] / q["query_median_ms"],
                    "x_duck16": d16["median_ms"] / q["query_median_ms"]})
            # Exactness on the full grouped result, as a sorted multiset: DuckDB
            # sorts the NULL group key last in ORDER BY, our gid=-1 row sorts
            # first, and both orders are legitimate -- the project's protocol is
            # "sorted multisets, NULL group matched as NULL" (TIERS §5).
            if label == "series_1b":
                rows_duck, fetch_ms = rc.duck_rows(con, sql_range(lo, hi))
                a, b = norm_ours_1b(ours_rows), rc.norm_duck(rows_duck)
                exact = sorted(a) == sorted(b)
                rec[label][name]["exact_vs_duckdb"] = exact
                rec[label][name]["exact_protocol"] = "sorted multiset of (group, SUM, " \
                                                     "COUNT(*), MIN, MAX), gid -1 == SQL NULL"
                rec[label][name]["duck_complete_fetchall_ms"] = fetch_ms
                if not exact:
                    sa, sb = set(a), set(b)
                    rec[label][name]["diff"] = {"only_ours": sorted(sa - sb)[:5],
                                                "only_duck": sorted(sb - sa)[:5]}
            else:
                # the 100M G=101 structure uses the tiers stream's synthetic key
                # (splitmix64 % 101), which has no DuckDB table: this series is the
                # n-scaling comparator only, the exactness claim is the 1B cell.
                exact = None
                rec[label][name]["exact_vs_duckdb"] = None
                rec[label][name]["exact_note"] = "no DuckDB equivalent for the synthetic " \
                                                 "key; n-scaling comparator only"
            r = rec[label][name]
            extra = (f" x1T={r['x_duck1']:.2f} x16T={r['x_duck16']:.2f}"
                     if label == "series_1b" else "")
            print(f"  [{label}/{name}] ours={r['ours_median_ms']:10.4f}ms "
                  f"P95={r['ours_p95_ms']:9.4f} rows={r['scan_rows']:,} "
                  f"ns/row={r['ns_per_matching_row']:.3f} exact={exact}{extra}", flush=True)

    # -------- the O(n) scan cost of the full-domain case --------
    print("=== full-domain O(n) scan (--no-fastpath) ===", flush=True)
    for label, path in (("1b", ST1B), ("100m_g101", CL100)):
        outj = os.path.join(SCRATCH, f"ours1b_scan_{label}.json")
        q = bench_fixed_reps(path, I32MIN, I32MAX, outj, reps=3, n_benches=2,
                            extra=["--no-fastpath"])
        key = "scan_1b" if label == "1b" else "scan_100m_g101"
        rec[key] = {"ours_median_ms": q["query_median_ms"], "rows": q["scan_rows"],
                    "reps": q["reps"],
                    "ns_per_matching_row": q["query_median_ms"] * 1e6 / q["scan_rows"]}
        print(f"  {label} full scan: {q['query_median_ms']:.2f}ms "
              f"({q['query_median_ms']*1e6/q['scan_rows']:.3f} ns/row)", flush=True)

    # -------- replica-map analytic check --------
    print("=== replica-map analytic check (1B: 10 identical replicas) ===", flush=True)
    rep = {"cases": {}, "note": "gid = passenger + 10*r, so for every passenger p in 0..9 "
                                "the 10 groups {p+10r} must have identical SUM/COUNT/MIN/MAX, "
                                "and the NULL group (-1) must be exactly 10x one replica"}
    for name, c in cuts.items():
        rows = json.load(open(os.path.join(SCRATCH, f"ours1b_series_1b_{name}.json")))[
            "rows"]
        by = {int(r[0]): tuple(int(x) for x in r[1:]) for r in rows}
        ok = True
        for p in range(10):
            ref = by.get(p)
            for r in range(10):
                g = p + 10 * r
                if g in by and by[g] != ref:
                    ok = False
        rep["cases"][name] = {"rows": len(rows), "replica_invariant": ok,
                              "null_group": by.get(-1)}
        print(f"  [{name}] rows={len(rows)} replica_invariant={ok} null={by.get(-1)}",
              flush=True)
    rep["all_ok"] = all(v["replica_invariant"] for v in rep["cases"].values())
    rec["replica_map"] = rep

    # -------- scale check: ns/matching-row, 100M vs 1B at G=101 --------
    sc = {"G_fixed": 101, "cases": {}}
    for name in cuts:
        a = rec["series_100m_g101"][name]["ns_per_matching_row"]
        b = rec["series_1b"][name]["ns_per_matching_row"]
        d = rec["series_1b"][name]
        sc["cases"][name] = {
            "rows_100m": rec["series_100m_g101"][name]["scan_rows"],
            "rows_1b": d["scan_rows"],
            "ns_per_row_100m": a, "ns_per_row_1b": b,
            "ratio_1b_over_100m": (b / a) if (a and b) else None,
            "n_ratio": (d["scan_rows"] /
                        rec["series_100m_g101"][name]["scan_rows"])}
    sc["note"] = ("p100 is answered from the stored per-group totals (O(G)), so its "
                  "ns-per-scan-row is not a scan cost and is excluded from the ratio "
                  "aggregate")
    valid = [v["ratio_1b_over_100m"] for k, v in sc["cases"].items()
             if v["ratio_1b_over_100m"] is not None and k != "p100"]
    sc["max_ratio"] = max(valid) if valid else None
    sc["min_ratio"] = min(valid) if valid else None
    rec["scale_check"] = sc

    # -------- footprint --------
    rec["footprint"] = {
        "rows": N1B, "tree_equivalent_bytes": TREE_EQ,
        "range_bytes": rec["build"]["range"]["file_bytes"],
        "range_ratio": rec["build"]["range"]["ratio_to_tree_equivalent"],
        "stats_bytes": rec["build"]["stats"]["file_bytes"],
        "stats_ratio": rec["build"]["stats"]["ratio_to_tree_equivalent"],
        "plain_columns_bytes": 2 * 4 * N1B,
        "ratio_to_plain": rec["build"]["stats"]["file_bytes"] / (8 * N1B),
        "bytes_per_row": rec["build"]["stats"]["file_bytes"] / N1B,
        "bits_per_row_gid": rec["build"]["stats"]["bits_per_row"]
                            if "bits_per_row" in rec["build"]["stats"] else h["W"],
        "within_budget_1.5x": rec["build"]["stats"]["ratio_to_tree_equivalent"] <= 1.5,
        "dense_cube_bytes_G101": 0,  # filled below from the cube alternative
        "dense_cube_note": "at G=101 the dense ECUBE02 cube fits (9,087,844 B = 0.0036x "
                           "tree-eq, SCALE_20261101 1.2) and is the right structure for "
                           "this G; CLST01's 1B row is the scale check of the high-G "
                           "design, whose cube alternative at G=100K/1M is 8.4/84.3 GB",
    }
    # dense cube alternative for THIS value column at the high-G cardinalities
    rec["footprint"]["dense_cube_bytes_G100k"] = 100_000 * 21084 * 4
    rec["footprint"]["dense_cube_bytes_G1m"] = 1_000_000 * 21084 * 4
    con.close()

    json.dump(rec, open(os.path.join(RECEIPT, "clustered_1b.json"), "w"), indent=1)
    json.dump(rec, open(os.path.join(SCRATCH, "clustered_1b.json"), "w"), indent=1)
    print("wrote", os.path.join(RECEIPT, "clustered_1b.json"), flush=True)
    print(json.dumps({kk: rec["footprint"][kk] for kk in
                      ("range_bytes", "range_ratio", "stats_bytes", "stats_ratio",
                       "bytes_per_row", "within_budget_1.5x")}, indent=1))
    print(json.dumps(rec["scale_check"], indent=1))


if __name__ == "__main__":
    main()
