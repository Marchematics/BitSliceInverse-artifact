#!/usr/bin/env python3
"""Build/amortization table for the exact-index family (tiers stream).

T_total(Q) = T_build + Q * T_query, compared with the strongest *applicable*
baseline per configuration:
  * single-column ECUBE02 cube 100M  -> DuckDB 2.0 scan (threads=1, warm)
  * MEC TPC-H SF2 conjunction        -> DuckDB 2.0 on the same 4 columns/conjunction
  * materialized 1M-group stats      -> DuckDB 2.0 full aggregation (engine time)
  * DuckDB scan baseline             -> itself (reference row, T_build = 0)

For each configuration: build seconds, per-query ms, break-even Q
(T_build / (T_base - T_query)), the Q at which the cumulative ratio reaches 2x, and
the cumulative cost + ratio at Q = 1, 10, 100, 1000.

Run after run_scale.py and run_groups.py:
    TIERS_SCRATCH=$ARTIFACT_ROOT/work/tiers/build python3 \
        src_tiers_20261101/run_amort.py
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import json
import os
import statistics
import subprocess
import sys
import time

import duckdb

REPO = os.path.abspath(os.getcwd())
SCRATCH = _artifact.expand(os.environ.get("TIERS_SCRATCH", "../../work/tiers/build"))
BIN = _artifact.expand(os.environ.get("TIERS_BIN", "../../work/tiers/bin"))
RECEIPT = os.path.join(_artifact.WORK, "receipts", "tiers_20261101")
CORE = _artifact.core()
MEC = os.path.join(REPO, "src_eai_multi_20261101", "mec")
MEC_DS = os.path.join(REPO, "results/eai_multi_20261101/datasets/tpch2")


def sh(cmd, pin=True):
    pre = ["nice", "-n", "5", "taskset", "-c", CORE] if pin else []
    t = time.perf_counter()
    p = subprocess.run(["/usr/bin/time", "-v"] + pre + cmd, capture_output=True, text=True)
    wall = time.perf_counter() - t
    rss = 0.0
    for line in p.stderr.splitlines():
        if "Maximum resident set size" in line:
            rss = float(line.split(":")[1].strip())
    if p.returncode != 0:
        raise RuntimeError(f"{cmd[:4]} rc={p.returncode}\n{p.stderr[-1200:]}\n{p.stdout[-1200:]}")
    return wall, p.stderr, p.stdout, rss


def last_json(s):
    for line in reversed(s.splitlines()):
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                return json.loads(line)
            except json.JSONDecodeError:
                continue
    raise RuntimeError("no json: " + s[-600:])


def mec_measure():
    os.makedirs(SCRATCH, exist_ok=True)
    idx = os.path.join(SCRATCH, "tpch2_index.mec")
    ds = MEC_DS
    budget = os.path.getsize(os.path.join(ds, "data.parquet")) // 2
    cols = [os.path.join(ds, c) for c in ("g.i64", "v.i64", "c1.i64", "c2.i64")]
    wall, stderr, stdout, rss = sh([MEC, "build"] + cols + [idx, "0", str(budget)])
    build = last_json(stderr) if stderr.strip().endswith("}") else None
    build_wall = wall
    wallb, stderrb, stdoutb, rssb = sh([MEC, "bench", idx, "200",
                                        os.path.join(ds, "cases.txt")])
    sweep = json.loads(stdoutb)
    # strongest applicable baseline: DuckDB 2.0 on the same 4 columns / same case
    con = duckdb.connect()
    con.execute("SET threads=1")
    con.execute(f"CREATE TEMP TABLE t AS SELECT * FROM read_parquet('{ds}/data.parquet')")
    case = sweep["cases"]["broad"]
    v0, v1, a0, a1, b0, b1 = case["params"]
    sql = ("SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM t "
           f"WHERE v BETWEEN {v0} AND {v1} AND c1 BETWEEN {a0} AND {a1} "
           f"AND c2 BETWEEN {b0} AND {b1} GROUP BY 1 ORDER BY 1")
    con.execute(sql).fetchall()
    ts = []
    for _ in range(30):
        t = time.perf_counter()
        rows = con.execute(sql).fetchall()
        ts.append((time.perf_counter() - t) * 1000.0)
    ts.sort()
    con.close()
    return {
        "mec_build_wall_s": build_wall, "mec_index_bytes": os.path.getsize(idx),
        "mec_build_json": build,
        "mec_bench_reps": 200,
        "case": "broad",
        "mec_params": case["params"],
        "mec_full_median_us": case["median_us"],
        "mec_full_p95_us": case["p95_us"],
        "mec_sc_median_us": case["sc"]["median_us"],
        "mec_sc_p95_us": case["sc"]["p95_us"],
        "mec_rows": len(case.get("rows", [])),
        "duckdb_median_ms": statistics.median(ts),
        "duckdb_p95_ms": ts[min(len(ts) - 1, int(len(ts) * 0.95))],
        "duckdb_sql": sql,
        "duckdb_version": duckdb.__version__,
        "note": "MEC full = SUM/COUNT/MIN/MAX (comparable to the cube's five-field "
                "result); sc = SUM/COUNT only; DuckDB baseline is the same five "
                "aggregates over the same 4 columns, 1 warmup + 30 reps.",
    }


def amort_row(name, builder, build_ms, query_ms, base_ms, detail=""):
    qstar = build_ms / (base_ms - query_ms) if base_ms > query_ms else None
    q2 = build_ms / (base_ms / 2.0 - query_ms) if base_ms / 2.0 > query_ms else None
    cum = {}
    for Q in (1, 10, 100, 1000):
        cost = build_ms + Q * query_ms
        ratio = (Q * base_ms) / cost
        cum[str(Q)] = {"cost_ms": cost, "ratio": ratio, "ge_2x": ratio >= 2.0}
    return {"name": name, "builder": builder, "build_ms": build_ms,
            "query_ms": query_ms, "baseline_ms": base_ms,
            "break_even_Q": qstar, "Q_2x": q2, "cumulative": cum, "detail": detail}


def main():
    os.makedirs(RECEIPT, exist_ok=True)
    scale = json.load(open(os.path.join(RECEIPT, "scale_tiers.json")))
    groups = json.load(open(os.path.join(RECEIPT, "group_tiers.json")))
    out = {"protocol": {
        "pin": f"nice -n 5 taskset -c {CORE}",
        "T_total": "T_build + Q*T_query, both sides single-thread, warm engine",
        "break_even_Q": "T_build/(T_base - T_query)",
        "Q_2x": "smallest Q with cumulative baseline cost / cumulative cost >= 2",
    }, "configs": {}}

    print("=== MEC TPC-H SF2 (build + bench, pinned) ===", flush=True)
    mec = mec_measure()
    out["mec_measurement"] = mec
    print(json.dumps({k: mec[k] for k in ("mec_build_wall_s", "mec_full_median_us",
                                         "mec_sc_median_us", "duckdb_median_ms")}, indent=1),
          flush=True)

    # 1) single-column ECUBE02 cube, NYC 100M, fixed GROUP BY
    nyc = scale["datasets"]["nyc100"]
    c = nyc["cases"]["fixed"]
    out["configs"]["cube_100m"] = amort_row(
        "single-column ECUBE02 cube, NYC-100M fixed GROUP BY",
        "build_enc from the 251.8 MB encoded tree -> 1.14 MB index",
        nyc["build"]["wall_s"] * 1000.0, c["cube_median_us"] / 1000.0,
        c["duckdb_median_ms"],
        detail=f"index {nyc['cube_bytes']} B; DuckDB scan median {c['duckdb_median_ms']:.1f} ms")

    # 2) MEC TPC-H SF2 multi-column conjunction (case broad)
    out["configs"]["mec_tpch2"] = amort_row(
        "MEC02 TPC-H SF2 3-predicate conjunction (case broad, SUM/COUNT/MIN/MAX)",
        "mec build from 4 raw i64 columns, auto-fit -> 36.9 MB index",
        mec["mec_build_wall_s"] * 1000.0, mec["mec_full_median_us"] / 1000.0,
        mec["duckdb_median_ms"],
        detail=f"index {mec['mec_index_bytes']} B; MEC sc(SUM/COUNT) "
               f"{mec['mec_sc_median_us']:.2f} us; DuckDB {mec['duckdb_median_ms']:.2f} ms")

    # 3) materialized 1M-group stats (router path), fixed query
    g1m = groups["tiers"]["G1000000"]
    st = g1m["stats"]
    dfix = g1m["duckdb"]["fixed"]
    out["configs"]["materialized_1m"] = amort_row(
        "materialized per-group stats, 1,000,000 groups (100M rows)",
        "one pass over value+gid columns -> 32 MB (32 B/group)",
        st["build_wall_s"] * 1000.0, st["emit"]["select_median_ms"],
        dfix["engine_median_ms"],
        detail=f"{st['bytes']} B; DuckDB engine (full aggregation, scalar result) "
               f"{dfix['engine_median_ms']:.1f} ms; complete fetchall "
               f"{dfix['complete_median_ms']:.1f} ms")

    # 4) DuckDB scan baseline reference row (no build; persistent database)
    base_ms = nyc["cases"]["fixed"]["duckdb_median_ms"]
    out["configs"]["duckdb_scan"] = amort_row(
        "DuckDB 2.0 scan baseline (reference row)", "persistent .duckdb table; load excluded",
        0.0, base_ms, base_ms,
        detail="T_build=0 (assumes an already-loaded DB); this is the denominator of "
               "every ratio in the table")

    json.dump(out, open(os.path.join(RECEIPT, "amortization.json"), "w"), indent=1)
    json.dump(out, open(os.path.join(SCRATCH, "amortization.json"), "w"), indent=1)
    print(json.dumps({k: {kk: v[kk] for kk in ("build_ms", "query_ms", "baseline_ms",
                                               "break_even_Q", "Q_2x")}
                      for k, v in out["configs"].items()}, indent=1), flush=True)
    print("wrote", os.path.join(RECEIPT, "amortization.json"))


if __name__ == "__main__":
    main()
