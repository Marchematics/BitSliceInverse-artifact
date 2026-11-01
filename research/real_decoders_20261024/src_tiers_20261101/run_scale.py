#!/usr/bin/env python3
"""Scale tiers for the ECUBE02 aggregate cube (tiers stream).

For each dataset tier (IG2 65K / 983K / 10M, NYC 12M, TPC-H SF2 12M, NYC 100M):
  * build the pinned ECUBE02 index from the encoded tree and time it (core 96,
    nice 5, /usr/bin/time RSS),
  * engine-resident fixed and range queries (open + warmup + reps, median + P95),
  * the same complete grouped result from DuckDB 2.0.0 (threads=1, warm,
    execute()+fetchall(), 1 warmup + 30 reps),
  * element-wise exactness of every case (cube rows vs DuckDB rows).

Also measures the "short query" class at 65K: cold complete-request latency over
>=20 fresh process launches vs a cold DuckDB 2.0 connection on the same SQL.

Run (from research/real_decoders_20261024):
    TIERS_SCRATCH=$ARTIFACT_ROOT/work/tiers/build python3 \
        src_tiers_20261101/run_scale.py

Writes receipts into ../real_decoders_20261025/receipts/tiers_20261101/.
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import hashlib
import json
import os
import re
import statistics
import subprocess
import sys
import time

import duckdb

REPO = os.path.abspath(os.getcwd())
SCRATCH = _artifact.expand(os.environ.get("TIERS_SCRATCH", "../../work/tiers/build"))
BIN = _artifact.expand(os.environ.get("TIERS_BIN", "../../work/tiers/bin"))
PY = _artifact.expand("python3")
RECEIPT = os.path.join(_artifact.WORK, "receipts", "tiers_20261101")
CORE = _artifact.core()
CUBE = os.path.join(BIN, "enc_cube")
DBS = {
    "nyc100": os.path.join(REPO, "results/nyc_taxi_100m_duckdb/nyc.duckdb"),
    "nyc12": os.path.join(REPO, "results/nyc_taxi_2023_duckdb/nyc.duckdb"),
}
TPCH_PARQUET = os.path.join(REPO, "results/tpch_sf2_arrow/lineitem.parquet")

# dataset name -> cube spec / reference source
CONFIGS = [
    dict(name="ig2_65k", rows=65536, enc="results/ig2_prep_20261025/Observed.encoded",
         prep="results/ig2_prep_20261025/prepared/Observed", kind="ig2",
         parquet=os.path.join(SCRATCH, "ig2_65k.parquet"), reps=2000,
         enc_tree="results/ig2_prep_20261025/Observed.encoded"),
    dict(name="ig2_983k", rows=983040, enc="results/ig2_scale15_prep/Observed.encoded",
         prep="results/ig2_scale15_prep/Observed", kind="ig2",
         parquet=os.path.join(SCRATCH, "ig2_983k.parquet"), reps=1000,
         enc_tree="results/ig2_scale15_prep/Observed.encoded"),
    dict(name="ig2_10m", rows=10027008, enc="results/ig2_scale10m_prep/Observed.encoded",
         prep="results/ig2_scale10m_prep/Observed", kind="ig2",
         parquet=os.path.join(SCRATCH, "ig2_10m.parquet"), reps=1000,
         enc_tree="results/ig2_scale10m_prep/Observed.encoded"),
    dict(name="nyc12", rows=12000000, enc="results/nyc_taxi_2023_prep/Observed.encoded",
         prep="results/nyc_taxi_2023_prep/Observed", kind="nyc", db="nyc12",
         reps=1000, enc_tree="results/nyc_taxi_2023_prep/Observed.encoded"),
    dict(name="tpch12", rows=11997996, enc="results/tpch_sf2_prep/Observed.encoded",
         prep="results/tpch_sf2_prep/Observed", kind="tpch",
         reps=1000, enc_tree="results/tpch_sf2_prep/Observed.encoded"),
    dict(name="nyc100", rows=100000000, enc="results/nyc_taxi_100m_prep/Observed.encoded",
         prep="results/nyc_taxi_100m_prep/Observed", kind="nyc", db="nyc100",
         reps=1000, enc_tree="results/nyc_taxi_100m_prep/Observed.encoded"),
]


def sh(cmd, pin=True):
    """Run a command; returns (wall_s, stderr, stdout, rss_kb)."""
    pre = ["nice", "-n", "5", "taskset", "-c", CORE] if pin else []
    t = time.perf_counter()
    p = subprocess.run(["/usr/bin/time", "-v"] + pre + cmd, capture_output=True, text=True)
    wall = time.perf_counter() - t
    rss = 0.0
    for line in p.stderr.splitlines():
        if "Maximum resident set size" in line:
            rss = float(line.split(":")[1].strip())
    if p.returncode != 0:
        raise RuntimeError(f"{cmd[:3]} failed rc={p.returncode}\n{p.stderr[-1500:]}")
    return wall, p.stderr, p.stdout, rss


def last_json(s):
    for line in reversed(s.splitlines()):
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                return json.loads(line)
            except json.JSONDecodeError:
                continue
    raise RuntimeError("no json in stderr: " + s[-800:])


def warm_core(seconds=0.5):
    """Keep the pinned core at its boosted clock before a microsecond-scale bench.

    The ECUBE02 query is 1-10 us; a short bench after an idle period runs at a
    lower P-state and can read 2-3x slow (observed; see the measurement-dispersion
    check in the TIERS doc). A deterministic 0.5 s pinned busy loop puts the core
    at a stable clock first; the timed region itself is unchanged.
    """
    subprocess.run(["nice", "-n", "5", "taskset", "-c", CORE, "timeout", f"{seconds}s",
                    "bash", "-c", "while :; do :; done"], capture_output=True)


def stable_bench(cube, dictp, reps, spec, n=3):
    runs = []
    for _ in range(n):
        warm_core()
        _, stderr, _, _ = sh([CUBE, "bench", cube, dictp, "/dev/null", str(reps)] + spec)
        runs.append(last_json(stderr))
    runs.sort(key=lambda j: j["query_median_ms"])
    pick = runs[len(runs) // 2]
    meds = [j["query_median_ms"] for j in runs]
    spread = (max(meds) / min(meds)) if min(meds) > 0 else None
    return pick, runs, spread


def sha(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def row_norm_cube(rows):
    out = []
    for r in rows:
        g = str(r[0])
        if g in ("NULL", "null", "None"):
            g = "NULL"
        out.append((g,) + tuple(str(x) for x in r[1:]))
    return sorted(out)


def row_norm_duck(rows):
    out = []
    for r in rows:
        g = "NULL" if r[0] is None else str(r[0])
        out.append((g,) + tuple(str(x) for x in r[1:]))
    return sorted(out)


def duck_connect_for(cfg):
    """Return (con, table) with a 2-column table t(g,v) holding the reference rows."""
    if cfg["kind"] == "ig2":
        con = duckdb.connect()
        con.execute("SET threads=1")
        con.execute(f"CREATE TEMP TABLE t AS SELECT state AS g, pop AS v FROM read_parquet('{cfg['parquet']}')")
        return con, "t"
    if cfg["kind"] == "nyc":
        con = duckdb.connect(DBS[cfg["db"]], read_only=True)
        con.execute("SET threads=1")
        con.execute("CREATE TEMP TABLE t AS SELECT passenger_count AS g, fare_cents AS v FROM nyc")
        return con, "t"
    if cfg["kind"] == "tpch":
        con = duckdb.connect()
        con.execute("SET threads=1")
        con.execute(
            "CREATE TEMP TABLE t AS SELECT l_returnflag AS g, "
            f"CAST(l_extendedprice*100 AS BIGINT) AS v FROM read_parquet('{TPCH_PARQUET}')")
        return con, "t"
    raise ValueError(cfg["kind"])


def duck_where(cfg, cut, group_pred=True):
    parts = []
    if cfg["kind"] == "ig2" and group_pred:
        parts.append("g NOT IN ('District of Columbia')")
    if cut is not None:
        parts.append(f"v <= {cut}")
    return (" WHERE " + " AND ".join(parts)) if parts else ""


def cube_specs(cfg, cuts):
    base = ["where_not_in=District of Columbia"] if cfg["kind"] == "ig2" else []
    st = cuts[{"tpch12": "tpch"}.get(cfg["name"], cfg["name"])]
    out = {
        "fixed": list(base),
        "range_p10": base + [f"hi={st['p10']}"],
        "range_p50": base + [f"hi={st['p50']}"],
    }
    if cfg["kind"] == "ig2":
        # the original IG2 SQL carries NOT IN ('District of Columbia'); this is the
        # same aggregation without the group-domain predicate, i.e. the pure cube cost
        out["fixed_all"] = []
    return out


def main():
    os.makedirs(RECEIPT, exist_ok=True)
    os.makedirs(SCRATCH, exist_ok=True)
    cuts = json.load(open(os.path.join(SCRATCH, "cuts.json")))["datasets"]
    out = {"protocol": {
        "host": "localhost (AMD EPYC 9554)",
        "pin": f"nice -n 5 taskset -c {CORE}",
        "cube": "pinned ECUBE02 binary built from eai_cube2.cpp sha256 99e4dcef...5f1543",
        "cube_protocol": "one process, open + 1 warmup + reps timed executions, median/P95; serialization timed separately",
        "duckdb": "2.0.0, threads=1, warm connection, 1 warmup + 30 execute()+fetchall()",
        "duckdb_version": duckdb.__version__,
        "core": CORE,
    }, "datasets": {}}
    for cfg in CONFIGS:
        name = cfg["name"]
        print(f"=== {name} ===", flush=True)
        rec = {"name": name, "rows": cfg["rows"], "cases": {}}
        enc = os.path.join(REPO, cfg["enc"])
        prep = os.path.join(REPO, cfg["prep"])
        outcube = os.path.join(SCRATCH, name + ".ecube")
        rec["encoded_tree_bytes"] = os.path.getsize(enc)
        # ---- build from the encoded tree -----------------------------------
        wall, stderr, _, rss = sh([CUBE, "build_enc", enc, prep, "17", "13", outcube])
        b = last_json(stderr)
        rec["build"] = {"wall_s": round(wall, 3), "build_ms": b["build_ms"],
                        "phase_ms": {k: b.get(k) for k in
                                     ("pass1_ms", "accum_ms", "prefix_ms", "write_ms")},
                        "peak_rss_kb": rss}
        rec["cube_bytes"] = os.path.getsize(outcube)
        rec["cube_sha256"] = sha(outcube)
        rec["G"] = b["groups"]
        rec["V"] = b["values"]
        rec["encoded"] = b.get("encoded", 1)
        rec["index_to_tree"] = rec["cube_bytes"] / rec["encoded_tree_bytes"]
        # ---- DuckDB reference connection / table ---------------------------
        con, table = duck_connect_for(cfg)
        specs = cube_specs(cfg, cuts)
        cutkey = {"tpch12": "tpch"}.get(name, name)
        for case, spec in specs.items():
            cut = cuts[cutkey]["p10"] if case == "range_p10" else (
                cuts[cutkey]["p50"] if case == "range_p50" else None)
            group_pred = case != "fixed_all"
            # cube engine-resident (median of 3 bench runs, each after a core warmup)
            cj, cj_runs, cj_spread = stable_bench(outcube, os.path.join(prep, "state.dict"),
                                                  cfg["reps"], spec)
            # cube complete result
            qout = os.path.join(SCRATCH, f"{name}_{case}.json")
            sh([CUBE, "query", outcube, os.path.join(prep, "state.dict"), qout] + spec)
            cube_rows = json.load(open(qout))["rows"]
            # duckdb warm engine
            sql = (f"SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM {table}"
                   + duck_where(cfg, cut, group_pred) + " GROUP BY 1 ORDER BY 1")
            con.execute(sql).fetchall()  # warmup
            ts, drows = [], None
            for _ in range(30):
                t = time.perf_counter()
                drows = con.execute(sql).fetchall()
                ts.append((time.perf_counter() - t) * 1000.0)
            ts.sort()
            dmed = statistics.median(ts)
            dp95 = ts[min(len(ts) - 1, int(len(ts) * 0.95))]
            cube_n = row_norm_cube(cube_rows)
            duck_n = row_norm_duck(drows)
            rec["cases"][case] = {
                "spec": spec,
                "cube_median_us": cj["query_median_ms"] * 1000,
                "cube_p95_us": cj["query_p95_ms"] * 1000,
                "cube_serialize_us": cj["serialize_median_ms"] * 1000,
                "cube_output_ms": cj["output_ms"],
                "duckdb_median_ms": dmed,
                "duckdb_p95_ms": dp95,
                "duckdb_rows": len(drows),
                "cube_rows": len(cube_rows),
                "selectivity": sum(int(r[2]) for r in cube_rows) / cfg["rows"],
                "exact": cube_n == duck_n,
                "ratio_median": dmed / cj["query_median_ms"] if cj["query_median_ms"] else None,
                "cube_bench_runs": [{"median_us": j["query_median_ms"] * 1000,
                                     "p95_us": j["query_p95_ms"] * 1000,
                                     "serialize_us": j["serialize_median_ms"] * 1000}
                                    for j in cj_runs],
                "cube_bench_spread": cj_spread,
                "duckdb_sql": sql,
            }
            if not rec["cases"][case]["exact"]:
                a, bb = set(cube_n), set(duck_n)
                rec["cases"][case]["diff_sample"] = {
                    "only_duckdb": sorted(bb - a)[:5],
                    "only_cube": sorted(a - bb)[:5],
                }
            print(f"  {case:10s} cube={cj['query_median_ms']*1000:8.2f}us "
                  f"duck={dmed:9.3f}ms rows={len(cube_rows)} exact={rec['cases'][case]['exact']}",
                  flush=True)
        con.close()
        json.dump(rec, open(os.path.join(SCRATCH, f"scale_{name}.json"), "w"), indent=1)
        out["datasets"][name] = rec

    # ------------------------------------------------------------------
    # 65K short query class: cold complete request vs cold DuckDB 2.0
    # ------------------------------------------------------------------
    print("=== cold complete request @65K ===", flush=True)
    cfg = CONFIGS[0]
    prep = os.path.join(REPO, cfg["prep"])
    cube = os.path.join(SCRATCH, "ig2_65k.ecube")
    dictp = os.path.join(prep, "state.dict")
    cold = {"launches": 25, "pin": f"nice -n 5 taskset -c {CORE}", "cube": [], "duckdb": [],
            "protocol": "each launch is a fresh process; one 0.5 s pinned warmup before the "
                        "launch sequence and between launches, so the number is not a "
                        "P-state artifact (the core is otherwise quiet)"}
    for i in range(25):
        warm_core(0.5)
        o = os.path.join(SCRATCH, f"cold_{i}.json")
        t = time.perf_counter()
        p = subprocess.run(["nice", "-n", "5", "taskset", "-c", CORE, CUBE, "query", cube,
                            dictp, o, "where_not_in=District of Columbia"],
                           capture_output=True, text=True)
        w = (time.perf_counter() - t) * 1000.0
        j = last_json(p.stderr)
        cold["cube"].append({"total_ms": w, "setup_ms": j["setup_ms"],
                             "query_us": j["query_median_ms"] * 1000,
                             "serialize_us": j["serialize_median_ms"] * 1000,
                             "output_ms": j["output_ms"]})
    child = r'''
import json,sys,time
t0=time.perf_counter()
import duckdb
t1=time.perf_counter()
con=duckdb.connect()
t2=time.perf_counter()
con.execute("CREATE TEMP TABLE t AS SELECT state AS g, pop AS v FROM read_parquet('%s')")
t3=time.perf_counter()
rows=con.execute("SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM t WHERE g NOT IN ('District of Columbia') GROUP BY 1").fetchall()
t4=time.perf_counter()
json.dump([[("NULL" if r[0] is None else str(r[0]))]+[str(x) for x in r[1:]] for r in rows],open('%s','w'))
t5=time.perf_counter()
print(json.dumps({"import_ms":(t1-t0)*1000,"connect_ms":(t2-t1)*1000,"table_ms":(t3-t2)*1000,"query_ms":(t4-t3)*1000,"write_ms":(t5-t4)*1000,"rows":len(rows)}))
''' % (cfg["parquet"].replace("\\", "/"),
       os.path.join(SCRATCH, "cold_duck.json").replace("\\", "/"))
    for i in range(21):
        warm_core(0.5)
        t = time.perf_counter()
        p = subprocess.run(["nice", "-n", "5", "taskset", "-c", CORE, PY, "-c", child],
                           capture_output=True, text=True)
        w = (time.perf_counter() - t) * 1000.0
        j = last_json(p.stdout)
        j["total_ms"] = w
        cold["duckdb"].append(j)
    for k in ("cube", "duckdb"):
        arr = sorted(x["total_ms"] for x in cold[k])
        cold[k + "_median_ms"] = statistics.median(arr)
        cold[k + "_p95_ms"] = arr[min(len(arr) - 1, int(len(arr) * 0.95))]
        cold[k + "_min_ms"] = arr[0]
    # cross-check the cold outputs on both sides
    a = row_norm_cube(json.load(open(os.path.join(SCRATCH, "cold_24.json")))["rows"])
    b = row_norm_duck(json.load(open(os.path.join(SCRATCH, "cold_duck.json"))))
    cold["exact"] = a == b
    cold["rows"] = len(a)
    cold["cube_median_sub"] = {
        "setup_ms": statistics.median(x["setup_ms"] for x in cold["cube"]),
        "query_us": statistics.median(x["query_us"] for x in cold["cube"]),
        "serialize_us": statistics.median(x["serialize_us"] for x in cold["cube"]),
        "output_ms": statistics.median(x["output_ms"] for x in cold["cube"]),
    }
    cold["duckdb_median_sub"] = {f: statistics.median(x[f] for x in cold["duckdb"])
                                 for f in ("import_ms", "connect_ms", "table_ms",
                                           "query_ms", "write_ms")}
    out["cold_request_65k"] = cold
    print(json.dumps({k: cold[k] for k in cold if not isinstance(cold[k], list)}, indent=1),
          flush=True)

    json.dump(out, open(os.path.join(RECEIPT, "scale_tiers.json"), "w"), indent=1)
    json.dump(out, open(os.path.join(SCRATCH, "scale_tiers.json"), "w"), indent=1)
    print("wrote", os.path.join(RECEIPT, "scale_tiers.json"), flush=True)


if __name__ == "__main__":
    main()
