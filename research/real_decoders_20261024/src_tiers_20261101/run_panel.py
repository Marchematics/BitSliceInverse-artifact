#!/usr/bin/env python3
"""50-query mixed panel for metric J (>=90% of queries faster than the strongest control).

Queries are grouped by the structure the router picks:
  A. NYC-100M single-column, ours = ECUBE03 (25 queries: fixed / ranges / IN / NOT IN / HAVING)
  B. TPC-H SF2 12M multi-column, ours = MEC02 (10 documented conjunction cases)
  C. NYC-100M high group cardinality, ours = CLST01 with per-group stats (10 range queries
     at G=100,000 and G=1,000,000)
  D. IG2 65,536 short-query class, ours = ECUBE03 (5 queries)

For every query, in the same window:
  * ours: engine-resident median + P95 (>=1,000 reps for the microsecond cube tiers,
    MEC's 200-rep in-process bench, CLST01's 100-rep in-process bench; 3 independent
    runs after a deterministic 0.5 s pinned busy loop, median of the medians);
  * the strongest applicable control: DuckDB 2.0.0 1T scan (forced full aggregation so
    the client result materialization is excluded), DuckDB materialized-view control for
    fixed-class queries, the sparse value index for single-column ranges, the per-group
    bitplane and the sorted row-id range index for multi-column conjunctions.
    The chosen control gets 100 reps; secondary candidates use the frozen protocol.
  * element-wise exactness of the full grouped result against DuckDB.

Writes results/tiers_20261101/panel_results.json + PANEL_20261101.md and the same JSON
into receipts/tiers_20261101/. Core affinity: subprocesses pinned to core 96, the driver
to core 97; every command line is recorded in the receipt.

usage:  taskset -c 97 python3 src_tiers_20261101/run_panel.py [--quick]
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import hashlib
import json
import math
import os
import statistics
import subprocess
import sys
import time

import duckdb

REPO = os.path.abspath(os.getcwd())
SCRATCH = _artifact.expand(os.environ.get("TIERS_SCRATCH", "../../work/tiers/build"))
BIN = _artifact.expand(os.environ.get("TIERS_BIN", "../../work/tiers/bin"))
CORE = _artifact.core()
RECEIPT = os.path.join(_artifact.WORK, "receipts", "tiers_20261101")
RESULTS = os.path.join(REPO, "results", "tiers_20261101")
CUBE3 = os.path.join(BIN, "enc_cube3")
MEC = os.path.join(REPO, "src_eai_multi_20261101", "mec")
SIDX = os.path.join(REPO, "src_eai_multi_20261101", "sorted_index")
BP = os.path.join(REPO, "src_encoded_ops_20261030", "multibits")
VIDX = os.path.join(REPO, "src_encoded_ops_20261030", "value_index_v2")
VIDX_BUILD = os.path.join(REPO, "src_encoded_ops_20261030", "value_index_build")
CLST = _artifact.expand("../../work/clustered/bin/clst_tool")
CLST_S = _artifact.expand("../../work/clustered/build")
NYC_DB = os.path.join(REPO, "results/nyc_taxi_100m_duckdb/nyc.duckdb")
GROUPS_DB = os.path.join(SCRATCH, "groups.duckdb")
IG2_PARQ = os.path.join(SCRATCH, "ig2_65k.parquet")
NYC_PREP = os.path.join(REPO, "results/nyc_taxi_100m_prep/Observed")
IG2_ENC = os.path.join(REPO, "results/ig2_prep_20261025/Observed.encoded")
IG2_PREP = os.path.join(REPO, "results/ig2_prep_20261025/prepared/Observed")
MEC_DS = os.path.join(REPO, "results/eai_multi_20261101/datasets/tpch2")
MEC_IDX = os.path.join(SCRATCH, "tpch2_index.mec")
BP_IDX = os.path.join(SCRATCH, "tpch2_mb.bin")
SIDX_IDX = os.path.join(SCRATCH, "tpch2_ridx.bin")
VIDX_IDX = os.path.join(SCRATCH, "nyc100.vidx")
VIDX_IG2 = os.path.join(SCRATCH, "ig2_65k.vidx")
QUICK = "--quick" in sys.argv


def sh(cmd, reps=1, warm=False):
    """Run cmd reps times, return list of (wall_s, stderr, stdout)."""
    out = []
    for _ in range(reps):
        if warm:
            warm_core()
        t = time.perf_counter()
        p = subprocess.run(["nice", "-n", "5", "taskset", "-c", CORE] + cmd,
                           capture_output=True, text=True)
        out.append((time.perf_counter() - t, p.stderr, p.stdout))
        if p.returncode != 0:
            raise RuntimeError(f"{cmd[:3]} rc={p.returncode}: {p.stderr[-1200:]}")
    return out


def warm_core(seconds=0.5):
    subprocess.run(["nice", "-n", "5", "taskset", "-c", CORE, "timeout", f"{seconds}s",
                    "bash", "-c", "while :; do :; done"], capture_output=True)


def last_json(s):
    for line in reversed(s.splitlines()):
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                return json.loads(line)
            except json.JSONDecodeError:
                continue
    raise RuntimeError("no json: " + s[-500:])


def sha(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def p95(xs):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * 0.95))]


def stat(xs):
    return {"median": statistics.median(xs), "p95": p95(xs), "min": min(xs),
            "max": max(xs), "n": len(xs)}


def canon_cube(rows):
    out = []
    for r in rows:
        g = str(r[0])
        if g in ("NULL", "None", "null"):
            g = "NULL"
        vals = []
        for x in r[1:]:
            s = str(x)
            vals.append("NULL" if s in ("None", "NULL", "null") else s)
        out.append(tuple([g] + vals))
    return sorted(out)


def canon_duck(rows):
    out = []
    for r in rows:
        g = "NULL" if r[0] is None else str(r[0])
        vals = []
        for x in r[1:]:
            vals.append("NULL" if x is None else str(x))
        out.append(tuple([g] + vals))
    return sorted(out)


def cube_bench(cube, dictp, reps, spec, runs=3):
    recs = []
    for _ in range(runs):
        warm_core()
        _, stderr, _ = sh([CUBE3, "bench", cube, dictp, "/dev/null", str(reps)] + spec)[0]
        recs.append(last_json(stderr))
    recs.sort(key=lambda j: j["query_median_ms"])
    pick = recs[len(recs) // 2]
    return {"median_us": pick["query_median_ms"] * 1000,
            "p95_us": pick["query_p95_ms"] * 1000,
            "serialize_us": pick["serialize_median_ms"] * 1000,
            "runs_us": [r["query_median_ms"] * 1000 for r in recs],
            "reps": reps, "runs": runs}


def duck_engine(con, sql, reps=100):
    con.execute(sql).fetchall()
    ts = []
    for _ in range(reps):
        t = time.perf_counter()
        con.execute(sql).fetchall()
        ts.append((time.perf_counter() - t) * 1000.0)
    return stat(ts), sql


def cli_internal(cmd, reps, key="query_ms"):
    xs = []
    for _ in range(reps):
        _, stderr, _ = sh(cmd)[0]
        xs.append(last_json(stderr)[key])
    return stat(xs), " ".join(cmd)


def build_cube(enc, prep, out):
    if os.path.exists(out):
        return None
    wall, stderr, _ = sh(["/usr/bin/time", "-v", CUBE3, "build_enc", enc, prep, "17", "13", out])[0]
    return {"wall_s": wall, "json": last_json(stderr)}


def build_vidx(enc, prep, out):
    if os.path.exists(out):
        return None
    wall, stderr, _ = sh(["/usr/bin/time", "-v", VIDX_BUILD, enc, prep, "17", "13", out])[0]
    return {"wall_s": wall, "json": last_json(stderr), "bytes": os.path.getsize(out)}


def main():
    os.makedirs(RECEIPT, exist_ok=True)
    os.makedirs(RESULTS, exist_ok=True)
    t_start = time.time()
    rec = {"protocol": {
        "host": "localhost (AMD EPYC 9554)", "date": "2026-11-01",
        "driver_pin": "nice -n 5 taskset -c 97",
        "subprocess_pin": f"nice -n 5 taskset -c {CORE}",
        "pstate": "0.5 s deterministic pinned busy loop before every timed bench run",
        "ours": "engine-resident, structure's own bench; no result serialization",
        "controls": "DuckDB 2.0.0 threads=1 (config), forced full aggregation "
                    "(SELECT COUNT(*) FROM (.. GROUP BY ..)) so client materialization "
                    "is excluded; MV control = same convention over the pre-aggregated "
                    "table; CLI controls = in-process query_ms over repeated launches",
        "reps": {"ours_cube": 1000, "ours_mec": 200, "ours_clst": 100,
                 "control_duckdb": 100, "control_secondary_duckdb": 30,
                 "control_bitplane_launches": 40, "control_sorted_index_launches": 20,
                 "control_value_index_launches": 100},
        "exactness": "element-wise full grouped result vs DuckDB 2.0.0 (group, SUM, "
                     "COUNT(*), MIN, MAX; NULL groups matched as NULL)",
        "artifacts": {
            "enc_cube3_bin_sha256": sha(CUBE3),
            "eai_cube3_src_sha256": sha(os.path.join(REPO, "src_eai_20261101/eai_cube3.cpp")),
            "mec_bin_sha256": sha(MEC), "mec_index_sha256": sha(MEC_IDX),
            "sorted_index_bin_sha256": sha(SIDX), "sorted_index_sha256": sha(SIDX_IDX),
            "bitplane_bin_sha256": sha(BP), "bitplane_sha256": sha(BP_IDX),
            "value_index_sha256": sha(VIDX_IDX),
            "clst_tool_sha256": sha(CLST),
            "clst_G100000_sha256": sha(os.path.join(CLST_S, "clst_g100000_stats.clst")),
            "clst_G1000000_sha256": sha(os.path.join(CLST_S, "clst_g1000000_stats.clst")),
        },
        "duckdb_version": duckdb.__version__,
    }, "queries": {}}

    # ------------------------------------------------------------------ A ---
    nyc_cube = os.path.join(SCRATCH, "nyc100_e3.ecube")
    rec.setdefault("builds", {})["nyc100_ecube03"] = build_cube(
        os.path.join(REPO, "results/nyc_taxi_100m_prep/Observed.encoded"), NYC_PREP, nyc_cube)
    if rec["builds"]["nyc100_ecube03"] is not None:
        rec["builds"]["nyc100_ecube03"]["bytes"] = os.path.getsize(nyc_cube)
    ig2_cube = os.path.join(SCRATCH, "ig2_65k_e3.ecube")
    rec["builds"]["ig2_65k_ecube03"] = build_cube(IG2_ENC, IG2_PREP, ig2_cube)
    if rec["builds"]["ig2_65k_ecube03"] is not None:
        rec["builds"]["ig2_65k_ecube03"]["bytes"] = os.path.getsize(ig2_cube)
    rec["builds"]["nyc100_value_index"] = build_vidx(
        os.path.join(REPO, "results/nyc_taxi_100m_prep/Observed.encoded"), NYC_PREP, VIDX_IDX)
    rec["builds"]["ig2_65k_value_index"] = build_vidx(IG2_ENC, IG2_PREP, VIDX_IG2)
    rec["value_index_bytes"] = os.path.getsize(VIDX_IDX)

    # ECUBE03 group-name / dict: NYC value column has no NULL payload; semantics = ECUBE02
    nyc_dict = os.path.join(NYC_PREP, "state.dict")
    full5 = ("SELECT passenger_count, CAST(SUM(fare_cents) AS BIGINT), COUNT(*), "
             "MIN(fare_cents), MAX(fare_cents) FROM nyc")
    A = [
        ("A01", "fixed all-four", [], "fixed", None, None),
        ("A02", "v <= -7000 (0.135%)", ["hi=-7000"], "range", "fare_cents <= -7000", (-2147483648, -7000)),
        ("A03", "v <= -720 (1.04%)", ["hi=-720"], "range", "fare_cents <= -720", (-2147483648, -720)),
        ("A04", "v <= 509 (5.8%)", ["hi=509"], "range", "fare_cents <= 509", (-2147483648, 509)),
        ("A05", "v <= 650 (11.9%)", ["hi=650"], "range", "fare_cents <= 650", (-2147483648, 650)),
        ("A06", "v <= 930 (28.4%)", ["hi=930"], "range", "fare_cents <= 930", (-2147483648, 930)),
        ("A07", "v <= 1350 (51%)", ["hi=1350"], "range", "fare_cents <= 1350", (-2147483648, 1350)),
        ("A08", "v <= 2500 (broad)", ["hi=2500"], "range", "fare_cents <= 2500", (-2147483648, 2500)),
        ("A09", "v <= 99900 (99.9999%)", ["hi=99900"], "range", "fare_cents <= 99900", (-2147483648, 99900)),
        ("A10", "needle 80M..90M (1 row)", ["range=80000000,90000000"], "range",
         "fare_cents BETWEEN 80000000 AND 90000000", (80000000, 90000000)),
        ("A11", "range -1000..1000", ["range=-1000,1000"], "range",
         "fare_cents BETWEEN -1000 AND 1000", (-1000, 1000)),
        ("A12", "range 0..300", ["range=0,300"], "range", "fare_cents BETWEEN 0 AND 300", (0, 300)),
        ("A13", "v >= 1300 (lo-only)", ["lo=1300"], "range", "fare_cents >= 1300", (1300, 2147483647)),
        ("A14", "g IN (1,2)", ["where_in=1,2"], "fixed", "passenger_count IN (1,2)", None),
        ("A15", "g IN (1)", ["where_in=1"], "fixed", "passenger_count IN (1)", None),
        ("A16", "g IN (1,2,3,4)", ["where_in=1,2,3,4"], "fixed", "passenger_count IN (1,2,3,4)", None),
        ("A17", "g NOT IN (1)", ["where_not_in=1"], "fixed", "passenger_count NOT IN (1)", None),
        ("A18", "g NOT IN (0)", ["where_not_in=0"], "fixed", "passenger_count NOT IN (0)", None),
        ("A19", "HAVING sum >= 1e10", ["having=sum>=1e10"], "fixed", None, None),
        ("A20", "HAVING count >= 1e6", ["having=count>=1e6"], "fixed", None, None),
        ("A21", "HAVING max >= 1e6", ["having=max>=1e6"], "fixed", None, None),
        ("A22", "HAVING min >= -100000", ["having=min>=-100000"], "fixed", None, None),
        ("A23", "empty range (lo > hi)", ["range=100,50"], "range", "fare_cents BETWEEN 100 AND 50", (100, 50)),
        ("A24", "full i32 range", ["range=-2147483648,2147483647"], "range",
         "fare_cents BETWEEN -2147483648 AND 2147483647", (-2147483648, 2147483647)),
        ("A25", "single extreme value", ["range=-226119,-226119"], "range",
         "fare_cents = -226119", (-226119, -226119)),
    ]
    MVS_WHERE = {
        "A01": "", "A14": "g IN (1,2)", "A15": "g IN (1)", "A16": "g IN (1,2,3,4)",
        "A17": "g NOT IN (1)", "A18": "g NOT IN (0)",
        "A19": "s >= 1e10", "A20": "c >= 1e6", "A21": "mx >= 1e6", "A22": "mn >= -100000",
    }
    con = duckdb.connect(NYC_DB, read_only=True, config={"threads": 1})
    rec["protocol"]["duckdb_threads_effective"] = con.execute(
        "SELECT current_setting('threads')").fetchall()[0][0]
    t = time.perf_counter()
    con.execute("CREATE TEMP TABLE mv AS SELECT passenger_count AS g, CAST(SUM(fare_cents) AS BIGINT) AS s, "
                "COUNT(*) AS c, MIN(fare_cents) AS mn, MAX(fare_cents) AS mx FROM nyc GROUP BY 1")
    con.execute("SELECT * FROM mv").fetchall()
    rec["mv_build_ms"] = (time.perf_counter() - t) * 1000
    a_ids = [q[0] for q in A]
    for qid, desc, spec, kind, where_sql, vidx_range in A:
        if QUICK and qid not in ("A01", "A05", "A14", "A19", "A23"):
            continue
        q = {"id": qid, "group": "A", "desc": desc, "structure": "ECUBE03 (cube)",
             "kind": kind, "spec": spec}
        q["ours"] = cube_bench(nyc_cube, nyc_dict, 1000, spec)
        # exactness dump
        outj = os.path.join(SCRATCH, f"panel_{qid}.json")
        sh([CUBE3, "query", nyc_cube, nyc_dict, outj] + spec)
        cube_rows = json.load(open(outj))["rows"]
        if qid == "A24":
            duck_sql = full5 + " WHERE " + where_sql + " GROUP BY 1 ORDER BY 1"
        elif qid == "A25":
            duck_sql = full5 + " WHERE " + where_sql + " GROUP BY 1 ORDER BY 1"
        elif qid == "A23":
            duck_sql = full5 + " WHERE " + where_sql + " GROUP BY 1 ORDER BY 1"
        elif qid in ("A19", "A20", "A21", "A22"):
            hav = {"A19": "SUM(fare_cents) >= 1e10", "A20": "COUNT(*) >= 1e6",
                   "A21": "MAX(fare_cents) >= 1e6", "A22": "MIN(fare_cents) >= -100000"}[qid]
            duck_sql = full5 + " GROUP BY 1 HAVING " + hav + " ORDER BY 1"
        elif where_sql:
            duck_sql = full5 + " WHERE " + where_sql + " GROUP BY 1 ORDER BY 1"
        else:
            duck_sql = full5 + " GROUP BY 1 ORDER BY 1"
        drows = con.execute(duck_sql).fetchall()
        q["exact"] = canon_cube(cube_rows) == canon_duck(drows)
        q["duckdb_rows"] = len(drows)
        # controls
        scan_engine_sql = ("SELECT COUNT(*) FROM (" + (duck_sql.replace(" ORDER BY 1", ""))
                           + ") AS q")
        cand = {}
        if kind == "fixed":
            mvw = MVS_WHERE[qid]
            mv_sql = ("SELECT COUNT(*) FROM (SELECT g,s,c,mn,mx FROM mv"
                      + (" WHERE " + mvw if mvw else "") + ") AS q")
            cand["duckdb_mv"], _ = duck_engine(con, mv_sql, 100)
            cand["duckdb_scan"], _ = duck_engine(con, scan_engine_sql, 30)
        else:
            cand["duckdb_scan"], _ = duck_engine(con, scan_engine_sql, 30)
            cand["value_index"], cmd = cli_internal(
                [VIDX, VIDX_IDX, nyc_dict, os.path.join(SCRATCH, f"vidx_{qid}.json"),
                 f"range={vidx_range[0]},{vidx_range[1]}"], 100)
            q["value_index_cmd"] = cmd
        q["controls"] = cand
        q["baseline_name"] = min(cand, key=lambda k: cand[k]["median"])
        q["baseline_median_ms"] = cand[q["baseline_name"]]["median"]
        q["baseline_p95_ms"] = cand[q["baseline_name"]]["p95"]
        q["S"] = q["baseline_median_ms"] * 1000.0 / q["ours"]["median_us"]
        q["duckdb_engine_sql"] = scan_engine_sql
        rec["queries"][qid] = q
        print(f"{qid} {desc:26s} ours={q['ours']['median_us']:9.2f}us "
              f"base({q['baseline_name']})={q['baseline_median_ms']:9.3f}ms "
              f"S={q['S']:12.1f}x exact={q['exact']}", flush=True)
    con.close()

    # ------------------------------------------------------------------ B ---
    con = duckdb.connect(config={"threads": 1})
    con.execute(f"CREATE TEMP TABLE t AS SELECT * FROM read_parquet('{MEC_DS}/data.parquet')")
    con.execute("SELECT COUNT(*) FROM t").fetchall()
    b = sh([MEC, "bench", MEC_IDX, "200", os.path.join(MEC_DS, "cases.txt")])[0]
    mec = json.loads(b[2])
    cases = []
    with open(os.path.join(MEC_DS, "cases.txt")) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            p = line.split()
            if len(p) == 7:
                cases.append(dict(name=p[0], v=[int(p[1]), int(p[2])], c1=[int(p[3]), int(p[4])],
                                  c2=[int(p[5]), int(p[6])]))
    sc_sql = ("SELECT g, SUM(v) AS s, COUNT(*) AS c FROM t WHERE v BETWEEN {v0} AND {v1} "
              "AND c1 BETWEEN {a0} AND {a1} AND c2 BETWEEN {b0} AND {b1} GROUP BY 1 ORDER BY 1")
    full_sql = ("SELECT g, CAST(SUM(CAST(v AS HUGEINT)) AS HUGEINT), COUNT(*), MIN(v), MAX(v) "
                "FROM t WHERE v BETWEEN {v0} AND {v1} AND c1 BETWEEN {a0} AND {a1} "
                "AND c2 BETWEEN {b0} AND {b1} GROUP BY 1 ORDER BY 1")
    for n, c in enumerate(cases):
        cid = f"B{n+1:02d}"
        if QUICK and cid not in ("B01", "B03", "B05"):
            continue
        f = dict(v0=c["v"][0], v1=c["v"][1], a0=c["c1"][0], a1=c["c1"][1],
                 b0=c["c2"][0], b1=c["c2"][1])
        m = mec["cases"][c["name"]]
        q = {"id": cid, "group": "B", "desc": f"tpch2:{c['name']}", "structure": "MEC02",
             "kind": "range", "params": f,
             "ours": {"median_us": m["median_us"], "p95_us": m["p95_us"],
                      "sc_median_us": m["sc"]["median_us"], "reps": mec["reps"]}}
        dsql = full_sql.format(**f)
        drows = con.execute(dsql).fetchall()
        # MEC full rows are in the bench output (rows: g,sum,count,min,max)
        mec_rows = [[r["g"], r["sum"], r["count"], r["min"], r["max"]] for r in m["rows"]]
        q["exact"] = canon_cube(mec_rows) == canon_duck(drows)
        q["duckdb_rows"] = len(drows)
        cand = {}
        scan_sql = ("SELECT COUNT(*) FROM (SELECT g, SUM(v), COUNT(*) FROM t WHERE "
                    f"v BETWEEN {f['v0']} AND {f['v1']} AND c1 BETWEEN {f['a0']} AND {f['a1']} "
                    f"AND c2 BETWEEN {f['b0']} AND {f['b1']} GROUP BY 1) AS q")
        cand["duckdb_scan_sc"], _ = duck_engine(con, scan_sql, 100)
        bpres, bcmd = cli_internal([BP, "query", BP_IDX, os.path.join(SCRATCH, f"bp_{cid}.json"),
                                    str(f["v0"]), str(f["v1"]), str(f["a0"]), str(f["a1"]),
                                    str(f["b0"]), str(f["b1"])], 40)
        cand["bitplane"] = bpres
        q["bitplane_cmd"] = bcmd
        sidxres, scmd = cli_internal([SIDX, "query", SIDX_IDX,
                                      os.path.join(MEC_DS, "g.i64"), os.path.join(MEC_DS, "v.i64"),
                                      os.path.join(MEC_DS, "c1.i64"), os.path.join(MEC_DS, "c2.i64"),
                                      os.path.join(SCRATCH, f"sidx_{cid}.json"),
                                      str(f["v0"]), str(f["v1"]), str(f["a0"]), str(f["a1"]),
                                      str(f["b0"]), str(f["b1"])], 20)
        cand["sorted_index"] = sidxres
        q["sorted_index_cmd"] = scmd
        q["controls"] = cand
        q["baseline_name"] = min(cand, key=lambda k: cand[k]["median"])
        q["baseline_median_ms"] = cand[q["baseline_name"]]["median"]
        q["baseline_p95_ms"] = cand[q["baseline_name"]]["p95"]
        q["S"] = q["baseline_median_ms"] * 1000.0 / q["ours"]["median_us"]
        q["S_sc"] = q["baseline_median_ms"] * 1000.0 / q["ours"]["sc_median_us"]
        rec["queries"][cid] = q
        print(f"{cid} {q['desc']:24s} ours={q['ours']['median_us']:8.2f}us "
              f"base({q['baseline_name']})={q['baseline_median_ms']:8.3f}ms "
              f"S={q['S']:8.1f}x exact={q['exact']}", flush=True)
    con.close()

    # ------------------------------------------------------------------ C ---
    con = duckdb.connect(GROUPS_DB, read_only=True, config={"threads": 1})
    cuts = {"p0.1": -7000, "p1": -720, "p10": 650, "p50": 1350, "p100": 2147483647}
    for G, gcol in ((100_000, "g100000"), (1_000_000, "g1000000")):
        mvname = f"mv_g{G}"
        t = time.perf_counter()
        con.execute(f"CREATE TEMP TABLE {mvname} AS SELECT {gcol} AS g, SUM(v) AS s, COUNT(*) AS c, "
                    f"MIN(v) AS mn, MAX(v) AS mx FROM groups GROUP BY 1")
        con.execute(f"SELECT COUNT(*) FROM {mvname}").fetchall()
        mv_build = (time.perf_counter() - t) * 1000
        for pk, cut in cuts.items():
            cid = f"C{G//1000 if G >= 1000 else G}_{pk}"
            if QUICK and cid not in ("C100_p10", "C1000_p10", "C100_p100"):
                continue
            clst = os.path.join(CLST_S, f"clst_g{G}_stats.clst")
            outj = os.path.join(SCRATCH, f"clst_{cid}.json")
            runs = []
            for _ in range(3):
                warm_core()
                _, stderr, _ = sh([CLST, "query", clst, "-2147483648", str(cut), outj,
                                   "/dev/null", "100"])[0]
                runs.append(last_json(stderr))
            runs.sort(key=lambda j: j["query_median_ms"])
            pick = runs[len(runs) // 2]
            q = {"id": cid, "group": "C", "desc": f"G={G:,} v <= {cut}", "structure":
                 "CLST01 (range scan; stats fast-path at p100)", "kind": "range",
                 "params": {"G": G, "lo": -2147483648, "hi": cut, "p": pk},
                 "ours": {"median_ms": pick["query_median_ms"], "p95_ms": pick["query_p95_ms"],
                          "runs_ms": [r["query_median_ms"] for r in runs], "reps": 100,
                          "fast_path": pick.get("fast_path", 0), "rows": pick.get("rows"),
                          "selectivity": pick.get("selectivity")}}
            drows = con.execute(
                f"SELECT {gcol} AS g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM groups "
                f"WHERE v BETWEEN -2147483648 AND {cut} GROUP BY 1 ORDER BY 1").fetchall()
            crows = json.load(open(outj))["rows"]
            q["exact"] = canon_cube(crows) == canon_duck(drows)
            q["duckdb_rows"] = len(drows)
            cand = {}
            engine_sql = (f"SELECT COUNT(*) FROM (SELECT {gcol}, SUM(v), COUNT(*), MIN(v), MAX(v) "
                          f"FROM groups WHERE v BETWEEN -2147483648 AND {cut} GROUP BY 1) AS q")
            cand["duckdb_scan"], _ = duck_engine(con, engine_sql, 100)
            q["duckdb_engine_sql"] = engine_sql
            if pk == "p100":
                mv_engine = f"SELECT COUNT(*) FROM (SELECT g,s,c,mn,mx FROM {mvname}) AS q"
                cand["duckdb_mv"], _ = duck_engine(con, mv_engine, 100)
                q["duckdb_mv_sql"] = mv_engine
            q["controls"] = cand
            q["baseline_name"] = min(cand, key=lambda k: cand[k]["median"])
            q["baseline_median_ms"] = cand[q["baseline_name"]]["median"]
            q["baseline_p95_ms"] = cand[q["baseline_name"]]["p95"]
            q["S"] = q["baseline_median_ms"] / q["ours"]["median_ms"]
            rec["queries"][cid] = q
            print(f"{cid} {q['desc']:28s} ours={q['ours']['median_ms']:9.3f}ms "
                  f"base({q['baseline_name']})={q['baseline_median_ms']:10.3f}ms "
                  f"S={q['S']:8.2f}x exact={q['exact']}", flush=True)
        rec.setdefault("mv_build_ms_C", {})[str(G)] = mv_build
    con.close()

    # ------------------------------------------------------------------ D ---
    con = duckdb.connect(config={"threads": 1})
    con.execute(f"CREATE TEMP TABLE t AS SELECT state AS g, pop AS v FROM read_parquet('{IG2_PARQ}')")
    con.execute("CREATE TEMP TABLE mv AS SELECT g, CAST(SUM(v) AS BIGINT) AS s, COUNT(*) AS c, "
                "MIN(v) AS mn, MAX(v) AS mx FROM t GROUP BY 1")
    ig2_full = "SELECT g, CAST(SUM(v) AS BIGINT), COUNT(*), MIN(v), MAX(v) FROM t"
    D = [
        ("D01", "original IG2 SQL (NOT IN DC)", ["where_not_in=District of Columbia"], "fixed",
         "g NOT IN ('District of Columbia')", None),
        ("D02", "fixed all 48 groups", [], "fixed", None, None),
        ("D03", "v <= 98 (p10)", ["hi=98"], "range", "v <= 98", (-2147483648, 98)),
        ("D04", "v <= 1109 (p50)", ["hi=1109"], "range", "v <= 1109", (-2147483648, 1109)),
        ("D05", "g IN (Alabama, Alaska)", ["where_in=Alabama,Alaska"], "fixed",
         "g IN ('Alabama','Alaska')", None),
    ]
    for qid, desc, spec, kind, where_sql, vrange in D:
        if QUICK and qid not in ("D01", "D03"):
            continue
        q = {"id": qid, "group": "D", "desc": desc, "structure": "ECUBE03 (cube)",
             "kind": kind, "spec": spec}
        q["ours"] = cube_bench(ig2_cube, os.path.join(IG2_PREP, "state.dict"), 2000, spec)
        outj = os.path.join(SCRATCH, f"panel_{qid}.json")
        sh([CUBE3, "query", ig2_cube, os.path.join(IG2_PREP, "state.dict"), outj] + spec)
        crows = json.load(open(outj))["rows"]
        dsql = ig2_full + (" WHERE " + where_sql if where_sql else "") + " GROUP BY 1 ORDER BY 1"
        drows = con.execute(dsql).fetchall()
        q["exact"] = canon_cube(crows) == canon_duck(drows)
        q["duckdb_rows"] = len(drows)
        scan_sql = "SELECT COUNT(*) FROM (" + dsql.replace(" ORDER BY 1", "") + ") AS q"
        cand = {}
        if kind == "fixed":
            mvw = {"D01": "g NOT IN ('District of Columbia')", "D05": "g IN ('Alabama','Alaska')"}.get(qid, "")
            mv_sql = ("SELECT COUNT(*) FROM (SELECT g,s,c,mn,mx FROM mv"
                      + (" WHERE " + mvw if mvw else "") + ") AS q")
            cand["duckdb_mv"], _ = duck_engine(con, mv_sql, 100)
            cand["duckdb_scan"], _ = duck_engine(con, scan_sql, 30)
        else:
            cand["duckdb_scan"], _ = duck_engine(con, scan_sql, 30)
            cand["value_index"], cmd = cli_internal(
                [VIDX, VIDX_IG2, os.path.join(IG2_PREP, "state.dict"),
                 os.path.join(SCRATCH, f"vidx_{qid}.json"),
                 f"range={vrange[0]},{vrange[1]}"], 100)
            q["value_index_cmd"] = cmd
        q["controls"] = cand
        q["baseline_name"] = min(cand, key=lambda k: cand[k]["median"])
        q["baseline_median_ms"] = cand[q["baseline_name"]]["median"]
        q["baseline_p95_ms"] = cand[q["baseline_name"]]["p95"]
        q["S"] = q["baseline_median_ms"] * 1000.0 / q["ours"]["median_us"]
        q["duckdb_engine_sql"] = scan_sql
        rec["queries"][qid] = q
        print(f"{qid} {desc:26s} ours={q['ours']['median_us']:8.2f}us "
              f"base({q['baseline_name']})={q['baseline_median_ms']:9.3f}ms "
              f"S={q['S']:10.1f}x exact={q['exact']}", flush=True)
    con.close()

    # ------------------------------------------------------------ summary ---
    qs = rec["queries"]
    S = [q["S"] for q in qs.values()]
    exact_n = sum(1 for q in qs.values() if q["exact"])
    rec["summary"] = {
        "n_queries": len(S), "n_exact": exact_n,
        "frac_S_ge_1": sum(1 for s in S if s >= 1) / len(S),
        "frac_S_ge_2": sum(1 for s in S if s >= 2) / len(S),
        "frac_S_ge_3": sum(1 for s in S if s >= 3) / len(S),
        "frac_S_ge_10": sum(1 for s in S if s >= 10) / len(S),
        "median_S": statistics.median(S),
        "geomean_S": math.exp(sum(math.log(s) for s in S) / len(S)),
        "min_S": min(S), "max_S": max(S),
        "by_group": {g: {"n": sum(1 for q in qs.values() if q["group"] == g),
                         "median_S": statistics.median([q["S"] for q in qs.values()
                                                       if q["group"] == g]),
                         "frac_ge_3": sum(1 for q in qs.values()
                                          if q["group"] == g and q["S"] >= 3)
                                      / max(1, sum(1 for q in qs.values() if q["group"] == g)),
                         "frac_ge_10": sum(1 for q in qs.values()
                                           if q["group"] == g and q["S"] >= 10)
                                       / max(1, sum(1 for q in qs.values() if q["group"] == g))}
                    for g in "ABCD"},
        "baseline_names_used": {k: sum(1 for q in qs.values() if q["baseline_name"] == k)
                                for k in {q["baseline_name"] for q in qs.values()}},
        "metric_J_pass": sum(1 for s in S if s >= 1) / len(S) >= 0.90,
        "wall_s": time.time() - t_start,
    }
    json.dump(rec, open(os.path.join(RECEIPT, "panel_results.json"), "w"), indent=1)
    json.dump(rec, open(os.path.join(RESULTS, "panel_results.json"), "w"), indent=1)
    print(json.dumps(rec["summary"], indent=1), flush=True)

    # ------------------------------------------------------------ markdown --
    lines = ["# 50-query mixed panel (metric J) — 2026-11-01", "",
             f"Host `localhost`, driver pinned core 97, subprocesses core 96, `nice -n 5`, "
             f"single thread. Ours = engine-resident (no result serialization); control = "
             f"strongest applicable measured in the same window (DuckDB 2.0.0 1T forced "
             f"aggregation / DuckDB MV for fixed / value index for single-column ranges / "
             f"bitplane + sorted index for multi-column).", "",
             "## Summary", "",
             f"* queries: **{len(S)}**, element-wise exact: **{exact_n}/{len(S)}**",
             f"* fraction with S_q >= 1: **{rec['summary']['frac_S_ge_1']*100:.1f}%** "
             f"(metric J target 90%)",
             f"* >= 2: **{rec['summary']['frac_S_ge_2']*100:.1f}%**, "
             f">= 3: **{rec['summary']['frac_S_ge_3']*100:.1f}%**, "
             f">= 10: **{rec['summary']['frac_S_ge_10']*100:.1f}%**",
             f"* S_q median **{rec['summary']['median_S']:.1f}x**, geomean "
             f"**{rec['summary']['geomean_S']:.1f}x**, min {rec['summary']['min_S']:.2f}x, "
             f"max {rec['summary']['max_S']:,.0f}x", "",
             "| id | group | query | ours (median / P95) | strongest control | baseline (median) | S_q | exact |",
             "|---|---|---|---:|---|---:|---:|:--:|"]
    for qid in sorted(qs):
        q = qs[qid]
        if q["group"] in ("A", "B", "D"):
            ours = f"{q['ours']['median_us']:,.2f} / {q['ours']['p95_us']:,.2f} µs"
        else:
            ours = f"{q['ours']['median_ms']:,.3f} / {q['ours']['p95_ms']:,.3f} ms"
        lines.append(f"| {qid} | {q['group']} | {q['desc']} | {ours} | {q['baseline_name']} | "
                     f"{q['baseline_median_ms']:,.3f} ms | {q['S']:,.1f}× | "
                     f"{'yes' if q['exact'] else 'NO'} |")
    lines += ["", "## Per-group", "",
              "| group | n | median S_q | frac >= 3 | frac >= 10 |", "|---|---:|---:|---:|---:|"]
    for g, s in rec["summary"]["by_group"].items():
        lines.append(f"| {g} | {s['n']} | {s['median_S']:,.1f}× | "
                     f"{s['frac_ge_3']*100:.0f}% | {s['frac_ge_10']*100:.0f}% |")
    md = "\n".join(lines) + "\n"
    open(os.path.join(RESULTS, "PANEL_20261101.md"), "w").write(md)
    open(os.path.join(RECEIPT, "PANEL_20261101.md"), "w").write(md)
    print("wrote", os.path.join(RESULTS, "PANEL_20261101.md"))


if __name__ == "__main__":
    main()
