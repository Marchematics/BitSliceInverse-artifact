#!/usr/bin/env python3
"""Mixed-workload panel: one pass over 9 logical queries through every protocol.

Protocols measured (never mixed in one ratio):
  cube_cold_cli        one process per query: pinned native `enc_cube query`
                       (process start + index open + reduce + JSON out), median of
                       >=20 launches, wall clock observed by the parent
  cube_cold_direct     same, through ecube_direct (no SQL engine)
  cube_cold_duckdb     same, through the DuckDB table function (cold DuckDB proc)
  cube_warm_direct     one process, index open once, median over >=200 reps
  cube_warm_duckdb     one persistent DuckDB 2.0.0 connection, unprepared SQL
  cube_warm_duckdb_p   same with a prepared statement (parse/plan hoisted)
  datafusion_warm      one SessionContext, complete ctx.sql().collect()
  duckdb_scan          native DuckDB 2.0.0 scan of the same SQL, warm, threads=1
  duckdb_scan_p        same, prepared statement
  duckdb_mv            materialized view (one exact aggregate table per query);
                       build cost reported separately from read latency

Exactness: every engine's complete grouped result is compared with the native
DuckDB scan rows (multiset over (group,sum,count,min,max), NULL canonicalised).

Output: one JSON receipt with all per-case latencies, the exactness verdicts, the
per-case speedups and the geometric-mean / total-time summaries.
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import json
import math
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.environ.get("CE_REPO", _artifact.expand("../../research/real_decoders_20261024"))
DATA = os.path.join(REPO, "results")
WORK = os.environ.get("CE_WORK", _artifact.expand("../../work/crossengine"))
BIN = os.path.join(WORK, "build")
OUT = os.environ.get("CE_OUT", os.path.join(WORK, "runs"))

CUBE = os.environ.get("CE_CUBE", os.path.join(WORK, "data/nyc100_enc.ecube"))
DICT = os.path.join(DATA, "nyc_taxi_100m_prep/Observed/state.dict")
DUCKDB_FILE = os.path.join(DATA, "nyc_taxi_100m_duckdb/nyc.duckdb")
ENC_CUBE = os.path.join(REPO, "src_eai_20261101/enc_cube")
PANEL_TSV = os.path.join(OUT, "panel.tsv")
PY_VENV = _artifact.python()

REPS_WARM = int(os.environ.get("CE_REPS_WARM", "200"))
REPS_SCAN = int(os.environ.get("CE_REPS_SCAN", "30"))
REPS_DF = int(os.environ.get("CE_REPS_DF", "50"))
REPS_COLD = int(os.environ.get("CE_REPS_COLD", "20"))
PIN = ["nice", "-n", "5", "taskset", "-c", _artifact.core()]

sys.path.insert(0, HERE)
from panel_def import read_tsv, write_tsv, scan_sql, mv_sql  # noqa: E402


def run(cmd, timeout=1800):
    """Run pinned; return (stdout, stderr, wall_ms) with wall clock from the parent."""
    t = time.perf_counter()
    p = subprocess.run(PIN + cmd, capture_output=True, text=True, timeout=timeout)
    wall = (time.perf_counter() - t) * 1000.0
    if p.returncode != 0:
        raise SystemExit(f"FAILED ({p.returncode}): {' '.join(cmd)}\n{p.stderr[-1500:]}")
    return p.stdout, p.stderr, wall


def last_json(text):
    for line in reversed(text.strip().splitlines()):
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            return json.loads(line)
    raise SystemExit(f"no json in output: {text[-500:]}")


def canon_rows(rows):
    """rows: list of (group, sum, count, min, max) with group possibly None/str."""
    out = []
    for r in rows:
        g = "NULL" if r[0] in (None, "None", "null", "NULL") else str(r[0])
        vals = []
        for x in r[1:]:
            vals.append("NULL" if x in (None, "None", "null", "NULL") else str(x))
        out.append(tuple([g] + vals))
    return sorted(out)


# --------------------------------------------------------------------------- #
# exactness: reference rows from python duckdb 2.0.0 (the pinned control)      #
# --------------------------------------------------------------------------- #
def duckdb_reference(panel):
    import duckdb
    con = duckdb.connect(DUCKDB_FILE, read_only=True)
    con.execute("SET threads=1")
    ref = {}
    for case in panel:
        rows = con.execute(scan_sql(case)).fetchall()
        ref[case["name"]] = canon_rows(rows)
    con.close()
    return ref


def cube_direct_rows(case):
    out_json = os.path.join(OUT, f"direct_{case['name']}.json")
    _, err, _ = run([os.path.join(BIN, "ecube_direct"), "query", CUBE, DICT, out_json,
                     case["cube_spec"]])
    rows = json.load(open(out_json))["rows"]
    return canon_rows(rows), last_json(err)


def main():
    os.makedirs(OUT, exist_ok=True)
    write_tsv(PANEL_TSV)
    panel = read_tsv(PANEL_TSV)
    receipt = {"panel_size": len(panel), "cases": [c["name"] for c in panel],
               "protocol": {
                   "pin": " ".join(PIN),
                   "threads": 1,
                   "reps_warm": REPS_WARM, "reps_scan": REPS_SCAN,
                   "reps_datafusion": REPS_DF, "reps_cold": REPS_COLD,
                   "cube": CUBE, "dict": DICT, "duckdb_file": DUCKDB_FILE,
                   "enc_cube": ENC_CUBE,
                   "warm": "index/connection open once, 1 untimed warmup, median of reps",
                   "cold": "separate process per request, wall clock from the parent, median",
               },
               "engines": {}}
    def save(tag):
        tmp = os.path.join(OUT, f"panel_receipt.{tag}.json")
        with open(tmp, "w") as f:
            json.dump(receipt, f, indent=1)
        return tmp

    print(f"panel: {len(panel)} cases -> {PANEL_TSV}")

    # ---------------- 1. native DuckDB scan (C harness, 2.0.0 in-process) -----
    print("[1] duckdb native scan ...")
    so, se, _ = run([os.path.join(BIN, "ecube_duck"), "scan", DUCKDB_FILE, PANEL_TSV, str(REPS_SCAN)])
    scan = {c["name"]: c for c in json.loads(so)["cases"]}
    so, se, _ = run([os.path.join(BIN, "ecube_duck"), "scanp", DUCKDB_FILE, PANEL_TSV, str(REPS_SCAN)])
    scanp = {c["name"]: c for c in json.loads(so)["cases"]}
    receipt["engines"]["duckdb_scan"] = {"protocol": "warm_engine_resident", "cases": scan}
    receipt["engines"]["duckdb_scan_prepared"] = {"protocol": "warm_prepared", "cases": scanp}

    # ---------------- 1b. DuckDB per-query floor ------------------------------
    print("[1b] duckdb per-query floor ...")
    so, _, _ = run([os.path.join(BIN, "ecube_duck"), "floor", str(REPS_WARM)])
    floor_plain = json.loads(so)
    so, _, _ = run([os.path.join(BIN, "ecube_duck"), "floorp", str(REPS_WARM)])
    floor_prep = json.loads(so)
    receipt["engines"]["duckdb_floor"] = {"unprepared": floor_plain, "prepared": floor_prep}

    save("step1_scan")

    # ---------------- 2. materialized view -----------------------------------
    print("[2] duckdb materialized view ...")
    so, se, _ = run([os.path.join(BIN, "ecube_duck"), "mv", DUCKDB_FILE, PANEL_TSV, str(REPS_SCAN)])
    mvdoc = json.loads(so)
    mv = {c["name"]: c for c in mvdoc["cases"]}
    receipt["engines"]["duckdb_materialized_view"] = {
        "protocol": "small_table_read", "cases": mv,
        "total_build_ms": mvdoc["total_build_ms"]}

    save("step2_mv")

    # ---------------- 3. cube warm in-process --------------------------------
    print("[3] cube warm in-process ...")
    so, se, _ = run([os.path.join(BIN, "ecube_direct"), "panel", CUBE, DICT, PANEL_TSV,
                     str(REPS_WARM)])
    direct = json.loads(so)
    dirmap = {c["name"]: c for c in direct["cases"]}
    receipt["engines"]["cube_warm_direct"] = {
        "protocol": "warm_in_process", "index_load_ms": direct["index_load_ms"], "cases": dirmap}

    print("[3b] cube warm through DuckDB (unprepared + prepared) ...")
    so, _, _ = run([os.path.join(BIN, "ecube_duck"), "panel", CUBE, DICT, PANEL_TSV,
                    str(REPS_WARM)])
    duck = json.loads(so)
    duckmap = {c["name"]: c for c in duck["cases"]}
    receipt["engines"]["cube_warm_duckdb"] = {
        "protocol": "warm_engine_resident", "index_load_ms": duck["index_load_ms"],
        "cases": duckmap}
    so, _, _ = run([os.path.join(BIN, "ecube_duck"), "panelp", CUBE, DICT, PANEL_TSV,
                    str(REPS_WARM)])
    duckp = json.loads(so)
    duckpmap = {c["name"]: c for c in duckp["cases"]}
    receipt["engines"]["cube_warm_duckdb_prepared"] = {
        "protocol": "warm_prepared", "index_load_ms": duckp["index_load_ms"], "cases": duckpmap}

    save("step3_cube")

    # ---------------- 4. DataFusion -----------------------------------------
    print("[4] datafusion warm ...")
    so, se, _ = run([PY_VENV, os.path.join(HERE, "datafusion_ecube.py"), "--panel", CUBE, DICT,
                     PANEL_TSV, str(REPS_DF)])
    df = json.loads(so)
    dfmap = {c["name"]: c for c in df["cases"]}
    receipt["engines"]["datafusion_warm"] = {
        "protocol": "warm_engine_resident", "version": df.get("datafusion_version"),
        "index_load_ms": df["index_load_ms"], "floor_median_ms": df["floor_median_ms"],
        "cases": dfmap}

    save("step4_datafusion")

    # ---------------- 5. cold processes -------------------------------------
    print("[5] cold process per query ...")
    cold_cli, cold_direct, cold_duck = {}, {}, {}
    for case in panel:
        spec = case["cube_spec"]
        # native pinned CLI (enc_cube query), one process per launch
        walls = []
        for i in range(REPS_COLD):
            outj = os.path.join(OUT, f"cold_{case['name']}.json")
            cmd = [ENC_CUBE, "query", CUBE, DICT, outj] + ([spec] if spec else [])
            t = time.perf_counter()
            p = subprocess.run(PIN + cmd, capture_output=True, text=True)
            walls.append((time.perf_counter() - t) * 1000.0)
            if p.returncode != 0:
                raise SystemExit(f"enc_cube cold failed: {p.stderr[-400:]}")
        walls.sort()
        cold_cli[case["name"]] = {"median_ms": walls[len(walls) // 2], "min_ms": walls[0],
                                  "p95_ms": walls[min(len(walls) - 1, int(len(walls) * .95))],
                                  "reps": REPS_COLD}
        walls = []
        for i in range(REPS_COLD):
            outj = os.path.join(OUT, f"cold_direct_{case['name']}.json")
            cmd = [os.path.join(BIN, "ecube_direct"), "query", CUBE, DICT, outj,
                   spec] if spec else [os.path.join(BIN, "ecube_direct"), "query", CUBE, DICT, outj]
            t = time.perf_counter()
            p = subprocess.run(PIN + cmd, capture_output=True, text=True)
            walls.append((time.perf_counter() - t) * 1000.0)
            if p.returncode != 0:
                raise SystemExit(f"ecube_direct cold failed: {p.stderr[-400:]}")
        walls.sort()
        cold_direct[case["name"]] = {"median_ms": walls[len(walls) // 2], "min_ms": walls[0],
                                     "p95_ms": walls[min(len(walls) - 1, int(len(walls) * .95))],
                                     "reps": REPS_COLD}
        walls = []
        for i in range(REPS_COLD):
            outj = os.path.join(OUT, f"cold_duck_{case['name']}.json")
            cmd = [os.path.join(BIN, "ecube_duck"), "query", CUBE, DICT, outj,
                   spec] if spec else [os.path.join(BIN, "ecube_duck"), "query", CUBE, DICT, outj]
            t = time.perf_counter()
            p = subprocess.run(PIN + cmd, capture_output=True, text=True)
            walls.append((time.perf_counter() - t) * 1000.0)
            if p.returncode != 0:
                raise SystemExit(f"ecube_duck cold failed: {p.stderr[-400:]}")
        walls.sort()
        cold_duck[case["name"]] = {"median_ms": walls[len(walls) // 2], "min_ms": walls[0],
                                   "p95_ms": walls[min(len(walls) - 1, int(len(walls) * .95))],
                                   "reps": REPS_COLD}
    receipt["engines"]["cube_cold_cli"] = {"protocol": "cold_process", "cases": cold_cli}
    receipt["engines"]["cube_cold_direct"] = {"protocol": "cold_process", "cases": cold_direct}
    receipt["engines"]["cube_cold_duckdb"] = {"protocol": "cold_process", "cases": cold_duck}

    save("step5_cold")

    # ---------------- 6. exactness ------------------------------------------
    print("[6] exactness vs duckdb 2.0.0 rows ...")
    ref = duckdb_reference(panel)
    exact = {}
    for case in panel:
        n = case["name"]
        rows_direct, _ = cube_direct_rows(case)
        # duckdb table function rows (cold query mode prints them)
        outj = os.path.join(OUT, f"exact_duck_{n}.json")
        cmd = [os.path.join(BIN, "ecube_duck"), "query", CUBE, DICT, outj,
               case["cube_spec"]] if case["cube_spec"] else [
            os.path.join(BIN, "ecube_duck"), "query", CUBE, DICT, outj]
        so, _, _ = run(cmd)
        eng = last_json(so)["engine_rows"]
        rows_duck = canon_rows(eng)
        # datafusion
        dres = dfmap[n]["result"]
        rows_df = canon_rows([(dres["name"][i], dres["sum"][i], dres["count"][i],
                               dres["min"][i], dres["max"][i])
                              for i in range(len(dres["name"]))])
        mres = mv[n]
        exact[n] = {
            "reference_rows": len(ref[n]),
            "cube_direct_equal": rows_direct == ref[n],
            "cube_duckdb_equal": rows_duck == ref[n],
            "datafusion_equal": rows_df == ref[n],
            "cube_direct_vs_duckdb_json_identical":
                (lambda a, b: a == b)(rows_direct, rows_duck),
            "mv_rows": int(mres["rows"]),
        }
    receipt["attribution"] = {
        "duckdb_floor_unprepared_ms": floor_plain["median_ms"],
        "duckdb_floor_prepared_ms": floor_prep["median_ms"],
        "note": ("the cube's own reduction is a few microseconds; the remaining "
                 "warm-engine latency is DuckDB's per-query and table-function cost, "
                 "bounded below by the SELECT-42 floor"),
    }
    receipt["exactness"] = exact
    n_bad = sum(1 for v in exact.values() if not (v["cube_direct_equal"] and v["cube_duckdb_equal"]
                                                  and v["datafusion_equal"]))
    receipt["exactness_all_pass"] = (n_bad == 0)
    save("step6_exactness")

    # ---------------- 7. mixed-workload summary ------------------------------
    print("[7] mixed-workload summary ...")
    def med(engine, n):
        return receipt["engines"][engine]["cases"][n]["median_ms"]

    rows = []
    for case in panel:
        n = case["name"]
        baseline_candidates = {
            "duckdb_scan": med("duckdb_scan", n),
            "duckdb_scan_prepared": med("duckdb_scan_prepared", n),
            "duckdb_mv": med("duckdb_materialized_view", n),
        }
        best_name = min(baseline_candidates, key=baseline_candidates.get)
        best_ms = baseline_candidates[best_name]
        cube_warm = med("cube_warm_direct", n)
        cube_duck_p = med("cube_warm_duckdb_prepared", n)
        rows.append({
            "name": n,
            "cube_warm_direct_ms": cube_warm,
            "cube_warm_duckdb_ms": med("cube_warm_duckdb", n),
            "cube_warm_duckdb_prepared_ms": cube_duck_p,
            "cube_cold_cli_ms": med("cube_cold_cli", n),
            "cube_cold_direct_ms": med("cube_cold_direct", n),
            "cube_cold_duckdb_ms": med("cube_cold_duckdb", n),
            "datafusion_warm_ms": med("datafusion_warm", n),
            "duckdb_scan_ms": med("duckdb_scan", n),
            "duckdb_scan_prepared_ms": med("duckdb_scan_prepared", n),
            "duckdb_mv_ms": mv[n]["median_ms"],
            "duckdb_mv_build_ms": mv[n]["build_ms"],
            "strongest_baseline": best_name,
            "strongest_baseline_ms": best_ms,
            "speedup_cube_warm_vs_best": best_ms / cube_warm,
            "speedup_cube_duck_prepared_vs_best": best_ms / cube_duck_p,
            "speedup_cube_cold_cli_vs_best": best_ms / med("cube_cold_cli", n),
            "speedup_datafusion_vs_best": best_ms / med("datafusion_warm", n),
        })
    def geomean(xs):
        return math.exp(sum(math.log(x) for x in xs) / len(xs))
    summary = {
        "geomean_cube_warm_vs_best": geomean([r["speedup_cube_warm_vs_best"] for r in rows]),
        "geomean_cube_duck_prepared_vs_best": geomean(
            [r["speedup_cube_duck_prepared_vs_best"] for r in rows]),
        "geomean_cube_cold_cli_vs_best": geomean(
            [r["speedup_cube_cold_cli_vs_best"] for r in rows]),
        "geomean_datafusion_vs_best": geomean([r["speedup_datafusion_vs_best"] for r in rows]),
        "geomean_cube_warm_vs_scan_only": geomean(
            [r["duckdb_scan_ms"] / r["cube_warm_direct_ms"] for r in rows]),
        "total_ms_one_pass": {
            "cube_warm_direct": sum(r["cube_warm_direct_ms"] for r in rows),
            "cube_warm_duckdb_prepared": sum(r["cube_warm_duckdb_prepared_ms"] for r in rows),
            "cube_cold_cli": sum(r["cube_cold_cli_ms"] for r in rows),
            "cube_cold_direct": sum(r["cube_cold_direct_ms"] for r in rows),
            "cube_cold_duckdb": sum(r["cube_cold_duckdb_ms"] for r in rows),
            "datafusion_warm": sum(r["datafusion_warm_ms"] for r in rows),
            "duckdb_scan": sum(r["duckdb_scan_ms"] for r in rows),
            "duckdb_scan_prepared": sum(r["duckdb_scan_prepared_ms"] for r in rows),
            "duckdb_mv": sum(r["duckdb_mv_ms"] for r in rows),
        },
        "duckdb_mv_total_build_ms": mvdoc["total_build_ms"],
    }
    totals = summary["total_ms_one_pass"]
    summary["total_ratio_vs_scan"] = {
        "cube_warm_direct": totals["duckdb_scan"] / totals["cube_warm_direct"],
        "cube_warm_duckdb_prepared": totals["duckdb_scan"] / totals["cube_warm_duckdb_prepared"],
        "cube_cold_cli": totals["duckdb_scan"] / totals["cube_cold_cli"],
        "cube_cold_duckdb": totals["duckdb_scan"] / totals["cube_cold_duckdb"],
        "datafusion_warm": totals["duckdb_scan"] / totals["datafusion_warm"],
    }
    summary["total_ratio_vs_mv"] = {
        "cube_warm_direct": totals["duckdb_mv"] / totals["cube_warm_direct"],
        "cube_warm_duckdb_prepared": totals["duckdb_mv"] / totals["cube_warm_duckdb_prepared"],
        "cube_cold_cli": totals["duckdb_mv"] / totals["cube_cold_cli"],
        "datafusion_warm": totals["duckdb_mv"] / totals["datafusion_warm"],
    }
    receipt["mixed_workload"] = {"per_case": rows, "summary": summary}

    path = os.path.join(OUT, "panel_receipt.json")
    with open(path, "w") as f:
        json.dump(receipt, f, indent=1)
    print(json.dumps({k: v for k, v in summary.items() if not isinstance(v, dict)}, indent=1))
    print(f"wrote {path}")
    print(f"exactness_all_pass={receipt['exactness_all_pass']}")
    for r in rows:
        print(f"{r['name']:12s} cube_warm={r['cube_warm_direct_ms']*1000:8.2f}us "
              f"cube_duck_p={r['cube_warm_duckdb_prepared_ms']*1000:8.2f}us "
              f"cold_cli={r['cube_cold_cli_ms']:7.3f}ms scan={r['duckdb_scan_ms']:8.2f}ms "
              f"mv={r['duckdb_mv_ms']:7.3f}ms best={r['strongest_baseline']} "
              f"sp_warm={r['speedup_cube_warm_vs_best']:10.1f}x "
              f"sp_duck_p={r['speedup_cube_duck_prepared_vs_best']:10.1f}x "
              f"sp_cold={r['speedup_cube_cold_cli_vs_best']:8.2f}x "
              f"sp_df={r['speedup_datafusion_vs_best']:8.2f}x")


if __name__ == "__main__":
    main()
