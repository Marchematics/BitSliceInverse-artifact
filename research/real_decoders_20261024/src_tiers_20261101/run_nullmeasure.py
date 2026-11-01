#!/usr/bin/env python3
"""NULL-measure latency + exactness at 100M rows (ECUBE03).

Uses the deterministic 5%-NULL + all-NULL-group(rank 9) nullable fare column from
make_nullmeasure_data.py, rebuilds ECUBE03 from the plain (gid, value, validity) columns,
and runs an 8-case sweep that exercises the NULL-measure semantics:

  fixed            COUNT(*) includes NULL measures; SUM/MIN/MAX ignore them
  g IN (1,2)       same, plus the group-domain predicate (NULL group dropped)
  HAVING sum>=1e10 HAVING is not TRUE for a NULL SUM (all-NULL group filtered out)
  HAVING min>=-1e5 same for MIN
  g IN (9)         the all-NULL group: COUNT(*)=105, SUM/MIN/MAX = NULL in the output
  v <= 650         a range predicate excludes NULL measures entirely (SQL NULL -> not TRUE)
  full i32 range   same as fixed but NULL measures excluded: COUNT(*) = non-NULL rows
  v>=0 AND g IN (1,2)  range + group predicate combined

Every case is verified element-wise against DuckDB 2.0.0 with the same NULLs.
Ours: enc_cube3 bench, 1,000 reps, 3 runs after a pinned P-state warmup, median/P95.
Writes receipts/tiers_20261101/nullmeasure_100m.json + NULLMEASURE_20261101.md and a
copy under results/tiers_20261101/.

usage: nice -n 5 taskset -c 97 python3 src_tiers_20261101/run_nullmeasure.py
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import hashlib
import json
import os
import statistics
import subprocess
import time

import duckdb

REPO = os.path.abspath(os.getcwd())
SCRATCH = _artifact.expand(os.environ.get("TIERS_SCRATCH", "../../work/tiers/build"))
BIN = _artifact.expand(os.environ.get("TIERS_BIN", "../../work/tiers/bin"))
CORE = _artifact.core()
RECEIPT = os.path.join(_artifact.WORK, "receipts", "tiers_20261101")
RESULTS = os.path.join(REPO, "results", "tiers_20261101")
CUBE3 = os.path.join(BIN, "enc_cube3")
META = json.load(open(os.path.join(SCRATCH, "nyc_nullmeasure_meta.json")))
GID = os.path.join(SCRATCH, "gid_nyc_g10.i32")
VAL = os.path.join(SCRATCH, "val_nyc100.i32")
BITS = os.path.join(SCRATCH, "nyc_null_valid.bits")
PARQ = os.path.join(SCRATCH, "nyc_nullmeasure.parquet")
CUBE = os.path.join(SCRATCH, "nyc100_e3_nullmeasure.ecube")
DICT = os.path.join(REPO, "results/nyc_taxi_100m_prep/Observed/state.dict")


def sh(cmd, reps=1, warm=False):
    out = []
    for _ in range(reps):
        if warm:
            warm_core()
        t = time.perf_counter()
        p = subprocess.run(["nice", "-n", "5", "taskset", "-c", CORE] + cmd,
                           capture_output=True, text=True)
        out.append((time.perf_counter() - t, p.stderr, p.stdout))
        if p.returncode != 0:
            raise RuntimeError(f"{cmd[:3]} rc={p.returncode}: {p.stderr[-1500:]}")
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
    raise RuntimeError("no json: " + s[-600:])


def sha(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


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
    return sorted(tuple(["NULL" if r[0] is None else str(r[0])] +
                        ["NULL" if x is None else str(x) for x in r[1:]]) for r in rows)


def main():
    os.makedirs(RECEIPT, exist_ok=True)
    os.makedirs(RESULTS, exist_ok=True)
    assert os.path.exists(BITS) and os.path.exists(PARQ), "run make_nullmeasure_data.py first"
    rec = {"protocol": {
        "host": "localhost (AMD EPYC 9554)",
        "pin": f"nice -n 5 taskset -c {CORE} (subprocesses), driver pinned to 97",
        "ours": "ECUBE03 build from plain gid/value/validity; bench 1,000 reps, 3 runs, "
                "0.5 s pinned P-state warmup, median of medians, P95 of the picked run",
        "duckdb": "2.0.0 threads=1 warm; 1 warmup + 30 reps for the engine reference",
        "exactness": "full grouped result element-wise (group, SUM, COUNT(*), MIN, MAX); "
                     "SQL NULLs compared as NULL",
        "meta": META,
        "artifacts": {"enc_cube3_sha256": sha(CUBE3),
                      "eai_cube3_src_sha256": sha(os.path.join(REPO, "src_eai_20261101/eai_cube3.cpp"))},
        "duckdb_version": duckdb.__version__,
    }, "cases": {}}

    # ---- build ECUBE03 with the NULL-measure column ------------------------
    t0 = time.perf_counter()
    _, stderr, _ = sh(["/usr/bin/time", "-v", CUBE3, "build", GID, VAL, CUBE, BITS])[0]
    rec["build"] = {"wall_s": time.perf_counter() - t0, "json": last_json(stderr),
                    "bytes": os.path.getsize(CUBE), "sha256": sha(CUBE)}
    print("build:", json.dumps(rec["build"]["json"]), flush=True)

    cases = [
        ("N1", "fixed (no predicate)", [], "",
         "SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM t GROUP BY 1 ORDER BY 1"),
        ("N2", "g IN (1,2)", ["where_in=1,2"], "",
         "SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM t WHERE g IN (1,2) "
         "GROUP BY 1 ORDER BY 1"),
        ("N3", "HAVING sum >= 1e10", ["having=sum>=1e10"], "",
         "SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM t GROUP BY 1 "
         "HAVING SUM(v) >= 1e10 ORDER BY 1"),
        ("N4", "HAVING min >= -100000", ["having=min>=-100000"], "",
         "SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM t GROUP BY 1 "
         "HAVING MIN(v) >= -100000 ORDER BY 1"),
        ("N5", "all-NULL group g IN (9)", ["where_in=9"], "",
         "SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM t WHERE g = 9 "
         "GROUP BY 1 ORDER BY 1"),
        ("N6", "range v <= 650 (NULLs excluded)", ["hi=650"], "",
         "SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM t WHERE v <= 650 "
         "GROUP BY 1 ORDER BY 1"),
        ("N7", "full i32 range (NULLs excluded)", ["range=-2147483648,2147483647"], "",
         "SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM t "
         "WHERE v BETWEEN -2147483648 AND 2147483647 GROUP BY 1 ORDER BY 1"),
        ("N8", "range v>=0 AND g IN (1,2)", ["range=0,2147483647", "where_in=1,2"], "",
         "SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM t "
         "WHERE v BETWEEN 0 AND 2147483647 AND g IN (1,2) GROUP BY 1 ORDER BY 1"),
    ]
    con = duckdb.connect(config={"threads": 1})
    t = time.perf_counter()
    con.execute(f"CREATE TEMP TABLE t AS SELECT g, v FROM read_parquet('{PARQ}')")
    con.execute("SELECT COUNT(*) FROM t").fetchall()
    rec["duckdb_table_load_ms"] = (time.perf_counter() - t) * 1000
    rec["protocol"]["duckdb_threads_effective"] = con.execute(
        "SELECT current_setting('threads')").fetchall()[0][0]

    for cid, desc, spec, _, sql in cases:
        q = {"id": cid, "desc": desc, "spec": spec, "duckdb_sql": sql}
        runs = []
        for _ in range(3):
            warm_core()
            wall, stderr, _ = sh([CUBE3, "bench", CUBE, DICT, "/dev/null", "1000"] + spec)[0]
            runs.append(last_json(stderr))
        runs.sort(key=lambda j: j["query_median_ms"])
        pick = runs[len(runs) // 2]
        q["ours"] = {"median_us": pick["query_median_ms"] * 1000,
                     "p95_us": pick["query_p95_ms"] * 1000,
                     "serialize_us": pick["serialize_median_ms"] * 1000,
                     "runs_us": [r["query_median_ms"] * 1000 for r in runs]}
        outj = os.path.join(SCRATCH, f"null_{cid}.json")
        sh([CUBE3, "query", CUBE, DICT, outj] + spec)
        crows = json.load(open(outj))["rows"]
        con.execute(sql).fetchall()
        ts, drows = [], None
        for _ in range(30):
            t = time.perf_counter()
            drows = con.execute(sql).fetchall()
            ts.append((time.perf_counter() - t) * 1000.0)
        ts.sort()
        q["duckdb_median_ms"] = statistics.median(ts)
        q["duckdb_p95_ms"] = ts[min(len(ts) - 1, int(len(ts) * 0.95))]
        q["duckdb_rows"] = len(drows)
        q["cube_rows"] = len(crows)
        q["exact"] = canon_cube(crows) == canon_duck(drows)
        q["cube_output"] = crows
        q["duckdb_output"] = [[("NULL" if x is None else x) for x in r] for r in drows]
        rec["cases"][cid] = q
        print(f"{cid} {desc:36s} ours={q['ours']['median_us']:8.2f}us "
              f"P95={q['ours']['p95_us']:8.2f} duck={q['duckdb_median_ms']:8.3f}ms "
              f"rows={len(crows)} exact={q['exact']}", flush=True)
    con.close()

    # explicit NULL-semantics assertions against the independent mask meta
    n = META["rows"]
    null_total = META["null_total"]
    g9 = META["per_group"]["9"]
    fixed = rec["cases"]["N1"]
    full = rec["cases"]["N7"]
    n5 = rec["cases"]["N5"]
    checks = {
        "fixed_total_count_is_100M": sum(int(r[2]) for r in fixed["cube_output"]) == n,
        "range_full_count_excludes_nulls": sum(int(r[2]) for r in full["cube_output"])
        == n - null_total,
        "all_null_group_count": (len(n5["cube_output"]) == 1
                                 and int(n5["cube_output"][0][2]) == g9["rows"]),
        "all_null_group_sum_min_max_null": (len(n5["cube_output"]) == 1
                                            and n5["cube_output"][0][1] == "None"
                                            and n5["cube_output"][0][3] == "None"
                                            and n5["cube_output"][0][4] == "None"),
    }
    rec["semantics_checks"] = checks
    rec["semantics_checks"]["all_pass"] = all(checks.values())
    rec["summary"] = {"n_cases": len(rec["cases"]),
                      "n_exact": sum(1 for q in rec["cases"].values() if q["exact"]),
                      "n_semantics_pass": sum(1 for k, v in checks.items()
                                              if k != "all_pass" and v),
                      "n_semantics": len(checks) - 1}
    json.dump(rec, open(os.path.join(RECEIPT, "nullmeasure_100m.json"), "w"), indent=1)
    json.dump(rec, open(os.path.join(RESULTS, "nullmeasure_100m.json"), "w"), indent=1)
    print(json.dumps(rec["summary"], indent=1), flush=True)

    # ------------------------------------------------------------ markdown --
    lines = ["# NULL-measure at 100M rows (ECUBE03) — exactness + latency, 2026-11-01", "",
             f"Column: NYC-100M `fare_cents` with a deterministic **5% NULL mask** "
             f"(`splitmix64(rowid) % 100 < 5`) plus **one all-NULL group** (passenger_count "
             f"rank 9, {g9['rows']} rows). Total NULLs **{null_total:,}** / {n:,} rows. "
             f"Validity bitmap 12,500,000 B; rebuilt ECUBE03 index "
             f"**{rec['build']['bytes']:,} B** in **{rec['build']['wall_s']:.3f} s** "
             f"(build JSON in the receipt).", "",
             "Ours = `enc_cube3 bench` 1,000 reps, 3 runs after a pinned P-state warmup "
             "(median/P95 of the picked run). DuckDB = 2.0.0 `threads=1`, warm, 30 reps, "
             "reference only. Exactness = full grouped result element-wise, SQL NULLs as NULL.", "",
             "| id | case | ours median / P95 (µs) | DuckDB ms | rows | exact |",
             "|---|---|---:|---:|---:|:--:|"]
    for cid in sorted(rec["cases"]):
        q = rec["cases"][cid]
        lines.append(f"| {cid} | {q['desc']} | {q['ours']['median_us']:,.2f} / "
                     f"{q['ours']['p95_us']:,.2f} | {q['duckdb_median_ms']:,.3f} | "
                     f"{len(q['cube_output'])} | {'yes' if q['exact'] else 'NO'} |")
    lines += ["", "## NULL-measure semantics checks (independent of the DuckDB comparison)", ""]
    for k, v in checks.items():
        lines.append(f"* `{k}`: {'PASS' if v else 'FAIL'}")
    lines += ["", f"All-NULL group sample: {json.dumps(n5['cube_output'][0])} "
              f"(name, SUM, COUNT(*), MIN, MAX).", ""]
    md = "\n".join(lines) + "\n"
    open(os.path.join(RESULTS, "NULLMEASURE_20261101.md"), "w").write(md)
    open(os.path.join(RECEIPT, "NULLMEASURE_20261101.md"), "w").write(md)
    print("wrote", os.path.join(RESULTS, "NULLMEASURE_20261101.md"))


if __name__ == "__main__":
    main()
