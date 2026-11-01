#!/usr/bin/env python3
"""Stability control: repeat the no-predicate query many times and keep every sample.

Purpose: the DuckDB 2.0 fixed-GROUP-BY measurement moved between 439.7 ms (EAI-cube
receipt) and 503.9 ms (this suite's median) on the same host/binary.  Before quoting a
headline number, quantify the within-run distribution and the run-to-run drift.

  nice -n 5 taskset -c 56 python3 bench_stability.py [out.json]
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import json
import os
import statistics
import sys
import time

import pyarrow as pa
import pyarrow.compute as pc

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as C

REPS = int(os.environ.get("STABILITY_REPS", "60"))
SQL = C.sql_for("")


def stats(ts):
    s = sorted(ts)
    n = len(s)
    return {"reps": n, "median_ms": statistics.median(s), "min_ms": s[0],
            "p25_ms": s[n // 4], "p75_ms": s[(3 * n) // 4],
            "p95_ms": s[min(n - 1, int(0.95 * n))], "mean_ms": statistics.fmean(s),
            "max_ms": s[-1], "times_ms": ts}


def duckdb_fixed():
    import duckdb
    con = duckdb.connect(C.DUCKDB_PATH, read_only=True)
    con.execute("SET threads=1")
    con.execute("SET preserve_insertion_order=true")
    con.execute(SQL).fetchall()
    ts = []
    for _ in range(REPS):
        t = time.perf_counter()
        rows = con.execute(SQL).fetchall()
        ts.append((time.perf_counter() - t) * 1000.0)
    return stats(ts), rows


def arrow_fixed():
    with pa.memory_map(C.ARROW_PATH, "r") as src:
        tbl = pa.ipc.open_file(src).read_all()
    aggs = [("fare_cents", "sum"), ("fare_cents", "count"),
            ("fare_cents", "min"), ("fare_cents", "max")]

    def run():
        r = tbl.group_by(["passenger_count"], use_threads=False).aggregate(aggs)
        cols = [r.column(i).to_pylist() for i in range(r.num_columns)]
        return [list(t) for t in zip(*cols)]

    run()
    ts = []
    for _ in range(max(10, REPS // 3)):
        t = time.perf_counter()
        rows = run()
        ts.append((time.perf_counter() - t) * 1000.0)
    return stats(ts), rows


def main():
    out = {"method": "stability_control",
           "engine_version": "DuckDB 2.0.0 / pyarrow 25.0.1",
           "input_format": "native DuckDB DB + Arrow IPC",
           "protocol": {"reps_stability": REPS, "sql_repeated": SQL},
           "engine_versions": {}, "cases": {}}
    import duckdb
    out["engine_versions"]["duckdb"] = duckdb.__version__
    out["engine_versions"]["pyarrow"] = pa.__version__
    out["sql"] = SQL
    st, rows = duckdb_fixed()
    out["cases"]["duckdb20_scan_fixed"] = st
    out["cases"]["duckdb20_scan_fixed"]["result"] = C.norm_rows(rows)
    print("duckdb fixed: median=%.3f min=%.3f p95=%.3f max=%.3f reps=%d" % (
        st["median_ms"], st["min_ms"], st["p95_ms"], st["max_ms"], st["reps"]))
    st, rows = arrow_fixed()
    out["cases"]["arrow25_acero_groupby_fixed"] = st
    out["cases"]["arrow25_acero_groupby_fixed"]["result"] = C.norm_rows(rows)
    print("arrow  fixed: median=%.3f min=%.3f p95=%.3f max=%.3f reps=%d" % (
        st["median_ms"], st["min_ms"], st["p95_ms"], st["max_ms"], st["reps"]))
    path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(C.RUNS, "stability_control.json")
    json.dump(out, open(path, "w"), indent=1)
    print("receipt ->", path)


if __name__ == "__main__":
    main()
