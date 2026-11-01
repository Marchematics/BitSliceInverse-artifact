#!/usr/bin/env python3
"""DuckDB 2.0.0 baseline bench for the clustered stream: one process per thread
count, affinity pinned by the caller, thread count set through the connection
config (DuckDB 2.0.0 ignores a bare `SET threads=N` here -- verified: the
session variable reads back as the hardware count and the timings match the
16-thread curve, so the config path is used and the effective setting is
recorded in the receipt).

    taskset -c 128            bench_duck.py DB 1  7 spec.json out1.json
    taskset -c 128-143        bench_duck.py DB 16 7 spec.json out16.json
    taskset -c 128            bench_duck.py --parquet FILE.parquet 1 3 spec out
                              (the 1B control: a view named `groups` over the parquet,
                               so the spec SQL is identical to the .duckdb case)

spec.json  = [{"label": str, "engine_sql": str}, ...]   (forced-aggregation SQL,
             i.e. SELECT COUNT(*) FROM (.. GROUP BY ..) so the engine time, not
             the client result materialization, is what is timed)
out.json   = {"duckdb_version", "threads_requested", "threads_effective", "reps",
              "affinity", "results": {label: {median_ms, min_ms, max_ms, p95_ms, reps}}}
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

import duckdb


def affinity():
    try:
        return sorted(os.sched_getaffinity(0))
    except AttributeError:
        return None


def main():
    args = list(sys.argv[1:])
    parquet = False
    if args and args[0] == "--parquet":
        parquet = True
        args = args[1:]
    db, threads, reps, spec_path, out_path = (
        args[0], int(args[1]), int(args[2]), args[3], args[4])
    spec = json.load(open(spec_path))
    if parquet:
        con = duckdb.connect(":memory:", config={"threads": threads})
        con.execute(f"CREATE VIEW groups AS SELECT * FROM read_parquet('{db}')")
    else:
        con = duckdb.connect(db, read_only=True, config={"threads": threads})
    eff = con.execute("select current_setting('threads')").fetchall()[0][0]
    # one full-table warmup so the page cache / buffer pool is warm on both paths
    con.execute("SELECT COUNT(*) FROM groups").fetchall()
    out = {"duckdb_version": duckdb.__version__, "threads_requested": threads,
           "threads_effective": int(eff), "reps": reps, "affinity": affinity(),
           "db": db, "parquet_view": parquet, "results": {}}
    for item in spec:
        label, sql = item["label"], item["engine_sql"]
        con.execute(sql).fetchall()  # 1 untimed warmup per case
        ts = []
        for _ in range(reps):
            t = time.perf_counter()
            con.execute(sql).fetchall()
            ts.append((time.perf_counter() - t) * 1000.0)
        ts.sort()
        out["results"][label] = {
            "median_ms": statistics.median(ts), "min_ms": ts[0], "max_ms": ts[-1],
            "p95_ms": ts[min(len(ts) - 1, int(len(ts) * 0.95))], "reps": reps,
            "engine_sql": sql}
    con.close()
    json.dump(out, open(out_path, "w"), indent=1)
    print(json.dumps({k: out[k] for k in ("threads_requested", "threads_effective",
                                          "duckdb_version", "affinity")}))


if __name__ == "__main__":
    main()
