#!/usr/bin/env python3
"""DuckDB 2.0.0: (a) reference rows, (b) native scan, (c) materialized-view variant.

Run with the pinned DuckDB-2.0 environment:  python3
  nice -n 5 taskset -c 56 python3 bench_duckdb.py [out.json]
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import json
import os
import sys
import time

import duckdb

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as C

OUT_REF = C.REFERENCE
DB = C.DUCKDB_PATH
TAG = os.environ.get("DUCKDB_TAG", "")


def get_con():
    con = duckdb.connect(DB, read_only=True)
    con.execute("SET threads=1")
    con.execute("SET preserve_insertion_order=true")
    return con


def gen_reference():
    con = get_con()
    ref = {"engine": "duckdb", "version": duckdb.__version__, "db": DB,
           "protocol": "threads=1, persistent read-only connection", "cases": {}}
    for name, where, label in C.CASES:
        sql = C.sql_for(where)
        rows = con.execute(sql).fetchall()
        ref["cases"][name] = {"sql": sql, "label": label,
                              "rows": [[None if r[0] is None else int(r[0])] +
                                       [int(x) for x in r[1:]] for r in rows]}
    os.makedirs(C.RUNS, exist_ok=True)
    with open(OUT_REF, "w") as f:
        json.dump(ref, f, indent=1)
    print("reference rows ->", OUT_REF)
    return ref


def bench_scan():
    con = get_con()
    cases = {}
    for name, where, label in C.CASES:
        sql = C.sql_for(where)
        C.warmup(lambda: con.execute(sql).fetchall())
        st, rows = C.timed(lambda: con.execute(sql).fetchall())
        cases[name] = (st, rows)
        print("%-12s median=%10.3f ms reps=%d" % (name, st["median_ms"], st["reps"]), flush=True)
    rec = C.make_receipt(
        "duckdb20_scan" + TAG, duckdb.__version__, "DuckDB native database (storage v69, BIGINT)",
        os.path.getsize(DB),
        notes="Native column-store scan; threads=1; latency excludes connection open. "
              "This is the same binary/protocol as the EAI-cube receipt (Lead owns that file).",
        cases=cases, load_ms=0.0)
    return C.write_receipt(rec, os.path.join(C.RUNS, "duckdb20_scan%s.json" % TAG))


def bench_materialized_view():
    """The trivial materialized-aggregate baseline: precompute one 11-row table per
    predicate, then answer the query by reading that table."""
    con = get_con()
    build_ms = {}
    for name, where, label in C.CASES:
        inner = C.SQL_TEMPLATE.format(w=where)
        t0 = time.perf_counter()
        con.execute("CREATE OR REPLACE TEMP TABLE agg_%s AS %s" % (name, inner))
        build_ms[name] = (time.perf_counter() - t0) * 1000.0
        print("built agg_%s in %.1f ms" % (name, build_ms[name]), flush=True)
    cases = {}
    for name, where, label in C.CASES:
        sql = "SELECT g, s, c, mn, mx FROM agg_%s ORDER BY g" % name
        C.warmup(lambda: con.execute(sql).fetchall())
        st, rows = C.timed(lambda: con.execute(sql).fetchall())
        cases[name] = (st, rows)
        print("%-12s median=%10.3f ms reps=%d" % (name, st["median_ms"], st["reps"]), flush=True)
    # storage: 11 rows x (key 8 + sum 8 + count 8 + min 4 + max 4) = 352 B per table
    storage = 11 * 32 * len(C.CASES)
    rec = C.make_receipt(
        "duckdb20_materialized", duckdb.__version__,
        "precomputed DuckDB TEMP TABLE per predicate",
        storage,
        notes="Materialized baseline: one 352 B aggregate table per predicate value "
              "(8 tables). Build time rows=SUM below; query reads the table only. "
              "Requires knowing every predicate in advance; cannot answer unseen predicates.",
        cases=cases, load_ms=0.0,
        extra={"mv_build_ms": build_ms,
               "mv_build_total_ms": sum(build_ms.values()),
               "mv_rows": 11, "mv_bytes_per_table": 352})
    return C.write_receipt(rec, os.path.join(C.RUNS, "duckdb20_materialized.json"))


if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    print("duckdb", duckdb.__version__, "db", DB)
    if which in ("all", "ref"):
        gen_reference()
    if which in ("all", "scan"):
        bench_scan()
    if which in ("all", "mv"):
        bench_materialized_view()
