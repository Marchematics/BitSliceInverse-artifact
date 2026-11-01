#!/usr/bin/env python3
"""Apache DataFusion 55 (system python3 datafusion bindings): same SQL over Parquet.

  nice -n 5 taskset -c 56 /usr/bin/python3 bench_datafusion.py
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import os
import sys
import time

import pyarrow as pa

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as C

import datafusion
from datafusion import SessionConfig, SessionContext


def make_ctx(register="parquet"):
    cfg = SessionConfig().with_target_partitions(1)
    ctx = SessionContext(cfg)
    if register == "parquet":
        ctx.register_parquet("nyc", C.PARQUET_PATH)
    else:
        tbl = load_arrow()
        ctx.register_record_batches("nyc", [tbl.to_batches()])
    return ctx


def load_arrow():
    with pa.memory_map(C.ARROW_PATH, "r") as src:
        return pa.ipc.open_file(src).read_all()


def rows_from_batches(batches):
    if not batches:
        return []
    tbl = pa.Table.from_batches(batches)
    return [[r["g"], r["s"], r["c"], r["mn"], r["mx"]] for r in tbl.to_pylist()]


def run_sql(ctx, sql):
    return rows_from_batches(ctx.sql(sql).collect())


def main():
    t0 = time.perf_counter()
    ctx = make_ctx("parquet")
    ctx.sql("SELECT count(*) FROM nyc").collect()  # force metadata
    load_ms = (time.perf_counter() - t0) * 1000.0
    print("datafusion coalesced; setup %.1f ms" % load_ms, flush=True)

    cases = {}
    for name, where, _label in C.CASES:
        sql = C.sql_for(where)
        try:
            C.warmup(lambda: run_sql(ctx, sql))
        except Exception as e:
            # ordinal GROUP BY not supported -> rewrite to the equivalent SQL text
            sql = sql.replace("GROUP BY 1", "GROUP BY passenger_count")
            print("note: GROUP BY 1 unsupported (%s); using %r" % (str(e).splitlines()[0], sql),
                  flush=True)
            run_sql(ctx, sql)
        st, rows = C.timed(lambda s=sql: run_sql(ctx, s))
        cases[name] = (st, rows)
        print("%-12s median=%10.3f ms reps=%d" % (name, st["median_ms"], st["reps"]), flush=True)
    rec = C.make_receipt(
        "datafusion55_parquet", "datafusion %s (Rust engine, Python bindings)" % datafusion.__version__,
        "Parquet file, registered as external table", os.path.getsize(C.PARQUET_PATH),
        notes="DataFusion executes the identical SQL over the Parquet file; "
              "target_partitions=1; per-rep scan+decode included.",
        cases=cases, load_ms=load_ms)
    rec["threading_check"] = "SessionConfig.with_target_partitions(1)"
    return C.write_receipt(rec, os.path.join(C.RUNS, "datafusion55_parquet.json"))


if __name__ == "__main__":
    print("datafusion", datafusion.__version__)
    main()
