#!/usr/bin/env python3
"""Apache Arrow 25.0.1: in-memory hash group-by, plus a Parquet dataset-scan path.

System python3 (/usr/bin/python3) has pyarrow 25.0.1.
  nice -n 5 taskset -c 56 /usr/bin/python3 bench_arrow.py
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import os
import sys
import time

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as pads

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as C

AGGS = [("fare_cents", "sum"), ("fare_cents", "count"),
        ("fare_cents", "min"), ("fare_cents", "max")]


def agg_rows(tbl):
    r = tbl.group_by(["passenger_count"], use_threads=False).aggregate(AGGS)
    cols = [r.column(i).to_pylist() for i in range(r.num_columns)]
    return [list(t) for t in zip(*cols)]


def load_ipc():
    t0 = time.perf_counter()
    try:
        with pa.memory_map(C.ARROW_PATH, "r") as src:
            tbl = pa.ipc.open_file(src).read_all()
    except Exception:
        with pa.memory_map(C.ARROW_PATH, "r") as src:
            tbl = pa.ipc.open_stream(src).read_all()
    print("loaded arrow ipc %.1f ms rows=%d cols=%s" % (
        (time.perf_counter() - t0) * 1000, tbl.num_rows, tbl.column_names), flush=True)
    return {"tbl": tbl}


def bench_memory():
    state = load_ipc()
    tbl = state["tbl"]
    fare = tbl["fare_cents"]

    def run(_state, name, where):
        if not where:
            return agg_rows(tbl)
        q = C.THRESHOLDS[name]
        return agg_rows(tbl.filter(pc.less_equal(fare, q)))

    cases = {}
    for name, where, _label in C.CASES:
        C.warmup(lambda: run(state, name, where))
        st, rows = C.timed(lambda: run(state, name, where))
        cases[name] = (st, rows)
        print("%-12s median=%10.3f ms reps=%d" % (name, st["median_ms"], st["reps"]), flush=True)
    rec = C.make_receipt(
        "arrow25_acero_groupby", "pyarrow %s (Apache Arrow C++ hash-aggregate)" % pa.__version__,
        "Arrow IPC file (2x int64, 100M rows)", os.path.getsize(C.ARROW_PATH),
        notes="Arrow in-memory columnar: IPC file loaded once (load excluded), then "
              "Table.group_by(use_threads=False) per query; filter is an Arrow compute "
              "kernel producing a new table each rep. Not a SQL engine.",
        cases=cases, load_ms=None)
    rec["threading_check"] = "pyarrow use_threads=False"
    return C.write_receipt(rec, os.path.join(C.RUNS, "arrow25_acero_groupby.json"))


def bench_dataset():
    """Acero scanner over Parquet (the Arrow stack's natural lakehouse path)."""
    t0 = time.perf_counter()
    ds = pads.dataset(C.PARQUET_PATH, format="parquet")
    load_ms = (time.perf_counter() - t0) * 1000.0
    print("dataset opened in %.1f ms" % load_ms, flush=True)

    def run(_state, name, where):
        filt = None
        if where:
            filt = pads.field("fare_cents") <= C.THRESHOLDS[name]
        tbl = ds.to_table(columns=["passenger_count", "fare_cents"], filter=filt,
                          use_threads=False)
        return agg_rows(tbl)

    cases = {}
    for name, where, _label in C.CASES:
        C.warmup(lambda: run(None, name, where))
        st, rows = C.timed(lambda: run(None, name, where))
        cases[name] = (st, rows)
        print("%-12s median=%10.3f ms reps=%d" % (name, st["median_ms"], st["reps"]), flush=True)
    rec = C.make_receipt(
        "arrow25_dataset_parquet", "pyarrow %s Acero scanner" % pa.__version__,
        "Parquet file (2x int64, 100M rows, 96 row groups)", os.path.getsize(C.PARQUET_PATH),
        notes="Per-query Parquet scan with row-group pruning + Arrow compute group_by. "
              "This path decodes each rep (no resident copy), i.e. a scan-engine path.",
        cases=cases, load_ms=load_ms)
    rec["threading_check"] = "pyarrow use_threads=False"
    return C.write_receipt(rec, os.path.join(C.RUNS, "arrow25_dataset_parquet.json"))


if __name__ == "__main__":
    print("pyarrow", pa.__version__)
    which = sys.argv[1] if len(sys.argv) > 1 else "both"
    if which in ("both", "mem"):
        bench_memory()
    if which in ("both", "ds"):
        bench_dataset()
