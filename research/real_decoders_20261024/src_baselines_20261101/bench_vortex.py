#!/usr/bin/env python3
"""Vortex 0.87 (vortex-data, 2025 file format): compressed file scan + Arrow aggregate.

Run in the pinned Vortex env:  python3 bench_vortex.py
  nice -n 5 taskset -c 56 python3 bench_vortex.py
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import os
import sys
import time

import pyarrow as pa
import pyarrow.dataset as pads

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as C

import vortex

AGGS = [("fare_cents", "sum"), ("fare_cents", "count"),
        ("fare_cents", "min"), ("fare_cents", "max")]


def agg_rows(tbl):
    r = tbl.group_by(["passenger_count"], use_threads=False).aggregate(AGGS)
    cols = [r.column(i).to_pylist() for i in range(r.num_columns)]
    return [list(t) for t in zip(*cols)]


def open_file():
    try:
        vortex.set_worker_threads(1)
    except Exception:
        pass
    t0 = time.perf_counter()
    vf = vortex.open(C.VORTEX_PATH)
    ds = vf.to_dataset()
    print("opened vortex %.1f ms, dtype=%s" % ((time.perf_counter() - t0) * 1000, vf.dtype),
          flush=True)
    return {"vf": vf, "ds": ds}


def bench_scan():
    state = open_file()
    ds = state["ds"]

    def run(_state, name, where):
        filt = None
        if where:
            filt = pads.field("fare_cents") <= C.THRESHOLDS[name]
        tbl = ds.to_table(columns=["passenger_count", "fare_cents"], filter=filt,
                          use_threads=False)
        return agg_rows(tbl)

    cases = {}
    for name, where, _label in C.CASES:
        C.warmup(lambda: run(state, name, where))
        st, rows = C.timed(lambda: run(state, name, where))
        cases[name] = (st, rows)
        print("%-12s median=%10.3f ms reps=%d" % (name, st["median_ms"], st["reps"]), flush=True)
    rec = C.make_receipt(
        "vortex087_scan", "vortex-data %s (Vortex 0.87 file format)" % getattr(vortex, "__version__", "0.87.0"),
        "Vortex file (2x int64, 100M rows) %d B" % os.path.getsize(C.VORTEX_PATH),
        os.path.getsize(C.VORTEX_PATH),
        notes="Vortex compressed columnar file: every rep decodes the needed columns from "
              "the Vortex file and aggregates with Arrow compute. Natural Vortex query path.",
        cases=cases, load_ms=None)
    rec["threading_check"] = "vortex.set_worker_threads(1); pyarrow use_threads=False"
    return C.write_receipt(rec, os.path.join(C.RUNS, "vortex087_scan.json"))


def bench_predecoded():
    """Vortex decode once -> Arrow in-memory, to separate decode from aggregate cost."""
    t0 = time.perf_counter()
    vf = vortex.open(C.VORTEX_PATH)
    tbl = vf.to_arrow()
    if isinstance(tbl, pa.RecordBatchReader):
        tbl = tbl.read_all()
    elif isinstance(tbl, list):
        tbl = pa.concat_tables(tbl)
    load_ms = (time.perf_counter() - t0) * 1000.0
    print("predecoded %.1f ms rows=%d" % (load_ms, tbl.num_rows), flush=True)
    import pyarrow.compute as pc
    fare = tbl["fare_cents"]

    def run(_state, name, where):
        if not where:
            return agg_rows(tbl)
        return agg_rows(tbl.filter(pc.less_equal(fare, C.THRESHOLDS[name])))

    cases = {}
    for name, where, _label in C.CASES:
        C.warmup(lambda: run(None, name, where))
        st, rows = C.timed(lambda: run(None, name, where))
        cases[name] = (st, rows)
        print("%-12s median=%10.3f ms reps=%d" % (name, st["median_ms"], st["reps"]), flush=True)
    rec = C.make_receipt(
        "vortex087_predecoded", "vortex-data %s" % getattr(vortex, "__version__", "0.87.0"),
        "Vortex file decoded once into Arrow (in-memory)", os.path.getsize(C.VORTEX_PATH),
        notes="Decode excluded from latency: isolates the aggregate/filter cost of the "
              "Arrow compute engine over Arrow buffers.",
        cases=cases, load_ms=load_ms)
    return C.write_receipt(rec, os.path.join(C.RUNS, "vortex087_predecoded.json"))


if __name__ == "__main__":
    print("vortex", getattr(vortex, "__version__", "?"))
    which = sys.argv[1] if len(sys.argv) > 1 else "both"
    if which in ("both", "scan"):
        bench_scan()
    if which in ("both", "pre"):
        bench_predecoded()
