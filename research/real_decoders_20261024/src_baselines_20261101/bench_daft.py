#!/usr/bin/env python3
"""Daft 0.7.26 (2024-2026 Rust/Python dataframe engine): same aggregate over Parquet.

  VENV=../../work/bench_baselines/venv/bin/python
  RAYON_NUM_THREADS=1 nice -n 5 taskset -c 56 $VENV bench_daft.py
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as C

import daft
from daft import col


def build_plan(q):
    df = daft.read_parquet(C.PARQUET_PATH).select("passenger_count", "fare_cents")
    if q is not None:
        df = df.where(col("fare_cents") <= q)
    return df.groupby("passenger_count").agg(
        col("fare_cents").sum().alias("s"),
        col("fare_cents").count().alias("c"),
        col("fare_cents").min().alias("mn"),
        col("fare_cents").max().alias("mx"),
    ).select("passenger_count", "s", "c", "mn", "mx")


def rows_from(d):
    pd = d.to_pydict()
    keys = pd["passenger_count"]
    out = []
    for i in range(len(keys)):
        out.append([keys[i], pd["s"][i], pd["c"][i], pd["mn"][i], pd["mx"][i]])
    return out


def exec_case(name):
    """Build a FRESH plan and collect it: daft.DataFrame.collect() memoises a
    materialised result (a second collect() on the same object returns the cached
    frame in 0.06 ms), so a naive 30-rep loop over one plan object would report a
    bogus 0.06 ms.  A new plan per rep is the honest end-to-end dataframe latency."""
    return rows_from(build_plan(C.THRESHOLDS.get(name)).collect())


def main():
    t0 = time.perf_counter()
    plans = {name: build_plan(C.THRESHOLDS.get(name)) for name, where, _l in C.CASES}
    print("daft plans built %.1f ms" % ((time.perf_counter() - t0) * 1000), flush=True)
    # demonstrate/quantify the memoisation trap once (recorded in the receipt)
    p = plans["fixed_all4"]
    rows_from(p.collect())
    t = time.perf_counter(); rows_from(p.collect()); cached_ms = (time.perf_counter() - t) * 1000
    print("WARNING: second collect() on the same daft DataFrame took %.4f ms "
          "(cached result) - not a valid query latency" % cached_ms, flush=True)
    cases = {}
    for name, where, _label in C.CASES:
        C.warmup(lambda n=name: exec_case(n))
        st, rows = C.timed(lambda n=name: exec_case(n))
        cases[name] = (st, rows)
        print("%-12s median=%10.3f ms reps=%d" % (name, st["median_ms"], st["reps"]), flush=True)
    rec = C.make_receipt(
        "daft0726", "daft %s (native local runner, Rust)" % daft.__version__,
        "Parquet file (per-query scan+decode)", os.path.getsize(C.PARQUET_PATH),
        notes="Daft dataframe engine: read_parquet -> filter -> groupby().agg(), "
              "collect() per rep. Single-thread requested via RAYON_NUM_THREADS=1 and a "
              "single-partition source; see threading_check.",
        cases=cases, load_ms=None)
    rec["threading_check"] = "RAYON_NUM_THREADS=1 (daft local runner)"
    rec["memoisation_trap_ms"] = cached_ms
    rec["notes"] += ("  IMPORTANT: DataFrame.collect() memoises; the recorded latency "
                     "builds a fresh plan per rep (plan construction ~0.8 ms included).")
    return C.write_receipt(rec, os.path.join(C.RUNS, "daft0726.json"))


if __name__ == "__main__":
    print("daft", daft.__version__)
    main()
