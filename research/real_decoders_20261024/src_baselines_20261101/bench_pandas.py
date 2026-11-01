#!/usr/bin/env python3
"""Pandas 2.3.3 (classic in-memory dataframe scan engine): same aggregate over Parquet.

  nice -n 5 taskset -c 56 /usr/bin/python3 bench_pandas.py
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import os
import sys
import time

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as C


def rows_from(res):
    res = res.reset_index()
    return [[None if pd.isna(g) else int(g), int(s), int(c), int(mn), int(mx)]
            for g, s, c, mn, mx in res.itertuples(index=False)]


def agg(df):
    return rows_from(df.groupby("passenger_count", dropna=False, sort=False).agg(
        s=("fare_cents", "sum"), c=("fare_cents", "count"),
        mn=("fare_cents", "min"), mx=("fare_cents", "max")))


def load():
    t0 = time.perf_counter()
    df = pd.read_parquet(C.PARQUET_PATH)
    print("loaded parquet %.1f ms shape=%s" % ((time.perf_counter() - t0) * 1000, df.shape),
          flush=True)
    return {"df": df}


def main():
    state = load()
    df = state["df"]
    fare = df["fare_cents"]
    cases = {}
    for name, where, _label in C.CASES:
        if where:
            q = C.THRESHOLDS[name]
            run = (lambda q=q: agg(df[fare <= q]))
        else:
            run = (lambda: agg(df))
        C.warmup(run)
        st, rows = C.timed(run)
        cases[name] = (st, rows)
        print("%-12s median=%10.3f ms reps=%d" % (name, st["median_ms"], st["reps"]), flush=True)
    rec = C.make_receipt(
        "pandas233", "pandas %s / numpy %s" % (pd.__version__, np.__version__),
        "Parquet read once into a pandas DataFrame (in-memory)",
        os.path.getsize(C.PARQUET_PATH),
        notes="Classic single-threaded in-memory dataframe engine (pandas groupby.agg, "
              "Cython kernels). Parquet read excluded from latency.",
        cases=cases, load_ms=None)
    rec["threading_check"] = "single process, numpy/pandas single-thread kernels"
    return C.write_receipt(rec, os.path.join(C.RUNS, "pandas233.json"))


if __name__ == "__main__":
    print("pandas", pd.__version__)
    main()
