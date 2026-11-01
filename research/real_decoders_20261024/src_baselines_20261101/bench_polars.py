#!/usr/bin/env python3
"""Polars 2.0 (system python3): in-memory dataframe group-by over the Parquet table.

  POLARS_MAX_THREADS=1 nice -n 5 taskset -c 56 /usr/bin/python3 bench_polars.py
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import os
import sys
import time

import polars as pl

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as C


def agg(df):
    return df.group_by("passenger_count", maintain_order=False).agg(
        pl.col("fare_cents").sum().alias("s"),
        pl.len().alias("c"),
        pl.col("fare_cents").min().alias("mn"),
        pl.col("fare_cents").max().alias("mx"),
    ).select("passenger_count", "s", "c", "mn", "mx").rows()


def load():
    t0 = time.perf_counter()
    df = pl.read_parquet(C.PARQUET_PATH)
    print("loaded parquet %.1f ms shape=%s" % ((time.perf_counter() - t0) * 1000, df.shape),
          flush=True)
    return {"df": df}


def main():
    state = load()
    df = state["df"]
    cases = {}
    for name, where, _label in C.CASES:
        if where:
            q = C.THRESHOLDS[name]
            run = (lambda d=df, q=q: agg(d.filter(pl.col("fare_cents") <= q)))
        else:
            run = (lambda d=df: agg(d))
        C.warmup(run)
        st, rows = C.timed(run)
        cases[name] = (st, rows)
        print("%-12s median=%10.3f ms reps=%d" % (name, st["median_ms"], st["reps"]), flush=True)
    rec = C.make_receipt(
        "polars20", "polars %s" % pl.__version__,
        "Parquet file read once into Polars DataFrame (in-memory)", os.path.getsize(C.PARQUET_PATH),
        notes="In-memory eager Polars dataframe: parquet read excluded, group_by().agg() "
              "per query; filter is an in-memory predicate pass.",
        cases=cases, load_ms=None)
    rec["threading_check"] = "POLARS_MAX_THREADS=1"
    return C.write_receipt(rec, os.path.join(C.RUNS, "polars20.json"))


if __name__ == "__main__":
    print("polars", pl.__version__)
    main()
