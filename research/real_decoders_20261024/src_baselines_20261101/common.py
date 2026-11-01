#!/usr/bin/env python3
"""Shared harness for the 2026-11-01 external-baseline comparison table.

Same SQL, same data (NYC-100M yellow-taxi 2023, 100,000,000 rows), same
protocol for every engine:

  * one untimed load/warmup, then up to REPS timed executions of the FULL query;
  * latency = execute + fetch of the complete 11-row grouped result;
  * single thread (engine-specific setting, recorded in the receipt);
  * exactness is checked against the DuckDB-2.0 rows in ``runs/reference.json``.

Run on the remote host only, pinned:  nice -n 5 taskset -c 56 <cmd>
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import json
import math
import os
import platform
import statistics
import time

REPO = _artifact.expand("../../research/real_decoders_20261024")
DATA = os.path.join(REPO, "results")
ARROW_PATH = os.path.join(DATA, "nyc_taxi_100m_arrow/nyc100m.arrow")
PARQUET_PATH = os.path.join(DATA, "nyc_taxi_100m_arrow/nyc100m.parquet")
VORTEX_PATH = os.path.join(DATA, "nyc_taxi_100m_arrow/nyc100m.vortex")
DUCKDB_PATH = os.path.join(DATA, "nyc_taxi_100m_duckdb/nyc.duckdb")
FARE_I32 = os.path.join(DATA, "nyc100_plain/fare.i32")
PASS_I32 = os.path.join(DATA, "nyc100_plain/passenger.i32")
RUNS = _artifact.expand("../../work/bench_baselines/runs")
REFERENCE = os.path.join(RUNS, "reference.json")

SQL_TEMPLATE = (
    "SELECT passenger_count AS g, SUM(fare_cents) AS s, COUNT(*) AS c, "
    "MIN(fare_cents) AS mn, MAX(fare_cents) AS mx FROM nyc {w} GROUP BY 1 ORDER BY 1"
)

# (name, WHERE clause, label).  Same eight cases as the EAI-cube receipt so the
# external family and the cube family answer literally the same SQL.
CASES = [
    ("fixed_all4", "", "no predicate, 100M rows"),
    ("le_m7000", "WHERE fare_cents <= -7000", "fare<=-7000 (0.135%)"),
    ("le_m720", "WHERE fare_cents <= -720", "fare<=-720 (1%)"),
    ("le_509", "WHERE fare_cents <= 509", "fare<=509 (5%)"),
    ("le_650", "WHERE fare_cents <= 650", "fare<=650 (10%)"),
    ("le_930", "WHERE fare_cents <= 930", "fare<=930 (25%)"),
    ("le_1350", "WHERE fare_cents <= 1350", "fare<=1350 (50%)"),
    ("le_99900", "WHERE fare_cents <= 99900", "fare<=99900 (100%)"),
]
THRESHOLDS = {"le_m7000": -7000, "le_m720": -720, "le_509": 509, "le_650": 650,
              "le_930": 930, "le_1350": 1350, "le_99900": 99900}

REPS = int(os.environ.get("BASELINE_REPS", "30"))
WARMUPS = int(os.environ.get("BASELINE_WARMUPS", "3"))  # untimed executions before timing
SLOW_STOP_MS = 2000.0     # if median exceeds this, stop early ...
MIN_REPS = 10             # ... but never below this many timed reps


def sql_for(where):
    return SQL_TEMPLATE.format(w=where)


def norm_rows(rows):
    """Normalise any engine's rows to [int|None key, int s, int c, int mn, int mx]."""
    out = []
    for r in rows:
        g = r[0]
        if isinstance(g, str):
            g = None if g.strip().lower() in ("null", "none") else int(g)
        elif g is not None:
            g = int(g)
        out.append([g, int(r[1]), int(r[2]), int(r[3]), int(r[4])])
    return out


def sort_rows(rows):
    return sorted(rows, key=lambda r: (r[0] is None, 0 if r[0] is None else r[0]))


def compare(rows, ref_rows):
    """Exactness vs DuckDB reference: every group key/measure must match exactly."""
    a = sort_rows(norm_rows(rows))
    b = sort_rows(norm_rows(ref_rows))
    if a == b:
        return {"exact": True, "n_groups": len(a)}
    mism = []
    keys = {r[0] for r in a} | {r[0] for r in b}
    am = {r[0]: r for r in a}
    bm = {r[0]: r for r in b}
    for k in sorted(keys, key=lambda x: (x is None, 0 if x is None else x)):
        if am.get(k) != bm.get(k):
            mism.append({"group": k, "got": am.get(k), "ref": bm.get(k)})
    return {"exact": False, "n_groups": len(a), "ref_groups": len(b),
            "n_mismatch": len(mism), "mismatch": mism[:4]}


def warmup(fn, n=None):
    """Untimed executions before the timed loop (engine caches/JIT settle)."""
    for _ in range(n or WARMUPS):
        fn()


def timed(fn, reps=None):
    """Run fn() reps times; return latency stats (ms) and the last result rows.

    Early stop (with the true rep count recorded) only for engines whose single
    execution is already > SLOW_STOP_MS, to keep the sweep bounded.
    """
    reps = reps or REPS
    ts, rows = [], None
    for _ in range(reps):
        t0 = time.perf_counter()
        rows = fn()
        ts.append((time.perf_counter() - t0) * 1000.0)
        if len(ts) >= MIN_REPS and statistics.median(ts) > SLOW_STOP_MS:
            break
    s = sorted(ts)
    n = len(s)
    return {
        "reps": n,
        "median_ms": statistics.median(s),
        "mean_ms": statistics.fmean(s),
        "min_ms": s[0],
        "p95_ms": s[min(n - 1, max(0, math.ceil(0.95 * n) - 1))],
    }, rows


def load_reference():
    with open(REFERENCE) as f:
        return json.load(f)


def make_receipt(method, version, input_format, storage_bytes, notes="",
                 extra=None, cases=None, load_ms=None, built_ms=None):
    ref = load_reference()
    out = {
        "method": method,
        "engine_version": version,
        "input_format": input_format,
        "storage_bytes": storage_bytes,
        "storage_human": human(storage_bytes),
        "declared_threads": 1,
        "host": platform.node(),
        "cpu": platform.processor() or "AMD EPYC 9554",
        "python": platform.python_version(),
        "protocol": {
            "reps_requested": REPS,
            "slow_stop_ms": SLOW_STOP_MS,
            "min_reps": MIN_REPS,
            "warmup": WARMUPS,
            "latency": "execute full query + fetch complete 11-row result",
            "threads": 1,
            "pinning": "taskset -c 56, nice -n 5",
        },
        "reference": "DuckDB 2.0.0 rows in runs/reference.json",
        "load_ms": load_ms,
        "build_ms": built_ms,
        "notes": notes,
        "cases": {},
        "exact_all": True,
    }
    if extra:
        out.update(extra)
    for name, where, label in CASES:
        if cases is None or name not in cases:
            continue
        st, rows = cases[name]
        cmp = compare(rows, ref["cases"][name]["rows"])
        out["cases"][name] = {
            "label": label,
            "where": where,
            "sql": sql_for(where),
            **st,
            "result": norm_rows(rows),
            "exactness": cmp,
        }
        if not cmp["exact"]:
            out["exact_all"] = False
    return out


def write_receipt(receipt, path=None):
    path = path or os.path.join(RUNS, receipt["method"] + ".json")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(receipt, f, indent=1)
    print("\n=== %s  (%s, input=%s, storage=%s) ===" % (
        receipt["method"], receipt["engine_version"], receipt["input_format"],
        receipt["storage_human"]))
    if receipt.get("load_ms") is not None:
        print("load/build: load %.1f ms  build %s" % (
            receipt["load_ms"], ("%.1f ms" % receipt["build_ms"]) if receipt.get("build_ms") is not None else "n/a"))
    print("%-12s %10s %10s %10s %6s %7s" % ("case", "median_ms", "min_ms", "p95_ms", "reps", "exact"))
    for name, c in receipt["cases"].items():
        print("%-12s %10.3f %10.3f %10.3f %6d %7s" % (
            name, c["median_ms"], c["min_ms"], c["p95_ms"], c["reps"],
            c["exactness"]["exact"]))
    print("receipt ->", path)
    return path


def human(n):
    if n is None:
        return "n/a"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024 or unit == "TB":
            return "%.1f %s" % (n, unit) if unit != "B" else "%d B" % n
        n /= 1024.0


def cli_main(load_fn, run_fn, *, method, version, input_format, storage_bytes,
             notes="", extra=None, receipt_path=None):
    """Standard driver: load once, warm up, time every case, write receipt."""
    t0 = time.perf_counter()
    state = load_fn()
    load_ms = (time.perf_counter() - t0) * 1000.0
    cases = {}
    for name, where, _label in CASES:
        # warmup
        warmup(lambda: run_fn(state, name, where))
        st, rows = timed(lambda: run_fn(state, name, where))
        cases[name] = (st, rows)
        print("%-12s median=%10.3f ms  reps=%d" % (name, st["median_ms"], st["reps"]), flush=True)
    rec = make_receipt(method, version, input_format, storage_bytes, notes=notes,
                       extra={**(extra or {}), "load_ms": load_ms},
                       cases=cases)
    return write_receipt(rec, receipt_path)
