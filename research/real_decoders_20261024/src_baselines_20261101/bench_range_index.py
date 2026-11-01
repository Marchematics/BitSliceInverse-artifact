#!/usr/bin/env python3
"""Independent sorted row-ID range index baseline (CUBIT/RABIT-like, single column).

  nice -n 5 taskset -c 56 /usr/bin/python3 bench_range_index.py
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import json
import os
import statistics
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as C

HERE = os.path.dirname(os.path.abspath(__file__))
BIN = os.path.join(HERE, "range_index_nyc")
IDX = os.path.join(_artifact.expand("../../work/bench_baselines/data"), "nyc100_fare_sorted.ridx")


def max_fare():
    # data max is 86,337,212; generous bound keeps the semantics "no predicate"
    return 1 << 60


def main():
    if not os.path.exists(BIN):
        raise SystemExit("missing %s (run build.sh)" % BIN)
    build = None
    if not os.path.exists(IDX):
        t0 = time.perf_counter()
        r = subprocess.run([BIN, "build", C.FARE_I32, C.PASS_I32, IDX],
                           capture_output=True, text=True, check=True)
        build = json.loads(r.stdout.strip().splitlines()[-1])
        build["wall_ms"] = (time.perf_counter() - t0) * 1000.0
        print("build:", build, flush=True)
    idx_bytes = os.path.getsize(IDX)
    cases, complete = {}, {}
    for name, where, _l in C.CASES:
        hi = C.THRESHOLDS.get(name, max_fare())
        lo = -(1 << 60)
        warm = subprocess.run([BIN, "query", IDX, C.FARE_I32, C.PASS_I32,
                               str(lo), str(hi), "1"], capture_output=True, text=True, check=True)
        r = subprocess.run([BIN, "query", IDX, C.FARE_I32, C.PASS_I32,
                            str(lo), str(hi), str(C.REPS)], capture_output=True, text=True, check=True)
        d = json.loads(r.stdout.strip().splitlines()[-1])
        rows = [[None if (x[0] is None or x[0] == "null") else int(x[0])] +
                [int(v) for v in x[1:]] for x in d["rows"]]
        # complete-request: 30 fresh processes (mmap + binary search + sweep + output)
        walls = []
        for _ in range(30):
            t0 = time.perf_counter()
            subprocess.run([BIN, "query", IDX, C.FARE_I32, C.PASS_I32,
                            str(lo), str(hi), "1"], capture_output=True, check=True)
            walls.append((time.perf_counter() - t0) * 1000.0)
        complete[name] = {"median_ms": statistics.median(walls), "min_ms": min(walls),
                          "p95_ms": sorted(walls)[int(0.95 * len(walls)) - 1], "reps": 30}
        cases[name] = ({"reps": d["reps"], "median_ms": d["median_ms"], "mean_ms": d["median_ms"],
                        "min_ms": d["min_ms"], "p95_ms": d["median_ms"]}, rows)
        print("%-12s range_rows=%-9d median=%10.3f ms complete=%8.3f ms" % (
            name, d["range_rows"], d["median_ms"], complete[name]["median_ms"]), flush=True)
    rec = C.make_receipt(
        "range_index_nyc", "independent reimplementation (C++17, single-column)",
        "two plain int32 files (fare, passenger) + sorted row-id index",
        idx_bytes,
        notes="CUBIT/RABIT-like sorted row-ID range index: 400 MB uint32 row ids sorted "
              "by fare; query binary-searches the range then gathers passenger_count and "
              "accumulates SUM/COUNT/MIN/MAX. Complete SQL aggregate output, exact. "
              "Input columns (2x400 MB) are the shared source, not counted as index storage. "
              "Not an official CUBIT build.",
        cases=cases, load_ms=None, built_ms=None,
        extra={"build": build, "index_bytes": idx_bytes,
               "complete_request_ms": complete,
               "source_columns_bytes": 2 * 400_000_000})
    return C.write_receipt(rec, os.path.join(C.RUNS, "range_index_nyc.json"))


if __name__ == "__main__":
    main()
