#!/usr/bin/env python3
"""Cabin (SIGMOD 2024) selection-only baseline on the NYC fare column.

Cabin is a compressed binned *scan index*: CreateIndex() then Scan(lo,hi) returns the
qualifying row positions (a bitmap). It has no aggregate API, so this receipt measures
the selection only and records "output = positions, not a SQL aggregate". The official
prototype's own CheckResult() validates the selection against a brute-force pass.

Uses the pre-built official prototype + the NYC harness already present on the host:
  ../../work/Cabin/cabin_nyc   (links the official Cabin sources)
which prints {"rows":N,"index_ms":..,"scan_ms":..,"check_ms":..,"correct":true} per run.

  nice -n 5 taskset -c 56 /usr/bin/python3 bench_cabin.py
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

BIN = os.environ.get("CABIN_BIN", _artifact.expand("../../work/Cabin/cabin_nyc"))
CORE = os.environ.get("CORE", "56")
REPS = int(os.environ.get("CABIN_REPS", "2"))
LO = -1000000000


def one(hi):
    t0 = time.perf_counter()
    r = subprocess.run(["nice", "-n", "5", "taskset", "-c", CORE, BIN, C.FARE_I32,
                        str(LO), str(hi)], capture_output=True, text=True)
    wall_ms = (time.perf_counter() - t0) * 1000.0
    d = json.loads(r.stdout.strip().splitlines()[-1])
    d["wall_ms"] = wall_ms
    return d


def main():
    if not os.path.exists(BIN):
        raise SystemExit("missing Cabin NYC harness: " + BIN)
    cases = {}
    for name, where, _l in C.CASES:
        hi = C.THRESHOLDS.get(name, 1000000000)
        runs = [one(hi) for _ in range(REPS)]
        cases[name] = {
            "reps": len(runs),
            "scan_ms": statistics.median([x["scan_ms"] for x in runs]),
            "index_ms": statistics.median([x["index_ms"] for x in runs]),
            "check_ms": statistics.median([x["check_ms"] for x in runs]),
            "complete_request_ms": statistics.median([x["wall_ms"] for x in runs]),
            "correct": all(x["correct"] for x in runs),
            "raw": runs,
        }
        print("%-12s index=%8.1f ms scan=%8.3f ms check=%8.1f ms wall=%8.1f ms correct=%s" % (
            name, cases[name]["index_ms"], cases[name]["scan_ms"],
            cases[name]["check_ms"], cases[name]["complete_request_ms"],
            cases[name]["correct"]), flush=True)
    rec = {
        "method": "cabin_sigmod24_scan",
        "engine_version": "Cabin official prototype (PACMMOD/SIGMOD 2024) + NYC harness",
        "venue": "Proc. ACM Manag. Data 2(1):57 (SIGMOD 2024), Yiyuan Chen, Shimin Chen",
        "input_format": "plain int32 fare column (400 MB); Cabin builds its index in-process",
        "output": "positions only (qualifying-row bitmap + CheckResult validation); NOT a SQL aggregate",
        "storage_bytes": None,
        "storage_note": "index bytes not exposed by the prototype harness; process RSS ~1.9 GB "
                        "for 100M values incl. the 400 MB source column",
        "declared_threads": 1,
        "pinning": "nice -n 5 taskset -c %s" % CORE,
        "protocol": {"reps_per_case": REPS, "index_rebuilt_per_process": True,
                     "latency": "Scan(lo,hi) selection only; no aggregation pass"},
        "cases": cases,
        "exact_all": all(c["correct"] for c in cases.values()),
        "notes": "Cabin answers 'which rows match', not 'what are the group aggregates'. "
                 "A fair SQL comparison would add a positions->values gather + aggregate pass "
                 "(At 100% selectivity a bare Arrow in-memory filter+aggregate already costs "
                 "~1.78 s, so Cabin cannot compete on this query class even ignoring the pass.)",
    }
    path = os.path.join(C.RUNS, "cabin_sigmod24_scan.json")
    json.dump(rec, open(path, "w"), indent=1)
    print("receipt ->", path)


if __name__ == "__main__":
    main()
