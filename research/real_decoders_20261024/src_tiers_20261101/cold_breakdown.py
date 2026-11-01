#!/usr/bin/env python3
"""Cold-complete-request floor decomposition for the 65K short-query class.

A complete request is: launch a fresh process, open the index/DB, run the query,
and write the serialized result. At 65K rows that is startup-dominated, so this
script measures the launch floor separately from the work:

  true            nice+taskset wrapper + /bin/true            (process-launch floor)
  enc_cube_query  the real ECUBE02 complete request           (pinned binary)
  python_noop     the DuckDB reference runtime floor          (python3 -c pass)

25 launches per variant, one 0.5 s pinned warmup before each launch so the numbers
are not P-state artifacts; median/P95/min reported. Pinned core 96, nice 5.

usage: cold_breakdown.py            (writes receipts/tiers_20261101/cold_breakdown.json)
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import json
import os
import statistics
import subprocess
import time

REPO = os.path.abspath(os.getcwd())
SCRATCH = _artifact.expand(os.environ.get("TIERS_SCRATCH", "../../work/tiers/build"))
BIN = _artifact.expand(os.environ.get("TIERS_BIN", "../../work/tiers/bin"))
RECEIPT = os.path.join(_artifact.WORK, "receipts", "tiers_20261101")
CORE = _artifact.core()
CUBE = os.path.join(BIN, "enc_cube")
DICT = os.path.join(REPO, "results/ig2_prep_20261025/prepared/Observed/state.dict")
CUBE_FILE = os.path.join(SCRATCH, "ig2_65k.ecube")
OUT = os.path.join(SCRATCH, "cold_same.json")


def warm():
    subprocess.run(["nice", "-n", "5", "taskset", "-c", CORE, "timeout", "0.5s",
                    "bash", "-c", "while :; do :; done"], capture_output=True)


def launch(cmd):
    t = time.perf_counter()
    subprocess.run(cmd, capture_output=True)
    return (time.perf_counter() - t) * 1000.0


def main():
    variants = {
        "true": ["nice", "-n", "5", "taskset", "-c", CORE, "/bin/true"],
        "enc_cube_query": ["nice", "-n", "5", "taskset", "-c", CORE, CUBE, "query",
                           CUBE_FILE, DICT, OUT, "where_not_in=District of Columbia"],
        "python_noop": ["nice", "-n", "5", "taskset", "-c", CORE,
                        _artifact.expand("python3"), "-c", "pass"],
    }
    res = {"launches": 25, "pin": f"nice -n 5 taskset -c {CORE}",
           "protocol": "fresh process per launch, 0.5 s pinned warmup before each launch",
           "variants": {}}
    for name, cmd in variants.items():
        xs = []
        for _ in range(25):
            warm()
            xs.append(launch(cmd))
        xs.sort()
        res["variants"][name] = {"median_ms": statistics.median(xs), "min_ms": xs[0],
                                 "p95_ms": xs[min(len(xs) - 1, int(len(xs) * 0.95))]}
    os.makedirs(RECEIPT, exist_ok=True)
    json.dump(res, open(os.path.join(RECEIPT, "cold_breakdown.json"), "w"), indent=1)
    json.dump(res, open(os.path.join(SCRATCH, "cold_breakdown.json"), "w"), indent=1)
    print(json.dumps(res, indent=1))


if __name__ == "__main__":
    main()
