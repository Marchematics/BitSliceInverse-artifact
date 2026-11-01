#!/usr/bin/env python3
"""A/B the engine-resident latency of two ECUBE binaries on the same 8 cases.

Purpose: ECUBE03 is a strict superset of ECUBE02 (NULL-measure support, +44 B index).
The Lead wants to know whether the hot path for non-null data is unchanged, i.e. whether
ECUBE02's numbers reproduce on ECUBE03.  Host load moves sub-microsecond latencies by tens
of percent, so the two binaries are run **alternately inside the same quiet window** and
the per-case medians are compared; the absolute numbers also land in the receipts.

  nice -n 5 taskset -c 56 /usr/bin/python3 bench_ab_cube.py
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import json
import os
import re
import statistics
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as C

R = _artifact.expand("../../research/real_decoders_20261024")
BINS = {
    "eai_ecube02_frozen": (os.path.join(R, "src_eai_20261101/enc_cube"),
                           _artifact.expand("../../work/runs/nyc100_v2.ecube")),
    "eai_ecube03_frozen": (os.path.join(R, "src_eai_20261101/enc_cube3"),
                           _artifact.expand("../../work/runs/nyc100_v3.ecube")),
}
DICT = os.path.join(C.DATA, "nyc_taxi_100m_prep/Observed/state.dict")
ROUNDS = int(os.environ.get("AB_ROUNDS", "5"))
REPS = int(os.environ.get("AB_REPS", "1000"))


def spec(name):
    return [] if name == "fixed_all4" else ["hi=%d" % C.THRESHOLDS[name]]


def one(key, case):
    p = subprocess.run([BINS[key][0], "bench", BINS[key][1], DICT, "/dev/null", str(REPS)]
                       + spec(case), capture_output=True, text=True, check=True)
    return json.loads(re.findall(r"\{.*\}", p.stderr)[-1])


def main():
    load = open("/proc/loadavg").read().strip()
    print("loadavg:", load, flush=True)
    data = {b: {c: [] for c, _w, _l in C.CASES} for b in BINS}
    for rnd in range(ROUNDS):
        for case, _w, _l in C.CASES:
            for b in BINS:                      # alternate inside the same window
                m = one(b, case)
                data[b][case].append((m["query_median_ms"], m["serialize_median_ms"]))
    out = {"window": {"loadavg": load, "rounds": ROUNDS, "reps": REPS,
                      "order": "alternating per case, same window",
                      "pinned": "taskset -c 56 inherited from parent"},
           "cases": {}}
    print("%-12s %10s %10s %8s %10s %10s %8s" % (
        "case", "ECUBE02 q", "ECUBE03 q", "delta", "ECUBE02 ser", "ECUBE03 ser", "delta"))
    for case, _w, _l in C.CASES:
        q2 = statistics.median([x[0] for x in data["eai_ecube02_frozen"][case]]) * 1000
        q3 = statistics.median([x[0] for x in data["eai_ecube03_frozen"][case]]) * 1000
        s2 = statistics.median([x[1] for x in data["eai_ecube02_frozen"][case]]) * 1000
        s3 = statistics.median([x[1] for x in data["eai_ecube03_frozen"][case]]) * 1000
        out["cases"][case] = {
            "ecube02_query_us": q2, "ecube03_query_us": q3,
            "ecube02_serialize_us": s2, "ecube03_serialize_us": s3,
            "query_delta_pct": (q3 - q2) / q2 * 100.0,
            "serialize_delta_pct": (s3 - s2) / s2 * 100.0,
        }
        print("%-12s %10.3f %10.3f %7.1f%% %10.3f %10.3f %7.1f%%" % (
            case, q2, q3, (q3 - q2) / q2 * 100, s2, s3, (s3 - s2) / s2 * 100), flush=True)
    deltas = [v["query_delta_pct"] for v in out["cases"].values()]
    out["summary"] = {"query_delta_pct_median": statistics.median(deltas),
                      "query_delta_pct_range": [min(deltas), max(deltas)]}
    print("query delta median %.1f%% (range %.1f..%.1f%%)" % (
        out["summary"]["query_delta_pct_median"],
        *out["summary"]["query_delta_pct_range"]), flush=True)
    json.dump(out, open(os.path.join(C.RUNS, "ab_ecube02_vs_ecube03.json"), "w"), indent=1)
    print("receipt ->", os.path.join(C.RUNS, "ab_ecube02_vs_ecube03.json"))


if __name__ == "__main__":
    main()
