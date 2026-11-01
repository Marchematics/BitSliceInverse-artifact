#!/usr/bin/env python3
"""Run the ECUBE01 index over the NYC-100M case set, verify exactness against
DuckDB 2.0, and emit the combined comparison used by the paper tables.

Every case is run in one warm process (REPS timed executions after a warmup);
latency is the median per-query time excluding result serialization. Output
rows are compared with DuckDB's rows as multisets of (group,sum,count,min,max).
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import json
import re
import subprocess
import sys

CUBE = sys.argv[1]
INDEX = sys.argv[2]
DICT = sys.argv[3]
DUCK = sys.argv[4]
OUTPATH = sys.argv[5]
REPS = int(sys.argv[6]) if len(sys.argv) > 6 else 500

CASES = [
    ("fixed_all4", []),
    ("le_m7000", ["hi=-7000"]),
    ("le_m720", ["hi=-720"]),
    ("le_509", ["hi=509"]),
    ("le_650", ["hi=650"]),
    ("le_930", ["hi=930"]),
    ("le_1350", ["hi=1350"]),
    ("le_99900", ["hi=99900"]),
    ("narrow_80_90M", ["range=80000000,90000000"]),
    ("in_1_2", ["where_in=1,2"]),
    ("having", ["having=sum>=1e10"]),
]


def canon(rows):
    def norm(x):
        x = str(x)
        return "None" if x in ("NULL", "None", "null") else x
    return sorted([tuple([norm(r[0])] + [str(x) for x in r[1:]]) for r in rows], key=lambda r: r[0])


def main():
    duck = json.load(open(DUCK))
    out = {"cube": CUBE, "reps": REPS, "cases": {}}
    for name, spec in CASES:
        cmd = [CUBE, "bench", INDEX, DICT, "/dev/null", str(REPS)] + spec
        p = subprocess.run(cmd, capture_output=True, text=True)
        if p.returncode != 0:
            raise SystemExit(f"{name}: {p.stderr[-400:]}")
        met = json.loads(re.findall(r"\{.*\}", p.stderr)[-1])
        out_file = "../../work/runs/eai_case.json"
        p2 = subprocess.run([CUBE, "query", INDEX, DICT, out_file] + spec,
                            capture_output=True, text=True)
        if p2.returncode != 0:
            raise SystemExit(f"{name} query: {p2.stderr[-400:]}")
        rows = json.load(open("../../work/runs/eai_case.json"))["rows"]
        d = duck["cases"][name]
        exact = canon(rows) == canon(d["rows"])
        out["cases"][name] = {
            "cube_median_ms": met["query_median_ms"],
            "cube_p95_ms": met["query_p95_ms"],
            "cube_min_ms": met["query_min_ms"],
            "duckdb_median_ms": d["median_ms"],
            "duckdb_min_ms": d["min_ms"],
            "duckdb_p95_ms": d["p95_ms"],
            "speedup_median": d["median_ms"] / met["query_median_ms"] if met["query_median_ms"] else None,
            "exact": exact,
            "rows": rows,
            "duckdb_rows": d["rows"],
        }
    json.dump(out, open(OUTPATH, "w"), indent=1)
    for k, v in out["cases"].items():
        sp = v["speedup_median"]
        print(f"{k:16s} cube={v['cube_median_ms']*1000:9.2f}us duckdb={v['duckdb_median_ms']:9.3f}ms "
              f"speedup={sp:12.1f}x exact={v['exact']}")


if __name__ == "__main__":
    main()
