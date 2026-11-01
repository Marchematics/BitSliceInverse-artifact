#!/usr/bin/env python3
"""Cold complete-request measurement for the 1B cube (and the DuckDB control).

Runs on the host. Measures parent-observed wall time of fresh processes:
  - our binary `enc_cube3 query <cube> <dict> <out>` (index open + query + serialized
    result), 100 launches, median/mean/p95;
  - a DuckDB 2.0.0 child process (import + connect + SET threads=1 + the same
    grouped SQL over the 1B parquet + fetchall), 3 launches (each ~9 s).
Prints one JSON document.
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import glob
import json
import os
import statistics
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # research/real_decoders_20261024
R = os.path.join(ROOT, "results/scale_20261101")
PY = _artifact.expand("python3")
ENC = os.path.join(ROOT, "src_eai_20261101/enc_cube3")


def bench(cmd, n):
    ts = []
    for _ in range(n):
        t = time.perf_counter()
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        ts.append((time.perf_counter() - t) * 1000.0)
    ts.sort()
    return {"n": n, "median_ms": statistics.median(ts), "mean_ms": statistics.mean(ts),
            "min_ms": ts[0], "p95_ms": ts[max(0, int(len(ts) * 0.95) - 1)]}


def main():
    out = {}
    cubes = {}
    g11 = "/tmp/nyc1b_v3.ecube"
    if os.path.exists(g11):
        cubes["g11_1b"] = g11
    # G=101 1B cube: rebuild from the plain arrays if present
    g101_gid = os.path.join(R, "fv1000000000_gid.i32")
    g101_val = os.path.join(R, "fv1000000000_val.i32")
    cand = glob.glob(os.path.join(R, "*1000000000*_gid.i32"))
    if not (os.path.exists(g101_gid) and os.path.exists(g101_val)):
        other = [c for c in cand if "g11" not in c]
        if other:
            g101_gid = other[0]
            g101_val = g101_gid.replace("_gid.i32", "_val.i32")
    if os.path.exists(g101_gid) and os.path.exists(g101_val) and "g11" not in g101_gid:
        dst = "/tmp/nyc1b_g101_v3.ecube"
        p = subprocess.run([ENC, "build", g101_gid, g101_val, dst], capture_output=True, text=True)
        out["g101_build_stderr"] = p.stderr.strip().splitlines()[-1] if p.stderr else ""
        if p.returncode == 0:
            cubes["g101_1b"] = dst
    out["cubes"] = {}
    for name, cube in cubes.items():
        out["cubes"][name] = {
            "bytes": os.path.getsize(cube),
            "sha256": subprocess.check_output(["sha256sum", cube]).decode().split()[0],
            "cold": bench([ENC, "query", cube, os.path.join(R, "dict_1b.bin"), "/tmp/cold_out.json"], 100),
        }
    parquet = os.path.join(R, "nyc1b.parquet")
    code = ("import duckdb;c=duckdb.connect();c.execute('SET threads=1');"
            "c.execute(\"select g,sum(v),count(*),min(v),max(v) from read_parquet('%s') "
            "group by 1 order by 1\").fetchall()" % parquet)
    chk = subprocess.run([PY, "-c", code], capture_output=True, text=True)
    out["duckdb_query_ok"] = chk.returncode == 0
    if chk.returncode != 0:
        out["duckdb_error"] = chk.stderr.strip().splitlines()[-1] if chk.stderr else ""
    else:
        out["duckdb_cold"] = bench([PY, "-c", code], 3)
    json.dump(out, sys.stdout, indent=1)


if __name__ == "__main__":
    main()
