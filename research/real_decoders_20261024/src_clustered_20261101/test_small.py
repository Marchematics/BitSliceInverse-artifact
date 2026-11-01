#!/usr/bin/env python3
"""Fast differential self-test for CLST01 on a small slice (debug loop only).

Builds a 1M-row slice of the NYC-100M value column with G=100,000 synthetic
gids, then checks the clustered structure against an independent NumPy
brute-force aggregation on ~40 ranges (random, boundary, empty, out-of-domain,
single-value, full).  This is the developer loop; the campaign's exactness
claim is the element-wise comparison against DuckDB 2.0.0 in run_clustered.py.

    python3 src_clustered_20261101/test_small.py
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import json
import os
import subprocess
import sys

import numpy as np

SCRATCH = _artifact.expand(os.environ.get("CLST_SCRATCH", "../../work/clustered/build"))
BIN = _artifact.expand(os.environ.get("CLST_BIN", "../../work/clustered/bin"))
TOOL = os.path.join(BIN, "clst_tool")
TIERS = _artifact.expand("../../work/tiers/build")
N = 1_000_000
G = 100_000
CORE = _artifact.core()


def sh(cmd):
    p = subprocess.run(["nice", "-n", "5", "taskset", "-c", CORE] + cmd,
                       capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"{cmd[:3]} rc={p.returncode}\n{p.stderr[-2000:]}")
    return p


def last_json(s):
    for line in reversed(s.splitlines()):
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            return json.loads(line)
    raise RuntimeError("no json: " + s[-500:])


def brute(vals, gids, lo, hi, G):
    """exact per-group SUM/COUNT/MIN/MAX over rows with lo <= v <= hi"""
    m = (vals >= lo) & (vals <= hi)
    v = vals[m].astype(np.int64)
    g = gids[m].astype(np.int64)
    if v.size == 0:
        return []
    order = np.lexsort((v, g))
    vs, gs = v[order], g[order]
    uniq, start = np.unique(gs, return_index=True)
    cnt = np.diff(np.append(start, gs.size))
    ssum = np.add.reduceat(vs, start)
    mn = vs[start]
    mx = vs[np.append(start[1:], gs.size) - 1]
    return [(int(a), int(b), int(c), int(d), int(e))
            for a, b, c, d, e in zip(uniq, ssum, cnt, mn, mx)]


def parse_rows(rows):
    return [(int(r[0]), int(r[1]), int(r[2]), int(r[3]), int(r[4])) for r in rows]


def main():
    os.makedirs(SCRATCH, exist_ok=True)
    valp = os.path.join(TIERS, "val_nyc100.i32")
    gidp = os.path.join(TIERS, f"gid_g{G}.i32")
    sv = os.path.join(SCRATCH, "slice_val.i32")
    sg = os.path.join(SCRATCH, "slice_gid.i32")
    if not os.path.exists(sv):
        np.memmap(valp, dtype=np.int32, mode="r", shape=(N,))[:].tofile(sv)
        np.memmap(gidp, dtype=np.int32, mode="r", shape=(N,))[:].tofile(sg)
    vals = np.fromfile(sv, dtype=np.int32)
    gids = np.fromfile(sg, dtype=np.int32)
    assert vals.size == N and gids.size == N

    out = os.path.join(SCRATCH, "slice.clst")
    p = sh([TOOL, "build", sg, sv, out, "--with-stats"])
    print("build:", last_json(p.stderr)["build_ms"], "ms")
    p = sh([TOOL, "info", out])
    print("info:", p.stdout.strip())
    p = sh([TOOL, "verify", sg, sv, out])
    vj = json.loads(p.stdout)
    print("verify:", vj)
    assert vj["exact"] == 1, "losslessness check failed"

    vmin, vmax = int(vals.min()), int(vals.max())
    uniq, cnts = np.unique(vals, return_counts=True)
    rng = np.random.default_rng(20261101)
    cases = []
    for lo, hi in [(int(vmin), int(vmin)), (int(vmax), int(vmax)),
                   (int(uniq[len(uniq) // 2]), int(uniq[len(uniq) // 2])),
                   (vmin, vmax), (-2**31, 2**31 - 1), (2**31 - 1, 2**31 - 1),
                   (-2**31, -2**31), (vmax, vmax - 1), (10**15, 2 * 10**15),
                   (0, -1)]:
        cases.append((lo, hi))
    for _ in range(20):
        a = int(rng.integers(0, len(uniq)))
        b = int(rng.integers(0, len(uniq)))
        lo, hi = sorted((int(uniq[a]), int(uniq[b])))
        cases.append((lo, hi))
    for _ in range(10):
        lo = int(rng.integers(vmin - 1000, vmax + 1000))
        hi = int(rng.integers(lo, vmax + 1000))
        cases.append((lo, hi))
    # single distinct values with the smallest / largest counts
    order = np.argsort(cnts)
    for i in (order[0], order[1], order[-1]):
        cases.append((int(uniq[i]), int(uniq[i])))

    bad = 0
    for lo, hi in cases:
        qj = os.path.join(SCRATCH, "slice_out.json")
        p = sh([TOOL, "query", out, str(lo), str(hi), qj, "/dev/null", "5"])
        q = last_json(p.stderr)
        mine = parse_rows(json.load(open(qj))["rows"])
        ref = brute(vals, gids, lo, hi, G)
        ok = mine == ref
        if not ok:
            bad += 1
            print(f"  MISMATCH lo={lo} hi={hi} mine={len(mine)} ref={len(ref)}")
            sm, sr = set(mine), set(ref)
            print("   only_mine", sorted(sm - sr)[:3], "only_ref", sorted(sr - sm)[:3])
        if q["fast_path"]:
            # also exercise the O(n) scan path on the full range
            p2 = sh([TOOL, "query", out, str(lo), str(hi), qj, "/dev/null", "3",
                     "--no-fastpath"])
            q2 = last_json(p2.stderr)
            mine2 = parse_rows(json.load(open(qj))["rows"])
            ok = ok and mine2 == ref
            print(f"  full-range scan path identical: {mine2 == ref} "
                  f"scan={q2['query_median_ms']:.3f}ms fast={q['query_median_ms']:.3f}ms")
    print(f"cases={len(cases)} mismatches={bad}")
    if bad:
        sys.exit(1)
    print("SMALL-SLICE DIFFERENTIAL: PASS")


if __name__ == "__main__":
    main()
