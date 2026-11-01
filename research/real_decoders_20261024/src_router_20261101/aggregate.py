#!/usr/bin/env python3
"""Aggregate S_q = T_strongest_baseline / T_ours over the measured case matrix.

ours     = the frozen structure for the class (cube for single-column queries,
           MEC for multi-column, materialized stats for high-cardinality fixed).
baseline = the strongest measured applicable baseline for that case among
           {DuckDB 2.0 scan, sorted row-id range index} plus the DuckDB
           materialized view for fixed GROUP BY (0.49 ms, baselines receipt).
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import csv
import math
import sys
from collections import defaultdict

path = sys.argv[1] if len(sys.argv) > 1 else "matrix_20261101.csv"
rows = list(csv.DictReader(open(path)))
by = defaultdict(dict)
meta = {}
for r in rows:
    key = (r["dataset"], r["case"])
    by[key][r["path"]] = float(r["lat_ms"])
    meta[key] = r
MV_FIXED_MS = 0.49  # DuckDB materialized TEMP tables, 2.8 KB, baselines receipt
ours_pref = ["cube", "mec", "clustered", "materialized", "block_summary"]
base_set = ["duckdb_scan", "sorted_index"]
out = []
for key, p in sorted(by.items()):
    ours_path = next((o for o in ours_pref if o in p), None)
    if ours_path is None:
        continue
    baselines = [p[b] for b in base_set if b in p]
    if meta[key]["case"] == "fixed" and meta[key]["dataset"] == "NYC100M":
        baselines.append(MV_FIXED_MS)
    if not baselines:
        continue
    strongest = min(baselines)
    S = strongest / p[ours_path]
    out.append((key[0], key[1], ours_path, p[ours_path], strongest, S))
S = [x[5] for x in out]
S_sorted = sorted(S)
n = len(S)
print(f"cases={n}")
print(f"min S_q = {S_sorted[0]:.2f}  ({out[S.index(S_sorted[0])][0]}/{out[S.index(S_sorted[0])][1]})")
print(f"median S_q = {S_sorted[n//2]:.1f}")
print(f"geomean S_q = {math.exp(sum(math.log(s) for s in S)/n):.1f}")
print(f"max S_q = {S_sorted[-1]:.1f}")
print(f"cases >=3x: {sum(1 for s in S if s >= 3)}/{n}   >=10x: {sum(1 for s in S if s >= 10)}/{n}")
for d, c, op, lat, base, s in out:
    print(f"  {d:10s} {c:14s} ours({op:12s})={lat:12.6f} ms  strongest_baseline={base:12.4f} ms  S={s:14.1f}")
