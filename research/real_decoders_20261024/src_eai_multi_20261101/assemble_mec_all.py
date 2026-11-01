#!/usr/bin/env python3
"""Assemble MEC_ALL.json from the per-dataset receipt files (sweep.json +
diff_fuzz.json).  Lets a partially re-run battery still produce one roll-up."""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import json
import os
import sys

base = sys.argv[1] if len(sys.argv) > 1 else "results/eai_multi_20261101"
names = sys.argv[2].split(",") if len(sys.argv) > 2 else \
    ["tpch2", "tpch2_null", "syn_cluster", "nyc3", "nyc3b"]
out = dict(protocol=dict(
    pin="nice -n 5 taskset -c 64",
    duckdb="2.0.0|threads=1|warm persistent connection",
    mec="warm process, 1 warmup + REP timed in-process queries (index mmap'd once)",
    bitplane="CLI re-run, internal query_ms",
    sorted_index="CLI re-run, internal query_ms",
    note="all ends exclude result serialisation"), datasets={})
for n in names:
    sp = os.path.join(base, n, "sweep.json")
    if not os.path.exists(sp):
        print("missing", sp)
        continue
    d = json.load(open(sp))
    fz = {}
    fp = os.path.join(base, n, "diff_fuzz.json")
    if os.path.exists(fp):
        j = json.load(open(fp))
        fz = dict(total=j["total"], mismatches=j["mismatches"],
                  group_filter_cases=j.get("group_filter_cases"))
    # recompute exactness from the per-case flags so schema drift cannot mask a failure
    keys = ("full", "sc", "bitplane_nonnull", "sorted_index")
    ex = all(all(c["exact"].get(k, k == "bitplane_nonnull") for k in keys)
             for c in d["cases"].values())
    out["datasets"][n] = dict(meta=d["meta"], plan=d["plan"], build=d["build"],
                              info=d["info"], storage=d["storage"],
                              baselines_build=d["baselines_build"],
                              exact_all=ex, fuzz=fz, cases=d["cases"])
    print(n, "exact:", ex, "fuzz mismatches:", fz.get("mismatches"))
json.dump(out, open(os.path.join(base, "MEC_ALL.json"), "w"), indent=1)
print("assembled", os.path.join(base, "MEC_ALL.json"), "datasets:", list(out["datasets"]))
print("exact everywhere:", all(v["exact_all"] and v["fuzz"].get("mismatches", 0) == 0
                              for v in out["datasets"].values()))
