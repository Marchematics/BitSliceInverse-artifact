#!/usr/bin/env python3
"""Interface probes for the ECUBE01 CLI, run on the patched SPECS binary.

These are *not* exactness specs: they pin down documented/undocumented
interface boundaries so the corpus and the fuzz campaign never rely on
behaviour the cube does not claim.

  P1  repeated `having=` arguments: last one wins (parse_spec assigns, not appends)
  P2  `where_in` + `where_not_in` together: both are applied (conjunction)
  P3  NULL group key: kept for fixed GROUP BY, dropped for IN and NOT IN
      (SQL three-valued logic; the NOT IN case is SPEC-RUNNER FIX 2)
  P4  inverted range lo>hi -> empty result
  P5  one-sided lo= / hi=
  P6  group names containing ',' cannot be expressed through where_in (CSV split)
  P7  HAVING on each of sum/count/min/max with each comparison operator
  P8  value NULLs are outside the structure (build reads int32 payloads only)
  P9  dense build precondition: contiguous observed domain (FIX 1)
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import json
import os
import struct
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pairs as P

BASE = P.BASE
BIN = f"{BASE}/bin/enc_cube_specs"


def cube_rows(cube, dic, args):
    out = f"{BASE}/out/_probe.json"
    p = subprocess.run([BIN, "query", cube, dic, out] + args,
                       capture_output=True, text=True)
    if p.returncode != 0:
        return None, p.stderr[-300:]
    return json.load(open(out))["rows"], None


def header(path):
    d = open(path, "rb").read(64)
    n, ng, nv = struct.unpack("<QQQ", d[8:32])
    vmin, = struct.unpack("<q", d[32:40])
    dense, nmeas, hnull, nullg = struct.unpack("<IIII", d[40:56])
    return dict(n=n, ngroups=ng, nvalues=nv, vmin=vmin, dense=dense,
                nmeasures=nmeas, has_null=hnull, null_group=nullg)


def main():
    probes = {}
    # P1: repeated having -> only the last predicate is applied
    rows, err = cube_rows(f"{BASE}/cubes/C2.ecube", f"{BASE}/tables/C2/d.dict",
                          ["having=count>=99999999", "having=count>=1000000"])
    probes["P1_repeated_having_last_wins"] = dict(
        args=["having=count>=99999999", "having=count>=1000000"],
        rows=len(rows) if rows is not None else None,
        verdict="last predicate only: first (impossible) condition ignored",
        supported=False)
    # P2: IN + NOT IN conjunctive
    rows, err = cube_rows(f"{BASE}/cubes/C2.ecube", f"{BASE}/tables/C2/d.dict",
                          ["where_in=1,2,3", "where_not_in=2"])
    probes["P2_in_and_not_in"] = dict(
        args=["where_in=1,2,3", "where_not_in=2"],
        rows=sorted(r[0] for r in rows) if rows is not None else None,
        verdict="both predicates applied (IN AND NOT IN)", supported=True)
    # P3: NULL group key (C1 has 9.6M NULL keys)
    m = json.load(open(f"{BASE}/out/pairs_meta.json"))["C1"]
    for tag, args in [("fixed", []),
                      ("in", ["where_in=1,2"]),
                      ("not_in", ["where_not_in=1,2"]),
                      ("not_in_all", ["where_not_in=" + ",".join(m["names"])])]:
        rows, err = cube_rows(f"{BASE}/cubes/C1.ecube", f"{BASE}/tables/C1/d.dict", args)
        probes[f"P3_null_group_{tag}"] = dict(
            args=args, nrows=len(rows) if rows is not None else None,
            has_null_group=any(r[0] == "NULL" for r in rows) if rows else None)
    probes["P3_verdict"] = ("NULL group kept only when no group-domain predicate "
                            "is present (SQL three-valued logic)")
    # P4/P5: inverted and one-sided ranges
    for tag, args in [("inverted", ["range=100,50"]), ("lo_only", ["lo=500"]),
                      ("hi_only", ["hi=500"]), ("equal", ["range=650,650"])]:
        rows, err = cube_rows(f"{BASE}/cubes/C2.ecube", f"{BASE}/tables/C2/d.dict", args)
        probes[f"P4_range_{tag}"] = dict(args=args, nrows=len(rows) if rows is not None else None)
    # P6: comma in a group name cannot be expressed
    probes["P6_comma_in_group_name"] = dict(
        verdict="where_in/where_not_in are comma-split, so a group name containing "
                "',' cannot be filtered through the CLI; no such name exists in the "
                "corpus datasets (passenger_count, returnflag, shipmode, "
                "shipinstruct, linenumber, partkey, orderkey, synthetic CASE)")
    # P7: HAVING matrix on one pair
    rows, err = cube_rows(f"{BASE}/cubes/C2.ecube", f"{BASE}/tables/C2/d.dict", [])
    base = {r[0]: r for r in rows}
    conds = ["sum>=1000000", "sum<=1000000", "count>=1000000", "count<=1000000",
             "min<=0", "min>=0", "max>=100000", "max<=100000"]
    p7 = {}
    for c in conds:
        rr, e = cube_rows(f"{BASE}/cubes/C2.ecube", f"{BASE}/tables/C2/d.dict", ["having=" + c])
        p7[c] = len(rr) if rr is not None else None
    probes["P7_having_matrix"] = dict(rows_per_condition=p7,
                                      expected_total_groups=len(base))
    # P8/P9: build-side boundaries
    meta = json.load(open(f"{BASE}/out/pairs_meta.json"))
    probes["P8_null_measure_values"] = dict(
        verdict="outside ECUBE01: build reads int32 payload columns, so a NULL "
                "measure must be removed before the cube is built; all corpus and "
                "fuzz pairs assert v IS NOT NULL at materialisation")
    probes["P9_dense_precondition"] = dict(
        contiguous_C9=header(f"{BASE}/cubes/C9.ecube"),
        gappy_C11=header(f"{BASE}/cubes/C11.ecube"),
        verdict="post-fix build_plain sets dense only when span == V (FIX 1); "
                "the pre-fix binary sets dense=1 for the gappy C11 domain")
    json.dump(probes, open(f"{BASE}/out/interface_probe.json", "w"), indent=1)
    print(json.dumps({k: (v if not isinstance(v, dict) else
                          {kk: vv for kk, vv in v.items() if kk != "rows"})
                      for k, v in probes.items()}, indent=1)[:2000])


if __name__ == "__main__":
    main()
