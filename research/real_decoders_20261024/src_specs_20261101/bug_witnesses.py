#!/usr/bin/env python3
"""Minimal witnesses for the four ECUBE01/ECUBE02 bugs fixed before the paper
run, re-executed on the FROZEN v3 (ECUBE03) binary.  Columns: v1 (ECUBE01,
pre-fix) must FAIL where the bug could occur; v2 (ECUBE02) and v3 (frozen) must
PASS.  v3 is the paper target; v2 is retained as the superseded-format ablation.

For each witness the same tiny fixture is built with
  * the superseded v1 binary (`enc_cube_orig`, verbatim enc_cube.cpp), and
  * the pinned v2 binary (`enc_cube_v2`, verbatim eai_cube2.cpp),
then the same query is run and compared with DuckDB 2.0.

BUG1 dense build on gappy domain      v1 FAIL -> v2 PASS      (SPEC-RUNNER FIX 1)
BUG2 NULL group key under NOT IN      v1 FAIL -> v2 PASS      (SPEC-RUNNER FIX 2)
BUG3 dense int64 range-bound overflow v1 FAIL -> v2 PASS      (SPEC-RUNNER FIX 3)
BUG4 v2 pre-release min/max block-search non-monotonicity: fixed by the Lead in
     v2 before this run; not reproducible in the pinned v2, so the witness here is
     a regression test (multi-block MIN/MAX on contiguous and gappy domains) that
     must stay PASS.
Plus the repeated-`having=` interface change: v1 applied only the last predicate,
v2 ANDs them (witness W5).

Writes <BASE>/out/bug_witnesses_v3.json
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import hashlib
import json
import os
import struct
import subprocess
import sys

import duckdb

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pairs as P
import bsstr

BASE = P.BASE
V1 = f"{BASE}/bin/enc_cube_orig"
V2 = f"{BASE}/bin/enc_cube_v2"
V3 = f"{BASE}/bin/enc_cube3_frozen"          # frozen ECUBE03 (paper target)
REPO_V3 = f"{P.REPO}/research/real_decoders_20261024/src_eai_20261101/enc_cube3"


def sha(p):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()


def make_fixture(tag, gvals, names, vals):
    d = f"{BASE}/tables/witness_{tag}"
    os.makedirs(d, exist_ok=True)
    open(f"{d}/g.i32", "wb").write(b"".join(struct.pack("<i", x) for x in gvals))
    open(f"{d}/v.i32", "wb").write(b"".join(struct.pack("<i", x) for x in vals))
    bsstr.write_bsstr(f"{d}/d.dict", names)
    return d


def empty_dict(path):
    with open(path, "wb") as f:
        f.write(b"BSSTR01\n" + b"e" * 40 + struct.pack("<I", 0))


def build(binary, d, cube):
    p = subprocess.run([binary, "build", f"{d}/g.i32", f"{d}/v.i32", cube],
                       capture_output=True, text=True)
    return p.returncode, p.stderr.strip().splitlines()[-1] if p.stderr.strip() else ""


def query(binary, d, cube, args):
    out = f"{BASE}/out/_witness_q.json"
    p = subprocess.run([binary, "query", cube, f"{d}/d.dict", out] + args,
                       capture_output=True, text=True)
    if p.returncode != 0:
        return None, p.stderr[-300:]
    return sorted(json.load(open(out))["rows"]), None


def dense_flag(cube):
    d = open(cube, "rb").read(64)
    magic = d[:8]
    if magic == b"ECUBE01\n":
        return struct.unpack("<I", d[40:44])[0]
    return struct.unpack("<I", d[40:44])[0]   # same offset in ECUBE02 header


def witness(tag, gvals, names, vals, sql, args, dict_names=None):
    d = make_fixture(tag, gvals, names, vals)
    if dict_names is not None:
        empty_dict(f"{d}/d.dict")
    res = dict(fixture=tag, rows=len(vals), args=args, sql=sql)
    for label, binary in [("v1", V1), ("v2", V2), ("v3", V3)]:
        cube = f"{d}/{label}.ecube"
        rc, err = build(binary, d, cube)
        if rc != 0:
            res[label] = dict(error=err)
            continue
        rows, qerr = query(binary, d, cube, args)
        res[label] = dict(dense_build=dense_flag(cube), rows=rows, error=qerr)
    con = duckdb.connect()
    con.execute("CREATE TABLE t(g VARCHAR, v BIGINT)")
    con.executemany("INSERT INTO t VALUES (?,?)",
                    [(None if g < 0 else names[g], v) for g, v in zip(gvals, vals)])
    exp = bsstr.canon_rows(con.execute(sql).fetchall())
    con.close()
    res["duckdb_expected"] = exp
    for label in ("v1", "v2", "v3"):
        rows = res[label].get("rows")
        # v3 renders SQL NULL aggregate values as the string "None"; the witness
        # fixtures here have no NULL measures, so this is a no-op safety net.
        if rows is not None:
            rows = [["NULL" if x == "None" else x for x in r] for r in rows]
            rows = sorted(rows)
        res[label]["ok"] = rows == exp
    return res


def main():
    out = dict(
        v1_binary=V1, v1_sha256=sha(V1),
        v2_binary=V2, v2_sha256=sha(V2),
        v3_binary=V3, v3_sha256=sha(V3),
        v3_source_sha256=sha(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                          "eai_cube3_frozen.cpp")),
        worktree_source_sha256=sha(f"{P.REPO}/research/real_decoders_20261024/"
                                   "src_eai_20261101/eai_cube3.cpp"),
        frozen_index_sha256="78bff19a0dbd89d0e3b16254cb2a0c807669d982b3a9233b1ba5504dd04c89ee",
        frozen_index_bytes=1140168,
        lead_v3_binary=REPO_V3, lead_v3_sha256=sha(REPO_V3),
        witnesses={})
    W = out["witnesses"]
    # ---------------- BUG 1: dense build on a gappy domain ----------------
    W["BUG1_dense_gap_range"] = witness(
        "dense_gap", [0, 0, 1, 1, 0, 0, 1, 1], ["a", "b"], [0, 8, 0, 8, 0, 8, 0, 8],
        "SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM t WHERE v BETWEEN 4 AND 8 GROUP BY g",
        ["range=4,8"])
    W["BUG1_dense_gap_low"] = witness(
        "dense_gap", [0, 0, 1, 1, 0, 0, 1, 1], ["a", "b"], [0, 8, 0, 8, 0, 8, 0, 8],
        "SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM t WHERE v BETWEEN 0 AND 4 GROUP BY g",
        ["range=0,4"])
    W["BUG1_dense_gap_fixed"] = witness(
        "dense_gap", [0, 0, 1, 1, 0, 0, 1, 1], ["a", "b"], [0, 8, 0, 8, 0, 8, 0, 8],
        "SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM t GROUP BY g", [])
    # ---------------- BUG 2: NULL group under NOT IN ----------------------
    W["BUG2_null_group_not_in"] = witness(
        "null_notin", [-1, 0, 0, 1], ["a", "b"], [1, 2, 3, 4],
        "SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM t WHERE g NOT IN ('a') GROUP BY g",
        ["where_not_in=a"])
    W["BUG2_null_group_in_control"] = witness(
        "null_notin", [-1, 0, 0, 1], ["a", "b"], [1, 2, 3, 4],
        "SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM t WHERE g IN ('a','b') GROUP BY g",
        ["where_in=a,b"])
    W["BUG2_null_group_fixed_control"] = witness(
        "null_notin", [-1, 0, 0, 1], ["a", "b"], [1, 2, 3, 4],
        "SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM t GROUP BY g", [])
    # ---------------- BUG 3: dense int64 bound overflow -------------------
    W["BUG3_dense_int64_bounds"] = witness(
        "dense_ok", [0] * 9 + [1] * 9, ["a", "b"], list(range(9)) * 2,
        "SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM t "
        "WHERE v BETWEEN -9223372036854775808 AND 9223372036854775807 GROUP BY g",
        ["range=-9223372036854775808,9223372036854775807"])
    # ---------------- BUG 4: multi-block MIN/MAX regression ---------------
    vals = list(range(200))                     # contiguous 0..199 -> dense, 4 blocks
    gvals, rv = [], []
    for r in vals:
        for k in range(3):
            gvals.append(0 if r % 3 == 0 else 1)
            rv.append(r)
    m = "SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM t WHERE v BETWEEN {lo} AND {hi} GROUP BY g"
    for lo, hi in [(5, 190), (63, 64), (64, 66), (127, 130), (191, 199), (0, 199)]:
        W[f"BUG4_multiblock_minmax_dense_{lo}_{hi}"] = witness(
            "multiblock_dense", gvals, ["a", "b"], rv,
            m.format(lo=lo, hi=hi), [f"range={lo},{hi}"])
    # gappy multi-block domain: values 0,3,6,...,597 (V=200, span 598 -> non-dense)
    gv2 = [v * 3 for v in rv]
    for lo, hi in [(0, 600), (150, 300), (297, 300), (591, 597)]:
        W[f"BUG4_multiblock_minmax_gappy_{lo}_{hi}"] = witness(
            "multiblock_gappy", gvals, ["a", "b"], gv2,
            m.format(lo=lo, hi=hi), [f"range={lo},{hi}"])
    # ---------------- W5: repeated HAVING now ANDs ------------------------
    rows_g = [0] * 4 + [1] * 4
    rows_v = [10, 20, 30, 40, 100, 200, 300, 400]
    W["W5_repeated_having_and"] = witness(
        "having_and", rows_g, ["a", "b"], rows_v,
        "SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM t GROUP BY g "
        "HAVING SUM(v) >= 500 AND COUNT(*) <= 4",
        ["having=sum>=500", "having=count<=4"])
    # ---------------- all-NULL group keys ---------------------------------
    W["S8_all_null_group_keys"] = witness(
        "all_null_group", [-1] * 6, [], [1, 2, 3, 4, 5, 6],
        "SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM t GROUP BY g", [])
    json.dump(out, open(f"{BASE}/out/bug_witnesses_v3.json", "w"), indent=1)
    n3 = 0
    for k, w in W.items():
        v1, v2, v3 = w["v1"], w["v2"], w["v3"]
        print("%-42s v1=%-5s v2=%-5s v3=%-5s %s" % (
            k, "PASS" if v1.get("ok") else "FAIL", "PASS" if v2.get("ok") else "FAIL",
            "PASS" if v3.get("ok") else "FAIL",
            "" if v3.get("ok") else json.dumps(v3)[:200]))
        n3 += 1 if v3.get("ok") else 0
    print(f"v3 PASS {n3}/{len(W)}; v1 PASS {sum(1 for w in W.values() if w['v1'].get('ok'))}/{len(W)}")


if __name__ == "__main__":
    main()
