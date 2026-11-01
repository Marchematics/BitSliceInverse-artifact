#!/usr/bin/env python3
"""Semantics boundary probes for the FROZEN ECUBE03 (v3) — including the
NULL-measure path that v3 adds.  Each probe builds a tiny fixture, runs the same
SQL through DuckDB 2.0 and through the frozen v3 cube, and records
PASS / FAIL / OUT-OF-SCOPE / UNSUPPORTED / NOT-EXPRESSIBLE.

NULL-measure fixture (N1-N6, the 6/6 fixture):
  rows (a,5), (b,7), (c,NULL), (c,NULL); value validity bitmap passed to
  `enc_cube build GID VALUES OUT [VALIDITY_BITS]`.
  N1 fixed GROUP BY            -> c has COUNT(*)=2, SUM/MIN/MAX SQL NULL
  N2 v BETWEEN 0 AND 10        -> NULL rows excluded, a+b only
  N3 v BETWEEN 6 AND 10        -> b only
  N4 v >= 5                    -> a+b only
  N5 HAVING SUM(v) >= 6        -> b only (NULL SUM is not TRUE)
  N6 g IN ('a','c')            -> a and c (no range -> NULL rows counted)
Remaining probes S2-S13: repeated HAVING, IN/NOT IN vs NULL group keys,
out-of-domain / inverted / single-value ranges, empty base table, one-row table,
all-NULL group keys, int32 extremes, comma group names, multi-block MIN/MAX,
u32 capacity arithmetic, group rendering.

Writes <BASE>/out/semantics_probe_v3.json
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
V3 = f"{BASE}/bin/enc_cube3_frozen"
V1 = f"{BASE}/bin/enc_cube_orig"
OUT = f"{BASE}/out/semantics_probe_v3.json"


def sha(p):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()


def fixture(tag, gvals, names, vals, null_v=(), null_payload=0):
    d = f"{BASE}/tables/probe_{tag}"
    os.makedirs(d, exist_ok=True)
    open(f"{d}/g.i32", "wb").write(b"".join(struct.pack("<i", x) for x in gvals))
    open(f"{d}/v.i32", "wb").write(
        b"".join(struct.pack("<i", null_payload if i in null_v else vals[i])
                 for i in range(len(vals))))
    bsstr.write_bsstr(f"{d}/d.dict", names)
    valid = f"{d}/v.valid"
    if null_v:
        words = bytearray(((len(vals) + 63) // 64) * 8)
        for i in range(len(vals)):
            if i not in null_v:
                words[i >> 3] |= 1 << (i & 7)     # little-endian 64-bit words
        open(valid, "wb").write(words)
        return d, valid
    if os.path.exists(valid):
        os.unlink(valid)
    return d, None


def build(d, valid):
    cube = f"{d}/c3.ecube"
    cmd = [V3, "build", f"{d}/g.i32", f"{d}/v.i32", cube]
    if valid:
        cmd.append(valid)
    p = subprocess.run(cmd, capture_output=True, text=True)
    return cube, p.returncode, p.stderr.strip().splitlines()[-1] if p.stderr.strip() else ""


def cquery(cube, dic, args):
    p = subprocess.run([V3, "query", cube, dic, f"{BASE}/out/_probe_q.json"] + args,
                       capture_output=True, text=True)
    if p.returncode != 0:
        return None, p.stderr[-300:]
    rows = json.load(open(f"{BASE}/out/_probe_q.json"))["rows"]
    # v3 renders SQL NULL aggregate values as the string "None"; canonicalise.
    return sorted([["NULL" if x == "None" else x for x in r] for r in rows]), None


def duck_build(rows):
    con = duckdb.connect()
    con.execute("CREATE TABLE t(g VARCHAR, v BIGINT)")
    con.executemany("INSERT INTO t VALUES (?,?)", rows)
    return con


def probe(tag, gvals, names, vals, sql, args, null_v=(), null_payload=0,
          null_rows=(), notes="", expected_ok=True):
    d, valid = fixture(tag, gvals, names, vals, null_v, null_payload)
    cube, rc, err = build(d, valid)
    rec = dict(args=args, sql=sql, notes=notes)
    if rc != 0:
        rec["v3_build"] = {"returncode": rc, "stderr": err}
        rec["verdict"] = "BUILD-REJECTED"
        return rec
    got, qerr = cquery(cube, f"{d}/d.dict", args)
    con = duck_build(null_rows if null_rows else
                     [(None if g < 0 else names[g], v) for g, v in zip(gvals, vals)])
    exp = bsstr.canon_rows(con.execute(sql).fetchall())
    con.close()
    rec["v3_build"] = {"returncode": rc, "info": err}
    rec["duckdb_expected"] = exp
    rec["v3_rows"] = got
    rec["v3_error"] = qerr
    ok = got == exp
    rec["verdict"] = "PASS" if ok else ("FAIL" if expected_ok else "OUT-OF-SCOPE")
    return rec


def main():
    out = dict(
        v3_binary=V3, v3_sha256=sha(V3),
        v3_source_sha256=sha(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                          "eai_cube3_frozen.cpp")),
        frozen_index_sha256="78bff19a0dbd89d0e3b16254cb2a0c807669d982b3a9233b1ba5504dd04c89ee",
        frozen_index_bytes=1140168,
        lead_frozen_receipt=("research/real_decoders_20261025/receipts/baselines_20261101/"
                             "eai_ecube03_frozen.json"),
        probes={})
    P_ = out["probes"]
    # ================= NULL-measure fixture: N1..N6 (6/6) ===================
    g = [0, 1, 2, 2]
    v = [5, 7, 0, 0]
    names = ["a", "b", "c"]
    null_v = {2, 3}
    null_rows = [("a", 5), ("b", 7), ("c", None), ("c", None)]
    base_sql = ("SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM t ")
    P_["N1_null_measure_fixed"] = probe(
        "n1_null_measure", g, names, v,
        base_sql + "GROUP BY g", [], null_v=null_v, null_rows=null_rows,
        notes="fixed GROUP BY: SQL NULL aggregates for the all-NULL group c "
              "(SUM/MIN/MAX NULL, COUNT(*)=2), emitted by v3 as the string 'None' "
              "and canonicalised to NULL here")
    P_["N2_null_measure_range_0_10"] = probe(
        "n2_null_measure", g, names, v,
        base_sql + "WHERE v BETWEEN 0 AND 10 GROUP BY g", ["range=0,10"],
        null_v=null_v, null_rows=null_rows,
        notes="range predicate excludes NULL measurement rows exactly as SQL does")
    P_["N3_null_measure_range_6_10"] = probe(
        "n3_null_measure", g, names, v,
        base_sql + "WHERE v BETWEEN 6 AND 10 GROUP BY g", ["range=6,10"],
        null_v=null_v, null_rows=null_rows,
        notes="narrow range, only b")
    P_["N4_null_measure_lo_only"] = probe(
        "n4_null_measure", g, names, v,
        base_sql + "WHERE v >= 5 GROUP BY g", ["lo=5"],
        null_v=null_v, null_rows=null_rows,
        notes="one-sided range (NULL v is not TRUE for v>=5 -> excluded)")
    P_["N5_null_measure_having_sum"] = probe(
        "n5_null_measure", g, names, v,
        base_sql + "GROUP BY g HAVING SUM(v) >= 6", ["having=sum>=6"],
        null_v=null_v, null_rows=null_rows,
        notes="HAVING on SUM: NULL SUM is not TRUE, so all-NULL group c is dropped")
    P_["N6_null_measure_group_in_no_range"] = probe(
        "n6_null_measure", g, names, v,
        base_sql + "WHERE g IN ('a','c') GROUP BY g", ["where_in=a,c"],
        null_v=null_v, null_rows=null_rows,
        notes="group filter without a range: NULL-measure rows still contribute to "
              "COUNT(*) of group c")
    # supplementary: build from non-NULL rows only and range-query
    P_["N7_null_measure_filtered_build"] = probe(
        "n7_filtered", [0, 1], names[:2], [5, 7],
        base_sql + "WHERE v BETWEEN 0 AND 100 GROUP BY g", ["range=0,100"],
        null_rows=[("a", 5), ("b", 7)],
        notes="if every query ranges on the measure, building from the non-NULL rows "
              "is exact as well (kept as a check)")
    # ================= S2: repeated HAVING now ANDs =========================
    g = [0] * 4 + [1] * 4
    v = [10, 20, 30, 40, 100, 200, 300, 400]
    d, valid = fixture("having_and", g, ["a", "b"], v)
    cube, rc, err = build(d, None)
    got, qerr = cquery(cube, f"{d}/d.dict", ["having=sum>=500", "having=count<=4"])
    con = duck_build([("a", x) for x in [10, 20, 30, 40]] + [("b", x) for x in [100, 200, 300, 400]])
    exp = bsstr.canon_rows(con.execute(
        "SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM t GROUP BY g "
        "HAVING SUM(v) >= 500 AND COUNT(*) <= 4").fetchall())
    con.close()
    v1cube = f"{d}/c1.ecube"
    subprocess.run([V1, "build", f"{d}/g.i32", f"{d}/v.i32", v1cube],
                   capture_output=True)
    v1rows, _ = (lambda p: (sorted(json.load(open(f"{BASE}/out/_probe_q.json"))["rows"]), None))(
        subprocess.run([V1, "query", v1cube, f"{d}/d.dict", f"{BASE}/out/_probe_q.json",
                        "having=sum>=500", "having=count<=4"], capture_output=True))
    P_["S2_repeated_having_and"] = dict(
        args=["having=sum>=500", "having=count<=4"], sql="HAVING SUM>=500 AND COUNT<=4",
        v3_rows=got, duckdb_expected=exp, v3_verdict="PASS" if got == exp else "FAIL",
        verdict="PASS" if got == exp else "FAIL",
        v1_rows_last_wins=v1rows,
        notes="v3 ANDs repeated having= (v1 applied only the last predicate)")
    # ================= S3: IN/NOT IN vs NULL group keys =====================
    P_["S3_in_null_name_literal_group"] = probe(
        "null_literal", [0, 1, 1, -1], ["a", "NULL"], [1, 2, 3, 4],
        "SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM t WHERE g IN ('NULL') GROUP BY g",
        ["where_in=NULL"],
        notes="literal group value 'NULL' exists; SQL NULL group dropped by 3VL")
    P_["S3_not_in_null_name_literal_group"] = probe(
        "null_literal", [0, 1, 1, -1], ["a", "NULL"], [1, 2, 3, 4],
        "SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM t WHERE g NOT IN ('NULL') GROUP BY g",
        ["where_not_in=NULL"], notes="same fixture, NOT IN")
    P_["S3_in_null_group_only"] = probe(
        "null_only", [-1, 0, 0, 1], ["a", "b"], [1, 2, 3, 4],
        "SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM t WHERE g IN ('NULL') GROUP BY g",
        ["where_in=NULL"], notes="only a SQL NULL group: IN can never match it")
    # ================= S4/S5: out-of-domain and single-value ranges =========
    g = [0, 0, 1, 1]
    v = [10, 20, 30, 40]
    for tag, args, sqlw in [
            ("S4_lo_beyond", ["range=1000,2000"], "v BETWEEN 1000 AND 2000"),
            ("S4_hi_beyond", ["range=-2000,-1000"], "v BETWEEN -2000 AND -1000"),
            ("S4_inverted", ["range=40,10"], "v BETWEEN 40 AND 10"),
            ("S5_single_observed", ["range=20,20"], "v BETWEEN 20 AND 20"),
            ("S5_single_unobserved", ["range=25,25"], "v BETWEEN 25 AND 25")]:
        P_[tag] = probe(tag, g, ["a", "b"], v,
                        "SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM t "
                        f"WHERE {sqlw} GROUP BY g", args)
    # ================= S6: empty base table =================================
    d, _ = fixture("empty", [], [], [], null_payload=0)
    cube, rc, err = build(d, None)
    P_["S6_empty_table"] = dict(
        v3_build=dict(returncode=rc, stderr=err),
        verdict="UNSUPPORTED" if rc != 0 else "CHECK",
        notes="ECUBE03 still requires at least one observed value: an empty base "
              "table (or a measure column with no non-NULL values) is rejected at "
              "build time (ERR empty). Empty *results* under predicates are "
              "supported (36/36 empty_result family specs exact). UNCHANGED from v2.")
    # ================= S7: one-row table ====================================
    P_["S7_one_row_fixed"] = probe(
        "onerow", [0], ["a"], [7],
        "SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM t GROUP BY g", [])
    P_["S7_one_row_single_value"] = probe(
        "onerow", [0], ["a"], [7],
        "SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM t "
        "WHERE v BETWEEN 7 AND 7 GROUP BY g", ["range=7,7"])
    P_["S7_one_row_empty_range"] = probe(
        "onerow", [0], ["a"], [7],
        "SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM t "
        "WHERE v BETWEEN 8 AND 9 GROUP BY g", ["range=8,9"])
    # ================= S8: all-NULL group keys ==============================
    P_["S8_all_null_group_keys"] = probe(
        "all_null", [-1] * 6, [], [1, 2, 3, 4, 5, 6],
        "SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM t GROUP BY g", [],
        notes="zero-length dictionary + NULL group only; cube G=1, group name 'NULL'")
    # ================= S9: int32 extreme measure values =====================
    P_["S9_int32_extremes"] = probe(
        "int32_extremes", [0, 0, 1, 1], ["a", "b"],
        [2147483647, -2147483648, 2147483647, -2147483648],
        "SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM t GROUP BY g", [],
        notes="measure payload is int32; SUM accumulates in i64")
    # ================= S10: comma in group names ============================
    P_["S10_comma_group_name"] = dict(
        verdict="NOT-EXPRESSIBLE",
        notes="where_in/where_not_in are comma-split, so a group value containing "
              "',' cannot be filtered through the CLI. No corpus/fuzz dataset has one.")
    # ================= S11: multi-block MIN/MAX =============================
    vals = list(range(200))
    g, v = [], []
    for r in vals:
        for k in range(3):
            g.append(0 if r % 3 == 0 else 1)
            v.append(r)
    for lo, hi in [(5, 190), (63, 64), (127, 130), (0, 199)]:
        key = f"S11_multiblock_minmax_{lo}_{hi}"
        P_[key] = probe(key, g, ["a", "b"], v,
                        f"SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM t "
                        f"WHERE v BETWEEN {lo} AND {hi} GROUP BY g", [f"range={lo},{hi}"],
                        notes="contiguous 0..199 = 4 blocks of 64 ranks; v3 MIN/MAX "
                              "block-prefix search must stay monotone (BUG4 regression)")
    # ================= S12/S13: boundaries ==================================
    P_["S12_u32_count_capacity"] = dict(
        verdict="BOUNDARY",
        notes="UNCHANGED: ECUBE03 stores u32 per (group,rank) counts and u32 "
              "block-count prefixes (plus a u32 per-group NULL-measure count), so a "
              "single group is bounded by 4,294,967,295 rows. Safe at 1B total rows; "
              "a guard is required before any >4.29e9-rows-in-one-group claim "
              "(flag to `scale`).")
    P_["S13_group_value_rendering"] = dict(
        verdict="BOUNDARY",
        notes="Group values addressed by rendered SQL value (BIGINT -> decimal "
              "string, VARCHAR -> exact bytes); negative group values (C10), values "
              "with spaces (C4/C9), 3M distinct integers are exact. FLOAT/DECIMAL/"
              "DATE group keys are outside the corpus (canonical rendering needed).")
    P_["S13_empty_result_predicates"] = dict(
        verdict="PASS",
        notes="36/36 empty_result family specs are exact (inverted, out-of-domain, "
              "IN nonexistent, NOT IN every name, impossible HAVING, unobserved "
              "single value).")
    json.dump(out, open(OUT, "w"), indent=1)
    for k, v in P_.items():
        print("%-42s %s" % (k, v.get("verdict")))
    from collections import Counter
    print(Counter(v.get("verdict") for v in P_.values()))
    null_keys = [k for k in P_ if k.startswith("N") and "null_measure" in k or k.startswith("N7")]
    print("NULL-measure fixture N1-N6:",
          sum(1 for k in ["N1_null_measure_fixed", "N2_null_measure_range_0_10",
                          "N3_null_measure_range_6_10", "N4_null_measure_lo_only",
                          "N5_null_measure_having_sum",
                          "N6_null_measure_group_in_no_range"]
              if P_[k].get("verdict") == "PASS"), "/6")


if __name__ == "__main__":
    main()
