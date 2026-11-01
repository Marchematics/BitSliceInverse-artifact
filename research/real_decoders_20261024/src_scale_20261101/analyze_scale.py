#!/usr/bin/env python3
"""Post-process the scale-campaign receipts into the tables used by
SCALE_20261101.md, and emit the exactness receipts that need cross-receipt joins.

Run after run_scale.py stages:
  python3 analyze_scale.py
Writes:
  receipts/scale_20261101/exactness_100m.json
  receipts/scale_20261101/crosscheck_v2_binary.json
  receipts/scale_20261101/multicore_tables.json
  receipts/scale_20261101/summary.json
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import json
import os

HOME = _artifact.expand("~")
REC = _artifact.WORK + "/receipts/scale_20261101"
TMP = _artifact.WORK + "/scale"
CASES = ["fixed", "le_m7000", "le_m720", "le_650", "le_99900"]


def load(name):
    with open(os.path.join(REC, name + ".json")) as f:
        return json.load(f)


def save(name, obj):
    with open(os.path.join(REC, name + ".json"), "w") as f:
        json.dump(obj, f, indent=1)
    print("receipt ->", os.path.join(REC, name + ".json"))


def canon(rows):
    out = []
    for r in rows:
        g = str(r[0])
        out.append([("NULL" if g.lower() in ("none", "null") else g)] + [str(x) for x in r[1:]])
    return sorted(out, key=lambda r: r[0])


def speedup_table(builds, base_wall, base_metric=None):
    rows = []
    for b in builds:
        m = b.get("metrics") or {}
        wall = b.get("wall_s")
        rows.append({
            "threads": b.get("threads"),
            "wall_s": wall,
            "build_ms": m.get("build_ms"),
            "rss_mb": (b.get("time") or {}).get("rss"),
            "speedup_vs_1t_wall": round(base_wall / wall, 3) if wall else None,
            "efficiency_pct": round(100.0 * (base_wall / wall) / b["threads"], 1) if wall else None,
        })
    return rows


def query_table(q, case):
    rows = []
    base = q["results"]["1"][case]["metrics"]
    for T in ("1", "4", "8", "16"):
        m = q["results"][T][case]["metrics"]
        med = m["query_median_ms"]
        rows.append({
            "threads": int(T),
            "median_us": round(med * 1000, 3),
            "p95_us": round(m["query_p95_ms"] * 1000, 3),
            "speedup_vs_1t": round(base["query_median_ms"] / med, 3) if med else None,
            "efficiency_pct": round(100.0 * (base["query_median_ms"] / med) / int(T), 1) if med else None,
            "rss_mb": (q["results"][T][case].get("time") or {}).get("rss"),
        })
    return rows


def main():
    b100 = load("build_mt_100m")
    b1b = load("build_mt_1b")
    q100 = load("qbench_100m")
    q1b = load("qbench_1b")
    v2_100 = load("v2_binary_crosscheck_100m")
    v2_1b = load("v2_binary_crosscheck_1b")
    d100 = load("duckdb_100m")
    fit = load("fit_table")

    # ---- exactness: cube vs DuckDB 2.0 at 100M (all 5 cases) ----
    checks = {}
    all_ok = True
    for c in CASES:
        got = canon(q100["results"]["1"][c]["rows"])
        want = canon(d100["results"]["1"][c]["rows"])
        ok = got == want
        all_ok &= ok
        checks[c] = {"exact": ok, "rows": len(got),
                     "cube_rows": got, "duckdb_rows": want}
    # v2-binary cross-check: qbench bytes vs the validated binary's output
    v2checks = {}
    for c in CASES:
        a = canon(q100["results"]["1"][c]["rows"])
        b = canon(v2_100["results"][c]["rows"])
        v2checks["100m_" + c] = {"same_rows": a == b}
    for c in CASES:
        a = canon(q1b["results"]["1"][c]["rows"])
        b = canon(v2_1b["results"][c]["rows"])
        v2checks["1b_" + c] = {"same_rows": a == b}
    save("exactness_100m", {"cases": checks, "all_exact_vs_duckdb_2_0": all_ok})
    # ---- exactness: cube vs DuckDB 2.0 at 1B (parquet control, same int32 columns) ----
    d1b = load("duckdb_1b")
    checks1b = {}
    ok1b = True
    for c in CASES:
        got = canon(q1b["results"]["1"][c]["rows"])
        if c not in d1b["results"]["1"]:
            continue
        want = canon(d1b["results"]["1"][c]["rows"])
        ok = got == want
        ok1b &= ok
        checks1b[c] = {"exact": ok, "rows": len(got), "duckdb_ms_1t": d1b["results"]["1"][c]["median_ms"]}
    save("exactness_1b_vs_duckdb", {"cases": checks1b, "all_exact_vs_duckdb_2_0": ok1b})
    save("crosscheck_v2_binary", {"checks": v2checks,
                                  "all_same": all(v["same_rows"] for v in v2checks.values()),
                                  "note": "qbench (parallel path) vs src_eai_20261101/enc_cube "
                                          "(validated v2 single-thread path) on the same ECUBE02 "
                                          "file: complete grouped result must be identical"})

    # ---- multi-core tables ----
    mc = {
        "build": {
            "100m": speedup_table(
                [r for r in b100["runs"] if r.get("variant", "").startswith("build_mt")],
                next(r["wall_s"] for r in b100["runs"]
                     if r.get("variant") == "build_mt_T1")),
            "1b": speedup_table(b1b["builds"], b1b["builds"][0]["wall_s"]),
            "100m_reference_v2_plain": next(r for r in b100["runs"]
                                            if r.get("variant") == "v2_plain_validated_path")
            if any(r.get("variant") == "v2_plain_validated_path" for r in b100["runs"]) else None,
        },
        "query": {},
        "notes": {
            "build_100m_T1_reference": "v2 plain build (validated binary) wall_s",
            "query_100m": "engine-resident, excl. serialization, 30 reps, warm process",
        },
    }
    for c in CASES:
        mc["query"]["100m_" + c] = query_table(q100, c)
        mc["query"]["1b_" + c] = query_table(q1b, c)

    # 10M-group materialized path
    q10 = load("q10m_materialized_vs_scan")
    rows = []
    base = q10["stats_query"]["1"]["metrics"]
    for T in ("1", "4", "8", "16"):
        m = q10["stats_query"][T]["metrics"]
        rows.append({
            "threads": int(T),
            "query_median_ms": round(m["query_median_ms"], 3),
            "query_p95_ms": round(m["query_p95_ms"], 3),
            "speedup_vs_1t": round(base["query_median_ms"] / m["query_median_ms"], 3),
            "efficiency_pct": round(100.0 * (base["query_median_ms"] / m["query_median_ms"]) / int(T), 1),
            "serialize_1t_median_ms": round(m["serialize1_median_ms"], 2),
            "serialize_Tt_median_ms": round(m["serializeT_median_ms"], 2),
            "rss_mb": (q10["stats_query"][T].get("time") or {}).get("rss"),
        })
    mc["query"]["10m_materialized_fixed"] = rows
    bsum = {}
    for c in CASES:
        m = q10["bsum_fallback"][c]["metrics"]
        bsum[c] = {"median_ms": round(m["query_median_ms"], 1),
                   "candidate_blocks": m["candidate_blocks"],
                   "total_blocks": m["total_blocks"],
                   "candidate_fraction": round(m["candidate_blocks"] / m["total_blocks"], 4),
                   "rows": m["rows"]}
    mc["10m_block_summary_scan_fallback_1t"] = bsum
    mc["10m_full_range_cross_check"] = q10["full_range_cross_check"]
    save("multicore_tables", mc)

    # ---- summary numbers used by the markdown ----
    b1b1 = b1b["builds"][0]["metrics"]
    q1b1 = q1b["results"]["1"]["fixed"]["metrics"]
    q1b8 = q1b["results"]["8"]["fixed"]["metrics"]
    q100_1 = q100["results"]["1"]["fixed"]["metrics"]
    summary = {
        "1b": {
            "rows": 1000000000, "groups": b1b1["groups"], "values": b1b1["values"],
            "cube_bytes": b1b["cube_bytes"],
            "encoded_tree_bytes": 10 * 251814420,
            "ratio_to_encoded_tree": round(b1b["cube_bytes"] / (10 * 251814420), 8),
            "build_wall_s": {r["threads"]: r["wall_s"] for r in b1b["builds"]},
            "build_ms": {r["threads"]: r["metrics"]["build_ms"] for r in b1b["builds"]},
            "deterministic_sha": b1b["deterministic_byte_identical"],
            "cube_sha256": b1b["builds"][0]["cube_sha256"],
            "query_median_us": {c: {T: round(q1b["results"][T][c]["metrics"]["query_median_ms"] * 1000, 2)
                                    for T in ("1", "4", "8", "16")} for c in CASES},
            "query_p95_us": {c: {T: round(q1b["results"][T][c]["metrics"]["query_p95_ms"] * 1000, 2)
                                 for T in ("1", "4", "8", "16")} for c in CASES},
            "rss_mb": {r["threads"]: r["time"].get("rss") for r in b1b["builds"]},
        },
        "100m": {
            "cube_bytes": b100["cube_bytes"],
            "build_wall_s": {r["threads"]: r["wall_s"] for r in b100["runs"]
                             if r.get("variant", "").startswith("build_mt")},
            "cube_sha256": b100["cube_sha256"],
            "all_byte_identical": b100["all_byte_identical"],
            "query_median_us": {c: {T: round(q100["results"][T][c]["metrics"]["query_median_ms"] * 1000, 2)
                                    for T in ("1", "4", "8", "16")} for c in CASES},
        },
        "duckdb_100m_ms": {c: {T: round(d100["results"][T][c]["median_ms"], 2)
                               for T in ("1", "4", "8", "16")} for c in CASES},
        "fit": fit["rows"],
    }
    # ---- O(G)-vs-n curve ----
    sc = load("scalecurve_ogn")
    curve = []
    for pt in sc["points"]:
        m = pt["query"]["T1_fixed"]["metrics"]
        r = pt["query"]["T8_fixed"]["metrics"]
        row = {"n_rows": pt["n_rows"], "groups": m["groups"], "values": m["values"],
               "cube_bytes": (pt.get("build") or {}).get("cube_bytes") or
                             os.path.getsize(pt["cube"]) if os.path.exists(pt["cube"]) else None,
               "q_fixed_t1_us": round(m["query_median_ms"] * 1000, 3),
               "q_fixed_t1_p95_us": round(m["query_p95_ms"] * 1000, 3),
               "q_fixed_t8_us": round(r["query_median_ms"] * 1000, 3),
               "q_fixed_t8_p95_us": round(r["query_p95_ms"] * 1000, 3),
               "rows_out": m["rows"],
               "build_wall_s": pt["build"]["wall_s"] if pt.get("build") else None}
        for k in ("T1_le_m720", "T8_le_m720"):
            mm = pt["query"][k]["metrics"]
            row["q_%s_us" % k.lower()] = round(mm["query_median_ms"] * 1000, 3)
        curve.append(row)
    summary["scalecurve_ogn"] = curve

    # ---- O(G=11)-vs-n curve (index bytes and latency must be identical) ----
    try:
        scg = load("scalecurve_ogn_g11")
        curve11 = []
        for pt in scg["points"]:
            m = pt["query"]["T1_fixed"]["metrics"]
            r8 = pt["query"]["T8_fixed"]["metrics"]
            mm = pt["query"]["T1_le_m720"]["metrics"]
            curve11.append({"n_rows": pt["n_rows"], "groups": m["groups"], "values": m["values"],
                            "cube_bytes": pt["cube_bytes"], "cube_sha256": pt["cube_sha256"],
                            "q_fixed_t1_us": round(m["query_median_ms"] * 1000, 3),
                            "q_fixed_t1_p95_us": round(m["query_p95_ms"] * 1000, 3),
                            "q_fixed_t8_us": round(r8["query_median_ms"] * 1000, 3),
                            "q_t1_le_m720_us": round(mm["query_median_ms"] * 1000, 3)})
        summary["scalecurve_ogn_g11"] = curve11
    except FileNotFoundError:
        summary["scalecurve_ogn_g11"] = None

    # ---- primary O(G)-vs-n curve: G=101 and V fixed ----
    try:
        fv = load("scalecurve_fixedvg")
        curvefv = []
        for pt in fv["points"]:
            m = pt["query"]["T1_fixed"]["metrics"]
            r8 = pt["query"]["T8_fixed"]["metrics"]
            mm = pt["query"]["T1_le_m720"]["metrics"]
            curvefv.append({"n_rows": pt["n_rows"], "groups": m["groups"], "values": m["values"],
                            "cube_bytes": pt["cube_bytes"], "cube_sha256": pt["cube_sha256"],
                            "q_fixed_t1_us": round(m["query_median_ms"] * 1000, 3),
                            "q_fixed_t1_p95_us": round(m["query_p95_ms"] * 1000, 3),
                            "q_fixed_t8_us": round(r8["query_median_ms"] * 1000, 3),
                            "q_t1_le_m720_us": round(mm["query_median_ms"] * 1000, 3),
                            "build_wall_s": pt["build"]["wall_s"] if pt.get("build") else None})
        summary["scalecurve_fixedvg"] = curvefv
    except FileNotFoundError:
        summary["scalecurve_fixedvg"] = None

    # ---- 10M materialized output path (serialization only + query) ----
    q10 = load("q10m_materialized_vs_scan")
    ser = []
    for T in ("1", "4", "8", "16"):
        m = q10["stats_query"][T]["metrics"]
        ser.append({"threads": int(T),
                    "query_median_ms": round(m["query_median_ms"], 3),
                    "query_p95_ms": round(m["query_p95_ms"], 3),
                    "serialize_1t_median_ms": round(m["serialize1_median_ms"], 2),
                    "serialize_1t_p95_ms": round(m["serialize1_p95_ms"], 2),
                    "serialize_Tt_median_ms": round(m["serializeT_median_ms"], 2),
                    "serialize_Tt_p95_ms": round(m["serializeT_p95_ms"], 2)})
    summary["materialized_10m"] = {
        "stats_bytes": load("build_materialized_10m")["stats_bytes"],
        "stats_build_wall_s": {r["threads"]: r["wall_s"]
                               for r in load("build_materialized_10m")["stats_runs"]},
        "query_and_serialize": ser,
    }
    d10 = load("duckdb_10m")
    summary["duckdb_10m_ms"] = {c: {T: round(d10["results"][T][c]["median_ms"], 2)
                                    for T in ("1", "4", "8", "16")} for c in CASES}
    summary["bsum_10m_1t"] = mc["10m_block_summary_scan_fallback_1t"]
    summary["duckdb_1b_ms"] = {c: {T: round(d1b["results"][T][c]["median_ms"], 2)
                                   for T in ("1", "4", "8", "16")} for c in CASES}
    try:
        dd = load("duckdb_diag")
        summary["duckdb_diag"] = dd
    except FileNotFoundError:
        summary["duckdb_diag"] = None
    # ---- ECUBE03 encoded-tree parallel build (follow-up) ----
    try:
        e3 = load("ecube3_build_mt")
        c = {}
        for name, d in e3["datasets"].items():
            c[name] = {
                "shipped_serial": {"wall_s": d["reference_1t"]["wall_s_median"],
                                   "build_ms": d["reference_1t"]["build_ms_median"],
                                   "sha256": d["reference_1t"]["sha256"]},
                "mt": {T: {k: d["mt"]["T" + T][k] for k in
                           ("wall_s_median", "build_ms_median", "peak_rss_mb", "sha256",
                            "byte_identical")} | {"metrics": d["mt"]["T" + T]["metrics"]}
                       for T in ("1", "4", "8", "16")},
            }
        summary["ecube3_build_mt"] = {
            "datasets": c,
            "null_measure": {"reference_sha256": e3["null_measure"]["reference_sha256"],
                             "reference_1t": {"wall_s": e3["null_measure"]["reference_1t"]["wall_s"],
                                              "build_ms": e3["null_measure"]["reference_1t"]["metrics"]["build_ms"]},
                             "mt": {T: {k: e3["null_measure"]["builds"]["T" + T][k] for k in
                                        ("wall_s_median", "build_ms_median", "sha256",
                                         "byte_identical", "peak_rss_mb")}
                                    for T in ("1", "16")}},
            "dense_rule_fixtures": {tag: {k: v[k] for k in
                                          ("bytes", "byte_identical", "header_flags_equal")}
                                    | {"dense": v["single_thread"]["header"]["dense"],
                                       "null_measure": v["single_thread"]["header"]["null_measure"]}
                                    for tag, v in e3["dense_rule_fixtures"].items()},
            "tpch_sf2_encoded_16t": {k: e3["tpch_sf2_encoded_16t"][k] for k in
                                     ("wall_s_median", "build_ms_median", "sha256",
                                      "byte_identical")},
            "query_1t": e3["query_1t"],
            "baseline_duckdb_1t": e3["baseline_duckdb_1t"],
            "amortization": e3["amortization"],
            "frozen_reference_sha256": e3["protocol"]["frozen_reference_sha256"],
        }
    except FileNotFoundError:
        summary["ecube3_build_mt"] = None

    try:
        qr = load("ecube3_query_regression")
        summary["ecube3_query_regression"] = {
            "protocol": qr["protocol"],
            "cases": {k: {"spec": v["spec"], "lead_reference_us": v["lead_reference_us"],
                          "frozen_us": v["binaries"]["frozen"]["median_us"],
                          "mt_us": v["binaries"]["mt"]["median_us"],
                          "frozen_p95_us": v["binaries"]["frozen"]["p95_us"],
                          "mt_p95_us": v["binaries"]["mt"]["p95_us"],
                          "paired_median_pct": v["paired_mt_vs_frozen_pct"]["median"],
                          "paired_positive": v["paired_mt_vs_frozen_pct"]["positive_pairs"],
                          "paired_n": v["paired_mt_vs_frozen_pct"]["pairs"],
                          "control_median_pct": v["paired_noise_control_pct"]["median"],
                          "control_positive": v["paired_noise_control_pct"]["positive_pairs"],
                          "rows_identical": v["result_rows_identical"],
                          "regression_detected": v["regression_detected"]}
                      for k, v in qr["cases"].items()},
        }
    except FileNotFoundError:
        summary["ecube3_query_regression"] = None

    save("summary", summary)


if __name__ == "__main__":
    main()
