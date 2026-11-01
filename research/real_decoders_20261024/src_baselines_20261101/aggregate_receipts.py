#!/usr/bin/env python3
"""Aggregate baseline receipts into the markdown tables for BASELINES_20261101.md.

  python3 aggregate_receipts.py <receipts_dir> [--json out.json]

Every number printed here comes from a receipt produced on the remote host by the
bench_*.py harnesses (see README.md).  Blocks follow the paper's budget classes:

  SCAN    = scan-and-aggregate, no precomputation
  MAT     = materialized-aggregate configurations (precomputation allowed)
  INDEX   = auxiliary index / selection baselines (build once, positions -> aggregate)
  KERNEL  = codec/file-format kernels, not SQL engines
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import json
import os
import sys

CASES = ["fixed_all4", "le_m7000", "le_m720", "le_509", "le_650", "le_930",
         "le_1350", "le_99900"]
SHORT = {"fixed_all4": "fixed", "le_m7000": "<=-7k", "le_m720": "<=-720",
         "le_509": "<=509", "le_650": "<=650", "le_930": "<=930",
         "le_1350": "<=1350", "le_99900": "<=99900"}

SCAN = ["duckdb20_scan", "duckdb20_scan_clean", "arrow25_acero_groupby", "arrow25_dataset_parquet",
        "vortex087_scan", "vortex087_predecoded", "polars20",
        "datafusion55_parquet", "clickhouse_chdb", "daft0726", "pandas233"]
MAT = ["eai_ecube03_frozen", "eai_ecube02_frozen", "eai_ecube01", "duckdb20_materialized", "materialized_summary"]
INDEX = ["range_index_nyc"]
POSITIONS = ["cabin_sigmod24_scan"]
KERNEL = ["btrblocks_nyc100_fare", "fastlanes_nyc100_fare"]


def load(receipts_dir):
    out = {}
    for name in sorted(os.listdir(receipts_dir)):
        if not name.endswith(".json"):
            continue
        p = os.path.join(receipts_dir, name)
        try:
            d = json.load(open(p))
        except Exception:
            continue
        if not isinstance(d, dict):
            continue
        if "method" in d and "cases" in d:
            out[d["method"]] = d
        elif name.startswith("stability_control") and "cases" in d:
            d["method"] = "stability_control"
            out["stability_control"] = d
        elif "codec" in d:
            out[d["codec"] + "_nyc100_fare"] = d
    return out


def human(n):
    if not isinstance(n, (int, float)):
        return "n/a"
    for u in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024 or u == "TB":
            return ("%d B" % n) if u == "B" else ("%.1f %s" % (n, u))
        n /= 1024.0


def norm(r, case, field):
    c = r.get("cases", {}).get(case)
    if not c:
        return None
    return c.get(field)


def matrix(rec, methods, field, fmt):
    print("| method | " + " | ".join(SHORT[c] for c in CASES) + " |")
    print("|---|" + "---:|" * len(CASES))
    for m in methods:
        r = rec.get(m)
        if not r:
            continue
        cells = []
        for c in CASES:
            v = norm(r, c, field)
            cells.append(fmt % v if isinstance(v, (int, float)) else "n/a")
        print("| %s | %s |" % (m, " | ".join(cells)))


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    d = args[0] if args else _artifact.expand("../../work/bench_baselines/runs")
    rec = load(d)
    ref = rec.get("duckdb20_scan")

    print("## Medians (ms), scan-and-aggregate block\n")
    matrix(rec, SCAN, "median_ms", "%.3f")
    print("\n## Min (ms), scan-and-aggregate block\n")
    matrix(rec, SCAN, "min_ms", "%.3f")
    print("\n## p95 (ms), scan-and-aggregate block\n")
    matrix(rec, SCAN, "p95_ms", "%.3f")

    print("\n## Conservative baseline: fastest observed scan-block latency per case\n")
    print("| case | fastest baseline | method | baseline min (ms) | DuckDB median (ms) | DuckDB median / baseline min |")
    print("|---|---|---:|---:|---:|---:|")
    cons = {}
    for c in CASES:
        best = None
        for m in SCAN:
            r = rec.get(m)
            v = norm(r, c, "min_ms") if r else None
            if v is not None and (best is None or v < best[1]):
                best = (m, v)
        if best:
            cons[c] = best
            dd = norm(ref, c, "median_ms") if ref else None
            print("| %s | %s | %.3f | %.3f | %.2fx |" % (
                SHORT[c], best[0], best[1], dd or float("nan"),
                (dd / best[1]) if dd else float("nan")))

    print("\n## Materialized-aggregate block (precomputation allowed)\n")
    print("| method | storage | build | " + " | ".join(SHORT[c] for c in CASES) + " |")
    print("|---|---:|---:|" + "---:|" * len(CASES))
    for m in MAT:
        r = rec.get(m)
        if not r:
            continue
        build = r.get("build_ms")
        if isinstance(build, dict):
            build = sum(v for k, v in build.items() if k.endswith("_ms") and isinstance(v, (int, float)))
        cells = []
        for c in CASES:
            v = norm(r, c, "median_ms")
            if v is None:
                cells.append("n/a")
            elif v < 1.0:
                cells.append("%.3f us" % (v * 1000))
            else:
                cells.append("%.3f ms" % v)
        print("| %s | %s | %s | %s |" % (
            m, r.get("storage_human") or human(r.get("storage_bytes")),
            ("%.3f s" % (build / 1000.0)) if isinstance(build, (int, float)) else "n/a",
            " | ".join(cells)))

    for cube_name in ("eai_ecube03_frozen", "eai_ecube02_frozen", "eai_ecube01"):
        cube = rec.get(cube_name)
        if not cube:
            continue
        print("\n## %s detail (this harness, core 56)\n" % cube_name)
        print("| case | query median (us) | query min (us) | query p95 (us) | serialize median (us) | setup (ms) | cold request median (ms) | exact |")
        print("|---|---:|---:|---:|---:|---:|---:|---|")
        for c in CASES:
            x = cube["cases"][c]
            print("| %s | %.3f | %.3f | %.3f | %.3f | %.3f | %.3f | %s |" % (
                SHORT[c], x["median_ms"] * 1000, x["min_ms"] * 1000, x["p95_ms"] * 1000,
                x["serialize_median_ms"] * 1000, x["setup_ms"],
                x.get("cold_complete_request_ms", float("nan")),
                x["exactness"]["exact"]))

    print("\n## Index / positions block\n")
    for m in INDEX + POSITIONS:
        r = rec.get(m)
        if not r:
            continue
        print("### %s\n" % m)
        print("| case | " + " | ".join(SHORT[c] for c in CASES) + " |")
        print("|---|" + "---:|" * len(CASES))
        for field, fmt in (("median_ms", "%.3f"), ("scan_ms", "%.3f"),
                           ("index_ms", "%.1f"), ("check_ms", "%.1f"),
                           ("complete_request_ms", "%.3f"),
                           ("complete_request_median_ms", "%.3f")):
            vals = {c: norm(r, c, field) for c in CASES}
            if not any(isinstance(v, dict) for v in vals.values()) and \
               not any(isinstance(v, (int, float)) for v in vals.values()):
                continue
            if field == "complete_request_ms" and any(isinstance(v, dict) for v in vals.values()):
                cells = ["%.3f" % vals[c]["median_ms"] if isinstance(vals[c], dict) else "n/a"
                         for c in CASES]
            else:
                cells = [fmt % vals[c] if isinstance(vals[c], (int, float)) else "n/a"
                         for c in CASES]
            print("| %s | %s |" % (field, " | ".join(cells)))
        if r.get("build"):
            print("\nbuild: %s" % json.dumps(r["build"]))

    print("\n## Kernel / format block (same fare_cents column, not SQL)\n")
    print("| codec | values | bytes | bytes/value | encode ms | decode ms | decode ns/value | exact |")
    print("|---|---:|---:|---:|---:|---:|---:|---|")
    for m in KERNEL:
        r = rec.get(m)
        if not r:
            continue
        print("| %s | %d | %d | %.4f | %.1f | %.1f | %.3f | %s |" % (
            r["codec"], r["values"],
            r.get("fls_bytes", r.get("compressed_bytes")),
            r["bytes_per_value"], r["encode_median_ms"], r["decode_median_ms"],
            r["decode_ns_per_value"], r["decoded_exact"]))

    print("\n## DuckDB scan: median [min, p95] and 60-rep stability control\n")
    if ref:
        print("| case | median | min | p95 | median/min |")
        print("|---|---:|---:|---:|---:|")
        for c in CASES:
            print("| %s | %.3f | %.3f | %.3f | %.2fx |" % (
                SHORT[c], norm(ref, c, "median_ms"), norm(ref, c, "min_ms"),
                norm(ref, c, "p95_ms"),
                norm(ref, c, "median_ms") / norm(ref, c, "min_ms")))
    st = rec.get("stability_control")
    if st:
        print("\nstability control (60 reps, fixed query):")
        for k, v in st["cases"].items():
            print("| %s | median %.3f | min %.3f | p25 %.3f | p75 %.3f | p95 %.3f | max %.3f | %d reps |" % (
                k, v["median_ms"], v["min_ms"], v["p25_ms"], v["p75_ms"],
                v["p95_ms"], v["max_ms"], v["reps"]))

    print("\n## Threading audit (user+sys)/wall from /usr/bin/time\n")
    print("| run | wall s | user s | sys s | cpu/wall | rss KB |")
    print("|---|---:|---:|---:|---:|---:|")
    runs = d.rstrip("/")
    if not any(n.endswith(".time") for n in os.listdir(runs)):
        runs = os.path.dirname(runs)
    for name in sorted(os.listdir(runs)):
        if not name.endswith(".time"):
            continue
        txt = open(os.path.join(runs, name)).read().strip()
        vals = dict(kv.split("=") for kv in txt.split() if "=" in kv)
        try:
            wall = float(vals["wall"]); user = float(vals["user"]); syst = float(vals["sys"])
            print("| %s | %.2f | %.2f | %.2f | %.2f | %s |" % (
                name[:-5], wall, user, syst, (user + syst) / wall if wall else 0, vals.get("rss_kb")))
        except Exception:
            print("| %s | %s |" % (name[:-5], txt))

    if "--json" in sys.argv:
        dest = sys.argv[sys.argv.index("--json") + 1]
        out = {"cases": CASES, "blocks": {"scan": SCAN, "materialized": MAT,
                                          "index": INDEX, "positions": POSITIONS,
                                          "kernel": KERNEL},
               "methods": {}, "conservative_baseline": cons}
        for m, r in rec.items():
            if "cases" in r:
                out["methods"][m] = {
                    "engine_version": r.get("engine_version"),
                    "input_format": r.get("input_format"),
                    "storage_bytes": r.get("storage_bytes"),
                    "exact_all": r.get("exact_all"),
                    "load_ms": r.get("load_ms"),
                    "build_ms": r.get("build_ms"),
                    "notes": r.get("notes"),
                    "median_ms": {c: norm(r, c, "median_ms") for c in CASES},
                    "min_ms": {c: norm(r, c, "min_ms") for c in CASES},
                    "p95_ms": {c: norm(r, c, "p95_ms") for c in CASES},
                    "reps": {c: norm(r, c, "reps") for c in CASES},
                }
        json.dump(out, open(dest, "w"), indent=1)
        print("\nsummary json ->", dest)


if __name__ == "__main__":
    main()
