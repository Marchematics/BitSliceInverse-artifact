#!/usr/bin/env python3
"""Render the MEC hardening receipts (multi-core, one-budget-rule, cold) into the
markdown section appended to MULTICOL_CUBE_20261101.md.

  python3 make_hardening_tables.py <results/eai_multi_20261101> [--append doc.md]

Reads  <root>/mt/mec_mt.json, <root>/budget/mec01_vs_mec02.json, <root>/cold/mec_cold.json
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import json
import os
import statistics
import sys

THREADS = ["1", "4", "8", "16"]
CASES = {
    "tpch2": ["all_full", "broad", "selective", "empty"],
    "nyc3": ["all_full", "v_mid", "v_top", "v_narrow"],
}


def eff(sp, t):
    return sp / t


def main():
    root = sys.argv[1]
    append = None
    if "--append" in sys.argv:
        append = sys.argv[sys.argv.index("--append") + 1]
    mt = json.load(open(os.path.join(root, "mt", "mec_mt.json")))
    bud = json.load(open(os.path.join(root, "budget", "mec01_vs_mec02.json")))
    cold = json.load(open(os.path.join(root, "cold", "mec_cold.json")))
    L = []

    # ---------------------------------------------------------------- build
    L.append("### 12.1 Parallel build (deterministic: byte-identical index for every thread count)\n")
    L.append("| dataset | threads | build ms | speedup | efficiency | RSS | index sha256 (first 12) |")
    L.append("|---|---:|---:|---:|---:|---:|---|")
    for ds, rec in mt["datasets"].items():
        b = rec["build"]
        t1 = b["1"]["build_ms"]
        for t in THREADS:
            r = b[t]
            L.append(f"| {ds} ({(rec['index_bytes'])/(1<<20):.1f} MB) | {t} | "
                     f"{r['build_ms']:.0f} | {t1 / r['build_ms']:.2f}x | "
                     f"{eff(t1 / r['build_ms'], int(t)):.2f} | {r['rss_kb']/1024:.0f} MB | "
                     f"`{r['sha256'][:12]}` |")
    L.append("")
    L.append("All four thread counts produce the **same bytes** "
             f"(`build_deterministic` = True for every dataset: "
             f"{', '.join(f'{ds} {r[chr(98)+chr(117)+chr(105)+chr(108)+chr(100)+chr(95)+chr(100)+chr(101)+chr(116)+chr(101)+chr(114)+chr(109)+chr(105)+chr(110)+chr(105)+chr(115)+chr(116)+chr(105)+chr(99)]}' for ds, r in mt['datasets'].items())}), "
             "because every parallel phase writes disjoint ranges and every merge is an "
             "integer sum/min/max over disjoint partials. The remaining serial part is the "
             "file write plus the `|span|` walks of the value dictionary; that is what limits "
             "the speedup to ~1.8-1.9x.\n")

    # ---------------------------------------------------------------- query
    L.append("### 12.2 Parallel query (spawn-once pool, so dispatch costs ~1-2 us)\n")
    for ds, rec in mt["datasets"].items():
        q = rec["query"]
        L.append(f"**{ds}** ({rec['mode']} mode, index {rec['index_bytes']/(1<<20):.0f} MB)\n")
        L.append("| case | 1T | 4T | 8T | 16T | best speedup | 16T p95 | 16T efficiency |")
        L.append("|---|---:|---:|---:|---:|---:|---:|---:|")
        for c in CASES[ds]:
            row = [q[t]["cases"][c]["sc"]["median_us"] for t in THREADS]
            p95 = q["16"]["cases"][c]["sc"]["p95_us"]
            sp = max(row[0] / x for x in row[1:])
            best = row.index(min(row))
            L.append(f"| {c} | " + " | ".join(f"{x:.2f} us" for x in row) +
                     f" | {sp:.2f}x ({THREADS[best]}T) | {p95:.2f} us | "
                     f"{eff(row[0] / row[3], 16):.2f} |")
        L.append("")
    L.append("Where it flattens, and why: at these latencies the work per query is a few "
             "microseconds, so the partition has nothing to amortise. Sub-3 us cases get "
             "**worse** with more threads (dispatch + merge cost ~2-4 us); wide cases with real "
             "residual work improve 1.5-2.6x and then saturate, because the per-run work is a "
             "*dependency chain* (four monotone binary searches in rank mode, one contiguous "
             "residual scan) rather than parallel-friendly bandwidth.\n")

    # ---------------------------------------------------------------- duckdb
    L.append("### 12.3 Same-thread DuckDB 2.0 control (one process per thread count)\n")
    L.append("Threads are passed as `config={\"threads\": N}` at connect time. We also measured "
             "the `SET threads=N`-after-connect form: in duckdb 2.0.0 both report the same "
             "`current_setting('threads')` and measure the same latency (5.34 vs 5.69 ms at 8T "
             "on TPC-H, 32.4 vs 32.8 ms on NYC), so the pinned protocol is config-at-connect and "
             "is verified per run in the receipt.\n")
    L.append("| dataset | engine | 1T | 4T | 8T | 16T | speedup @16T | efficiency @16T |")
    L.append("|---|---|---:|---:|---:|---:|---:|---:|")
    for ds, rec in mt["datasets"].items():
        d = rec["duckdb"]
        med = [statistics.median(x["median"] for x in d[t]["cases"].values()) for t in THREADS]
        L.append(f"| {ds} | DuckDB 2.0 | " + " | ".join(f"{x:.2f} ms" for x in med) +
                 f" | {med[0]/med[3]:.2f}x | {eff(med[0]/med[3], 16):.2f} |")
        q = rec["query"]
        mm = [statistics.median(q[t]["cases"][c]["sc"]["median_us"] for c in CASES[ds])
              for t in THREADS]
        L.append(f"| {ds} | MEC02 | " + " | ".join(f"{x:.2f} us" for x in mm) +
                 f" | {mm[0]/mm[3]:.2f}x | {eff(mm[0]/mm[3], 16):.2f} |")
        L.append(f"| {ds} | **ratio (DuckDB / MEC)** | " +
                 " | ".join(f"**{med[i]/mm[i]*1000:,.0f}x**" for i in range(4)) + " | | |")
    L.append("")
    L.append("This is the honest core of the multi-core story: **DuckDB parallelises almost "
             "linearly (9.6x at 16 threads on TPC-H, 15.0x on NYC) while MEC02 parallelises "
             "1.5-2.6x**, so the single-thread ratio (~2,950x on TPC-H) shrinks to ~740x at "
             "16-vs-16 threads. MEC stays the faster engine in absolute terms by roughly three "
             "orders of magnitude, but the *marginal* value of a core is much lower for MEC: its "
             "work is latency-bound bookkeeping, not a bandwidth-bound scan. For a multi-core "
             "deployment the right conclusion is that MEC should be run at 1-4 threads per query "
             "and scaled out across queries, not within one query.\n")

    # ---------------------------------------------------------------- budget
    L.append("### 12.4 MEC01 vs MEC02 under one budget rule\n")
    L.append(f"Rule: `{bud['rule']}`. MEC01 is the shipped first version (value blocks only, "
             "`uint16` cumulative planes, no first/last planes, no NULL group, `W` fixed at 16384). "
             "Its footprint below is reconstructed from the same dataset shape "
             "(`2n + (BP+1)*runs*10 B`), its latency comes from its archived receipt on the same "
             "host/core/pin, and its applicability is decided by its own two hard limits.\n")
    L.append("| dataset | source parquet | budget | MEC01 bytes | MEC01 / source | MEC01 verdict | "
             "MEC02 bytes | MEC02 / source | MEC02 footprint |")
    L.append("|---|---:|---:|---:|---:|---|---:|---:|---:|")
    for ds, r in bud["datasets"].items():
        m1, m2 = r["mec01"], r["mec02"]
        verdict = "ok" if m1["applicable"] else ("REJECTED: " + "; ".join(m1["rejects"]))
        L.append(f"| {ds} | {r['source_parquet_bytes']/(1<<20):.1f} MB | "
                 f"{r['budget_bytes']/(1<<20):.1f} MB | {m1['bytes']/(1<<20):.1f} MB | "
                 f"{m1['bytes_over_source']:.3f}x | {verdict} | {m2['bytes']/(1<<20):.1f} MB | "
                 f"{m2['bytes_over_source']:.3f}x | {m2['footprint']:.3f}x |")
    L.append("")
    tp = bud["datasets"]["tpch2"]
    L.append("**Do not read this as an apples-to-apples speedup.** MEC01 can only answer 1 of the "
             "5 datasets: it rejects NULL group keys outright (`tpch2_null`, `syn_cluster`) and "
             "it cannot represent a run longer than 65,535 rows (`syn_cluster` 8.4 M, `nyc3` "
             "60.9 M, `nyc3b` 3.3 M). On the one comparable dataset, `tpch2`, both versions sit "
             "under the same budget (MEC01 32.9 MB reconstructed, MEC02 36.9 MB measured, budget "
             f"39.6 MB) at `W=16384`/`BP=635`, and their SUM/COUNT path is the same code, so the "
             "`sc` latency is the same number "
             f"(MEC01 archived {min(tp['mec01']['mec_sc_us'].values()):.2f}-"
             f"{max(tp['mec01']['mec_sc_us'].values()):.2f} us, MEC02 "
             f"{min(tp['mec02']['mec_sc_us'].values()):.2f}-"
             f"{max(tp['mec02']['mec_sc_us'].values()):.2f} us). The differences that matter are "
             "elsewhere: **MIN/MAX is 2.0x cheaper in MEC02** "
             f"(broad: {tp['mec01']['mec_full_us']['broad']:.1f} us -> "
             f"{tp['mec02']['mec_full_us']['broad']:.1f} us, all_full: "
             f"{tp['mec01']['mec_full_us']['all_full']:.1f} us -> "
             f"{tp['mec02']['mec_full_us']['all_full']:.1f} us) thanks to the first/last planes, "
             "and MEC02 is the only version that answers the other four datasets at all. "
             "Note also that MEC01 would breach the budget on both NYC datasets even if the run "
             "cap did not reject them (211 MB vs a 99.7 MB budget, 856 MB vs 160.7 MB).\n")

    # ---------------------------------------------------------------- cold
    L.append("### 12.5 Cold complete request (fresh process per launch, 21 launches)\n")
    L.append("| case | MEC parent wall | MEC child in-process | MEC child query only | "
             "DuckDB parent wall | DuckDB child in-process | DuckDB connect | DuckDB query |")
    L.append("|---|---:|---:|---:|---:|---:|---:|---:|")
    rec = cold["datasets"]["tpch2"]
    for name in ("broad", "selective", "empty"):
        m, d = rec["mec"][name], rec["duckdb"][name]
        L.append(f"| {name} | {m['parent_wall_ms']['median']:.3f} ms | "
                 f"{m['child_stored_ms']['median']:.3f} ms | "
                 f"{m['child_query_ms']['median']:.3f} ms | "
                 f"{d['parent_wall_ms']['median']:.2f} ms | "
                 f"{d['child_in_process_ms']['median']:.2f} ms | "
                 f"{d['child_connect_ms']['median']:.2f} ms | "
                 f"{d['child_query_ms']['median']:.2f} ms |")
    L.append("")
    tp = rec
    L.append("A complete MEC request (fork/exec + mmap + query + JSON result) is "
             f"**{tp['mec']['broad']['parent_wall_ms']['median']:.2f}-"
             f"{tp['mec']['selective']['parent_wall_ms']['median']:.2f} ms**, of which the "
             f"engine itself is **{tp['mec']['broad']['child_stored_ms']['median']:.3f}-"
             f"{tp['mec']['empty']['child_stored_ms']['median']:.3f} ms**: process start-up "
             "dominates, not the index. A cold DuckDB request is "
             f"**{tp['duckdb']['broad']['parent_wall_ms']['median']:.0f}-"
             f"{tp['duckdb']['selective']['parent_wall_ms']['median']:.0f} ms** (about "
             f"{tp['duckdb']['broad']['child_connect_ms']['median']:.0f} ms of it is "
             "`duckdb_connect` on the 4-column database), so the complete-request ratio is "
             f"~{min(tp['duckdb'][c]['parent_wall_ms']['median']/tp['mec'][c]['parent_wall_ms']['median'] for c in ('broad','selective','empty')):.0f}x-"
             f"~{max(tp['duckdb'][c]['parent_wall_ms']['median']/tp['mec'][c]['parent_wall_ms']['median'] for c in ('broad','selective','empty')):.0f}x "
             "— smaller than the engine-resident ratio, because neither side's engine time "
             "matters at process scale. This is the number a client actually observes.\n")

    text = "\n".join(L)
    if append:
        doc = open(append).read()
        tag = "<!--TBL:HARDENING-->"
        if tag in doc:
            doc = doc.replace(tag, text)
        else:
            doc = doc.rstrip() + "\n\n## 12. Multi-core, one budget rule, and cold complete requests\n\n" + text
        open(append, "w").write(doc)
        print("appended to", append)
    else:
        print(text)


if __name__ == "__main__":
    main()
