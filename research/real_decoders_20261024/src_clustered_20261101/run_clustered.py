#!/usr/bin/env python3
"""Clustered stream campaign (2026-11-01): value-clustered (CLST01) exact
aggregation for high group cardinality on NYC-100M.

Closes the one class that was still below 2x: a dynamic value range with
G >= 100,000 groups, where the dense ECUBE cube is infeasible (O(G*V) =
8.83 GB at G=100K, 88.3 GB at G=1M) and the block-summary fallback is O(n)
(1.78x / 1.13x vs DuckDB 2.0 single thread, TIERS 2026-11-01 section 2).

What it produces (all numbers from this run, no transcription):
  * build: sort/re-encode time + exact structure bytes for both G
  * losslessness: clst_tool verify vs the input columns
  * selectivity series 0.1/1/5/10/25/50/100 % for G=100K and G=1M:
      ours (CLST01), DuckDB 2.0.0 threads=1, DuckDB threads=16,
      block-summary fallback (tiers_tool bsum_query), x DuckDB @1T
  * element-wise exactness vs DuckDB 2.0.0 on every case + boundary, empty,
    out-of-domain and needle cases
  * footprint: structure bytes / the 251,814,420 B encoded tree

Run on localhost from research/real_decoders_20261024:
    nice -n 5 taskset -c 129 python3 src_clustered_20261101/run_clustered.py
(the C++ tool subprocesses are pinned to core 128; the driver to 129; both
inside the clustered stream's core set 128-143.)
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import hashlib
import json
import os
import statistics
import struct
import subprocess
import sys
import time

import duckdb

REPO = os.path.abspath(os.getcwd())
SCRATCH = _artifact.expand(os.environ.get("CLST_SCRATCH", "../../work/clustered/build"))
BIN = _artifact.expand(os.environ.get("CLST_BIN", "../../work/clustered/bin"))
RECEIPT = os.path.join(_artifact.WORK, "receipts", "clustered_20261101")
TIERS = _artifact.expand("../../work/tiers/build")
TTOOL = _artifact.expand("../../work/tiers/bin/tiers_tool")
TOOL = os.path.join(BIN, "clst_tool")
BENCH_DUCK = os.path.join(REPO, "src_clustered_20261101/bench_duck.py")
PY = _artifact.expand("python3")
CORES16 = _artifact.cores(16)
VAL = os.path.join(TIERS, "val_nyc100.i32")
GROUPS_DB = os.path.join(TIERS, "groups.duckdb")
BSUM = os.path.join(TIERS, "bsum_nyc100.bin")
ENC_TREE = os.path.join(REPO, "results/nyc_taxi_100m_prep/Observed.encoded")
CORE = _artifact.core()
N = 100_000_000
GS = (100_000, 1_000_000)
TARGETS = (0.001, 0.01, 0.05, 0.10, 0.25, 0.50, 1.00)
DUCK_REPS_1T = 7
DUCK_REPS_16T = 7
I32MIN, I32MAX = -2**31, 2**31 - 1


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def sh(cmd, pin=True, cores=CORE, timeout=None):
    pre = ["nice", "-n", "5", "taskset", "-c", cores] if pin else []
    t = time.perf_counter()
    p = subprocess.run(["/usr/bin/time", "-v"] + pre + cmd, capture_output=True,
                       text=True, timeout=timeout)
    wall = time.perf_counter() - t
    rss = 0.0
    for line in p.stderr.splitlines():
        if "Maximum resident set size" in line:
            rss = float(line.split(":")[1].strip())
    if p.returncode != 0:
        raise RuntimeError(f"{cmd[:4]} rc={p.returncode}\n{p.stderr[-2500:]}")
    return wall, p.stderr, p.stdout, rss


def last_json(s):
    for line in reversed(s.splitlines()):
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                return json.loads(line)
            except json.JSONDecodeError:
                continue
    raise RuntimeError("no json: " + s[-800:])


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 22), b""):
            h.update(chunk)
    return h.hexdigest()


def warm_core(seconds=0.5):
    """Deterministic pinned busy loop: P-state precondition for us-scale benches."""
    subprocess.run(["nice", "-n", "5", "taskset", "-c", CORE, "timeout", f"{seconds}s",
                    "bash", "-c", "while :; do :; done"], capture_output=True)


def read_varints(buf, count, pos):
    out = []
    for _ in range(count):
        v = 0
        sh_ = 0
        while True:
            b = buf[pos]
            pos += 1
            v |= (b & 0x7F) << sh_
            if not (b & 0x80):
                break
            sh_ += 7
        out.append(v)
    return out, pos


def load_clst_meta(path):
    """Parse the CLST01 header + the (small) value/count/start blocks."""
    d = open(path, "rb").read(4096)
    f = open(path, "rb")
    hdr = f.read(156)
    h = {}
    h["magic"] = hdr[:8]
    h["n"], h["G"], h["V"] = struct.unpack_from("<QQQ", hdr, 8)
    h["W"], h["flags"], h["pad0"] = struct.unpack_from("<III", hdr, 32)
    h["vmin"], h["vmax"] = struct.unpack_from("<qq", hdr, 44)
    (h["off_values"], h["len_values"], h["off_counts"], h["len_counts"],
     h["off_starts"], h["len_starts"], h["off_gids"], h["len_gids"],
     h["off_stats"], h["len_stats"], h["file_bytes"], h["reserved"]) = \
        struct.unpack_from("<QQQQQQQQQQQQ", hdr, 60)
    h["gmin"] = struct.unpack_from("<q", hdr, 148)[0]
    f.seek(h["off_values"])
    vbuf = f.read(h["len_values"])
    f.seek(h["off_counts"])
    cbuf = f.read(h["len_counts"])
    f.seek(h["off_starts"])
    starts = list(struct.unpack_from("<%dQ" % (h["V"] + 1), f.read(h["len_starts"])))
    f.close()
    assert h["magic"] == b"CLST01\n\x00", h["magic"]
    deltas, _ = read_varints(vbuf, h["V"], 0)
    counts, _ = read_varints(cbuf, h["V"], 0)
    vals, prev = [], 0
    for d_ in deltas:
        z = d_
        prev += (z >> 1) ^ -(z & 1)
        vals.append(prev)
    assert len(starts) == h["V"] + 1
    return h, vals, counts, starts


def cut_for_rank(vals, counts, starts, rank):
    """value cut so that exactly starts[r+1] rows satisfy v <= cut"""
    import bisect
    r = bisect.bisect_right(starts, rank) - 1
    r = max(0, min(r, len(vals) - 1))
    return vals[r], starts[r + 1]


# ---------------------------------------------------------------------------
# DuckDB side
# ---------------------------------------------------------------------------
def duck_baselines(spec):
    """threads=1 and threads=16, each in its own externally pinned process."""
    spec_path = os.path.join(SCRATCH, "duck_spec.json")
    json.dump(spec, open(spec_path, "w"), indent=1)
    outs = {}
    for threads, cores, reps in ((1, CORE, DUCK_REPS_1T), (16, CORES16, DUCK_REPS_16T)):
        outp = os.path.join(SCRATCH, f"duck_{threads}t.json")
        wall, _, so, _ = sh([PY, BENCH_DUCK, GROUPS_DB, str(threads), str(reps),
                             spec_path, outp], cores=cores)
        j = json.load(open(outp))
        j["wall_s"] = wall
        j["pin"] = f"nice -n 5 taskset -c {cores}"
        outs[threads] = j
        print(f"  duckdb threads={threads}: effective={j['threads_effective']} "
              f"cores={len(j['affinity'] or [])} wall={wall:.1f}s", flush=True)
    return outs


def duck_rows(con, sql):
    t = time.perf_counter()
    rows = con.execute(sql).fetchall()
    return rows, (time.perf_counter() - t) * 1000.0


def norm_ours(rows):
    return [(str(int(r[0])), str(int(r[1])), str(int(r[2])), str(int(r[3])), str(int(r[4])))
            for r in rows]


def norm_duck(rows):
    return [("NULL" if r[0] is None else str(int(r[0])), str(int(r[1])), str(int(r[2])),
             str(int(r[3])), str(int(r[4]))) for r in rows]


def sql_for(G, lo, hi):
    g = f"g{G}"
    base = f"SELECT {g} AS g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM groups"
    if lo <= I32MIN and hi >= I32MAX:
        return base + " GROUP BY 1 ORDER BY 1"
    return base + f" WHERE v BETWEEN {lo} AND {hi} GROUP BY 1 ORDER BY 1"


def sql_engine_for(G, lo, hi):
    g = f"g{G}"
    inner = f"SELECT {g}, SUM(v), COUNT(*), MIN(v), MAX(v) FROM groups"
    if lo <= I32MIN and hi >= I32MAX:
        return f"SELECT COUNT(*) FROM ({inner} GROUP BY 1)"
    return f"SELECT COUNT(*) FROM ({inner} WHERE v BETWEEN {lo} AND {hi} GROUP BY 1)"


# ---------------------------------------------------------------------------
# our side
# ---------------------------------------------------------------------------
def our_bench(path, lo, hi, out_json, n_benches=3, target_ms=150.0, extra=()):
    """3 independent bench runs, each after a 0.5 s pinned P-state warmup;
    median of the 3 bench medians (the project's microsecond protocol)."""
    # estimate the per-query cost with a short run, then size the reps
    _, stderr, _, _ = sh([TOOL, "query", path, str(lo), str(hi), out_json, "/dev/null", "3"]
                         + list(extra))
    est = last_json(stderr)["query_median_ms"]
    reps = int(max(5, min(500, target_ms / max(est, 1e-4))))
    runs = []
    for _ in range(n_benches):
        warm_core()
        _, stderr, _, _ = sh([TOOL, "query", path, str(lo), str(hi), out_json, "/dev/null",
                              str(reps)] + list(extra))
        runs.append(last_json(stderr))
    runs.sort(key=lambda j: j["query_median_ms"])
    med = runs[len(runs) // 2]
    out = dict(med)
    out["reps"] = reps
    out["bench_medians_ms"] = [r["query_median_ms"] for r in runs]
    out["bench_spread"] = (max(out["bench_medians_ms"]) /
                           max(1e-12, min(out["bench_medians_ms"])))
    out["query_median_ms"] = med["query_median_ms"]
    out["query_p95_ms"] = med["query_p95_ms"]
    return out


def ours_once(path, lo, hi, out_json, reps=5, extra=()):
    _, stderr, _, _ = sh([TOOL, "query", path, str(lo), str(hi), out_json, "/dev/null",
                          str(reps)] + list(extra))
    return last_json(stderr)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main():
    os.makedirs(RECEIPT, exist_ok=True)
    os.makedirs(SCRATCH, exist_ok=True)
    cuts = json.load(open(os.path.join(TIERS, "cuts.json")))["datasets"]["nyc100"]
    rec = {
        "stream": "clustered",
        "date": "2026-11-01",
        "protocol": {
            "host": "localhost (AMD EPYC 9554, 256 hardware threads, no GPU)",
            "pin_tool": f"nice -n 5 taskset -c {CORE}",
            "pin_driver": "nice -n 5 taskset -c 129 for the 1T driver runs; the DuckDB 16T "
                           "control is measured in a driver pinned to the whole stream core "
                           "set 128-143 (a single-core pin would serialize its 16 threads)",
            "value_column": "NYC yellow taxi 2023-01..2025-09 fare_amount cents, "
                            "100,000,000 rows, V=21084 (vmin/vmax read from the "
                            "structure header in cuts)",
            "gid": "splitmix64(rowid) % G (C++ gen_groups, same bytes both sides)",
            "ours": "engine-resident: mmap + 1 untimed warmup + N reps; timed region = "
                    "reset of the previous query state + aggregation scan (row "
                    "collection/serialization timed apart and never mixed in); "
                    "3 bench runs after a 0.5 s pinned busy loop, median of the 3 medians",
            "duckdb": "2.0.0, read-only connection, threads set through the connection "
                      "config, one process per thread count: SET threads=N is database-global in "
                      "2.0.0 and is shared by all connections of one database in a "
                      "process (a 1T and a 16T connection in one process silently both "
                      "run at 16T -- verified), so the effective "
                      "current_setting('threads') is recorded per run; "
                      "threads=1 in a process pinned to one core and threads=16 pinned to "
                      f"16 cores; 1 warmup + {DUCK_REPS_1T} timed execute() of the "
                      "forced-aggregation engine SQL "
                      "SELECT COUNT(*) FROM (... GROUP BY ...)",
            "block_fallback": "tiers_tool bsum_query (TIERBS01 1,024-row block min/max "
                              "summaries + pruned exact scan, dense 32 B/group state)",
            "exactness": "element-wise full grouped result vs DuckDB 2.0.0 threads=1 "
                         "(group, SUM, COUNT(*), MIN, MAX)",
            "duckdb_version": duckdb.__version__,
        },
        "artifacts": {
            "clst_tool_sha256": sha256(TOOL),
            "clst_tool_cpp_sha256": sha256(os.path.join(
                REPO, "src_clustered_20261101/clst_tool.cpp")),
            "enc_tree_bytes": os.path.getsize(ENC_TREE),
            "enc_tree": ENC_TREE,
        },
        "build": {},
        "cuts": {},
        "series": {},
        "boundary_cases": {},
        "duckdb_baseline": {},
        "footprint": {},
    }
    print("=== build (sort + encode) ===", flush=True)
    metas = {}
    for G in GS:
        gid = os.path.join(TIERS, f"gid_g{G}.i32")
        entry = {"rows": N, "G": G, "gid_column": gid, "tree_bytes": os.path.getsize(ENC_TREE)}
        for variant, flags in (("range", []), ("stats", ["--with-stats"])):
            path = os.path.join(SCRATCH, f"clst_g{G}_{variant}.clst")
            wall, stderr, _, rss = sh([TOOL, "build", gid, VAL, path] + flags)
            b = last_json(stderr)
            b["wall_s"] = wall
            b["peak_rss_kb"] = rss
            b["file_bytes"] = os.path.getsize(path)
            b["sha256"] = sha256(path)
            b["ratio_to_tree"] = b["file_bytes"] / os.path.getsize(ENC_TREE)
            # sort cost: the scatter phase is the physical reordering (the sort)
            b["sort_ms"] = b["scatter_ms"]
            entry[variant] = b
            print(f"  G={G} {variant}: {b['build_ms']:.1f}ms bytes={b['file_bytes']} "
                  f"({b['ratio_to_tree']:.4f}x tree) scatter={b['scatter_ms']:.1f}ms "
                  f"bitpack={b['bitpack_ms']:.1f}ms write={b['write_ms']:.1f}ms "
                  f"sha={b['sha256'][:12]}", flush=True)
            if variant == "stats":
                _, _, so, _ = sh([TOOL, "info", path])
                entry["info"] = json.loads(so.strip())
                wv, verr, vout, vrss = sh([TOOL, "verify", gid, VAL, path])
                entry["verify"] = json.loads(vout.strip())
                entry["verify"]["wall_s"] = wv
                entry["verify"]["peak_rss_kb"] = vrss
                print(f"  verify: {entry['verify']}", flush=True)
        # dense-cube alternative for this class (infeasible) -- cited arithmetic
        entry["dense_cube_alt_bytes"] = G * 21084 * 4
        entry["dense_cube_alt_ratio"] = entry["dense_cube_alt_bytes"] / os.path.getsize(ENC_TREE)
        rec["build"][f"G{G}"] = entry
        h, vals, counts, starts = load_clst_meta(
            os.path.join(SCRATCH, f"clst_g{G}_stats.clst"))
        metas[G] = (h, vals, counts, starts)

    # ---------------- selectivity cuts ----------------
    print("=== cuts ===", flush=True)
    h, vals, counts, starts = metas[GS[0]]
    case_cuts = {}
    for s in TARGETS:
        if s >= 1.0:
            cut, rows = I32MAX, N
        else:
            rank = int(round(s * N))
            cut, rows = cut_for_rank(vals, counts, starts, rank)
        case_cuts[f"p{s*100:g}"] = {"target": s, "cut": cut,
                                    "actual_selectivity": rows / N, "rows": rows}
        print(f"  p{s*100:g}: cut={cut} rows={rows} sel={rows/N:.6f}", flush=True)
    rec["cuts"] = {"value_cuts": case_cuts, "V": h["V"], "vmin": h["vmin"],
                   "vmax": h["vmax"], "gid_bits": {"G100000": metas[100_000][0]["W"],
                                                   "G1000000": metas[1_000_000][0]["W"]}}
    vmin, vmax = h["vmin"], h["vmax"]
    midv = vals[len(vals) // 2]
    mincnt_idx = min(range(len(counts)), key=lambda i: counts[i])
    needle = {"value": vals[mincnt_idx], "rows": counts[mincnt_idx]}

    # ---------------- boundary / empty / out-of-domain cases ---------------
    print("=== DuckDB baselines (threads=1 / 16, separate pinned processes) ===",
          flush=True)
    spec = []
    for G in GS:
        for name, c in case_cuts.items():
            spec.append({"label": f"G{G}/{name}",
                         "engine_sql": sql_engine_for(G, I32MIN, c["cut"])})
    base = duck_baselines(spec)
    rec["duckdb_baseline"] = base

    boundary = [
        ("vmin_single", vmin, vmin),
        ("vmax_single", vmax, vmax),
        ("mid_single", midv, midv),
        ("needle_mincount_value", needle["value"], needle["value"]),
        ("empty_lo_gt_hi", vmax, vmax - 1),
        ("empty_above_domain", 10**15, 2 * 10**15),
        ("below_all", -10**15, vmin - 1),
        ("above_all", vmax + 1, I32MAX),
        ("full_i32", I32MIN, I32MAX),
        ("zero_zero", 0, 0),
    ]

    # ---------------- DuckDB connections ----------------
    con = duckdb.connect(GROUPS_DB, read_only=True, config={"threads": 1})

    # ---------------- main series ----------------
    for G in GS:
        path = os.path.join(SCRATCH, f"clst_g{G}_stats.clst")
        rec["series"][f"G{G}"] = {}
        print(f"=== G={G} selectivity series ===", flush=True)
        for name, c in case_cuts.items():
            lo, hi = I32MIN, c["cut"]
            outj = os.path.join(SCRATCH, f"ours_G{G}_{name}.json")
            q = our_bench(path, lo, hi, outj)
            ours_rows = json.load(open(outj))["rows"]
            # DuckDB 1T engine + full result
            sql = sql_for(G, lo, hi)
            drows, fetch_ms = duck_rows(con, sql)
            a, b = norm_ours(ours_rows), norm_duck(drows)
            # sorted multiset: no NULL group keys in these cases, so the order also
            # coincides, but do not rely on that
            exact = sorted(a) == sorted(b)
            d1 = base[1]["results"][f"G{G}/{name}"]
            d16 = base[16]["results"][f"G{G}/{name}"]
            # block-summary fallback (same lo/hi)
            fb = []
            for _ in range(2):
                warm_core()
                _, fstderr, _, _ = sh([TTOOL, "bsum_query", BSUM,
                                       os.path.join(TIERS, f"gid_g{G}.i32"), VAL,
                                       str(lo), str(hi),
                                       os.path.join(SCRATCH, f"bsum_G{G}_{name}.json"),
                                       "/dev/null", "3"])
                fb.append(last_json(fstderr))
            fb.sort(key=lambda j: j["query_median_ms"])
            fbm = fb[len(fb) // 2]
            case = {
                "lo": lo, "hi": hi, "target_selectivity": c["target"],
                "actual_selectivity": q["selectivity"],
                "scan_rows": q["scan_rows"], "rows": len(ours_rows),
                "fast_path": q["fast_path"],
                "ours_median_ms": q["query_median_ms"],
                "ours_p95_ms": q["query_p95_ms"],
                "ours_bench_medians_ms": q["bench_medians_ms"],
                "ours_spread": q["bench_spread"],
                "ours_aggregate_ms": q["aggregate_median_ms"],
                "ours_reset_ms": q["reset_median_ms"],
                "ours_reps": q["reps"],
                "ours_serialize_json_ms": q["serialize_json_ms"],
                "duck1_median_ms": d1["median_ms"],
                "duck1_min_ms": d1["min_ms"], "duck1_max_ms": d1["max_ms"],
                "duck1_reps": d1["reps"],
                "duck16_median_ms": d16["median_ms"],
                "duck16_reps": d16["reps"],
                "duck_complete_fetchall_ms": fetch_ms,
                "duck_engine_sql": sql_engine_for(G, lo, hi), "duck_sql": sql,
                "block_fallback_median_ms": fbm["query_median_ms"],
                "block_fallback_p95_ms": fbm["query_p95_ms"],
                "block_fallback_candidate_fraction": fbm.get("candidate_fraction"),
                "x_duck1": d1["median_ms"] / q["query_median_ms"],
                "x_duck16": d16["median_ms"] / q["query_median_ms"],
                "x_block_fallback": fbm["query_median_ms"] / q["query_median_ms"],
                "exact": exact,
            }
            if not exact:
                sa, sb = set(a), set(b)
                case["diff"] = {"only_ours": sorted(sa - sb)[:5],
                                "only_duck": sorted(sb - sa)[:5]}
            rec["series"][f"G{G}"][name] = case
            print(f"  [{G}/{name}] ours={q['query_median_ms']:9.4f}ms "
                  f"duck1={d1['median_ms']:9.2f}ms duck16={d16['median_ms']:8.2f}ms "
                  f"block={fbm['query_median_ms']:9.2f}ms "
                  f"x1T={case['x_duck1']:8.2f} exact={exact} rows={len(ours_rows)} "
                  f"fast={q['fast_path']}", flush=True)
        # the O(n) scan cost of the full-domain case, for transparency
        outj = os.path.join(SCRATCH, f"ours_G{G}_p100_scan.json")
        qs = our_bench(path, I32MIN, I32MAX, outj, n_benches=2, extra=["--no-fastpath"])
        rec["series"][f"G{G}"]["p100_scan_no_fastpath"] = {
            "ours_median_ms": qs["query_median_ms"], "reps": qs["reps"],
            "rows": N, "note": "same answer, O(n) scan instead of the stored per-group "
                               "totals; reported to separate the index property from the "
                               "fixed-query fast path"}
        print(f"  [{G}/p100 scan] {qs['query_median_ms']:.2f}ms", flush=True)

    # ---------------- boundary / empty / out-of-domain exactness -----------
    print("=== boundary / empty / out-of-domain exactness ===", flush=True)
    for G in GS:
        path = os.path.join(SCRATCH, f"clst_g{G}_stats.clst")
        rec["boundary_cases"][f"G{G}"] = {}
        for name, lo, hi in boundary:
            outj = os.path.join(SCRATCH, f"bnd_G{G}_{name}.json")
            q = ours_once(path, lo, hi, outj, reps=3)
            ours_rows = json.load(open(outj))["rows"]
            sql = sql_for(G, lo, hi)
            drows, fetch_ms = duck_rows(con, sql)
            a, b = norm_ours(ours_rows), norm_duck(drows)
            exact = sorted(a) == sorted(b)
            rec["boundary_cases"][f"G{G}"][name] = {
                "lo": lo, "hi": hi, "rows": len(ours_rows),
                "ours_median_ms": q["query_median_ms"], "fast_path": q["fast_path"],
                "duck_complete_fetchall_ms": fetch_ms, "duck_sql": sql,
                "exact": exact}
            if not exact:
                sa, sb = set(a), set(b)
                rec["boundary_cases"][f"G{G}"][name]["diff"] = {
                    "only_ours": sorted(sa - sb)[:5], "only_duck": sorted(sb - sa)[:5]}
            print(f"  [{G}/{name}] ours={q['query_median_ms']:.4f}ms rows={len(ours_rows)} "
                  f"exact={exact}", flush=True)

    # ---------------- summary + footprint ----------------
    tree = os.path.getsize(ENC_TREE)
    for G in GS:
        e = rec["build"][f"G{G}"]
        rec["footprint"][f"G{G}"] = {
            "tree_bytes": tree,
            "clst_range_bytes": e["range"]["file_bytes"],
            "clst_range_ratio": e["range"]["ratio_to_tree"],
            "clst_with_stats_bytes": e["stats"]["file_bytes"],
            "clst_with_stats_ratio": e["stats"]["ratio_to_tree"],
            "dense_cube_bytes": e["dense_cube_alt_bytes"],
            "dense_cube_ratio": e["dense_cube_alt_ratio"],
            "budget": "clustered representation <= 1.5x the encoded tree, with the "
                      "frozen ECUBE03 index dropped for this class (the router builds "
                      "only the chosen structure)",
            "within_budget": e["stats"]["ratio_to_tree"] <= 1.5,
        }
        # headline: does the class clear the bars?
        s = rec["series"][f"G{G}"]
        p1 = s.get("p1")
        p10 = s.get("p10")
        rec["footprint"][f"G{G}"]["clears_3x_at_1pct"] = bool(p1 and p1["x_duck1"] >= 3.0)
        rec["footprint"][f"G{G}"]["clears_2x_at_10pct"] = bool(p10 and p10["x_duck1"] >= 2.0)
        rec["footprint"][f"G{G}"]["x_duck1_at_1pct"] = p1 and p1["x_duck1"]
        rec["footprint"][f"G{G}"]["x_duck1_at_10pct"] = p10 and p10["x_duck1"]
        rec["footprint"][f"G{G}"]["x_duck16_at_10pct"] = p10 and p10["x_duck16"]
    con.close()

    json.dump(rec, open(os.path.join(RECEIPT, "clustered_series.json"), "w"), indent=1)
    json.dump(rec, open(os.path.join(SCRATCH, "clustered_series.json"), "w"), indent=1)
    print("wrote", os.path.join(RECEIPT, "clustered_series.json"), flush=True)
    print(json.dumps(rec["footprint"], indent=1))


if __name__ == "__main__":
    main()
