#!/usr/bin/env python3
"""Group-cardinality tiers on the NYC-100M value column (tiers stream).

G = 10 (production passenger_count cube, "existing"), 1, 1,000, 100,000, 1,000,000
with gid = splitmix64(rowid) % G (the C++ gen_groups binary writes the gid column,
so both the cube path and the DuckDB reference read the same bytes).

The dense ECUBE02 cube is O(G*V); at V = 21,084 it fits the space budget up to
G = 1,000 (88.5 MB = 0.35x the encoded tree) and is infeasible at G = 100K
(8.8 GB = 35x tree) and G = 1M (88 GB = 351x tree). For those two the router path
is measured instead: materialized per-group stats (32 B/group) for the fixed
query, exact block min/max summaries + pruned scan for value ranges, plus the
complete-output serialization latency (the 1M-row output is the answer, not an
index problem).

Run (from research/real_decoders_20261024):
    TIERS_SCRATCH=$ARTIFACT_ROOT/work/tiers/build python3 \
        src_tiers_20261101/run_groups.py
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import json
import os
import re
import statistics
import struct
import subprocess
import sys
import time

import duckdb

REPO = os.path.abspath(os.getcwd())
SCRATCH = _artifact.expand(os.environ.get("TIERS_SCRATCH", "../../work/tiers/build"))
BIN = _artifact.expand(os.environ.get("TIERS_BIN", "../../work/tiers/bin"))
PY = _artifact.expand("python3")
RECEIPT = os.path.join(_artifact.WORK, "receipts", "tiers_20261101")
CORE = _artifact.core()
CUBE = os.path.join(BIN, "enc_cube")
TTOOL = os.path.join(BIN, "tiers_tool")
GEN = os.path.join(BIN, "gen_groups")
NYC100 = os.path.join(REPO, "results/nyc_taxi_100m_duckdb/nyc.duckdb")
VAL = os.path.join(SCRATCH, "val_nyc100.i32")
GROUPS_PARQUET = os.path.join(SCRATCH, "nyc_groups.parquet")
GROUPS_DB = os.path.join(SCRATCH, "groups.duckdb")
N = 100_000_000


def sh(cmd, pin=True):
    pre = ["nice", "-n", "5", "taskset", "-c", CORE] if pin else []
    t = time.perf_counter()
    p = subprocess.run(["/usr/bin/time", "-v"] + pre + cmd, capture_output=True, text=True)
    wall = time.perf_counter() - t
    rss = 0.0
    for line in p.stderr.splitlines():
        if "Maximum resident set size" in line:
            rss = float(line.split(":")[1].strip())
    if p.returncode != 0:
        raise RuntimeError(f"{cmd[:4]} rc={p.returncode}\n{p.stderr[-1500:]}")
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


def warm_core(seconds=0.5):
    """Keep the pinned core at its boosted clock before a microsecond-scale bench.

    The ECUBE02 query is 1-10 us; a short bench after an idle period runs at a
    lower P-state and can read 2-3x slow (observed; see the measurement-dispersion
    check in the TIERS doc). A deterministic 0.5 s pinned busy loop puts the core
    at a stable clock first; the timed region itself is unchanged.
    """
    subprocess.run(["nice", "-n", "5", "taskset", "-c", CORE, "timeout", f"{seconds}s",
                    "bash", "-c", "while :; do :; done"], capture_output=True)


def stable_bench(cube, dictp, reps, spec, n=3):
    runs = []
    for _ in range(n):
        warm_core()
        _, stderr, _, _ = sh([CUBE, "bench", cube, dictp, "/dev/null", str(reps)] + spec)
        runs.append(last_json(stderr))
    runs.sort(key=lambda j: j["query_median_ms"])
    return runs[len(runs) // 2], runs


def write_dict(path, names):
    d = bytearray()
    d += b"BSSTR01\n"
    d += b"0" * 40
    d += struct.pack("<I", len(names))
    for n in names:
        b = n.encode()
        d += struct.pack("<I", len(b)) + b
    open(path, "wb").write(bytes(d))


def ensure_gid(G):
    p = os.path.join(SCRATCH, f"gid_g{G}.i32")
    if os.path.exists(p) and os.path.getsize(p) == 4 * N:
        return p
    wall, stderr, stdout, _ = sh([GEN, str(N), str(G), p])
    print(f"  gen_groups G={G}: {wall:.2f}s", flush=True)
    return p


def ensure_gid_one():
    p = os.path.join(SCRATCH, "gid_g1.i32")
    if os.path.exists(p) and os.path.getsize(p) == 4 * N:
        return p
    with open(p, "wb") as f:
        z = b"\x00" * (1 << 24)
        left = 4 * N
        while left:
            take = min(left, len(z))
            f.write(z[:take])
            left -= take
    return p


def cases(cut10, cut50):
    return {"fixed": None, "range_p10": cut10, "range_p50": cut50}


def duck_case_cube(con, sql):
    con.execute(sql).fetchall()
    ts, rows = [], None
    for _ in range(30):
        t = time.perf_counter()
        rows = con.execute(sql).fetchall()
        ts.append((time.perf_counter() - t) * 1000.0)
    ts.sort()
    return {"median_ms": statistics.median(ts),
            "p95_ms": ts[min(len(ts) - 1, int(len(ts) * 0.95))],
            "rows": rows}


def norm_cube(rows):
    out = []
    for r in rows:
        g = str(r[0])
        out.append((g,) + tuple(str(x) for x in r[1:]))
    return out


def norm_duck(rows):
    return [("NULL" if r[0] is None else str(r[0]),) + tuple(str(x) for x in r[1:])
            for r in rows]


def cube_tier(name, cube, dictp, spec_map, duck_sql_map, outrec, n_duck=30):
    for case, spec in spec_map.items():
        s = list(spec) if spec else []
        cj, cj_runs = stable_bench(cube, dictp, 2000, s)
        qout = os.path.join(SCRATCH, f"{name}_{case}.json")
        sh([CUBE, "query", cube, dictp, qout] + s)
        rows = json.load(open(qout))["rows"]
        sql = duck_sql_map[case]
        con = duckdb.connect(NYC100, read_only=True)
        con.execute("SET threads=1")
        d = duck_case_cube(con, sql)
        con.close()
        a, b = norm_cube(rows), norm_duck(d["rows"])
        exact = a == b
        outrec[name]["cases"][case] = {
            "spec": s,
            "cube_median_us": cj["query_median_ms"] * 1000,
            "cube_p95_us": cj["query_p95_ms"] * 1000,
            "cube_serialize_us": cj["serialize_median_ms"] * 1000,
            "cube_bench_medians_us": [j["query_median_ms"] * 1000 for j in cj_runs],
            "cube_bench_spread": (max(j["query_median_ms"] for j in cj_runs) /
                                  min(j["query_median_ms"] for j in cj_runs)),
            "duck_median_ms": d["median_ms"],
            "duck_p95_ms": d["p95_ms"],
            "duckdb_sql": sql,
            "rows": len(rows),
            "selectivity": sum(int(r[2]) for r in rows) / float(N),
            "exact": exact,
            "ratio_median": d["median_ms"] / cj["query_median_ms"],
        }
        if not exact:
            sa, sb = set(a), set(b)
            outrec[name]["cases"][case]["diff"] = {
                "only_cube": sorted(sa - sb)[:5], "only_duck": sorted(sb - sa)[:5]}
        print(f"  [{name}/{case}] cube={cj['query_median_ms']*1000:8.2f}us "
              f"duck={d['median_ms']:7.2f}ms rows={len(rows)} exact={exact}", flush=True)


def ensure_groups_db():
    """One persistent DuckDB table with v + the three synthetic gid columns."""
    if os.path.exists(GROUPS_DB):
        return
    t = time.perf_counter()
    con = duckdb.connect(GROUPS_DB)
    con.execute("SET threads=1")
    con.execute(f"CREATE TABLE groups AS SELECT v, g1000, g100000, g1000000 "
                f"FROM read_parquet('{GROUPS_PARQUET}')")
    con.close()
    print(f"  built {GROUPS_DB} in {time.perf_counter()-t:.1f}s", flush=True)


def main():
    os.makedirs(RECEIPT, exist_ok=True)
    cuts = json.load(open(os.path.join(SCRATCH, "cuts.json")))["datasets"]
    cut10 = cuts["nyc100"]["p10"]
    cut50 = cuts["nyc100"]["p50"]
    os.makedirs(SCRATCH, exist_ok=True)
    rec = {"protocol": {
        "host": "localhost (AMD EPYC 9554)", "pin": f"nice -n 5 taskset -c {CORE}",
        "value_column": "NYC yellow taxi 2023-01..2025-09 fare_amount cents, 100,000,000 rows, V=21084",
        "gid": "splitmix64(rowid) % G (C++ gen_groups), same bytes on both sides",
        "cube_protocol": "open + warmup + 2000 reps, median/P95",
        "duckdb": "2.0.0 threads=1 warm, 1 warmup + 30 execute()+fetchall()",
        "duckdb_version": duckdb.__version__,
    }, "tiers": {}}
    print("=== prep gid columns ===", flush=True)
    g1 = ensure_gid_one()
    g1000 = ensure_gid(1000)
    g100k = ensure_gid(100000)
    g1m = ensure_gid(1000000)
    dict_g1 = os.path.join(SCRATCH, "dict_g1.txt")
    dict_g1000 = os.path.join(SCRATCH, "dict_g1000.txt")
    write_dict(dict_g1, ["0"])
    write_dict(dict_g1000, [str(i) for i in range(1000)])

    # ---------------- G = 10: production cube (existing) -------------------
    print("=== G=10 (production passenger_count cube) ===", flush=True)
    rec["tiers"]["G10"] = {"name": "G10", "rows": N, "G": 11, "V": 21084,
                           "path": "ECUBE02 dense cube (from encoded tree)",
                           "cases": {}}
    name = "G10"
    cube = os.path.join(SCRATCH, "nyc100_G10.ecube")
    prep = os.path.join(REPO, "results/nyc_taxi_100m_prep/Observed")
    enc = os.path.join(REPO, "results/nyc_taxi_100m_prep/Observed.encoded")
    wall, stderr, _, rss = sh([CUBE, "build_enc", enc, prep, "17", "13", cube])
    b = last_json(stderr)
    rec["tiers"][name].update({
        "build_wall_s": wall, "build_ms": b["build_ms"], "index_bytes": os.path.getsize(cube),
        "enc_tree_bytes": os.path.getsize(enc), "peak_rss_kb": rss})
    spec_map = {"fixed": None, "range_p10": [f"hi={cut10}"], "range_p50": [f"hi={cut50}"]}
    base = ("SELECT passenger_count AS g, SUM(fare_cents), COUNT(*), MIN(fare_cents), "
            "MAX(fare_cents) FROM nyc")
    sql_map = {"fixed": base + " GROUP BY 1 ORDER BY 1",
               "range_p10": base + f" WHERE fare_cents <= {cut10} GROUP BY 1 ORDER BY 1",
               "range_p50": base + f" WHERE fare_cents <= {cut50} GROUP BY 1 ORDER BY 1"}
    cube_tier(name, cube, os.path.join(prep, "state.dict"), spec_map, sql_map, rec["tiers"])

    # ---------------- G = 1 ------------------------------------------------
    print("=== G=1 ===", flush=True)
    name = "G1"
    rec["tiers"][name] = {"name": name, "rows": N, "G": 1, "V": 21084,
                          "path": "ECUBE02 dense cube (built from plain gid+value)",
                          "cases": {}}
    cube = os.path.join(SCRATCH, "nyc100_G1.ecube")
    wall, stderr, _, rss = sh([CUBE, "build", g1, VAL, cube])
    b = last_json(stderr)
    rec["tiers"][name].update({"build_wall_s": wall, "build_ms": b["build_ms"],
                               "index_bytes": os.path.getsize(cube), "peak_rss_kb": rss})
    spec_map = {"fixed": None, "range_p10": [f"hi={cut10}"], "range_p50": [f"hi={cut50}"]}
    base = "SELECT 0 AS g, SUM(fare_cents), COUNT(*), MIN(fare_cents), MAX(fare_cents) FROM nyc"
    sql_map = {"fixed": base,
               "range_p10": base + f" WHERE fare_cents <= {cut10}",
               "range_p50": base + f" WHERE fare_cents <= {cut50}"}
    cube_tier(name, cube, dict_g1, spec_map, sql_map, rec["tiers"])

    # ---------------- G = 1,000: dense cube (fits) ------------------------
    print("=== G=1000 (dense cube, fits the budget) ===", flush=True)
    name = "G1000"
    rec["tiers"][name] = {"name": name, "rows": N, "G": 1000, "V": 21084,
                          "path": "ECUBE02 dense cube (built from plain gid+value)",
                          "cases": {}}
    cube = os.path.join(SCRATCH, "nyc100_G1000.ecube")
    wall, stderr, _, rss = sh([CUBE, "build", g1000, VAL, cube])
    b = last_json(stderr)
    rec["tiers"][name].update({"build_wall_s": wall, "build_ms": b["build_ms"],
                               "index_bytes": os.path.getsize(cube),
                               "enc_tree_bytes": os.path.getsize(
                                   os.path.join(REPO, "results/nyc_taxi_100m_prep/Observed.encoded")),
                               "peak_rss_kb": rss})
    ensure_groups_db()
    con = duckdb.connect(GROUPS_DB, read_only=True)
    con.execute("SET threads=1")
    spec_map = {"fixed": None, "range_p10": [f"hi={cut10}"], "range_p50": [f"hi={cut50}"]}
    for case, spec in spec_map.items():
        s = list(spec) if spec else []
        cj, cj_runs = stable_bench(cube, dict_g1000, 2000, s)
        qout = os.path.join(SCRATCH, f"{name}_{case}.json")
        sh([CUBE, "query", cube, dict_g1000, qout] + s)
        rows = json.load(open(qout))["rows"]
        sql = ("SELECT g1000 AS g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM groups"
               + ("" if spec is None else f" WHERE v <= {spec[0].split('=')[1]}")
               + " GROUP BY 1 ORDER BY 1")
        con.execute(sql).fetchall()
        ts = []
        drows = None
        for _ in range(10):
            t = time.perf_counter()
            drows = con.execute(sql).fetchall()
            ts.append((time.perf_counter() - t) * 1000.0)
        ts.sort()
        a, b2 = norm_cube(rows), norm_duck(drows)
        rec["tiers"][name]["cases"][case] = {
            "spec": s, "cube_median_us": cj["query_median_ms"] * 1000,
            "cube_p95_us": cj["query_p95_ms"] * 1000,
            "cube_serialize_us": cj["serialize_median_ms"] * 1000,
            "cube_bench_medians_us": [j["query_median_ms"] * 1000 for j in cj_runs],
            "duck_median_ms": statistics.median(ts),
            "duck_p95_ms": ts[min(len(ts) - 1, int(len(ts) * 0.95))],
            "duckdb_sql": sql, "rows": len(rows), "exact": a == b2,
            "ratio_median": statistics.median(ts) / cj["query_median_ms"]}
        print(f"  [{name}/{case}] cube={cj['query_median_ms']*1000:8.2f}us "
              f"duck={statistics.median(ts):7.2f}ms rows={len(rows)} exact={a==b2}", flush=True)
    con.close()

    # ---------------- G = 100,000 / 1,000,000: router path -----------------
    for G, gid in ((100_000, g100k), (1_000_000, g1m)):
        name = f"G{G}"
        print(f"=== {name} (router path: materialized stats + block summaries) ===", flush=True)
        rec["tiers"][name] = {"name": name, "rows": N, "G": G, "V": 21084,
                              "path": "materialized 32 B/group stats (fixed) + "
                                      "block min/max summaries + pruned scan (ranges)",
                              "cases": {}}
        stats = os.path.join(SCRATCH, f"stats_{G}.bin")
        wall, stderr, _, rss = sh([TTOOL, "stats_build", gid, VAL, stats])
        b = last_json(stderr)
        rec["tiers"][name]["stats"] = {"build_wall_s": wall, "build_ms": b["build_ms"],
                                       "bytes": os.path.getsize(stats),
                                       "phase_ms": {k: b.get(k) for k in
                                                    ("domain_ms", "accum_ms", "write_ms")},
                                       "peak_rss_kb": rss}
        # fixed query + full-output serialization (JSON and binary are timed apart)
        reps = 20 if G <= 100_000 else 10
        outj = os.path.join(SCRATCH, f"{name}_fixed.json")
        outb = os.path.join(SCRATCH, f"{name}_fixed.bin")
        wall2, stderr2, _, _ = sh([TTOOL, "stats_emit", stats, outj, outb, str(reps)])
        e = last_json(stderr2)
        rec["tiers"][name]["stats"]["emit"] = e
        rec["tiers"][name]["stats"]["emit"]["out_json_mb"] = os.path.getsize(outj) / 1e6
        rec["tiers"][name]["stats"]["emit"]["out_bin_mb"] = os.path.getsize(outb) / 1e6
        # range fallback: block summaries + pruned exact scan
        bsum = os.path.join(SCRATCH, "bsum_nyc100.bin")
        if not os.path.exists(bsum):
            wallb, stderrb, _, _ = sh([TTOOL, "bsum_build", gid, VAL, bsum])
            rec.setdefault("bsum_build", {})[str(G)] = last_json(stderrb)
            rec["bsum_build"][str(G)]["wall_s"] = wallb
            rec["bsum_build"][str(G)]["bytes"] = os.path.getsize(bsum)
        rj = os.path.join(SCRATCH, f"{name}_range.json")
        rb = os.path.join(SCRATCH, f"{name}_range.bin")
        repq = 3 if G > 100_000 else 5
        wall3, stderr3, _, _ = sh([TTOOL, "bsum_query", bsum, gid, VAL,
                                   "-2147483648", str(cut10), rj, rb, str(repq)])
        rj2 = last_json(stderr3)
        rec["tiers"][name]["range_fallback"] = rj2
        rec["tiers"][name]["range_fallback"]["candidate_bytes_rows"] = None
        # DuckDB reference: engine (full aggregation forced) + complete fetchall
        ensure_groups_db()
        con = duckdb.connect(GROUPS_DB, read_only=True)
        con.execute("SET threads=1")
        gcol = f"g{G}"
        base = (f"SELECT {gcol} AS g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM groups")
        fix_sql = base + " GROUP BY 1 ORDER BY 1"
        rng_sql = base + f" WHERE v <= {cut10} GROUP BY 1 ORDER BY 1"
        eng_sql = f"SELECT COUNT(*) FROM (SELECT {gcol}, SUM(v) FROM groups GROUP BY 1)"
        eng_rng = (f"SELECT COUNT(*) FROM (SELECT {gcol}, SUM(v) FROM groups "
                   f"WHERE v <= {cut10} GROUP BY 1)")
        nrep = 3 if G > 100_000 else 5
        for label, sql, engsql in (("fixed", fix_sql, eng_sql), ("range_p10", rng_sql, eng_rng)):
            con.execute(engsql).fetchall()
            te, tc = [], []
            rows = eng = None
            for _ in range(nrep):
                t = time.perf_counter()
                eng = con.execute(engsql).fetchall()
                te.append((time.perf_counter() - t) * 1000.0)
                t = time.perf_counter()
                rows = con.execute(sql).fetchall()
                tc.append((time.perf_counter() - t) * 1000.0)
            te.sort(); tc.sort()
            d = {"engine_median_ms": statistics.median(te),
                 "complete_median_ms": statistics.median(tc),
                 "engine_sql": engsql, "sql": sql,
                 "duckdb_rows": len(rows),
                 "selectivity": sum(int(r[2]) for r in rows) / float(N)}
            if label == "fixed":
                a, b2 = None, None
                our_rows = json.load(open(outj))["rows"]
                a, b2 = norm_cube(our_rows), norm_duck(rows)
                d["exact"] = a == b2
                d["our_rows"] = len(our_rows)
                if not d["exact"]:
                    sa, sb = set(a), set(b2)
                    d["diff"] = {"only_cube": sorted(sa - sb)[:5],
                                 "only_duck": sorted(sb - sa)[:5]}
            else:
                our_rows = json.load(open(rj))["rows"]
                a, b2 = norm_cube(our_rows), norm_duck(rows)
                d["exact"] = a == b2
                d["our_rows"] = len(our_rows)
                if not d["exact"]:
                    sa, sb = set(a), set(b2)
                    d["diff"] = {"only_cube": sorted(sa - sb)[:5],
                                 "only_duck": sorted(sb - sa)[:5]}
            rec["tiers"][name].setdefault("duckdb", {})[label] = d
            print(f"  [{name}/{label}] duck engine={d['engine_median_ms']:8.2f}ms "
                  f"complete={d['complete_median_ms']:8.2f}ms rows={len(rows)} "
                  f"exact={d['exact']}", flush=True)
        con.close()

    # ---------------- output/serialization: 1M-row complete result --------
    print("=== output: 1M-row complete result, ours vs DuckDB ===", flush=True)
    ensure_groups_db()
    outrec = {"rows": 1_000_000, "ours": {}, "duckdb": {},
              "note": "JSON and binary measured in separate processes; formats never mixed"}
    stats = os.path.join(SCRATCH, "stats_1000000.bin")
    e1m = rec["tiers"]["G1000000"]["stats"]["emit"]
    outrec["ours"]["json"] = {
        "serialize_median_ms": e1m["json_median_ms"], "serialize_p95_ms": e1m["json_p95_ms"],
        "write_ms": e1m["json_write_ms"], "complete_ms": e1m["complete_json_ms"],
        "out_bytes": e1m["json_bytes"], "peak_rss_kb": e1m["peak_rss_kb"], "rows": e1m["groups_nonempty"]}
    outrec["ours"]["binary"] = {
        "serialize_median_ms": e1m["bin_median_ms"], "serialize_p95_ms": e1m["bin_p95_ms"],
        "write_ms": e1m["bin_write_ms"], "complete_ms": e1m["complete_bin_ms"],
        "out_bytes": e1m["bin_bytes"], "peak_rss_kb": e1m["peak_rss_kb"], "rows": e1m["groups_nonempty"]}
    sql1m = ("SELECT g1000000 AS g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM groups "
             "GROUP BY 1 ORDER BY 1")
    for label, mode, ext in (("json", "copy_json", "json"),
                             ("parquet", "copy_parquet", "parquet"),
                             ("fetch", "fetch", "fetch.json")):
        op = os.path.join(SCRATCH, f"duck_1m_{label}.{ext}")
        wall, stderr, stdout, rss = sh([PY, os.path.join(REPO, "src_tiers_20261101/duck_output.py"),
                                        GROUPS_DB, sql1m, op, mode])
        j = last_json(stdout)
        j["wall_s"] = wall
        j["peak_rss_kb"] = rss
        j["out_bytes"] = os.path.getsize(op) if os.path.exists(op) else None
        outrec["duckdb"][label] = j
        print(f"  duckdb {label}: wall={wall:.2f}s rss={rss/1024:.0f}MB "
              f"bytes={j['out_bytes']}", flush=True)
    # exactness of the 1M output vs DuckDB fixed result (element-wise, gid order)
    con = duckdb.connect(GROUPS_DB, read_only=True)
    con.execute("SET threads=1")
    drows = con.execute(sql1m).fetchall()
    con.close()
    our_rows = json.load(open(os.path.join(SCRATCH, "G1000000_fixed.json")))["rows"]
    a, b = norm_cube(our_rows), norm_duck(drows)
    outrec["exact"] = a == b
    outrec["rows_compared"] = len(a)
    rec["output"] = outrec
    print(f"  exact(1M rows)={outrec['exact']} rows={len(a)}", flush=True)

    json.dump(rec, open(os.path.join(RECEIPT, "group_tiers.json"), "w"), indent=1)
    json.dump(rec, open(os.path.join(SCRATCH, "group_tiers.json"), "w"), indent=1)
    print("wrote", os.path.join(RECEIPT, "group_tiers.json"), flush=True)


if __name__ == "__main__":
    main()
