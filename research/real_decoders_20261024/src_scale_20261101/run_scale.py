#!/usr/bin/env python3
"""Scale + multi-core campaign driver (scale stream, 2026-11-01).

Runs every timing measurement used by SCALE_20261101.md on the remote host and
writes one JSON receipt per stage into
  research/real_decoders_20261025/receipts/scale_20261101/

Protocol (every timed run):
  * pinned: . 1T = nice -n 5 taskset -c 80
               4T = taskset -c 80-83, 8T = 80-87, 16T = 80-95
  * /usr/bin/time -f 'wall=%e rss=%M' recorded (wall, peak RSS)
  * warmup outside the timed loop; >= 10 timed repetitions; median + P95 reported
  * page cache: inputs are pre-read (cold-read cost reported separately) so the
    build numbers measure CPU work + copy from page cache, not raw disk
  * the exact command line, core mask, thread count, host and timestamp are
    stored in every receipt

Stages:
  build1b  1B cube builds T=1,4,8,16 + determinism sha256
  q1b      1B queries T=1,4,8,16 for fixed / 0.135% / 1.04% / 11.9% / 100%
  v2q1b    cross-check T=1 against the validated v2 binary (same format)
  exact1b  analytic replica-map exactness vs the DuckDB-verified 100M cube
  duck1b   DuckDB 2.0 parquet control, threads = 1/4/8/16
  build100 100M builds: v2 plain, v2 encoded, build_mt T=1,4,8,16
  q100m    100M queries T=1,4,8,16 + v2 binary cross-check
  duck100m DuckDB 2.0 nyc.duckdb control, threads = 1/4/8/16
  build10m materialized 10M-group stats T=1,4,8,16 + block summaries
  q10m     10M-group fixed query + output serialization T=1,4,8,16; bsum fallback
  duck10m  DuckDB 2.0 10M-group control, threads = 1/4/8/16
  exact10m exactness of 10M-group paths vs DuckDB (aggregate + 1000-group sample)
  fit      G x V feasibility table
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import datetime
import hashlib
import json
import math
import os
import re
import statistics
import subprocess
import sys
import time

HOME = _artifact.expand("~")
ROOT = _artifact.ROOT + "/research/real_decoders_20261024"
OUT = ROOT + "/results/scale_20261101"
RECDIR = _artifact.WORK + "/receipts/scale_20261101"
BIN = ROOT + "/src_scale_20261101/cube_scale"
GEN = ROOT + "/src_scale_20261101/gen_scale"
V2 = ROOT + "/src_eai_20261101/enc_cube"
DICT100 = ROOT + "/results/nyc_taxi_100m_prep/Observed/state.dict"
DICT1B = OUT + "/dict_1b.bin"
TMP = _artifact.WORK + "/scale"
GID100 = ROOT + "/results/nyc100_plain/passenger.i32"
VAL100 = ROOT + "/results/nyc100_plain/fare.i32"
GID1B = OUT + "/nyc1b_gid.i32"
VAL1B = OUT + "/nyc1b_val.i32"
GID10M = OUT + "/g10m_gid.i32"
VAL10M = OUT + "/g10m_val.i32"
CUBE1B = OUT + "/nyc1b.ecube"
CUBE100 = OUT + "/nyc100.ecube"
STATS10M = OUT + "/g10m.mstats"
BSUM10M = OUT + "/g10m.bsum"
PARQ1B = OUT + "/nyc1b.parquet"
PARQ10M = OUT + "/g10m.parquet"

MASK = {t: _artifact.cores(t) for t in (1,4,8,16)}
REPS = 30          # cube query repetitions (P95 needs >= 10)
REPS_STATS = 15    # 10M-group materialized query repetitions
CASES = [
    ("fixed", []),
    ("le_m7000", ["hi=-7000"]),   # measured 0.135%
    ("le_m720", ["hi=-720"]),     # measured 1.04%
    ("le_650", ["hi=650"]),       # measured 11.9%
    ("le_99900", ["hi=99900"]),   # measured 99.9999%
]


def now():
    return datetime.datetime.now().astimezone().isoformat()


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 24), b""):
            h.update(chunk)
    return h.hexdigest()


def warm(*paths):
    for p in paths:
        with open(p, "rb") as f:
            while f.read(1 << 26):
                pass


def parse_timefile(path):
    d = {}
    try:
        for line in open(path):
            for kv in line.split():
                if "=" in kv:
                    k, v = kv.split("=", 1)
                    d[k] = float(v) if v.replace(".", "", 1).isdigit() else v
    except FileNotFoundError:
        pass
    return d


def invoke(cmd, T, warm_paths=(), timeout=7200, timefile=None):
    """Run cmd under nice/taskset/time; return receipt fragment with its tool JSON."""
    tf = timefile or os.path.join(TMP, "time_%d.txt" % os.getpid())
    full = (["/usr/bin/time", "-f", "wall=%e rss=%M", "-o", tf,
             "nice", "-n", "5", "taskset", "-c", MASK[T]] + [str(c) for c in cmd])
    t0 = time.time()
    p = subprocess.run(full, capture_output=True, text=True, timeout=timeout)
    wall = time.time() - t0
    tm = parse_timefile(tf)
    metrics = None
    for line in reversed(p.stderr.strip().splitlines()):
        line = line.strip()
        if line.startswith("{"):
            try:
                metrics = json.loads(line)
                break
            except json.JSONDecodeError:
                continue
    frag = {
        "cmd": " ".join(full),
        "exit": p.returncode,
        "core_mask": MASK[T],
        "threads": T,
        "wall_s": round(wall, 4),
        "time": tm,
        "metrics": metrics,
    }
    if p.returncode != 0:
        frag["stderr_tail"] = p.stderr[-2000:]
    return frag


def write_receipt(name, obj):
    os.makedirs(RECDIR, exist_ok=True)
    obj.setdefault("receipt", name)
    obj.setdefault("host", "localhost (AMD EPYC 9554, 2x64C, 256T, 503GB)")
    obj.setdefault("date", now())
    obj.setdefault("git_branch", "research/groups-e2e-repro-20261025")
    path = os.path.join(RECDIR, name + ".json")
    with open(path, "w") as f:
        json.dump(obj, f, indent=1)
    print("receipt ->", path, flush=True)
    return path


def make_1b_dict():
    """Synthesize a 100-name dictionary (groups 0..99 + NULL) for the 1B replica
    dataset; the 100M dictionary only names 10 groups.  Layout is BSSTR01\n +
    40 bytes of header (copied from the real dictionary; unused by the readers) +
    u32 count + per-name u32 length/bytes."""
    if os.path.exists(DICT1B):
        return
    raw = open(DICT100, "rb").read()
    hdr = raw[8:48]
    names = [str(i).encode() for i in range(100)]
    with open(DICT1B, "wb") as f:
        f.write(b"BSSTR01\n")
        f.write(hdr)
        f.write(len(names).to_bytes(4, "little"))
        for n in names:
            f.write(len(n).to_bytes(4, "little"))
            f.write(n)
    print("dict ->", DICT1B)


# ---------------------------------------------------------------------------
# 1B stage
# ---------------------------------------------------------------------------
def stage_build1b():
    make_1b_dict()
    runs = []
    for T in (1, 4, 8, 16):
        warm(GID1B, VAL1B)
        cube = CUBE1B if T == 1 else "%s/nyc1b_t%d.ecube" % (OUT, T)
        r = invoke([BIN, "build_mt", GID1B, VAL1B, cube, T, "24"], T, timeout=3600)
        r["cube"] = cube
        r["cube_bytes"] = os.path.getsize(cube)
        r["cube_sha256"] = sha256(cube)
        runs.append(r)
        print("1B build T=%d wall=%.2fs rss=%s sha=%s" %
              (T, r["wall_s"], r["time"].get("rss"), r["cube_sha256"][:16]), flush=True)
    shas = {r["cube_sha256"] for r in runs}
    deterministic = len(shas) == 1
    r1 = runs[0]["metrics"]
    # single-thread plain-file read cost (cold vs warm page cache) for the receipt
    warm(GID1B, VAL1B)
    t0 = time.time()
    with open(GID1B, "rb") as f:
        while f.read(1 << 26):
            pass
    with open(VAL1B, "rb") as f:
        while f.read(1 << 26):
            pass
    read_warm_s = time.time() - t0
    write_receipt("build_mt_1b", {
        "dataset": {"rows": 1000000000, "replicas": 10, "gid_offset": 10,
                    "groups": r1["groups"] if r1 else None, "values": r1["values"] if r1 else None,
                    "gid_file": GID1B, "val_file": VAL1B,
                    "gid_bytes": os.path.getsize(GID1B), "val_bytes": os.path.getsize(VAL1B),
                    "note": "10 deterministic replicas of NYC-100M (gid + 10*r, NULL preserved), "
                            "identical value distribution; build/query/storage scaling, not "
                            "distributional generalization"},
        "builds": runs,
        "deterministic_byte_identical": deterministic,
        "cube_bytes": runs[0]["cube_bytes"],
        "warm_input_read_s": round(read_warm_s, 3),
        "protocol": "inputs pre-read into page cache before each timed build; "
                    "wall includes mmap + domain + distinct + accumulate + prefix + write",
    })
    return deterministic


def stage_q1b():
    make_1b_dict()
    res = {}
    for T in (1, 4, 8, 16):
        res[str(T)] = {}
        for name, spec in CASES:
            out = os.path.join(TMP, "q1b_t%d_%s.json" % (T, name))
            warm(CUBE1B)
            r = invoke([BIN, "qbench", CUBE1B, DICT1B, out, str(REPS), T] + spec, T)
            r["rows"] = json.load(open(out))["rows"]
            res[str(T)][name] = r
            m = r["metrics"] or {}
            print("1B q T=%02d %-10s median=%.3f us p95=%.3f us rows=%s" %
                  (T, name, m.get("query_median_ms", 0) * 1000, m.get("query_p95_ms", 0) * 1000,
                   m.get("rows")), flush=True)
    write_receipt("qbench_1b", {"cube": CUBE1B, "cube_sha256": sha256(CUBE1B),
                                "dict": DICT1B, "reps": REPS, "results": res})


def stage_v2q1b():
    make_1b_dict()
    res = {}
    for name, spec in CASES:
        out = os.path.join(TMP, "v2q1b_%s.json" % name)
        r = invoke([V2, "bench", CUBE1B, DICT1B, out, str(REPS)] + spec, 1)
        r["rows"] = json.load(open(out))["rows"]
        res[name] = r
    write_receipt("v2_binary_crosscheck_1b", {"binary": V2, "cube": CUBE1B, "results": res})


def stage_exact1b():
    """Analytic exactness: replica r, group p+10r must hold exactly the 100M cube row
    for group p; the NULL group must be 10x the 100M NULL row (same min/max)."""
    q1 = json.load(open(os.path.join(RECDIR, "qbench_1b.json")))
    checks = {}
    ok_all = True
    for name, spec in CASES:
        base = {row[0]: [str(x) for x in row[1:]] for row in
                json.load(open(os.path.join(TMP, "qbench_100m_%s.json" % name)))["rows"]}
        got = {row[0]: [str(x) for x in row[1:]] for row in q1["results"]["1"][name]["rows"]}
        ok = True
        reasons = []
        for gname, vals in base.items():
            if gname == "NULL":
                expect = [str(int(vals[0]) * 10), str(int(vals[1]) * 10), vals[2], vals[3]]
                if got.get("NULL") != expect:
                    ok = False
                    reasons.append("NULL group mismatch got=%s want=%s" %
                                   (got.get("NULL"), expect))
            else:
                for r in range(10):
                    cand = str(int(gname) + 10 * r)
                    if got.get(cand) != vals:
                        ok = False
                        reasons.append("group %s (replica %d) mismatch" % (cand, r))
        checks[name] = {"ok": ok, "rows_got": len(got), "rows_expected": (len(base) - 1) * 10 + 1,
                        "reasons": reasons[:5]}
        ok_all = ok_all and ok
    write_receipt("exactness_1b_analytic", {"checks": checks, "all_exact": ok_all,
                                            "note": "1B replica map vs the 100M cube verified "
                                                    "against DuckDB 2.0"})


def duck_bench(con, sql, reps, store_rows=True):
    con.execute(sql).fetchall()  # warmup
    ts = []
    rows = None
    for _ in range(reps):
        t = time.perf_counter()
        rows = con.execute(sql).fetchall()
        ts.append((time.perf_counter() - t) * 1000.0)
    ts.sort()
    out = {"median_ms": statistics.median(ts), "min_ms": ts[0],
           "p95_ms": ts[min(len(ts) - 1, int(len(ts) * 0.95))], "reps": reps,
           "result_rows": len(rows) if rows is not None else None}
    if store_rows:
        out["rows"] = [[("NULL" if x is None else str(x)) for x in r] for r in rows]
    return out


def stage_duck1b():
    import duckdb
    res = {}
    for T in (1, 4, 8, 16):
        con = duckdb.connect()
        con.execute("SET threads=%d" % T)
        con.execute("SET preserve_insertion_order=false")
        con.execute("SET memory_limit='120GB'")
        out = {}
        for name, spec in CASES:
            where = "" if not spec else "WHERE " + " AND ".join(
                "v <= %s" % s.split("=")[1] for s in spec)
            sql = ("SELECT g, SUM(v)::HUGEINT AS s, COUNT(*)::HUGEINT AS c, "
                   "MIN(v)::BIGINT AS mn, MAX(v)::BIGINT AS mx FROM read_parquet('%s') %s "
                   "GROUP BY 1 ORDER BY 1" % (PARQ1B, where))
            reps = 10 if name in ("fixed", "le_m720", "le_650") else 5
            out[name] = duck_bench(con, sql, reps)
            out[name]["sql"] = sql
            print("duck1b T=%02d %-10s median=%.1f ms" %
                  (T, name, out[name]["median_ms"]), flush=True)
        con.close()
        res[str(T)] = out
    write_receipt("duckdb_1b", {"parquet": PARQ1B, "parquet_bytes": os.path.getsize(PARQ1B),
                                "duckdb_version": duckdb.__version__,
                                "results": res,
                                "protocol": "DuckDB 2.0 reads the same int32 columns as parquet "
                                            "(zstd); execute+fetchall latency; 1 warmup + N reps"})


# ---------------------------------------------------------------------------
# 100M stage
# ---------------------------------------------------------------------------
def stage_build100():
    make_1b_dict()
    runs = []
    warm(GID100, VAL100)
    r = invoke([V2, "build", GID100, VAL100, CUBE100], 1)
    r["cube"] = CUBE100
    r["cube_sha256"] = sha256(CUBE100)
    runs.append(dict(r, variant="v2_plain_validated_path"))
    warm(GID100, VAL100)
    r = invoke([V2, "build_enc", ROOT + "/results/nyc_taxi_100m_prep/Observed.encoded",
                ROOT + "/results/nyc_taxi_100m_prep/Observed", "17", "13",
                os.path.join(TMP, "nyc100_enc.ecube")], 1)
    r["cube"] = os.path.join(TMP, "nyc100_enc.ecube")
    r["cube_sha256"] = sha256(r["cube"])
    runs.append(dict(r, variant="v2_encoded_tree"))
    for T in (1, 4, 8, 16):
        warm(GID100, VAL100)
        cube = os.path.join(TMP, "nyc100_mt%d.ecube" % T)
        r = invoke([BIN, "build_mt", GID100, VAL100, cube, T, "24"], T)
        r["cube"] = cube
        r["cube_sha256"] = sha256(cube)
        runs.append(dict(r, variant="build_mt_T%d" % T))
        print("100M build T=%d wall=%.3fs" % (T, r["wall_s"]), flush=True)
    shas = {x["cube_sha256"] for x in runs}
    write_receipt("build_mt_100m", {"runs": runs, "all_byte_identical": len(shas) == 1,
                                    "cube_sha256": sorted(shas)[0],
                                    "cube_bytes": os.path.getsize(CUBE100)})
    return len(shas) == 1


def stage_q100m():
    res = {}
    for T in (1, 4, 8, 16):
        res[str(T)] = {}
        for name, spec in CASES:
            out = os.path.join(TMP, "qbench_100m_%s.json" % name) if T == 1 else \
                os.path.join(TMP, "q100m_t%d_%s.json" % (T, name))
            warm(CUBE100)
            r = invoke([BIN, "qbench", CUBE100, DICT100, out, str(REPS), T] + spec, T)
            r["rows"] = json.load(open(out))["rows"]
            res[str(T)][name] = r
            m = r["metrics"] or {}
            print("100M q T=%02d %-10s median=%.3f us p95=%.3f us" %
                  (T, name, m.get("query_median_ms", 0) * 1000,
                   m.get("query_p95_ms", 0) * 1000), flush=True)
    # also keep the per-case T=1 rows at the name the exactness stage reads
    for name, spec in CASES:
        with open(os.path.join(TMP, "qbench_100m_%s.json" % name), "w") as f:
            json.dump({"rows": res["1"][name]["rows"]}, f)
    # validated v2 binary cross-check at T=1 (same format, same rows?)
    v2res = {}
    for name, spec in CASES:
        out = os.path.join(TMP, "v2q100_%s.json" % name)
        r = invoke([V2, "bench", CUBE100, DICT100, out, str(REPS)] + spec, 1)
        r["rows"] = json.load(open(out))["rows"]
        v2res[name] = r
    write_receipt("qbench_100m", {"cube": CUBE100, "cube_sha256": sha256(CUBE100),
                                  "dict": DICT100, "reps": REPS, "results": res})
    write_receipt("v2_binary_crosscheck_100m", {"binary": V2, "cube": CUBE100,
                                                "results": v2res})


def stage_duck100m():
    import duckdb
    db = ROOT + "/results/nyc_taxi_100m_duckdb/nyc.duckdb"
    res = {}
    for T in (1, 4, 8, 16):
        con = duckdb.connect(db, read_only=True)
        con.execute("SET threads=%d" % T)
        con.execute("SET preserve_insertion_order=true")
        out = {}
        for name, spec in CASES:
            where = "" if not spec else "WHERE " + " AND ".join(
                "fare_cents <= %s" % s.split("=")[1] for s in spec)
            sql = ("SELECT passenger_count AS g, SUM(fare_cents) AS s, COUNT(*) AS c, "
                   "MIN(fare_cents) AS mn, MAX(fare_cents) AS mx FROM nyc %s "
                   "GROUP BY 1 ORDER BY 1" % where)
            out[name] = duck_bench(con, sql, 20)
            out[name]["sql"] = sql
            print("duck100m T=%02d %-10s median=%.1f ms" %
                  (T, name, out[name]["median_ms"]), flush=True)
        con.close()
        res[str(T)] = out
    write_receipt("duckdb_100m", {"db": db, "duckdb_version": duckdb.__version__,
                                  "results": res})


# ---------------------------------------------------------------------------
# 10M-group stage
# ---------------------------------------------------------------------------
def stage_build10m():
    make_1b_dict()
    runs = []
    for T in (1, 4, 8, 16):
        warm(GID10M, VAL10M)
        st = STATS10M if T == 1 else "%s/g10m_t%d.mstats" % (OUT, T)
        r = invoke([BIN, "build_stats", GID10M, VAL10M, st, T], T, timeout=1800)
        r["stats"] = st
        r["stats_bytes"] = os.path.getsize(st)
        r["stats_sha256"] = sha256(st)
        runs.append(r)
        print("10M stats build T=%d wall=%.2fs" % (T, r["wall_s"]), flush=True)
    warm(GID10M, VAL10M)
    rb = invoke([BIN, "build_bsum", GID10M, VAL10M, BSUM10M, 16, "1024"], 16)
    rb["bsum"] = BSUM10M
    rb["bsum_bytes"] = os.path.getsize(BSUM10M)
    rb["bsum_sha256"] = sha256(BSUM10M)
    shas = {r["stats_sha256"] for r in runs}
    write_receipt("build_materialized_10m", {
        "dataset": {"rows": 100000000, "groups": 10000000,
                    "group_key": "splitmix64(rowid) % 10000000",
                    "value": "real NYC fare cents (V=21084)"},
        "stats_runs": runs, "stats_deterministic": len(shas) == 1,
        "stats_bytes": runs[0]["stats_bytes"], "stats_bytes_per_group": 32,
        "bsum_run": rb,
        "note": "dense ECUBE02 at G=10M,V=21084 would need G*V*4 = 844 GB: infeasible "
                "(this is the O(G*V) boundary, see fit table); the router therefore uses "
                "materialized per-group stats for the fixed query and a scan for ranges",
    })


def stage_q10m():
    res = {}
    for T in (1, 4, 8, 16):
        out = os.path.join(TMP, "stats10m_t%d.json" % T)
        r = invoke([BIN, "bench_stats", STATS10M, out, str(REPS_STATS), T], T, timeout=1800)
        r["rows"] = None
        res[str(T)] = r
        m = r["metrics"] or {}
        print("10M stats q T=%02d median=%.2f ms serialize1=%.2f ms serializeT=%.2f ms" %
              (T, m.get("query_median_ms", 0), m.get("serialize1_median_ms", 0),
               m.get("serializeT_median_ms", 0)), flush=True)
    # block-summary scan fallback (exact, block-pruned) on the same 10M groups
    bres = {}
    for name, spec in CASES:
        out = os.path.join(TMP, "bsum10m_%s.json" % name)
        r = invoke([BIN, "bench_bsum", BSUM10M, GID10M, VAL10M, out, "5"] + spec, 1,
                   timeout=1800)
        r["rows"] = None
        bres[name] = r
        m = r["metrics"] or {}
        print("10M bsum %-10s median=%.1f ms cand_blocks=%s" %
              (name, m.get("query_median_ms", 0), m.get("candidate_blocks")), flush=True)
    # exactness anchor: the bsum full-range (fixed, no predicate) result must be
    # byte-identical to the matstats full-range result
    stats_out = os.path.join(TMP, "stats10m_full.json")
    r = invoke([BIN, "bench_stats", STATS10M, stats_out, "1", 1], 1, timeout=1800)
    bsum_full = os.path.join(TMP, "bsum10m_fixed.json")
    cross = {"stats_full_sha256": sha256(stats_out), "bsum_99900_sha256": sha256(bsum_full),
             "byte_identical": sha256(stats_out) == sha256(bsum_full)}
    write_receipt("q10m_materialized_vs_scan", {"stats_query": res, "bsum_fallback": bres,
                                                "full_range_cross_check": cross,
                                                "reps_stats": REPS_STATS,
                                                "reps_bsum": 5})


def stage_duck10m():
    import duckdb
    GID_COL = "g"
    res = {}
    for T in (1, 4, 8, 16):
        con = duckdb.connect()
        con.execute("SET threads=%d" % T)
        con.execute("SET preserve_insertion_order=false")
        con.execute("SET memory_limit='120GB'")
        out = {}
        for name, spec in CASES:
            where = "" if not spec else "WHERE " + " AND ".join(
                "v <= %s" % s.split("=")[1] for s in spec)
            sql = ("SELECT g, SUM(v)::HUGEINT AS s, COUNT(*)::HUGEINT AS c, "
                   "MIN(v)::BIGINT AS mn, MAX(v)::BIGINT AS mx "
                   "FROM read_parquet('%s') %s GROUP BY 1" % (PARQ10M, where))
            reps = 5
            out[name] = duck_bench(con, sql, reps, store_rows=False)
            out[name]["sql"] = sql
            # order-independent checksum over all groups (same formula as the C++ Agg)
            chk_sql = ("SELECT SUM(CAST(g AS HUGEINT)*2654435761 + CAST(s AS HUGEINT)*97 + "
                       "CAST(c AS HUGEINT)*13 + CAST(mn AS HUGEINT)*7 + CAST(mx AS HUGEINT)*3), "
                       "SUM(c), SUM(s), MIN(mn), MAX(mx) FROM (%s) t" % sql)
            out[name]["agg"] = [str(x) for x in con.execute(chk_sql).fetchone()]
            print("duck10m T=%02d %-10s median=%.1f ms" %
                  (T, name, out[name]["median_ms"]), flush=True)
        con.close()
        res[str(T)] = out
    write_receipt("duckdb_10m", {"parquet": PARQ10M, "parquet_bytes": os.path.getsize(PARQ10M),
                                 "duckdb_version": duckdb.__version__, "results": res,
                                 "note": "agg = [group_chk, total_cnt, total_sum, min_mn, max_mx]"})


def stage_exact10m():
    """Compare the materialized 10M-group result with DuckDB via the order-independent
    aggregate (all 10M groups) plus a 1000-group random sample with full rows."""
    q = json.load(open(os.path.join(RECDIR, "q10m_materialized_vs_scan.json")))
    d = json.load(open(os.path.join(RECDIR, "duckdb_10m.json")))
    m_stats = q["stats_query"]["1"]["metrics"]
    m_duck = d["results"]["1"]["fixed"]["agg"]
    cube_agg = [m_stats["group_chk"], str(m_stats["total_cnt"]), str(m_stats["total_sum"]),
                str(m_stats["min_mn"]), str(m_stats["max_mx"])]
    checks = {"matstats_vs_duckdb_fixed": {"cube": cube_agg, "duckdb": m_duck,
                                           "equal": cube_agg == m_duck}}
    # 1000-group sample: DuckDB rows for a deterministic sample, compare to the
    # materialized stats file directly.
    gen = __import__("random")
    rnd = gen.Random(20261101)
    sample = sorted(rnd.sample(range(10000000), 1000))
    import duckdb
    con = duckdb.connect()
    con.execute("SET threads=4")
    inlist = ",".join(str(x) for x in sample)
    sql = ("SELECT g, SUM(v)::HUGEINT, COUNT(*)::HUGEINT, MIN(v)::BIGINT, MAX(v)::BIGINT "
           "FROM read_parquet('%s') WHERE g IN (%s) GROUP BY 1 ORDER BY 1" % (PARQ10M, inlist))
    drows = {str(r[0]): [str(x) for x in r[1:]] for r in con.execute(sql).fetchall()}
    con.close()
    # read the materialized binary directly
    import struct
    with open(STATS10M, "rb") as f:
        magic = f.read(8)
        n, G, has_null, null_group = struct.unpack("<QQQQ", f.read(32))
        f.seek(8 + 32 + 32 * sample[0])
        bad = []
        for g in sample:
            f.seek(8 + 32 + 32 * g)
            s, c, mn, mx = struct.unpack("<qqqq", f.read(32))
            got = [str(s), str(c), str(mn), str(mx)]
            if drows.get(str(g)) != got:
                bad.append({"g": g, "stats": got, "duckdb": drows.get(str(g))})
    checks["sample_1000_groups"] = {"matched": 1000 - len(bad), "mismatched": len(bad),
                                    "first_bad": bad[:3]}
    ok = checks["matstats_vs_duckdb_fixed"]["equal"] and len(bad) == 0
    write_receipt("exactness_10m", {"checks": checks, "all_exact": ok,
                                    "note": "full 10M-group aggregate checksum + 1000 random "
                                            "groups compared row-for-row against DuckDB 2.0"})


# ---------------------------------------------------------------------------
# O(G)-vs-n curve: fixed G=101, n in {100M, 300M, 600M, 1B}
# ---------------------------------------------------------------------------
SC_POINTS = [(100_000_000, 10_000_000), (300_000_000, 30_000_000), (600_000_000, 60_000_000)]


def stage_scalecurve():
    """Fixed G=101, varied n.  Each dataset takes the first per_replica source rows
    from all 10 replica blocks (nested samples: 100M subset 300M subset 600M subset
    1B), so G and the value support stay fixed and only n changes."""
    points = []
    for n, per in SC_POINTS:
        gid = OUT + "/sc%d_gid.i32" % n
        val = OUT + "/sc%d_val.i32" % n
        g = invoke([GEN, "replica", GID100, VAL100, gid, val, "10", "10", str(per)], 1,
                   timeout=1800)
        warm(gid, val)
        cube = OUT + "/sc%d.ecube" % n
        b = invoke([BIN, "build_mt", gid, val, cube, "1", "24"], 1, timeout=3600)
        b["cube"] = cube
        b["cube_bytes"] = os.path.getsize(cube)
        b["cube_sha256"] = sha256(cube)
        points.append({"n_rows": n, "per_replica_rows": per, "gen": g["metrics"],
                       "build": b, "cube": cube})
        print("scalecurve n=%d build wall=%.2fs bytes=%d" %
              (n, b["wall_s"], b["cube_bytes"]), flush=True)
        del g
    # 1B point: the already-built cube
    points.append({"n_rows": 1000000000, "per_replica_rows": 100000000,
                   "gen": None, "build": None, "cube": CUBE1B})
    for pt in points:
        pt["query"] = {}
        for T in (1, 8):
            for name, spec in (("fixed", []), ("le_m720", ["hi=-720"])):
                out = os.path.join(TMP, "scq_%d_t%d_%s.json" % (pt["n_rows"], T, name))
                r = invoke([BIN, "qbench", pt["cube"], DICT1B, out, str(REPS), T] + spec, T)
                r["rows"] = json.load(open(out))["rows"]
                pt["query"]["T%d_%s" % (T, name)] = r
                m = r["metrics"] or {}
                print("scalecurve n=%d T=%d %-8s median=%.3f us p95=%.3f us V=%s rows=%s" %
                      (pt["n_rows"], T, name, m.get("query_median_ms", 0) * 1000,
                       m.get("query_p95_ms", 0) * 1000, m.get("values"), m.get("rows")),
                      flush=True)
    write_receipt("scalecurve_ogn", {
        "design": "G=101 fixed (10 replicas x 10 passenger values + 1 NULL group), V unchanged",
        "points": points,
        "protocol": "build T=1 warm page cache; qbench T=1 and T=8, 30 reps, engine-resident "
                    "latency excluding serialization; same dict/spec for every n",
    })


# ---------------------------------------------------------------------------
# O(G)-vs-n curve B: G=11 and V=21084 exactly fixed, n = k*100M (k = 1,3,6,10
# identical replicas, NO group offset) -> index bytes and latency must be identical
# ---------------------------------------------------------------------------
SC_G11 = [(100_000_000, 1), (300_000_000, 3), (600_000_000, 6), (1_000_000_000, 10)]


def stage_scalecurve_g11():
    points = []
    for n, k in SC_G11:
        if n == 100_000_000:
            gid, val, cube = GID100, VAL100, CUBE100
            g = {"reused": True}
        else:
            gid = OUT + "/g11_%d_gid.i32" % n
            val = OUT + "/g11_%d_val.i32" % n
            g = invoke([GEN, "replica", GID100, VAL100, gid, val, str(k), "0"], 1,
                       timeout=1800)
        if n == 100_000_000:
            b = None
        else:
            warm(gid, val)
            cube = OUT + "/g11_%d.ecube" % n
            b = invoke([BIN, "build_mt", gid, val, cube, "1", "24"], 1, timeout=3600)
            b["cube"] = cube
            b["cube_bytes"] = os.path.getsize(cube)
            b["cube_sha256"] = sha256(cube)
        pt = {"n_rows": n, "replicas": k, "gen": g, "build": b, "cube": cube,
              "cube_bytes": os.path.getsize(cube), "cube_sha256": sha256(cube),
              "query": {}}
        for T in (1, 8):
            for name, spec in (("fixed", []), ("le_m720", ["hi=-720"])):
                out = os.path.join(TMP, "g11q_%d_t%d_%s.json" % (n, T, name))
                r = invoke([BIN, "qbench", cube, DICT100, out, str(REPS), T] + spec, T)
                r["rows"] = json.load(open(out))["rows"]
                pt["query"]["T%d_%s" % (T, name)] = r
                m = r["metrics"] or {}
                print("g11curve n=%d T=%d %-8s median=%.3f us p95=%.3f us bytes=%s V=%s G=%s"
                      % (n, T, name, m.get("query_median_ms", 0) * 1000,
                         m.get("query_p95_ms", 0) * 1000, m.get("bytes"), m.get("values"),
                         m.get("groups")), flush=True)
        points.append(pt)
    write_receipt("scalecurve_ogn_g11", {
        "design": "G=11, V=21084, n = k x 100M identical replicas with NO group offset; "
                  "index bytes and latency must be flat in n by construction of the structure "
                  "(O(G*V) bytes, O(G*(V/R)) query)",
        "points": points,
        "protocol": "build T=1 warm page cache; qbench T=1 and T=8, 30 reps, engine-resident",
    })


# ---------------------------------------------------------------------------
# O(G)-vs-n curve (primary): G=101 AND V=21,084 held fixed, n = 100M/300M/600M/1B
# via stratified systematic sampling + one row per missing distinct value, so the
# observed value domain is complete at every n.
# ---------------------------------------------------------------------------
SC_FV = [(100_000_000, 10_000_000), (300_000_000, 30_000_000), (600_000_000, 60_000_000)]


def _bench_point(pt, cube, gid_unused=None):
    for T in (1, 8):
        for name, spec in (("fixed", []), ("le_m720", ["hi=-720"])):
            out = os.path.join(TMP, "fvq_%d_t%d_%s.json" % (pt["n_rows"], T, name))
            r = invoke([BIN, "qbench", cube, DICT1B, out, str(REPS), T] + spec, T)
            r["rows"] = json.load(open(out))["rows"]
            pt["query"]["T%d_%s" % (T, name)] = r
            m = r["metrics"] or {}
            print("fixedvg n=%d T=%d %-8s median=%.3f us p95=%.3f us bytes=%s V=%s G=%s"
                  % (pt["n_rows"], T, name, m.get("query_median_ms", 0) * 1000,
                     m.get("query_p95_ms", 0) * 1000, m.get("bytes"), m.get("values"),
                     m.get("groups")), flush=True)


def stage_scalecurve_fixedvg():
    points = []
    for n, per in SC_FV:
        gid = OUT + "/fv%d_gid.i32" % n
        val = OUT + "/fv%d_val.i32" % n
        g = invoke([GEN, "stratified", GID100, VAL100, gid, val, "10", "10", str(per)], 1,
                   timeout=1800)
        warm(gid, val)
        cube = OUT + "/fv%d.ecube" % n
        b = invoke([BIN, "build_mt", gid, val, cube, "1", "24"], 1, timeout=3600)
        b["cube"] = cube
        b["cube_bytes"] = os.path.getsize(cube)
        b["cube_sha256"] = sha256(cube)
        pt = {"n_rows": n, "gen": g["metrics"], "build": b, "cube": cube,
              "cube_bytes": os.path.getsize(cube), "cube_sha256": sha256(cube),
              "query": {}}
        _bench_point(pt, cube)
        points.append(pt)
    # 1B: the full 10-replica cube already built (G=101, V=21,084)
    pt = {"n_rows": 1000000000, "gen": {"reused": True}, "build": None, "cube": CUBE1B,
          "cube_bytes": os.path.getsize(CUBE1B), "cube_sha256": sha256(CUBE1B),
          "query": {}}
    _bench_point(pt, CUBE1B)
    points.append(pt)
    write_receipt("scalecurve_fixedvg", {
        "design": "G=101 and V=21,084 held exactly fixed; n varied 100M/300M/600M/1B by "
                  "stratified systematic sampling (stride 100M/per) of the source rows, "
                  "replicated 10x with the +10 group offset, plus one row per distinct value "
                  "missed by the stride sample so the observed domain stays complete",
        "points": points,
        "protocol": "build T=1 warm page cache; qbench T=1 and T=8, 30 reps, engine-resident",
    })


# ---------------------------------------------------------------------------
# ECUBE03 (frozen format) parallel build from the encoded tree
# ---------------------------------------------------------------------------
E3 = ROOT + "/src_scale_20261101/ecube3_mt"
E3F = _artifact.WORK + "/specs/bin/enc_cube3_frozen"
E3_CUBES = _artifact.WORK + "/specs/cubes"
TIERS = _artifact.WORK + "/tiers/build"
E3_CASES = [
    {"name": "nyc100m", "prep": ROOT + "/results/nyc_taxi_100m_prep/Observed",
     "enc": ROOT + "/results/nyc_taxi_100m_prep/Observed.encoded", "gid": 17, "vid": 13,
     "frozen_sha": "78bff19a0dbd89d0e3b16254cb2a0c807669d982b3a9233b1ba5504dd04c89ee",
     "frozen_cube": E3_CUBES + "/C1.ecube"},
    {"name": "nyc12m", "prep": ROOT + "/results/nyc_taxi_2023_prep/Observed",
     "enc": ROOT + "/results/nyc_taxi_2023_prep/Observed.encoded", "gid": 17, "vid": 13,
     "frozen_sha": "748029efe222b8de0a108e451085251f5bebe827f9d01b206d5f5bce5aab51e8",
     "frozen_cube": E3_CUBES + "/C2.ecube"},
]
E3_NULL_SHA = "914de2e7977c264bba0757332a74a24b8a24803aa405f99b4852db60b5edbd9b"
E3_TPCH = {"prep": ROOT + "/results/tpch_sf2_prep/Observed",
           "enc": ROOT + "/results/tpch_sf2_prep/Observed.encoded", "gid": 17, "vid": 13,
           "frozen_sha": "f245f7db6ea887429e9158355e23930046d1e5e3530d6f9127ea262622ba5eaf",
           "frozen_cube": E3_CUBES + "/C3.ecube"}


def e3_header(path):
    """ECUBE03 header: 8s magic, Q n, Q ngroups, Q nvalues, q vmin, I dense,
    I has_null, I null_group, I block_ranks, I null_measure, 4s pad (=64 B)."""
    import struct
    with open(path, "rb") as f:
        b = f.read(64)
    magic, n, ng, nv, vmin, dense, has_null, null_group, R, null_measure, _pad = \
        struct.unpack("<8sQQQqIIIII4s", b)
    return {"magic": magic.decode().strip(), "n": n, "groups": ng, "values": nv,
            "vmin": vmin, "dense": dense, "has_null": has_null,
            "null_group": null_group, "block_ranks": R, "null_measure": null_measure}


def _timed_build(cmd, T, warm_paths, reps=3, timeout=1800):
    """1 warmup (also P-state warm-up; /usr/bin/time measured only on the timed
    runs) + reps timed executions; returns median wall/build_ms and all samples."""
    invoke(cmd, T, timeout=timeout)  # warmup
    runs = []
    for _ in range(reps):
        warm(*warm_paths)
        r = invoke(cmd, T, timeout=timeout)
        runs.append(r)
    walls = sorted(x["wall_s"] for x in runs)
    builds = sorted(x["metrics"]["build_ms"] for x in runs if x.get("metrics"))
    rss = max((x["time"] or {}).get("rss", 0) for x in runs)
    return {"threads": T, "core_mask": MASK[T], "reps": reps,
            "wall_s_median": statistics.median(walls), "wall_s_all": walls,
            "build_ms_median": statistics.median(builds) if builds else None,
            "build_ms_all": builds, "peak_rss_mb": rss,
            "metrics": runs[len(runs) // 2]["metrics"],
            "cmd": runs[0]["cmd"], "exit": runs[0]["exit"]}


def stage_ecube3mt():
    os.makedirs(TMP, exist_ok=True)
    rec = {"protocol": {
        "host": "localhost (AMD EPYC 9554, 2x64C/256T)",
        "pin": "nice -n 5 taskset -c 80 / 80-83 / 80-87 / 80-95 for 1/4/8/16T",
        "timing": "1 warmup build (also P-state/ page-cache warm-up) + 3 timed builds per "
                  "configuration; medians reported; /usr/bin/time -f 'wall=%e rss=%M' per run",
        "page_cache": "encoded tree pre-read before each timed run",
        "binary": E3 + " (copy of src_eai_20261101/eai_cube3.cpp + build_enc_mt/build_plain_mt)",
        "frozen_reference_binary": E3F,
        "frozen_reference_sha256": "37ad8f3648c04102221b54941c4f2f82eb673a750c5f6cc9d1a24959e8f8742e",
        "identity_rule": "every *_mt build must equal the frozen single-thread build sha256",
    }, "datasets": {}}
    # ---- encoded-tree parallel builds at 100M and 12M ----
    for case in E3_CASES:
        d = {"frozen_sha256": case["frozen_sha"], "reference_1t": {}, "mt": {}}
        # shipped single-thread path through the frozen binary (the amortization baseline)
        ref_out = os.path.join(TMP, "e3_%s_ref.ecube" % case["name"])
        r = _timed_build([E3F, "build_enc", case["enc"], case["prep"], str(case["gid"]),
                          str(case["vid"]), ref_out], 1, (case["enc"],))
        d["reference_1t"] = {**r,
                             "sha256": sha256(ref_out)}
        for T in (1, 4, 8, 16):
            out = os.path.join(TMP, "e3_%s_t%d.ecube" % (case["name"], T))
            r = _timed_build([E3, "build_enc_mt", case["enc"], case["prep"], str(case["gid"]),
                              str(case["vid"]), out, str(T), "24"], T, (case["enc"],))
            r["sha256"] = sha256(out)
            r["byte_identical"] = (r["sha256"] == case["frozen_sha"])
            d["mt"]["T%d" % T] = r
            print("E3 %s T=%02d wall=%.3fs build_ms=%.1f sha=%s identical=%s"
                  % (case["name"], T, r["wall_s_median"], r["build_ms_median"],
                     r["sha256"][:16], r["byte_identical"]), flush=True)
        rec["datasets"][case["name"]] = d
    # ---- NULL-measure path (plain columns + validity bitmap) ----
    nm = {"reference_sha256": E3_NULL_SHA, "builds": {}}
    ref = invoke([E3F, "build", TIERS + "/gid_nyc_g10.i32", TIERS + "/val_nyc100.i32",
                  os.path.join(TMP, "e3_null_ref.ecube"), TIERS + "/nyc_null_valid.bits"], 1)
    nm["reference_1t"] = {"wall_s": ref["wall_s"], "metrics": ref["metrics"],
                          "sha256": sha256(os.path.join(TMP, "e3_null_ref.ecube"))}
    for T in (1, 16):
        out = os.path.join(TMP, "e3_null_t%d.ecube" % T)
        r = _timed_build([E3, "build_plain_mt", TIERS + "/gid_nyc_g10.i32",
                          TIERS + "/val_nyc100.i32", out, str(T),
                          TIERS + "/nyc_null_valid.bits", "24"], T,
                         (TIERS + "/gid_nyc_g10.i32", TIERS + "/val_nyc100.i32"))
        r["sha256"] = sha256(out)
        r["byte_identical"] = (r["sha256"] == E3_NULL_SHA)
        nm["builds"]["T%d" % T] = r
        print("E3 null-measure T=%02d wall=%.3fs sha=%s identical=%s"
              % (T, r["wall_s_median"], r["sha256"][:16], r["byte_identical"]), flush=True)
    rec["null_measure"] = nm
    # ---- dense / contiguity-rule fixtures (header dense=1), with and without NULLs ----
    fx = os.path.join(TMP, "e3_dense")
    dense = {}
    for with_nulls in (False, True):
        tag = "null" if with_nulls else "plain"
        subprocess.run([sys.executable, ROOT + "/src_scale_20261101/make_dense_fixture.py",
                        fx + "_" + tag] + (["--nulls"] if with_nulls else []),
                       check=True, capture_output=True)
        vp = fx + "_" + tag + "/dense_valid.bits" if with_nulls else "-"
        ref_out = os.path.join(TMP, "e3_dense_%s_ref.ecube" % tag)
        st = invoke([E3, "build", fx + "_" + tag + "/dense_gid.i32",
                     fx + "_" + tag + "/dense_val.i32", ref_out, vp] + ([] if with_nulls else []), 1)
        mt_out = os.path.join(TMP, "e3_dense_%s_mt.ecube" % tag)
        mt = invoke([E3, "build_plain_mt", fx + "_" + tag + "/dense_gid.i32",
                     fx + "_" + tag + "/dense_val.i32", mt_out, "16", vp, "24"], 16)
        rh, mh = e3_header(ref_out), e3_header(mt_out)
        dense[tag] = {"single_thread": {"metrics": st["metrics"], "sha256": sha256(ref_out),
                                        "header": rh},
                      "parallel_16t": {"metrics": mt["metrics"], "sha256": sha256(mt_out),
                                       "header": mh},
                      "bytes": os.path.getsize(ref_out),
                      "byte_identical": sha256(ref_out) == sha256(mt_out),
                      "header_flags_equal": (rh["dense"] == mh["dense"] and
                                             rh["null_measure"] == mh["null_measure"])}
        print("E3 dense(%s): dense=%d null_measure=%d bytes=%d identical=%s flags_equal=%s"
              % (tag, rh["dense"], rh["null_measure"], dense[tag]["bytes"],
                 dense[tag]["byte_identical"], dense[tag]["header_flags_equal"]), flush=True)
    rec["dense_rule_fixtures"] = dense
    # ---- TPC-H SF2 encoded tree (large V exercise) ----
    th_out = os.path.join(TMP, "e3_tpch_t16.ecube")
    th = _timed_build([E3, "build_enc_mt", E3_TPCH["enc"], E3_TPCH["prep"],
                       str(E3_TPCH["gid"]), str(E3_TPCH["vid"]), th_out, "16", "24"], 16,
                      (E3_TPCH["enc"],), reps=1)
    th["sha256"] = sha256(th_out)
    th["byte_identical"] = (th["sha256"] == E3_TPCH["frozen_sha"])
    rec["tpch_sf2_encoded_16t"] = th
    print("E3 tpch t16 sha=%s identical=%s" % (th["sha256"][:16], th["byte_identical"]), flush=True)
    # ---- ECUBE03 query latency (frozen engine, frozen cube, 30 reps) ----
    q = invoke([E3F, "bench", E3_CUBES + "/C1.ecube",
                ROOT + "/results/nyc_taxi_100m_prep/Observed/state.dict",
                os.path.join(TMP, "e3_q.json"), "30"], 1)
    rec["query_1t"] = {"median_ms": q["metrics"]["query_median_ms"],
                       "p95_ms": q["metrics"]["query_p95_ms"],
                       "reps": q["metrics"]["reps"], "cmd": q["cmd"]}
    # ---- DuckDB baseline re-measurement (same host, threads=1, 30 reps) ----
    duck = subprocess.run([_artifact.python(),
                           ROOT + "/src_eai_20261101/bench_duckdb_nyc.py",
                           ROOT + "/results/nyc_taxi_100m_duckdb/nyc.duckdb",
                           os.path.join(TMP, "duck_amort.json"), "30"],
                          capture_output=True, text=True)
    dj = json.load(open(os.path.join(TMP, "duck_amort.json")))
    base = dj["cases"]["fixed_all4"]["median_ms"]
    rec["baseline_duckdb_1t"] = {"fixed_all4_median_ms": base,
                                 "duckdb_version": dj["duckdb_version"],
                                 "receipt": "results/nyc_taxi_100m_duckdb/nyc.duckdb"}
    # ---- amortization rows for ECUBE03 ----
    base_frozen = 450.7991  # TIERS_20261101 frozen program number (same query/DB)
    qms = rec["query_1t"]["median_ms"]
    def amort(build_ms):
        be = build_ms / (base - qms)
        ng = math.ceil(2.0 * build_ms / (base - 2.0 * qms))
        rows = {}
        for Q in (1, 10, 100, 1000):
            cost = build_ms + Q * qms
            rows["Q%d" % Q] = {"index_cost_ms": cost,
                               "baseline_cost_ms": Q * base,
                               "ratio": (Q * base) / cost}
        return {"build_ms": build_ms, "query_ms": qms, "baseline_ms": base,
                "break_even_Q": be, "Q_at_2x_real": 2.0 * build_ms / (base - 2.0 * qms),
                "Q_at_2x_first_integer": ng, "rows": rows}
    serial = rec["datasets"]["nyc100m"]["reference_1t"]["build_ms_median"]
    mt1 = rec["datasets"]["nyc100m"]["mt"]["T1"]["build_ms_median"]
    mt16 = rec["datasets"]["nyc100m"]["mt"]["T16"]["build_ms_median"]
    rec["amortization"] = {
        "baseline": {"duckdb_1t_fixed_median_ms": base, "frozen_program_number_ms": base_frozen,
                     "source": "DuckDB 2.0.0 NYC-100M fixed GROUP BY, threads=1, fresh "
                               "re-measurement on the same host (frozen TIERS number %.4f ms)"
                               % base_frozen},
        "rows": {
            "ecube03_shipped_serial_1t": amort(serial),
            "ecube03_build_enc_mt_1t": amort(mt1),
            "ecube03_build_enc_mt_16t": amort(mt16),
            "ecube02_tiers_frozen_row": {"build_ms": 3242.0, "query_ms": 0.0012,
                                          "baseline_ms": base, "break_even_Q": 7.19,
                                          "Q_at_2x": 14.38,
                                          "note": "TIERS_20261101 frozen row, superseded by "
                                                  "ECUBE03; kept for continuity"},
        },
        "formula": "break_even Q = T_build/(T_base - T_query); Q@2x = ceil("
                   "2*T_build/(T_base - 2*T_query))",
    }
    write_receipt("ecube3_build_mt", rec)


# ---------------------------------------------------------------------------
# ECUBE03 single-thread query regression check: frozen binary vs ecube3_mt
# ---------------------------------------------------------------------------
E3Q_CASES = [
    # (label, spec, Lead's frozen reference median in us)
    ("fixed", [], 0.921),
    ("le_m720_1.04pct", ["hi=-720"], 0.711),
    ("le_509_5.8pct", ["hi=509"], 0.741),
    ("in_1_2", ["where_in=1,2"], 0.230),
    ("having_sum_1e10", ["having=sum>=1e10"], 0.912),
]


def pstate_warm(secs=0.7):
    """Busy-spin on the benchmark core so schedutil brings the P-state up before a
    sub-microsecond query benchmark (the host sits at ~1.5 GHz when idle; the
    binary's own 1-query warmup is not enough to ramp the frequency)."""
    subprocess.run(["/usr/bin/python3", "-c",
                    "import time\nt=time.perf_counter()\nwhile time.perf_counter()-t<%.2f: pass" % secs],
                   capture_output=True)


def cur_freq(cpu=80):
    try:
        return int(open("/sys/devices/system/cpu/cpu%d/cpufreq/scaling_cur_freq" % cpu).read())
    except OSError:
        return None


def stage_ecube3q():
    """The Lead requires that additively adding the parallel-build code (a bigger
    translation unit) must not slow the untouched single-thread query path. Runs
    alternate frozen -> mt -> frozen -> mt ... with a pinned P-state warm-up before
    every invocation, so frequency drift cannot masquerade as a layout effect."""
    cube = E3_CUBES + "/C1.ecube"
    dictp = ROOT + "/results/nyc_taxi_100m_prep/Observed/state.dict"
    REPS = 2000
    PAIRS = 10
    rec = {"protocol": {
        "host": "localhost (AMD EPYC 9554)",
        "pin": "nice -n 5 taskset -c 80",
        "cube": cube, "cube_sha256": sha256(cube),
        "dict": dictp, "reps_per_run": REPS, "paired_repetitions": PAIRS,
        "measurement": "engine-resident query latency from the binary's own bench "
                       "(median + P95 over REPS); paired A/B design: each pair measures "
                       "frozen then parallel-build binary (order alternates) after a "
                       "0.5 s pinned P-state warm-up; a same-binary pair (frozen vs "
                       "frozen) quantifies pure run-to-run noise; serialization "
                       "excluded (reported separately by the bench)",
        "binaries": {"frozen": E3F, "frozen_sha256": sha256(E3F),
                     "parallel_build_binary": E3, "parallel_build_sha256": sha256(E3)},
        "regression_rule": "the ecube3_mt query path is the frozen source's code, "
                           "untouched; this check detects only code-layout effects",
    }, "cases": {}}
    for label, spec, ref_us in E3Q_CASES:
        d = {"spec": spec, "lead_reference_us": ref_us, "binaries": {}}
        runs = {"frozen": [], "mt": []}
        pairs_mt = []
        pairs_control = []
        outs = {}
        freq_after_warm = []
        def one(b, tag, out):
            pstate_warm(0.5)
            freq_after_warm.append(cur_freq())
            r = invoke([b, "bench", cube, dictp, out, str(REPS)] + spec, 1)
            runs[tag].append(r)
            outs[tag] = hashlib.sha256(open(out, "rb").read()).hexdigest()
            return r["metrics"]["query_median_ms"] * 1000.0
        for i in range(PAIRS):
            # paired A/B experiment, order alternates to cancel drift
            first_frozen = (i % 2 == 0)
            fa = os.path.join(TMP, "e3q_%s_f1_%d.json" % (label, i))
            fb = os.path.join(TMP, "e3q_%s_f2_%d.json" % (label, i))
            mt = os.path.join(TMP, "e3q_%s_mt_%d.json" % (label, i))
            if first_frozen:
                a = one(E3F, "frozen", fa)
                b = one(E3, "mt", mt)
                c = one(E3F, "frozen", fb)
            else:
                b = one(E3, "mt", mt)
                a = one(E3F, "frozen", fa)
                c = one(E3F, "frozen", fb)
            pairs_mt.append((b - a) / a * 100.0)
            pairs_control.append((c - a) / a * 100.0)
        for tag in ("frozen", "mt"):
            res = runs[tag]
            us_all = [x["metrics"]["query_median_ms"] * 1000.0 for x in res]
            p95_all = [x["metrics"]["query_p95_ms"] * 1000.0 for x in res]
            d["binaries"][tag] = {
                "median_us": statistics.median(us_all),
                "p95_us": statistics.median(p95_all),
                "runs_us": us_all, "run_min_us": min(us_all), "run_max_us": max(us_all),
                "out_sha256": outs[tag], "cmd": res[0]["cmd"],
            }
        d["cur_freq_khz_after_warm"] = freq_after_warm
        f, m = d["binaries"]["frozen"], d["binaries"]["mt"]
        d["delta_pct"] = (m["median_us"] - f["median_us"]) / f["median_us"] * 100.0
        d["paired_mt_vs_frozen_pct"] = {"values": pairs_mt,
                                        "median": statistics.median(pairs_mt),
                                        "positive_pairs": sum(1 for x in pairs_mt if x > 0),
                                        "pairs": PAIRS}
        d["paired_noise_control_pct"] = {"values": pairs_control,
                                         "median": statistics.median(pairs_control),
                                         "positive_pairs": sum(1 for x in pairs_control if x > 0),
                                         "note": "same frozen binary measured twice in the same "
                                                 "pair; this is pure run-to-run noise"}
        d["result_rows_identical"] = (f["out_sha256"] == m["out_sha256"])
        d["vs_lead_reference_pct"] = (f["median_us"] - ref_us) / ref_us * 100.0
        # regression verdict: only if the paired median is positive beyond both 3%
        # and twice the same-binary noise median
        noise = abs(statistics.median(pairs_control))
        d["regression_detected"] = (statistics.median(pairs_mt) > max(3.0, 2.0 * noise))
        rec["cases"][label] = d
        print("E3Q %-18s frozen=%.3f mt=%.3f paired_med=%+.2f%% (pos %d/%d) control=%+.2f%% (pos %d/%d) regress=%s"
              % (label, f["median_us"], m["median_us"],
                 statistics.median(pairs_mt), d["paired_mt_vs_frozen_pct"]["positive_pairs"], PAIRS,
                 statistics.median(pairs_control), d["paired_noise_control_pct"]["positive_pairs"],
                 PAIRS, d["regression_detected"]), flush=True)
    write_receipt("ecube3_query_regression", rec)


# ---------------------------------------------------------------------------
# fit table
# ---------------------------------------------------------------------------
def stage_fit():
    trees = {"nyc100": {"rows": 100000000, "encoded_tree": 251814420, "label": "NYC-100M"},
             "tpch12": {"rows": 11997996, "encoded_tree": 44249316, "label": "TPC-H SF2"}}
    Gs = [10, 1000, 100000, 1000000, 10000000]
    Vs = [64, 21084, 984297]
    rows = []
    for G in Gs:
        for V in Vs:
            nb = (V + 63) // 64
            dense = (V <= (1 << 22) and V == 64)  # only the V=64 synthetic domain is contiguous 0..63
            distinct_bytes = 0 if dense else V * 8
            counts = G * V * 4
            prefix = G * (nb + 1) * 12
            idx = 64 + distinct_bytes + counts + prefix
            tree = trees["nyc100"]["encoded_tree"] if V != 984297 else trees["tpch12"]["encoded_tree"]
            ratio = idx / tree
            rows.append({"G": G, "V": V, "dense": dense, "index_bytes": idx,
                         "counts_bytes": counts, "prefix_bytes": prefix,
                         "distinct_bytes": distinct_bytes,
                         "encoded_tree_bytes": tree, "ratio_to_tree": round(ratio, 4),
                         "total_footprint_ratio": round(1 + ratio, 4),
                         "fits_1.5x_budget": (1 + ratio) <= 1.5})
    write_receipt("fit_table", {"rows": rows,
                                "definition": "index = header + distinct(V*8 unless dense) + "
                                              "G*V*4 counts + G*ceil(V/64)*12 block-prefix+1",
                                "budget": "total (encoded tree + index) <= 1.5x encoded tree "
                                          "=> index <= 0.5x tree",
                                "tree_assumption": "NYC-100M 251,814,420 B; TPC-H SF2 44,249,316 B "
                                                   "for the V=984,297 rows"})
    for r in rows:
        print("%9d %8d  index=%14s  ratio=%.4f  fits=%s" %
              (r["G"], r["V"], r["index_bytes"], r["ratio_to_tree"],
               r["fits_1.5x_budget"]), flush=True)


STAGES = {
    "build1b": stage_build1b,
    "q1b": stage_q1b,
    "v2q1b": stage_v2q1b,
    "exact1b": stage_exact1b,
    "duck1b": stage_duck1b,
    "build100": stage_build100,
    "q100m": stage_q100m,
    "duck100m": stage_duck100m,
    "build10m": stage_build10m,
    "q10m": stage_q10m,
    "duck10m": stage_duck10m,
    "exact10m": stage_exact10m,
    "scalecurve": stage_scalecurve,
    "scalecurve_g11": stage_scalecurve_g11,
    "scalecurve_fixedvg": stage_scalecurve_fixedvg,
    "ecube3mt": stage_ecube3mt,
    "ecube3q": stage_ecube3q,
    "fit": stage_fit,
}

if __name__ == "__main__":
    todo = sys.argv[1:] if len(sys.argv) > 1 else ["fit"]
    for name in todo:
        print("=== stage", name, "===", flush=True)
        STAGES[name]()
