#!/usr/bin/env python3
"""MEC hardening measurements (run with python3 -> duckdb 2.0.0).

Three independent parts, each writing its own JSON receipt:

  --part mt      multi-core MEC: 1/4/8/16 threads for the build and for the query
                 (spawn-once pool, median/p95), plus RSS, plus the same-thread
                 DuckDB 2.0 control (one process per thread count, threads passed
                 at connect time -- NOT via SET after connect, see the note below).
  --part budget  MEC01 vs MEC02 under one budget rule (index <= 0.5x projected
                 source parquet).  MEC01's latency is its archived receipt; its
                 footprint is reconstructed from the section sizes (it had no
                 first/last planes) and its *inapplicability* is recomputed from the
                 dataset shape (uint16-only run cap, no NULL groups).
  --part cold    complete-request latency: >=20 fresh process launches, parent
                 observed wall vs child in-process time, for broad/selective/empty
                 on TPC-H, plus the DuckDB 1-thread cold-connection control.

The DuckDB thread pitfall: `SET threads=N` executed on an already-open connection
does not resize the task scheduler that the database instance created in
`duckdb_connect`, so the query keeps running with the old thread count; passing
`config={"threads": N}` at connect time is what actually applies N.  Part `mt`
measures both forms side by side as evidence.
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import argparse
import json
import os
import re
import statistics
import subprocess
import sys
import time

import duckdb

ROOT = _artifact.expand("../../research/real_decoders_20261024")
RES = os.path.join(ROOT, "results", "eai_multi_20261101")
MEC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mec")
PY_DUCK = _artifact.expand("python3")
PIN = ["nice", "-n", "5", "taskset", "-c", _artifact.core()]


def sh(cmd, **kw):
    return subprocess.run(cmd, capture_output=True, text=True, **kw)


def stats(vals):
    s = sorted(vals)
    return dict(median=statistics.median(vals), p05=s[max(0, int(len(s) * 0.05))],
                p95=s[min(len(s) - 1, int(len(s) * 0.95))], min=s[0], max=s[-1],
                mean=statistics.fmean(vals), reps=len(vals), raw=vals)


def rss_of(cmd):
    """Run cmd under /usr/bin/time -v and return (stdout, stderr, max_rss_kb, wall_ms)."""
    t0 = time.perf_counter()
    p = subprocess.run(["/usr/bin/time", "-v"] + cmd, capture_output=True, text=True)
    wall = (time.perf_counter() - t0) * 1e3
    rss = 0
    for line in p.stderr.splitlines():
        m = re.search(r"Maximum resident set size \(kbytes\): (\d+)", line)
        if m:
            rss = int(m.group(1))
    return p.stdout, p.stderr, rss, wall


# --------------------------------------------------------------------- part mt --
DUCK_SQL_SC = ("SELECT g, SUM(v), COUNT(*) FROM t WHERE v BETWEEN {v0} AND {v1} "
               "AND c1 BETWEEN {a0} AND {a1} AND c2 BETWEEN {b0} AND {b1} GROUP BY 1")


def duck_threads_case(db, cases, threads, reps, mode):
    """One process per thread count.  mode='config' passes threads at connect time,
    mode='set' opens with the default and issues SET threads afterwards (the pitfall)."""
    code = r"""
import json, statistics, sys, time
import duckdb
db, threads, reps, mode = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
sqls = json.loads(sys.argv[5])
cfg = {"threads": threads} if mode == "config" else {}
t0 = time.perf_counter()
con = duckdb.connect(db, read_only=True, config=cfg)
connect_ms = (time.perf_counter() - t0) * 1e3
if mode == "set":
    con.execute(f"SET threads={threads}")
applied = con.execute("SELECT current_setting('threads')").fetchone()[0]
out = {}
for name, sql in sqls.items():
    con.execute(sql).fetchall()
    ts = []
    for _ in range(reps):
        t = time.perf_counter()
        con.execute(sql).fetchall()
        ts.append((time.perf_counter() - t) * 1e3)
    s = sorted(ts)
    out[name] = dict(median=statistics.median(ts), p05=s[max(0,int(len(s)*0.05))],
                     p95=s[min(len(s)-1,int(len(s)*0.95))], min=s[0], max=s[-1],
                     mean=statistics.fmean(ts), reps=len(ts), raw=ts)
con.close()
print(json.dumps(dict(connect_ms=connect_ms, threads_requested=threads,
                      threads_applied=int(applied), mode=mode, cases=out)))
"""
    sqls = {}
    for c in cases:
        sqls[c["name"]] = DUCK_SQL_SC.format(v0=c["v"][0], v1=c["v"][1], a0=c["c1"][0],
                                            a1=c["c1"][1], b0=c["c2"][0], b1=c["c2"][1])
    p = sh(PIN + [PY_DUCK, "-c", code, db, str(threads), str(reps), mode, json.dumps(sqls)])
    if p.returncode != 0:
        raise SystemExit(f"duckdb child failed: {p.stderr[-800:]}")
    return json.loads(p.stdout)


def part_mt(args):
    os.makedirs(os.path.join(RES, "mt"), exist_ok=True)
    out = dict(protocol=dict(
        pin=" ".join(PIN), threads_env="MEC: --threads=N spawn-once pool; DuckDB: "
        "config={'threads': N} at connect, one process per N",
        duckdb=duckdb.__version__, note="median/p95 over reps; RSS from /usr/bin/time -v"),
        datasets={})
    cases_all = {}
    for ds, reps, duck_reps in (("tpch2", args.reps, args.duckdb_reps),
                                ("nyc3", args.reps_nyc, args.duckdb_reps_nyc)):
        dsd = os.path.join(RES, "datasets", ds)
        idx = os.path.join(RES, ds, "index.mec")
        cases = []
        for line in open(os.path.join(dsd, "cases.txt")):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            f = line.split()
            if len(f) != 7:
                continue
            cases.append(dict(name=f[0], v=[int(f[1]), int(f[2])], c1=[int(f[3]), int(f[4])],
                              c2=[int(f[5]), int(f[6])]))
        info = json.loads(sh([MEC, "info", idx]).stdout)
        rec = dict(index_bytes=info["bytes"], mode=info.get("mode"), rows=info["rows"],
                   build={}, query={}, duckdb={}, build_threads={})
        # ---- build scaling (deterministic: same bytes for every thread count)
        hashes = {}
        for t in (1, 4, 8, 16):
            tgz = os.path.join(RES, ds, f"index_t{t}.mec")
            budget = os.path.getsize(os.path.join(dsd, "data.parquet")) // 2
            cmd = PIN + [MEC, "build", os.path.join(dsd, "g.i64"), os.path.join(dsd, "v.i64"),
                         os.path.join(dsd, "c1.i64"), os.path.join(dsd, "c2.i64"), tgz, "0",
                         str(budget), f"--threads={t}"]
            _, err, rss, wall = rss_of(cmd)
            j = None
            for line in err.splitlines():
                if line.startswith('{"build_ms"'):
                    j = json.loads(line)
            if not j:
                raise SystemExit(f"build failed: {err[-600:]}")
            h = sh(["sha256sum", tgz]).stdout.split()[0]
            hashes[t] = dict(sha256=h, bytes=j["bytes"], build_ms=j["build_ms"],
                             phases=j.get("phase_ms"), rss_kb=rss,
                             threads_reported=j.get("threads"))
            os.remove(tgz)
            print(f"  {ds} build t={t:2d}: {j['build_ms']:8.1f} ms  rss={rss/1024:7.1f} MB", flush=True)
        rec["build"] = hashes
        rec["build_deterministic"] = len({v["sha256"] for v in hashes.values()}) == 1
        # ---- query scaling (pool)
        for t in (1, 4, 8, 16):
            cmd = PIN + [MEC, "bench", idx, str(reps), os.path.join(dsd, "cases.txt"),
                         f"--threads={t}"]
            stdout, err, rss, wall = rss_of(cmd)
            j = json.loads(stdout)
            rec["query"][str(t)] = dict(
                rss_kb=rss, wall_ms=wall, threads=j.get("threads"),
                cases={k: dict(sc=v["sc"], full=dict(median=v["median_us"],
                                                     p05=v["p05_us"], p95=v["p95_us"]))
                       for k, v in j["cases"].items()})
            print(f"  {ds} query t={t:2d}: " + " ".join(
                f"{k}={v['sc']['median_us']:.2f}us" for k, v in
                list(rec["query"][str(t)]["cases"].items())[:4]), flush=True)
        # ---- DuckDB same-thread control
        for t in (1, 4, 8, 16):
            rec["duckdb"][str(t)] = duck_threads_case(
                os.path.join(RES, ds, "oracle.duckdb"), cases, t, duck_reps, "config")
            med = statistics.median(x["median"] for x in rec["duckdb"][str(t)]["cases"].values())
            print(f"  {ds} duckdb t={t:2d}: median-of-cases {med:9.2f} ms "
                  f"(threads_applied={rec['duckdb'][str(t)]['threads_applied']})", flush=True)
        # ---- the SET-vs-config pitfall, same process family, 8 threads
        rec["duckdb_set_pitfall"] = dict(
            config=duck_threads_case(os.path.join(RES, ds, "oracle.duckdb"), cases, 8,
                                     max(3, duck_reps // 2), "config"),
            set_after_connect=duck_threads_case(os.path.join(RES, ds, "oracle.duckdb"), cases, 8,
                                                max(3, duck_reps // 2), "set"))
        out["datasets"][ds] = rec
        json.dump(out, open(os.path.join(RES, "mt", "mec_mt.json"), "w"), indent=1)
    json.dump(out, open(os.path.join(RES, "mt", "mec_mt.json"), "w"), indent=1)
    print("wrote", os.path.join(RES, "mt", "mec_mt.json"))


# ----------------------------------------------------------------- part budget --
def part_budget(args):
    """MEC01 vs MEC02 under one budget rule.  MEC01 = the shipped first version:
    value blocks only, uint16 cumulative planes, no first/last planes, no NULL group,
    W fixed at 16384.  Its footprint is reconstructed arithmetically from the same
    dataset shape, its latency comes from its archived receipt (same host/core/pin),
    and its *applicability* is decided by its own two hard limits."""
    MB = 1 << 20
    arch_path = args.mec01_receipt
    arch = {}
    if os.path.exists(arch_path):
        a = json.load(open(arch_path))
        arch = {k: dict(mec_sc_us=v["mec_sc"]["median_us"], mec_full_us=v["mec"]["median_us"])
                for k, v in a["cases"].items()}
    archived_build_ms = None
    if os.path.exists(arch_path):
        try:
            archived_build_ms = json.load(open(arch_path))["build"]["mec"]["build_ms"]
        except Exception:
            archived_build_ms = None
    out = dict(rule="index <= 0.5x projected source parquet (footprint <= 1.5x incl. source)",
               mec01=dict(source="receipts/multicol_cube_20261101/mec01_archive/mec_sweep.json",
                          w=16384, count_width=2, planes_bytes_per_entry=10,
                          limits=["uint16 cumulative counts -> run <= 65535 rows",
                                  "no NULL group keys", "value blocks only"]),
               datasets={})
    for ds in ("tpch2", "tpch2_null", "syn_cluster", "nyc3", "nyc3b"):
        meta = json.load(open(os.path.join(RES, "datasets", ds, "meta.json")))
        info = json.load(open(os.path.join(RES, ds, "info.json")))
        build = json.load(open(os.path.join(RES, ds, "build.json")))
        sweep = json.load(open(os.path.join(RES, ds, "sweep.json")))
        src = meta["parquet_bytes"]
        budget = src // 2
        R_mec01 = (meta["g0_distinct"] + (1 if meta["null_group_rows"] else 0)) * \
                  meta["c1_distinct"] * meta["c2_distinct"]
        vspan = meta["v_max"] - meta["v_min"] + 1
        W = 16384
        bp = vspan // W + 1
        planes = (bp + 1) * R_mec01 * 10
        bytes_mec01 = int(meta["rows"]) * 2 + planes + 8192
        rejects = []
        if meta["null_group_rows"]:
            rejects.append(f"NULL group keys ({meta['null_group_rows']:,} rows)")
        if build.get("max_run_rows", 0) > 65535:
            rejects.append(f"run of {build['max_run_rows']:,} rows > 65,535 (uint16 cap)")
        over_budget = bytes_mec01 > budget
        rec = dict(
            source_parquet_bytes=src, budget_bytes=budget,
            mec02=dict(bytes=info["bytes"], bytes_over_source=info["bytes"] / src,
                       footprint=(src + info["bytes"]) / src, mode=info.get("mode"),
                       build_ms=build["build_ms"],
                       mec_sc_us={k: v["mec_sc_us"] for k, v in sweep["cases"].items()},
                       mec_full_us={k: v["mec_full_us"] for k, v in sweep["cases"].items()}),
            mec01=dict(bytes=bytes_mec01, bytes_over_source=bytes_mec01 / src,
                       footprint=(src + bytes_mec01) / src, W=W, BP=bp, runs=R_mec01,
                       over_budget=over_budget, rejects=rejects,
                       applicable=(not rejects) and (not over_budget)),
            budget_verdict=sorted(
                {"MEC01" if (rejects or over_budget) else "MEC01 ok",
                 "MEC02" if info["bytes"] > budget else "MEC02 ok"}))
        if not rejects and not over_budget and ds == "tpch2":
            if arch:
                rec["mec01"].update(
                    build_ms=archived_build_ms,
                    mec_sc_us={k: arch[k]["mec_sc_us"] for k in arch},
                    mec_full_us={k: arch[k]["mec_full_us"] for k in arch},
                    latency_source="archived MEC01 receipt (same host/core/pin/protocol)")
            # the sc code path is identical in both versions, so MEC02 value mode
            # re-measures it on the same pins under the current harness
            rec["mec01"]["mec_sc_us_current"] = {k: v["mec_sc_us"]
                                                 for k, v in sweep["cases"].items()}
        out["datasets"][ds] = rec
        print(f"{ds:12s} MEC01 {'REJECTED: ' + '; '.join(rejects) if rejects else ('OVER BUDGET' if over_budget else 'ok')}"
              f" [{bytes_mec01/MB:.1f} MB vs budget {budget/MB:.1f} MB]  |  "
              f"MEC02 {info['bytes']/MB:.1f} MB ({info['bytes']/src:.3f}x)", flush=True)
    os.makedirs(os.path.join(RES, "budget"), exist_ok=True)
    json.dump(out, open(os.path.join(RES, "budget", "mec01_vs_mec02.json"), "w"), indent=1)
    print("wrote", os.path.join(RES, "budget", "mec01_vs_mec02.json"))


# ------------------------------------------------------------------- part cold --
DUCK_COLD_CHILD = r"""
import json, sys, time
import duckdb
db, sql = sys.argv[1], sys.argv[2]
t0 = time.perf_counter()
con = duckdb.connect(db, read_only=True, config={"threads": 1})
t1 = time.perf_counter()
rows = con.execute(sql).fetchall()
t2 = time.perf_counter()
con.close()
t3 = time.perf_counter()
print(json.dumps(dict(connect_ms=(t1-t0)*1e3, query_ms=(t2-t1)*1e3,
                      close_ms=(t3-t2)*1e3, in_process_ms=(t3-t0)*1e3,
                      nrows=len(rows))))
"""


def part_cold(args):
    out = dict(protocol=dict(
        pin=" ".join(PIN), launches=args.launches,
        mec="fresh `mec query` process per launch: parent wall (subprocess) and the child's "
            "own in-process timer (mmap + query + row formatting) reported separately",
        duckdb="fresh python process per launch, duckdb.connect(config={'threads':1}) + one "
               "execute+fetchall, parent wall and child in-process (connect+query+close)"),
        datasets={})
    ds = "tpch2"
    dsd = os.path.join(RES, "datasets", ds)
    idx = os.path.join(RES, ds, "index.mec")
    cases = [("broad", [0, 2000000, 0, 100, 1, 50]),
             ("selective", [1000000, 2000000, 0, 5, 1, 20]),
             ("empty", [5000000, 6000000, 0, 1, 1, 10])]
    rec = dict(mec={}, duckdb={})
    for name, p in cases:
        walls, child_wall, child_q, child_stored = [], [], [], []
        for _ in range(args.launches):
            t0 = time.perf_counter()
            pr = sh(PIN + [MEC, "query", idx] + [str(x) for x in p])
            walls.append((time.perf_counter() - t0) * 1e3)
            j = json.loads(pr.stderr.strip().splitlines()[-1])
            child_q.append(j["query_ms"])
            child_stored.append(j["stored_ms"])
        rec["mec"][name] = dict(params=p, parent_wall_ms=stats(walls),
                                child_query_ms=stats(child_q),
                                child_stored_ms=stats(child_stored))
        sql = (f"SELECT g, SUM(v), COUNT(*) FROM t WHERE v BETWEEN {p[0]} AND {p[1]} "
               f"AND c1 BETWEEN {p[2]} AND {p[3]} AND c2 BETWEEN {p[4]} AND {p[5]} GROUP BY 1")
        walls, inproc, connect, query = [], [], [], []
        for _ in range(args.launches):
            t0 = time.perf_counter()
            pr = sh(PIN + [PY_DUCK, "-c", DUCK_COLD_CHILD,
                           os.path.join(RES, ds, "oracle.duckdb"), sql])
            walls.append((time.perf_counter() - t0) * 1e3)
            j = json.loads(pr.stdout.strip().splitlines()[-1])
            inproc.append(j["in_process_ms"])
            connect.append(j["connect_ms"])
            query.append(j["query_ms"])
        rec["duckdb"][name] = dict(sql=sql, parent_wall_ms=stats(walls),
                                   child_in_process_ms=stats(inproc),
                                   child_connect_ms=stats(connect),
                                   child_query_ms=stats(query))
        print(f"{name:10s} MEC parent {rec['mec'][name]['parent_wall_ms']['median']:7.3f} ms "
              f"child {rec['mec'][name]['child_stored_ms']['median']:7.3f} ms | "
              f"DuckDB parent {rec['duckdb'][name]['parent_wall_ms']['median']:8.2f} ms "
              f"child {rec['duckdb'][name]['child_in_process_ms']['median']:8.2f} ms", flush=True)
    out["datasets"][ds] = rec
    os.makedirs(os.path.join(RES, "cold"), exist_ok=True)
    json.dump(out, open(os.path.join(RES, "cold", "mec_cold.json"), "w"), indent=1)
    print("wrote", os.path.join(RES, "cold", "mec_cold.json"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--part", required=True, choices=["mt", "budget", "cold"])
    ap.add_argument("--reps", type=int, default=200)
    ap.add_argument("--reps-nyc", type=int, default=30)
    ap.add_argument("--duckdb-reps", type=int, default=20)
    ap.add_argument("--duckdb-reps-nyc", type=int, default=3)
    ap.add_argument("--launches", type=int, default=21)
    ap.add_argument("--mec01-receipt",
                    default=os.path.join(RES, "mec01_archive", "mec_sweep.json"))
    args = ap.parse_args()
    {"mt": part_mt, "budget": part_budget, "cold": part_cold}[args.part](args)


if __name__ == "__main__":
    main()
