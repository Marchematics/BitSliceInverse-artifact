#!/usr/bin/env python3
"""MEC02 master runner: for every prepared dataset, fit W, build the index, then
measure DuckDB 2.0, the per-group bitplane path and the sorted row-id range index
on the identical case set, verify exactness against DuckDB and write receipts.

Run with the PINNED DuckDB interpreter:  python3 (duckdb 2.0.0).
Every timing step runs pinned by the caller (run_mec.sh wraps this in
`nice -n 5 taskset -c 64`).
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import argparse
import hashlib
import json
import os
import statistics
import subprocess
import sys
import time

import duckdb

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, ".."))
RES = os.path.join(ROOT, "results")
OUT = os.path.join(RES, "eai_multi_20261101")
DSDIR = os.path.join(OUT, "datasets")

# per-dataset protocol budget: DuckDB on 100 M rows costs ~1-2 s per query
PROTO = {
    "tpch2":       dict(reps=100, duckdb_reps=100, bp_reps=40, sidx_reps=20, cli_reps=50, fuzz=300),
    "tpch2_null":  dict(reps=100, duckdb_reps=60,  bp_reps=30, sidx_reps=15, cli_reps=20, fuzz=200),
    "syn_cluster": dict(reps=100, duckdb_reps=60,  bp_reps=30, sidx_reps=15, cli_reps=20, fuzz=200),
    "nyc3":        dict(reps=20,  duckdb_reps=5,   bp_reps=10, sidx_reps=10, cli_reps=10, fuzz=100),
    "nyc3b":       dict(reps=20,  duckdb_reps=5,   bp_reps=10, sidx_reps=10, cli_reps=10, fuzz=100),
}

SQL_SC = ("SELECT g, SUM(v) AS s, COUNT(*) AS c FROM t WHERE v BETWEEN {v0} AND {v1} "
          "AND c1 BETWEEN {a0} AND {a1} AND c2 BETWEEN {b0} AND {b1} GROUP BY 1 ORDER BY 1")
SQL_FULL = ("SELECT g, CAST(SUM(CAST(v AS HUGEINT)) AS HUGEINT) AS s, COUNT(*) AS c, "
            "MIN(v) AS mn, MAX(v) AS mx FROM t WHERE v BETWEEN {v0} AND {v1} "
            "AND c1 BETWEEN {a0} AND {a1} AND c2 BETWEEN {b0} AND {b1} GROUP BY 1 ORDER BY 1")


def load_cases(path):
    cases = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            p = line.split()
            if len(p) != 7:
                continue
            cases.append(dict(name=p[0], v=[int(p[1]), int(p[2])], c1=[int(p[3]), int(p[4])],
                              c2=[int(p[5]), int(p[6])]))
    return cases


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def stats(vals):
    s = sorted(vals)
    return dict(median_ms=statistics.median(vals), min_ms=s[0],
                p95_ms=s[min(len(s) - 1, int(len(s) * 0.95))],
                mean_ms=statistics.fmean(vals), reps=len(vals), raw_ms=vals)


def canon_g(x):
    """Group key as a string: "NULL" for a NULL group, str(int) otherwise, so the
    canonical tuple list sorts identically on the DuckDB and MEC sides."""
    if x is None:
        return "NULL"
    s = str(x)
    if s in ("None", "NULL", "null"):
        return "NULL"
    return str(int(s))


def canon(rows, width):
    return sorted(tuple([canon_g(r[0])] + [int(x) for x in r[1:1 + width]]) for r in rows)


def run_cli(cmd, reps, parse):
    vals, last = [], None
    for _ in range(reps):
        p = subprocess.run(cmd, capture_output=True, text=True)
        if p.returncode != 0:
            raise SystemExit(f"failed: {' '.join(cmd)}\n{p.stderr[-500:]}")
        vals.append(parse(p))
        last = p
    return stats(vals), " ".join(cmd), last


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datasets", default="tpch2,tpch2_null,syn_cluster,nyc3,nyc3b")
    ap.add_argument("--outdir", default=OUT)
    ap.add_argument("--pin", default="nice -n 5 taskset -c 64")
    ap.add_argument("--skip-baselines", action="store_true")
    args = ap.parse_args()
    datasets = [d.strip() for d in args.datasets.split(",") if d.strip()]

    mec = os.path.join(HERE, "mec")
    bp = os.path.join(ROOT, "src_encoded_ops_20261030", "multibits")
    sidx = os.path.join(HERE, "sorted_index")
    master = dict(protocol=dict(
        pin=args.pin, duckdb="|".join([duckdb.__version__, "threads=1", "warm persistent conn"]),
        mec="warm process, 1 warmup + REP timed in-process queries (index mmap'd once)",
        bitplane="CLI re-run, internal query_ms", sorted_index="CLI re-run, internal query_ms",
        note="all ends exclude result serialisation"), datasets={})

    for name in datasets:
        ds = os.path.join(DSDIR, name)
        meta = json.load(open(os.path.join(ds, "meta.json")))
        cases = load_cases(os.path.join(ds, "cases.txt"))
        proto = PROTO.get(name, dict(reps=20, duckdb_reps=5, bp_reps=10, sidx_reps=10,
                                     cli_reps=5, fuzz=50))
        dout = os.path.join(args.outdir, name)
        os.makedirs(dout, exist_ok=True)
        src_bytes = meta["parquet_bytes"]
        budget = src_bytes // 2  # MEC <= 0.5x the projected source parquet
        idx = os.path.join(dout, "index.mec")
        print(f"\n=== {name}: rows={meta['rows']} G0={meta['g0_distinct']} nulls="
              f"{meta['null_group_rows']} D1={meta['c1_distinct']} D2={meta['c2_distinct']} "
              f"src={src_bytes/MB:.1f}MB budget={budget/MB:.1f}MB", flush=True)

        # ---- 1. plan + build ------------------------------------------------
        p = subprocess.run([mec, "plan", os.path.join(ds, "g.i64"), os.path.join(ds, "v.i64"),
                            os.path.join(ds, "c1.i64"), os.path.join(ds, "c2.i64"), str(budget)],
                           capture_output=True, text=True)
        if p.returncode != 0:
            raise SystemExit(p.stderr[-500:])
        plan = json.loads(p.stdout)
        json.dump(plan, open(os.path.join(dout, "plan.json"), "w"), indent=1)
        p = subprocess.run([mec, "build", os.path.join(ds, "g.i64"), os.path.join(ds, "v.i64"),
                            os.path.join(ds, "c1.i64"), os.path.join(ds, "c2.i64"), idx, "0",
                            str(budget)], capture_output=True, text=True)
        if p.returncode != 0:
            raise SystemExit(p.stderr[-800:])
        build = json.loads(p.stderr.strip().splitlines()[-1])
        json.dump(build, open(os.path.join(dout, "build.json"), "w"), indent=1)
        info = json.loads(subprocess.run([mec, "info", idx], capture_output=True,
                                         text=True).stdout)
        json.dump(info, open(os.path.join(dout, "info.json"), "w"), indent=1)
        bs = (f"W={build['W']} BP={build['BP']}" if info.get("mode", "value") == "value"
              else f"rank BP={build['BP']} cap={build['BPcap']}")
        print(f"  plan: mode={plan.get('mode')} "
              f"pick={plan.get('picked_W', plan.get('picked_BP'))} "
              f"bytes={plan['picked_bytes']/MB:.1f}MB | build {build['build_ms']:.0f} ms "
              f"cw={build['count_width']} {bs} index={info['bytes']/MB:.1f}MB", flush=True)

        # ---- 2. DuckDB oracle + timing --------------------------------------
        con = duckdb.connect(os.path.join(dout, "oracle.duckdb"))
        con.execute("CREATE OR REPLACE TABLE t AS SELECT * FROM read_parquet(?)",
                    [os.path.join(ds, "data.parquet")])
        con.execute("SET threads=1")
        con.execute("SET preserve_insertion_order=true")
        duck = {}
        for c in cases:
            sql_sc = SQL_SC.format(v0=c["v"][0], v1=c["v"][1], a0=c["c1"][0], a1=c["c1"][1],
                                   b0=c["c2"][0], b1=c["c2"][1])
            sql_full = SQL_FULL.format(v0=c["v"][0], v1=c["v"][1], a0=c["c1"][0], a1=c["c1"][1],
                                       b0=c["c2"][0], b1=c["c2"][1])
            con.execute(sql_sc).fetchall()
            ts = []
            for _ in range(proto["duckdb_reps"]):
                t = time.perf_counter()
                rows_sc = con.execute(sql_sc).fetchall()
                ts.append((time.perf_counter() - t) * 1e3)
            ts2 = []
            con.execute(sql_full).fetchall()
            for _ in range(proto["duckdb_reps"]):
                t = time.perf_counter()
                rows_full = con.execute(sql_full).fetchall()
                ts2.append((time.perf_counter() - t) * 1e3)
            duck[c["name"]] = dict(
                sql_sc=sql_sc, sql_full=sql_full,
                sc=stats(ts), full=stats(ts2),
                rows=[[str(x) for x in r] for r in rows_full])
            print(f"  duckdb {c['name']:20s} sc {duck[c['name']]['sc']['median_ms']:9.3f} ms "
                  f"full {duck[c['name']]['full']['median_ms']:9.3f} ms rows={len(rows_full)}",
                  flush=True)
        con.close()

        # ---- 3. bitplane baseline -------------------------------------------
        bp_res = {}
        if not args.skip_baselines:
            bpidx = os.path.join(dout, "mb.bin")
            t0 = time.time()
            p = subprocess.run([bp, "build", os.path.join(ds, "g.i64"), os.path.join(ds, "v.i64"),
                                os.path.join(ds, "c1.i64"), os.path.join(ds, "c2.i64"), bpidx],
                               capture_output=True, text=True)
            if p.returncode != 0:
                raise SystemExit(f"bitplane build failed: {p.stderr[-400:]}")
            bp_build = dict(build_ms=json.loads(p.stderr.strip().splitlines()[-1])["build_ms"],
                            wall_ms=(time.time() - t0) * 1e3,
                            bytes=os.path.getsize(bpidx))
            for c in cases:
                cmd = [bp, "query", bpidx, "/tmp/_bp_rows.json",
                       str(c["v"][0]), str(c["v"][1]), str(c["c1"][0]), str(c["c1"][1]),
                       str(c["c2"][0]), str(c["c2"][1])]
                st, cmdline, last = run_cli(
                    cmd, proto["bp_reps"],
                    lambda pr: json.loads(pr.stderr.strip().splitlines()[-1])["query_ms"])
                rows = json.load(open("/tmp/_bp_rows.json"))["rows"]
                bp_res[c["name"]] = dict(stats=st, cmd=cmdline,
                                         rows=[[str(x) for x in r] for r in rows])
                print(f"  bitplane {c['name']:20s} {st['median_ms']:9.3f} ms", flush=True)
            os.remove(bpidx)
        else:
            bp_build = None

        # ---- 4. sorted row-id range index baseline --------------------------
        sidx_res, sidx_build = {}, None
        if not args.skip_baselines:
            sidxidx = os.path.join(dout, "ridx.bin")
            t0 = time.time()
            p = subprocess.run([sidx, "build", os.path.join(ds, "g.i64"), os.path.join(ds, "v.i64"),
                                os.path.join(ds, "c1.i64"), os.path.join(ds, "c2.i64"), sidxidx],
                               capture_output=True, text=True)
            if p.returncode != 0:
                raise SystemExit(f"sorted index build failed: {p.stderr[-400:]}")
            sidx_build = dict(build_ms=json.loads(p.stderr.strip().splitlines()[-1])["build_ms"],
                              wall_ms=(time.time() - t0) * 1e3,
                              bytes=os.path.getsize(sidxidx))
            for c in cases:
                cmd = [sidx, "query", sidxidx, os.path.join(ds, "g.i64"),
                       os.path.join(ds, "v.i64"), os.path.join(ds, "c1.i64"),
                       os.path.join(ds, "c2.i64"), "/tmp/_sidx_rows.json",
                       str(c["v"][0]), str(c["v"][1]), str(c["c1"][0]), str(c["c1"][1]),
                       str(c["c2"][0]), str(c["c2"][1])]
                st, cmdline, last = run_cli(
                    cmd, proto["sidx_reps"],
                    lambda pr: json.loads(pr.stderr.strip().splitlines()[-1])["query_ms"])
                rows = json.load(open("/tmp/_sidx_rows.json"))["rows"]
                sidx_res[c["name"]] = dict(stats=st, cmd=cmdline,
                                           rows=[[str(x) for x in r] for r in rows])
                print(f"  sorted-idx {c['name']:20s} {st['median_ms']:9.3f} ms", flush=True)
            os.remove(sidxidx)

        # ---- 5. MEC warm bench ---------------------------------------------
        p = subprocess.run([mec, "bench", idx, str(proto["reps"]), os.path.join(ds, "cases.txt")],
                           capture_output=True, text=True, timeout=3600)
        if p.returncode != 0:
            raise SystemExit(f"mec bench failed: {p.stderr[-800:]}")
        mecres = json.loads(p.stdout)

        # ---- 6. MEC one-shot CLI (same boundary as the baseline CLIs) -------
        cli_res = {}
        if proto["cli_reps"] > 0:
            for c in cases:
                cmd = [mec, "query", idx, str(c["v"][0]), str(c["v"][1]), str(c["c1"][0]),
                       str(c["c1"][1]), str(c["c2"][0]), str(c["c2"][1]), "sc"]
                st, cmdline, _ = run_cli(
                    cmd, proto["cli_reps"],
                    lambda pr: json.loads(pr.stderr.strip().splitlines()[-1])["query_ms"])
                cli_res[c["name"]] = dict(stats=st, cmd=cmdline)

        # ---- 7. exactness + speedups ---------------------------------------
        res = dict(name=name, meta=meta, proto=proto, plan=plan, build=build, info=info,
                   storage=dict(source_parquet_bytes=src_bytes, mec_bytes=info["bytes"],
                                budget_bytes=budget,
                                footprint_ratio=(src_bytes + info["bytes"]) / src_bytes,
                                mec_over_source=info["bytes"] / src_bytes,
                                bitplane_bytes=(bp_build["bytes"] if bp_build else None),
                                sorted_index_bytes=(sidx_build["bytes"] if sidx_build else None)),
                   baselines_build=dict(bitplane=bp_build, sorted_index=sidx_build),
                   cases={})
        exact_all = True
        for c in cases:
            n = c["name"]
            d = duck[n]
            m = mecres["cases"][n]
            # SQL_FULL projects (g, SUM, COUNT, MIN, MAX)
            duck_full = [(r[0], r[1], r[2], r[3], r[4]) for r in d["rows"]]
            duck_sc = [(r[0], r[1], r[2]) for r in d["rows"]]
            mec_full = [(r["g"], r["sum"], r["count"], r["min"], r["max"]) for r in m["rows"]]
            mec_sc = [(r["g"], r["sum"], r["count"]) for r in m["rows"]]
            ex_full = canon(duck_full, 4) == canon(mec_full, 4)
            ex_sc = canon(duck_sc, 2) == canon(mec_sc, 2)
            rec = dict(params=dict(v=c["v"], c1=c["c1"], c2=c["c2"]),
                       mec_sc_us=m["sc"]["median_us"], mec_sc_p95_us=m["sc"]["p95_us"],
                       mec_full_us=m["median_us"], mec_full_p95_us=m["p95_us"],
                       mec_rows=m["rows"],
                       duckdb_sc_ms=d["sc"]["median_ms"], duckdb_full_ms=d["full"]["median_ms"],
                       duckdb_rows=d["rows"], exact=dict(full=ex_full, sc=ex_sc))
            if n in bp_res:
                rec["bitplane_ms"] = bp_res[n]["stats"]["median_ms"]
                # the repository bitplane builder skips rows with g < 0, so on a dataset
                # with NULL group keys it cannot emit that group at all; compare on the
                # rows it does emit and record the gap explicitly.
                bp_rows = [tuple(str(x) for x in r) for r in bp_res[n]["rows"]]
                dn = [(r[0], r[1], r[2]) for r in duck_sc if canon_g(r[0]) != "NULL"]
                rec["exact"]["bitplane_nonnull"] = canon(bp_rows, 2) == canon(dn, 2)
                rec["exact"]["bitplane_null_group_supported"] = not bool(meta["null_group_rows"])
            if n in sidx_res:
                rec["sorted_index_ms"] = sidx_res[n]["stats"]["median_ms"]
                rec["exact"]["sorted_index"] = canon(sidx_res[n]["rows"], 4) == canon(duck_full, 4)
            if n in cli_res:
                rec["mec_cli_ms"] = cli_res[n]["stats"]["median_ms"]
            rec["speedup"] = dict(
                vs_duckdb_sc=d["sc"]["median_ms"] / (m["sc"]["median_us"] / 1000.0),
                vs_duckdb_full=d["full"]["median_ms"] / (m["median_us"] / 1000.0),
                vs_bitplane=(rec["bitplane_ms"] / (m["sc"]["median_us"] / 1000.0)
                             if "bitplane_ms" in rec else None),
                vs_sorted_index=(rec["sorted_index_ms"] / (m["sc"]["median_us"] / 1000.0)
                                 if "sorted_index_ms" in rec else None),
                vs_bitplane_cli=(rec["bitplane_ms"] / rec["mec_cli_ms"]
                                 if ("bitplane_ms" in rec and "mec_cli_ms" in rec) else None),
                vs_sorted_index_cli=(rec["sorted_index_ms"] / rec["mec_cli_ms"]
                                     if ("sorted_index_ms" in rec and "mec_cli_ms" in rec) else None),
            )
            exact_all = exact_all and ex_full and ex_sc
            # bitplane is compared on the rows it can emit (it drops NULL group keys)
            exact_all = exact_all and rec["exact"].get("bitplane_nonnull", True)
            exact_all = exact_all and rec["exact"].get("sorted_index", True)
            res["cases"][n] = rec
            print(f"  MEC {n:20s} sc {rec['mec_sc_us']:8.2f}us full {rec['mec_full_us']:8.2f}us "
                  f"| xduck {rec['speedup']['vs_duckdb_sc']:8.1f}x "
                  f"xbp {(rec['speedup']['vs_bitplane'] or 0):8.1f}x "
                  f"xsorted {(rec['speedup']['vs_sorted_index'] or 0):8.1f}x "
                  f"exact={'yes' if ex_full and ex_sc else 'NO'}", flush=True)
        res["exact_all"] = exact_all
        json.dump(res, open(os.path.join(dout, "sweep.json"), "w"), indent=1)

        # ---- 8. randomised differential fuzz --------------------------------
        fz = os.path.join(dout, "diff_fuzz.json")
        p = subprocess.run([sys.executable, os.path.join(HERE, "diff_fuzz_mec.py"),
                            "--mec", mec, "--index", idx, "--meta", os.path.join(ds, "meta.json"),
                            "--db", os.path.join(dout, "oracle.duckdb"),
                            "--cases", os.path.join(ds, "cases.txt"), "--out", fz,
                            "--random-cases", str(proto["fuzz"])],
                           capture_output=True, text=True, timeout=3600)
        if p.returncode != 0:
            print(p.stdout[-2000:]); print(p.stderr[-2000:])
            raise SystemExit(f"fuzz failed for {name}")
        fuzz = json.load(open(fz))
        print(f"  fuzz: {fuzz['total']} cases, {fuzz['mismatches']} mismatches", flush=True)

        master["datasets"][name] = dict(
            meta=meta, plan=plan, build=build, info=info, storage=res["storage"],
            baselines_build=res["baselines_build"], exact_all=exact_all,
            fuzz=dict(total=fuzz["total"], mismatches=fuzz["mismatches"]),
            cases=res["cases"])

    json.dump(master, open(os.path.join(args.outdir, "MEC_ALL.json"), "w"), indent=1)
    print("\nwrote", os.path.join(args.outdir, "MEC_ALL.json"))
    ok = all(v["exact_all"] and v["fuzz"]["mismatches"] == 0 for v in master["datasets"].values())
    print("exact everywhere:", ok)
    if not ok:
        sys.exit(3)


MB = 1 << 20

if __name__ == "__main__":
    main()
