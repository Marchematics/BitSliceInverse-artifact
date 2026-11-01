#!/usr/bin/env python3
"""Materialise every (group, value) pair as int32 exports + BSSTR01 dict and
build the ECUBE01 cube with the SPECS binary.  Also creates the 1M-row narrow
fuzz tables (deterministic reservoir samples) and records per-pair statistics
used by the spec generator.

Usage:  materialize.py [--only ID[,ID...]] [--force] [--skip-eval]
Outputs: <BASE>/tables/<pair>/{g.i32,v.i32,d.dict,meta.json}
         <BASE>/cubes/<pair>.ecube
         <BASE>/out/pairs_meta.json
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
import struct
import subprocess
import sys
import time

import duckdb
import pyarrow as pa

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pairs as P
import bsstr

CUBE = os.environ.get("CUBE", f"{P.BASE}/bin/enc_cube_specs")
SEED = 20261101


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def q1(con, sql):
    return con.execute(sql).fetchone()


def ensure_eval(force=False):
    os.makedirs(os.path.dirname(P.EVAL), exist_ok=True)
    con = duckdb.connect(P.EVAL)
    con.execute("SET threads=1")
    have = {r[0] for r in con.execute(
        "SELECT table_name FROM information_schema.tables").fetchall()}
    missing = [t for t, _, _ in P.FUZZ_TABLES if t not in have]
    if force:
        missing = [t for t, _, _ in P.FUZZ_TABLES]
    if missing:
        con.execute(f"ATTACH '{P.NYC2023}' AS src_nyc (READ_ONLY)")
        con.execute(f"ATTACH '{P.TPCH}' AS src_tpch (READ_ONLY)")
        for tid, sql, note in P.FUZZ_TABLES:
            if tid not in missing:
                continue
            t0 = time.time()
            con.execute(f"DROP TABLE IF EXISTS {tid}")
            con.execute(f"CREATE TABLE {tid} AS {sql}")
            n = q1(con, f"SELECT count(*) FROM {tid}")[0]
            print(f"[eval] {tid}: n={n} ({time.time()-t0:.2f}s) {note}", flush=True)
        con.execute("DETACH src_nyc")
        con.execute("DETACH src_tpch")
    else:
        print("[eval] all fuzz tables present", flush=True)
    con.close()


def export_pair(con, pr, force):
    d = f"{P.BASE}/tables/{pr['id']}"
    os.makedirs(d, exist_ok=True)
    meta_path = f"{d}/meta.json"
    if os.path.exists(meta_path) and not force:
        old = json.load(open(meta_path))
        cube = f"{P.BASE}/cubes/{pr['id']}.ecube"
        if os.path.exists(cube) and os.path.getsize(cube) == old.get("cube_bytes"):
            print(f"[{pr['id']}] cached", flush=True)
            return old
    g_expr, v_expr, src = pr["g_expr"], pr["v_expr"], pr["src"]
    base = f"(SELECT {g_expr} AS g, {v_expr} AS v FROM {src})"
    t0 = time.time()
    n, G, V, vmin, vmax, gnull, vnull = q1(con, f"""
        SELECT count(*), count(DISTINCT g), count(DISTINCT v), min(v), max(v),
               sum(CASE WHEN g IS NULL THEN 1 ELSE 0 END),
               sum(CASE WHEN v IS NULL THEN 1 ELSE 0 END)
        FROM {base}""")
    assert vnull == 0, f"{pr['id']}: NULL measure values are outside ECUBE01"
    assert vmin is not None, f"{pr['id']}: empty pair"
    if not (-2**31 <= vmin and vmax <= 2**31 - 1):
        raise SystemExit(f"{pr['id']}: measure does not fit int32: [{vmin},{vmax}]")
    names = [str(r[0]) for r in con.execute(
        f"SELECT g FROM (SELECT DISTINCT g AS g FROM {base}) WHERE g IS NOT NULL ORDER BY g").fetchall()]
    assert len(names) == G, (pr["id"], len(names), G)
    # int32 exports: dense rank over non-NULL group keys; NULL -> -1
    mat_sql = f"""
        SELECT CASE WHEN b.g IS NULL THEN -1 ELSE m.gid END AS gid,
               CAST(b.v AS INTEGER) AS val
        FROM {base} b
        LEFT JOIN (SELECT g AS gv,
                          CAST(DENSE_RANK() OVER (ORDER BY g NULLS LAST) - 1 AS INTEGER) AS gid
                   FROM (SELECT DISTINCT g AS g FROM {base}) WHERE g IS NOT NULL) m
          ON b.g = m.gv"""
    reader = con.sql(mat_sql).to_arrow_reader(1 << 20)
    ng = open(f"{d}/g.i32", "wb")
    nv = open(f"{d}/v.i32", "wb")
    written = 0
    for batch in reader:
        assert batch.num_columns == 2
        ga, va = batch.column(0), batch.column(1)
        assert ga.type == pa.int32() and va.type == pa.int32(), (ga.type, va.type)
        assert ga.null_count == 0 and va.null_count == 0, "materialiser produced NULLs"
        for arr, fh in ((ga, ng), (va, nv)):
            off = arr.offset * 4
            buf = memoryview(arr.buffers()[1]).cast("B")
            fh.write(buf[off:off + len(arr) * 4])
        written += len(ga)
    ng.close()
    nv.close()
    assert written == n, (pr["id"], written, n)
    bsstr.write_bsstr(f"{d}/d.dict", names)
    cube = f"{P.BASE}/cubes/{pr['id']}.ecube"
    t_build = time.time()
    p = subprocess.run([CUBE, "build", f"{d}/g.i32", f"{d}/v.i32", cube],
                       capture_output=True, text=True)
    if p.returncode != 0:
        raise SystemExit(f"{pr['id']} build failed: {p.stderr[-500:]}")
    build_info = json.loads(p.stderr.strip().splitlines()[-1])
    # --- statistics for the spec generator -----------------------------------
    vals = [r[0] for r in con.execute(
        f"SELECT v FROM (SELECT DISTINCT v AS v FROM {base}) USING SAMPLE 64 ROWS (reservoir, {SEED})").fetchall()]
    if V <= 40000:
        allv = [r[0] for r in con.execute(
            f"SELECT v FROM (SELECT DISTINCT v AS v FROM {base}) ORDER BY v").fetchall()]
        win_lo, win_hi = allv[:40], allv[-40:]
        adj = [[allv[i], allv[i + 1]] for i in range(min(30, len(allv) - 1))]
        full_vals = allv
    else:
        win_lo = [r[0] for r in con.execute(
            f"SELECT v FROM (SELECT DISTINCT v AS v FROM {base}) ORDER BY v LIMIT 40").fetchall()]
        win_hi = [r[0] for r in con.execute(
            f"SELECT v FROM (SELECT DISTINCT v AS v FROM {base}) ORDER BY v DESC LIMIT 40").fetchall()]
        adj = [[win_lo[i], win_lo[i + 1]] for i in range(min(30, len(win_lo) - 1))]
        full_vals = None
    agg_sql = f"""
        SELECT min(s), max(s), min(c), max(c), min(mn), max(mn), min(mx), max(mx),
               quantile_cont(s, 0.1), quantile_cont(s, 0.5), quantile_cont(s, 0.9),
               quantile_cont(c, 0.1), quantile_cont(c, 0.5), quantile_cont(c, 0.9),
               quantile_cont(mn, 0.1), quantile_cont(mn, 0.5), quantile_cont(mn, 0.9),
               quantile_cont(mx, 0.1), quantile_cont(mx, 0.5), quantile_cont(mx, 0.9)
        FROM (SELECT g, CAST(SUM(v) AS BIGINT) s, COUNT(*) c, MIN(v) mn, MAX(v) mx
              FROM {base} GROUP BY g)"""
    a = q1(con, agg_sql)
    def iv(x):
        return None if x is None else int(x)
    agg = dict(s_min=iv(a[0]), s_max=iv(a[1]), c_min=iv(a[2]), c_max=iv(a[3]),
               mn_min=iv(a[4]), mn_max=iv(a[5]), mx_min=iv(a[6]), mx_max=iv(a[7]),
               s_q=[iv(a[8]), iv(a[9]), iv(a[10])],
               c_q=[iv(a[11]), iv(a[12]), iv(a[13])],
               mn_q=[iv(a[14]), iv(a[15]), iv(a[16])],
               mx_q=[iv(a[17]), iv(a[18]), iv(a[19])])
    sample_names = [str(r[0]) for r in con.execute(
        f"SELECT g FROM (SELECT DISTINCT g AS g FROM {base}) WHERE g IS NOT NULL "
        f"USING SAMPLE 300 ROWS (reservoir, {SEED})").fetchall()]
    old_meta = {}
    if os.path.exists(meta_path):
        try:
            old_meta = json.load(open(meta_path))
        except Exception:
            old_meta = {}
    meta = dict(id=pr["id"], label=pr["label"], note=pr["note"], db=pr["db"], src=src,
                g_expr=g_expr, v_expr=v_expr, g_type=pr["g_type"],
                n=n, ngroups=G, nvalues=V, vmin=vmin, vmax=vmax,
                has_null=gnull > 0, null_rows=gnull,
                dense_in_cube=int(build_info.get("dense", -1)),
                dense_valid=(int(build_info.get("dense", 0)) == 1 and
                             (vmax - vmin + 1) == V),
                span=vmax - vmin + 1,
                build_ms=build_info.get("build_ms"), cube_bytes=os.path.getsize(cube),
                cube_sha256=sha256_file(cube),
                dict=f"{d}/d.dict", dict_sha256=sha256_file(f"{d}/d.dict"),
                names_n=len(names),
                names=names if G <= 2000 else None,
                sample_names=sample_names,
                sample_vals=vals, window_lo=win_lo, window_hi=win_hi, adj=adj,
                all_vals=full_vals,
                agg=agg, materialize_ms=(time.time() - t0) * 1000.0)
    # keep the encoded-build identity if it was recorded before
    if "enc_cube_sha256" in old_meta:
        meta["enc_cube_sha256"] = old_meta["enc_cube_sha256"]
    json.dump(meta, open(meta_path, "w"))
    print(f"[{pr['id']}] n={n} G={G} V={V} dense={meta['dense_in_cube']} "
          f"cube={meta['cube_bytes']/1e6:.1f}MB mat={meta['materialize_ms']/1000:.2f}s "
          f"build={build_info.get('build_ms',0)/1000:.2f}s", flush=True)
    return meta


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default="")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--skip-eval", action="store_true")
    args = ap.parse_args()
    if not args.skip_eval:
        ensure_eval(force=args.force)
    all_pairs = P.CORPUS_PAIRS + P.FUZZ_PAIRS
    if args.only:
        want = set(args.only.split(","))
        all_pairs = [p for p in all_pairs if p["id"] in want]
    os.makedirs(f"{P.BASE}/cubes", exist_ok=True)
    os.makedirs(f"{P.BASE}/out", exist_ok=True)
    metas = {}
    for pr in all_pairs:
        t0 = time.time()
        con = duckdb.connect(pr["db"], read_only=True)
        con.execute("SET threads=1")
        con.execute("SET preserve_insertion_order=true")
        q = dict(pr)
        m = export_pair(con, q, args.force)
        m["src_sql"] = pr["src"]
        metas[pr["id"]] = m
        con.close()
        print(f"  total {time.time()-t0:.1f}s", flush=True)
    # merge with previously computed metas for pairs not touched
    out_path = f"{P.BASE}/out/pairs_meta.json"
    merged = {}
    if os.path.exists(out_path):
        try:
            merged = json.load(open(out_path))
        except Exception:
            merged = {}
    merged.update(metas)
    json.dump(merged, open(out_path, "w"), indent=1)
    print(f"wrote {out_path}: {len(merged)} pairs", flush=True)


if __name__ == "__main__":
    main()
