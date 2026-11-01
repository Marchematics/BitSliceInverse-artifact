#!/usr/bin/env python3
"""Randomised differential exactness test for MEC02 against DuckDB 2.0.

Answers every case twice -- the MEC index (through its own `mec query` path) and
DuckDB on the dataset's oracle table -- and compares the complete grouped result
element-wise on (g, SUM(v), COUNT(*), MIN(v), MAX(v)), with NULL groups compared as
"NULL".  Both operand sets (`full` and `sc`) are checked.

Value ranges are drawn from several regimes so the residual layer, the whole-block
layer, exact block boundaries, empty intersections and point values are all covered;
the (c1,c2) ranges are drawn over the real domains.  A fraction of cases also carries
a `g IN (...)` / `g NOT IN (...)` filter, which must drop the NULL group (SQL
semantics) -- that is the ECUBE02 NULL-group contract.
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import argparse
import hashlib
import json
import random
import subprocess
import sys

import duckdb

SQL = ("SELECT g, CAST(SUM(CAST(v AS HUGEINT)) AS HUGEINT) AS s, COUNT(*) AS c, "
       "MIN(v) AS mn, MAX(v) AS mx FROM t WHERE v BETWEEN {v0} AND {v1} "
       "AND c1 BETWEEN {a0} AND {a1} AND c2 BETWEEN {b0} AND {b1}{gf} GROUP BY 1 ORDER BY 1")


def canon(rows, width):
    out = []
    for r in rows:
        g = r[0]
        g = "NULL" if g is None or str(g) in ("None", "NULL", "null") else str(int(str(g)))
        out.append(tuple([g] + [int(x) for x in r[1:1 + width]]))
    return sorted(out)


def gen_cases(rng, n, vmin, vmax, w, a_rng, b_rng, groups):
    cases = []
    nb = max(1, (vmax - vmin) // w)
    span = max(1, vmax - vmin)
    for _ in range(n):
        mode = rng.choice(["full", "uniform", "narrow", "block_align", "outside", "point",
                           "uniform", "narrow"])
        if mode == "full":
            vlo, vhi = vmin - 1000, vmax + 1000
        elif mode == "uniform":
            a = rng.randint(vmin - span // 100, vmax + span // 100)
            b = rng.randint(vmin - span // 100, vmax + span // 100)
            vlo, vhi = min(a, b), max(a, b)
        elif mode == "narrow":
            vlo = rng.randint(vmin, vmax)
            vhi = vlo + rng.randint(0, max(1, span // 500))
        elif mode == "block_align":
            k = rng.randint(0, nb - 1)
            vlo = vmin + k * w
            k2 = rng.randint(k, nb)
            vhi = vmin + k2 * w - 1 if rng.random() < 0.5 else vmin + k2 * w
        elif mode == "outside":
            if rng.random() < 0.5:
                vlo, vhi = vmax + 1, vmax + span // 100 + 1
            else:
                vlo, vhi = vmin - span // 100 - 1, vmin - 1
        else:
            vlo = rng.randint(vmin, vmax)
            vhi = vlo
        if rng.random() < 0.2:
            alo = ahi = rng.randint(a_rng[0], a_rng[1])
        elif rng.random() < 0.15:
            alo, ahi = a_rng
        else:
            alo = rng.randint(a_rng[0], a_rng[1])
            ahi = rng.randint(alo, a_rng[1])
        if rng.random() < 0.2:
            blo = bhi = rng.randint(b_rng[0], b_rng[1])
        elif rng.random() < 0.15:
            blo, bhi = b_rng
        else:
            blo = rng.randint(b_rng[0], b_rng[1])
            bhi = rng.randint(blo, b_rng[1])
        gf, mecg, tag = "", [], ""
        r = rng.random()
        if groups and r < 0.12:
            k = min(len(groups), rng.randint(1, max(1, len(groups) - 1)))
            sel = rng.sample(groups, k)
            gf = " AND g IN (" + ",".join(str(x) for x in sel) + ")"
            mecg = ["gin=" + ",".join(str(x) for x in sel)]
            tag = "+gin"
        elif groups and r < 0.20:
            sel = [rng.choice(groups)]
            gf = " AND g NOT IN (" + ",".join(str(x) for x in sel) + ")"
            mecg = ["gnotin=" + ",".join(str(x) for x in sel)]
            tag = "+gnotin"
        cases.append(dict(mode=mode + tag, v=[vlo, vhi], c1=[alo, ahi], c2=[blo, bhi], gf=gf,
                          mecg=mecg))
    return cases


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mec", required=True)
    ap.add_argument("--index", required=True)
    ap.add_argument("--meta", required=True)
    ap.add_argument("--db", required=True)
    ap.add_argument("--cases", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--random-cases", type=int, default=200)
    ap.add_argument("--seed", type=int, default=20261101)
    args = ap.parse_args()

    meta = json.load(open(args.meta))
    info = json.loads(subprocess.run([args.mec, "info", args.index], capture_output=True,
                                     text=True).stdout)
    h = hashlib.sha256()
    with open(args.index, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)

    con = duckdb.connect(args.db, read_only=True)
    con.execute("SET threads=1")
    vmin, vmax, c1min, c1max, c2min, c2max = [
        int(x) for x in con.execute(
            "SELECT MIN(v), MAX(v), MIN(c1), MAX(c1), MIN(c2), MAX(c2) FROM t").fetchone()]
    groups = [int(x[0]) for x in con.execute(
        "SELECT DISTINCT g FROM t WHERE g IS NOT NULL ORDER BY 1").fetchall()]

    fixed = []
    with open(args.cases) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            p = line.split()
            if len(p) != 7:
                continue
            fixed.append(dict(mode="fixed:" + p[0], v=[int(p[1]), int(p[2])],
                              c1=[int(p[3]), int(p[4])], c2=[int(p[5]), int(p[6])], gf="",
                              mecg=[]))
    # in rank mode W is 0; use the average block width for the block-alignment regime
    w = info.get("W") or max(1, (vmax - vmin) // max(1, info.get("BP", 1)))
    rng = random.Random(args.seed)
    cases = fixed + gen_cases(rng, args.random_cases, vmin, vmax, w, (c1min, c1max),
                              (c2min, c2max), groups)

    fails = []
    for i, c in enumerate(cases):
        sql = SQL.format(v0=c["v"][0], v1=c["v"][1], a0=c["c1"][0], a1=c["c1"][1],
                         b0=c["c2"][0], b1=c["c2"][1], gf=c["gf"])
        duck_rows = con.execute(sql).fetchall()
        base = [args.mec, "query", args.index, str(c["v"][0]), str(c["v"][1]), str(c["c1"][0]),
                str(c["c1"][1]), str(c["c2"][0]), str(c["c2"][1])] + c["mecg"]
        p = subprocess.run(base, capture_output=True, text=True)
        if p.returncode != 0:
            raise SystemExit(f"mec query failed: {p.stderr[-400:]}")
        mrows = [(r["g"], r["sum"], r["count"], r["min"], r["max"]) for r in json.loads(p.stdout)]
        p2 = subprocess.run(base + ["sc"], capture_output=True, text=True)
        mrows_sc = [(r["g"], r["sum"], r["count"]) for r in json.loads(p2.stdout)]
        ok = (canon(duck_rows, 4) == canon(mrows, 4) and
              canon([(r[0], r[1], r[2]) for r in duck_rows], 3) == canon(mrows_sc, 3))
        if not ok:
            fails.append(dict(idx=i, case=c, duck=[[str(x) for x in r] for r in duck_rows],
                              mec=[[str(x) for x in r] for r in mrows]))
        if (i + 1) % 100 == 0:
            print(f"  {i+1}/{len(cases)} checked, {len(fails)} mismatches", flush=True)

    out = dict(seed=args.seed, total=len(cases), fixed=len(fixed), random=args.random_cases,
               mismatches=len(fails), index=args.index, index_sha256=h.hexdigest(),
               dataset=meta.get("name"), value_domain=[vmin, vmax],
               c1_domain=[c1min, c1max], c2_domain=[c2min, c2max],
               mode=info.get("mode"), block_width=info.get("W"), BP=info.get("BP"),
               groups=groups, modes=sorted({c["mode"] for c in cases}),
               group_filter_cases=sum(1 for c in cases if c["mecg"]), failures=fails[:10])
    json.dump(out, open(args.out, "w"), indent=1)
    print(json.dumps({k: v for k, v in out.items() if k != "failures"}, indent=1))
    if fails:
        sys.exit(3)


if __name__ == "__main__":
    main()
