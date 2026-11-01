#!/usr/bin/env python3
"""Randomized differential campaign: ECUBE01 vs DuckDB 2.0 as the reference.

The campaign is deterministic (single seed).  Query plans are generated once and
DuckDB's exact grouped result is computed once per plan; the *same* plans are
then executed against two ECUBE01 binaries + their own cube sets:

  patched : enc_cube_specs.cpp  (verbatim enc_cube.cpp + SPEC-RUNNER FIX 1/2/3)
  prefix  : enc_cube_orig.cpp   (verbatim enc_cube.cpp, unmodified)

so the receipt measures both post-fix exactness and the pre-fix bug rate.

Usage:
  fuzz.py --n 100000 [--limit] [--seed 20261101] [--patched-bin ...]
          [--prefix-bin ...] [--patched-cubes ...] [--prefix-cubes ...]
          [--out ...] [--plans-out ...]
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import argparse
import collections
import hashlib
import json
import os
import platform
import random
import subprocess
import sys
import time

import duckdb

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pairs as P
import bsstr
import gen_specs as G

BASE = P.BASE
EVAL = P.EVAL
SEED = 20261101

# category -> sampling weight (sums to 100)
CATS = [("fixed_groupby", 2), ("range_two_sided", 22), ("range_one_sided", 10),
        ("single_value", 7), ("boundary_values", 9), ("empty_result", 7),
        ("group_in", 8), ("group_not_in", 8), ("having", 9), ("having_and", 5),
        ("combined_predicates", 9), ("multi_aggregate", 4)]
CAT_NAMES = [c for c, _ in CATS]
CAT_WEIGHTS = [w for _, w in CATS]
PAIR_WEIGHT = {"F10": 0.5}   # 25k-group pair is heavier per query


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def sql_and_args(m, preds, hconds):
    where = ""
    if preds:
        where = " WHERE " + " AND ".join(p["sql"] for p in preds)
    having = ""
    if hconds:
        having = " HAVING " + " AND ".join(
            f"{G.A_SQL[a].format(v='v')} {op} {val}" for a, op, val in hconds)
    sql = (f"SELECT g, SUM(v) AS s, COUNT(*) AS c, MIN(v) AS mn, MAX(v) AS mx "
           f"FROM {m['src']}{where} GROUP BY g{having}")
    args = [p["arg"] for p in preds] + [
        "having=" + f"{a}{op}{val}" for a, op, val in hconds]
    return sql, args


def pick_pair(rng, pids):
    w = [PAIR_WEIGHT.get(p, 1.0) for p in pids]
    return rng.choices(pids, weights=w)[0]


def gen_one(rng, pid, m):
    cat = rng.choices(CAT_NAMES, weights=CAT_WEIGHTS)[0]
    vp = "v"
    pool = G.names_of(m)
    preds, hconds = [], []
    if cat == "fixed_groupby":
        pass
    elif cat == "range_two_sided":
        mode = rng.choice(["prefix", "centered", "suffix", "two_obs", "two_obs"])
        frac = rng.choice([0.0001, 0.01, 0.05, 0.1, 0.25, 0.5, 0.9, 0.99])
        lo, hi = G.rng_range(m, rng, mode=mode, frac=frac)
        preds = [G.pred_range(vp, lo, hi)]
    elif cat == "range_one_sided":
        vals = G.observed(m)
        v = vals[rng.randrange(len(vals))]
        preds = [G.pred_range(vp, lo=v)] if rng.random() < 0.5 else [G.pred_range(vp, hi=v)]
    elif cat == "single_value":
        if rng.random() < 0.6:
            vals = G.observed(m)
            v = vals[rng.randrange(len(vals))]
        else:
            v = rng.randint(m["vmin"], m["vmax"])
        preds = [G.pred_range(vp, v, v)]
    elif cat == "boundary_values":
        vals = G.observed(m)
        v = vals[rng.randrange(len(vals))]
        if m.get("adj") and rng.random() < 0.4:
            a, b = rng.choice(m["adj"])
            lo, hi = rng.choice([(a, b), (a - 1, a), (a, a + 1), (a - 1, b + 1)])
        else:
            lo, hi = rng.choice([(v, v), (m["vmin"] - 1, m["vmin"]),
                                 (m["vmax"], m["vmax"] + 1), (m["vmin"], m["vmax"]),
                                 (-2 ** 31, 2 ** 31 - 1), (-2 ** 63, 2 ** 63 - 1)])
        preds = [G.pred_range(vp, lo, hi)]
    elif cat == "empty_result":
        vals = G.observed(m)
        v = vals[rng.randrange(len(vals))]
        kind = rng.randrange(6)
        if kind == 0:
            preds = [G.pred_range(vp, m["vmax"] + rng.randint(1, 1000),
                                  m["vmax"] + rng.randint(1001, 5000))]
        elif kind == 1:
            preds = [G.pred_range(vp, v + 1 + rng.randint(0, 10), v)]
        elif kind == 2:
            preds = [G.pred_in("g", m, [G.nonexistent(m, rng)])]
        elif kind == 3:
            names = G.names_of(m)
            preds = [G.pred_notin("g", m, names)]
        elif kind == 4:
            hconds = [("sum", ">=", 10 ** 18)]
        else:
            un = G.unobserved_inside(m)
            if un is not None:
                preds = [G.pred_range(vp, un, un)]
            else:
                preds = [G.pred_range(vp, m["vmin"] - 5000, m["vmin"] - 1)]
    elif cat == "group_in":
        k = rng.choice([1, 1, 2, 3, min(5, len(pool))])
        names = rng.sample(list(pool), min(k, len(pool)))
        if rng.random() < 0.1:
            names = names + [G.nonexistent(m, rng)]
        preds = [G.pred_in("g", m, names)]
    elif cat == "group_not_in":
        if len(pool) <= 1:
            return None
        k = rng.choice([1, 1, 2, 3, min(4, len(pool) - 1)])
        names = rng.sample(list(pool), min(k, len(pool)))
        preds = [G.pred_notin("g", m, names)]
    elif cat == "having":
        a = rng.choice(G.A_ORDER)
        op = rng.choice([">=", "<=", ">", "<", "="])
        q = [x for x in m["agg"][G.A_QKEY[a]] if x is not None]
        if rng.random() < 0.15:
            agg = m["agg"]
            thr = (agg[G.A_MAXKEY[a]] or 0) + 1 if op in (">=", ">") else \
                  (agg[G.A_MINKEY[a]] or 0) - 1
        else:
            thr = q[rng.randrange(len(q))] if q else 0
            if op == "=":
                thr = (q[0] if q else 0) + rng.choice([0, 0, 1, -1])
        hconds = [(a, op, thr)]
        if rng.random() < 0.5:
            lo, hi = G.rng_range(m, rng, mode="centered", frac=rng.choice([0.1, 0.5, 0.99]))
            preds = [G.pred_range(vp, lo, hi)]
    elif cat == "having_and":
        # v2 supports repeated `having=` as an AND; this category checks the
        # conjunction (v1 silently applied only the last predicate).
        keys = [x for x in G.A_ORDER if m["agg"][G.A_QKEY[x]]]
        a1, a2 = rng.sample(keys, 2)
        q1 = m["agg"][G.A_QKEY[a1]]
        q2 = m["agg"][G.A_QKEY[a2]]
        hconds = [(a1, rng.choice([">=", "<=", ">", "<"]), q1[rng.randrange(len(q1))]),
                  (a2, rng.choice([">=", "<=", ">", "<"]), q2[rng.randrange(len(q2))])]
        if rng.random() < 0.5:
            lo, hi = G.rng_range(m, rng, mode="centered", frac=rng.choice([0.1, 0.5, 0.99]))
            preds = [G.pred_range(vp, lo, hi)]
    elif cat == "combined_predicates":
        lo, hi = G.rng_range(m, rng, mode=rng.choice(["centered", "prefix", "suffix"]),
                             frac=rng.choice([0.05, 0.25, 0.5, 0.9]))
        if rng.random() < 0.5 and len(pool) > 1:
            names = rng.sample(list(pool), min(rng.choice([1, 2, 3]), len(pool)))
            gp = G.pred_in("g", m, names)
        elif len(pool) > 2:
            names = rng.sample(list(pool), min(rng.choice([1, 2, 3]), len(pool) - 1))
            gp = G.pred_notin("g", m, names)
        else:
            gp = None
        a = rng.choice(G.A_ORDER)
        op = rng.choice([">=", "<=", ">", "<"])
        q = [x for x in m["agg"][G.A_QKEY[a]] if x is not None]
        hconds = [(a, op, q[rng.randrange(len(q))])] if q else []
        preds = [G.pred_range(vp, lo, hi)] + ([gp] if gp else [])
    elif cat == "multi_aggregate":
        # every query projects/checks all four aggregates; this category adds an
        # exact-equality or extreme HAVING predicate on MIN or MAX (single
        # predicate: the documented ECUBE01 interface is one HAVING per query)
        a = rng.choice(["min", "max"])
        q = [x for x in m["agg"][G.A_QKEY[a]] if x is not None]
        if rng.random() < 0.7 and q:
            hconds = [(a, "=", q[rng.randrange(len(q))])]
        elif q:
            hconds = [(a, rng.choice([">=", "<="]), q[rng.randrange(len(q))])]
        if rng.random() < 0.5:
            lo, hi = G.rng_range(m, rng, mode="centered", frac=rng.choice([0.25, 0.5, 0.9]))
            preds = [G.pred_range(vp, lo, hi)]
        else:
            preds = [G.pred_range(vp, m["vmin"], m["vmax"])]
    return cat, sql_and_args(m, preds, hconds)


def cube_run(binary, cube, dic, args, tmp):
    p = subprocess.run([binary, "query", cube, dic, tmp] + args,
                       capture_output=True, text=True)
    if p.returncode != 0:
        return None, p.stderr[-400:]
    try:
        return json.load(open(tmp))["rows"], None
    except Exception as e:
        return None, str(e)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=100000)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--seed", type=int, default=SEED)
    ap.add_argument("--patched-bin", default=f"{BASE}/bin/enc_cube_v2")
    ap.add_argument("--prefix-bin", default=f"{BASE}/bin/enc_cube_orig")
    ap.add_argument("--patched-cubes", default=f"{BASE}/cubes_v2")
    ap.add_argument("--prefix-cubes", default=f"{BASE}/cubes_prefix")
    ap.add_argument("--meta", default=f"{BASE}/out/pairs_meta.json")
    ap.add_argument("--out", default=f"{BASE}/out/fuzz_receipt_20261101.json")
    ap.add_argument("--plans-out", default=f"{BASE}/out/fuzz_queries_20261101.jsonl")
    ap.add_argument("--max-witness", type=int, default=30)
    args = ap.parse_args()
    metas = json.load(open(args.meta))
    pids = [p["id"] for p in P.FUZZ_PAIRS]
    n_target = args.limit or args.n
    rng = random.Random(args.seed)
    con = duckdb.connect(EVAL, read_only=True)
    con.execute("SET threads=1")
    # ---- phase 1: deterministic plans + one DuckDB reference result each ----
    plans = []
    sig_seen = set()
    stall = {pid: 0 for pid in pids}
    t0 = time.time()
    while len(plans) < n_target and pids:
        pid = pick_pair(rng, pids)
        m = metas[pid]
        got = None
        for _ in range(30):
            g = gen_one(rng, pid, m)
            if g is None:
                continue
            cat, (sql, cargs) = g
            sig = (pid, sql)
            if sig in sig_seen:
                continue
            sig_seen.add(sig)
            got = (cat, sql, cargs)
            break
        if got is None:
            # a pair whose finite query space is exhausted (e.g. the one-row
            # table) must be dropped, otherwise the reject loop spins forever
            stall[pid] += 1
            if stall[pid] > 60 and len(pids) > 1:
                pids.remove(pid)
                print(f"  pair {pid} query space exhausted ({len(plans)} plans so far)",
                      flush=True)
            continue
        stall[pid] = 0
        cat, sql, cargs = got
        exp = con.execute(sql).fetchall()
        rows = bsstr.canon_rows(exp)
        plan = dict(i=len(plans), pair=pid, cat=cat, sql=sql, args=cargs)
        if len(rows) <= 2000:
            plan["exp_rows"] = rows
        else:
            plan["exp_digest"] = bsstr.rows_digest(rows)
            plan["exp_n"] = len(rows)
        plans.append(plan)
        if (len(plans)) % 5000 == 0:
            print(f"  plans {len(plans)}/{n_target} ({time.time()-t0:.0f}s, "
                  f"last {pid}/{cat} rows={len(rows)})", flush=True)
    gen_s = time.time() - t0
    print(f"plans ready: {len(plans)} unique={len(sig_seen)} in {gen_s:.0f}s", flush=True)
    with open(args.plans_out, "w") as f:
        for p in plans:
            f.write(json.dumps(dict(i=p["i"], pair=p["pair"], cat=p["cat"],
                                    sql=p["sql"], args=p["args"])) + "\n")
    plans_sha = sha256_file(args.plans_out)
    # ---- phase 2: run the same plans on both binaries -----------------------
    tmp = f"{BASE}/out/_fuzz_rows_{os.getpid()}.json"
    results = {}
    for tag, binary, cubedir in [("patched", args.patched_bin, args.patched_cubes),
                                 ("prefix", args.prefix_bin, args.prefix_cubes)]:
        bycat = collections.defaultdict(lambda: dict(n=0, exact=0, fail=0))
        bypair = collections.defaultdict(lambda: dict(n=0, exact=0, fail=0))
        mismatches = []
        t1 = time.time()
        for k, plan in enumerate(plans):
            pid = plan["pair"]
            cube = f"{cubedir}/{pid}.ecube"
            dic = f"{BASE}/tables/{pid}/d.dict"
            rows, err = cube_run(binary, cube, dic, plan["args"], tmp)
            ok = False
            detail = None
            if rows is None:
                detail = {"error": err}
            elif "exp_rows" in plan:
                ok = sorted(rows) == plan["exp_rows"]
                if not ok:
                    detail = {"expected_n": len(plan["exp_rows"]), "got_n": len(rows),
                              "expected_head": plan["exp_rows"][:10],
                              "got_head": sorted(rows)[:10]}
            else:
                ok = (bsstr.digest_cube_rows(rows) == plan["exp_digest"] and
                      len(rows) == plan["exp_n"])
                if not ok:
                    detail = {"expected_n": plan["exp_n"], "got_n": len(rows),
                              "expected_digest": plan["exp_digest"],
                              "got_digest": bsstr.digest_cube_rows(rows)}
            bycat[plan["cat"]]["n"] += 1
            bypair[pid]["n"] += 1
            if ok:
                bycat[plan["cat"]]["exact"] += 1
                bypair[pid]["exact"] += 1
            else:
                bycat[plan["cat"]]["fail"] += 1
                bypair[pid]["fail"] += 1
                if len(mismatches) < args.max_witness:
                    mismatches.append(dict(i=plan["i"], pair=pid, cat=plan["cat"],
                                           sql=plan["sql"], cube_args=plan["args"],
                                           detail=detail))
            if (k + 1) % 10000 == 0:
                print(f"  [{tag}] {k+1}/{len(plans)} ({time.time()-t1:.0f}s)", flush=True)
        results[tag] = dict(binary=binary, binary_sha256=sha256_file(binary),
                            cubes=cubedir,
                            n=sum(v["n"] for v in bycat.values()),
                            exact=sum(v["exact"] for v in bycat.values()),
                            fail=sum(v["fail"] for v in bycat.values()),
                            by_category={c: dict(bycat[c]) for c in CAT_NAMES},
                            by_pair={p: dict(bypair[p]) for p in pids},
                            mismatches=mismatches,
                            run_s=round(time.time() - t1, 1))
        print(f"[{tag}] exact={results[tag]['exact']}/{results[tag]['n']} "
              f"fail={results[tag]['fail']} ({results[tag]['run_s']}s)", flush=True)
    con.close()
    if os.path.exists(tmp):
        os.unlink(tmp)
    receipt = dict(
        campaign="specs_20261101_differential", seed=args.seed,
        n_plans=len(plans), n_unique=(len(sig_seen)), plans_file=args.plans_out,
        plans_sha256=plans_sha, duckdb_version=duckdb.__version__,
        host=platform.node(), core_pin="taskset -c 72, nice -n 5",
        eval_db=EVAL,
        eval_tables={t: dict(n=metas[f["id"]]["n"], G=metas[f["id"]]["ngroups"],
                             V=metas[f["id"]]["nvalues"], vmin=metas[f["id"]]["vmin"],
                             vmax=metas[f["id"]]["vmax"],
                             has_null=metas[f["id"]]["has_null"],
                             dense_build=metas[f["id"]]["dense_in_cube"])
                     for t, f in zip([q["table"] for q in P.FUZZ_PAIRS], P.FUZZ_PAIRS)},
        pairs={pid: dict(label=metas[pid]["label"], note=metas[pid]["note"],
                         n=metas[pid]["n"], G=metas[pid]["ngroups"],
                         V=metas[pid]["nvalues"], vmin=metas[pid]["vmin"],
                         vmax=metas[pid]["vmax"], has_null=metas[pid]["has_null"])
               for pid in pids},
        categories={c: w for c, w in CATS},
        plan_gen_s=round(gen_s, 1),
        results=results)
    json.dump(receipt, open(args.out, "w"), indent=1)
    print("wrote", args.out)


if __name__ == "__main__":
    main()
