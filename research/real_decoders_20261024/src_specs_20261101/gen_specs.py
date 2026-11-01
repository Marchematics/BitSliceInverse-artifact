#!/usr/bin/env python3
"""Generate the >=300-spec ECUBE01 exactness corpus.

For every spec the expected result is computed once by DuckDB 2.0 and stored
(full rows when <= 5000 rows, otherwise a sha256 row digest).  Every spec also
stores the exact cube invocation (cube file, BSSTR01 dict, CLI args).

Families:
  fixed_groupby, group_in, group_not_in, range_two_sided, range_one_sided,
  having, nullable_group_keys, empty_result, high_cardinality_grouping,
  combined_predicates, boundary_values, multi_aggregate, encoded_build
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import argparse
import json
import os
import random
import sys
import time

import duckdb

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pairs as P
import bsstr

SEED = 20261101
A_SQL = {"sum": "SUM({v})", "count": "COUNT(*)", "min": "MIN({v})", "max": "MAX({v})"}
A_ORDER = ["sum", "count", "min", "max"]
A_QKEY = {"sum": "s_q", "count": "c_q", "min": "mn_q", "max": "mx_q"}
A_MAXKEY = {"sum": "s_max", "count": "c_max", "min": "mn_max", "max": "mx_max"}
A_MINKEY = {"sum": "s_min", "count": "c_min", "min": "mn_min", "max": "mx_min"}


def lit(names, g_type):
    if g_type == "int":
        return ", ".join(names)
    return ", ".join("'" + n.replace("'", "''") + "'" for n in names)


class Ctx:
    def __init__(self, metas):
        self.metas = metas
        self.con = {}
        self.used = set()
        self.n = 0

    def pair(self, pid):
        return self.metas[pid]

    def conn(self, pid):
        db = self.metas[pid]["db"]
        if db not in self.con:
            c = duckdb.connect(db, read_only=True)
            c.execute("SET threads=1")
            self.con[db] = c
        return self.con[db]

    # ---- reference SQL / spec construction ---------------------------------
    def make(self, family, pid, preds, hconds):
        m = self.pair(pid)
        vp = f"({m['v_expr']})"
        ge = f"({m['g_expr']})"
        where = ""
        if preds:
            where = " WHERE " + " AND ".join(p["sql"] for p in preds)
        having = ""
        if hconds:
            having = " HAVING " + " AND ".join(
                f"{A_SQL[a].format(v=vp)} {op} {val}" for a, op, val in hconds)
        sql = (f"SELECT {m['g_expr']} AS g, SUM({vp}) AS s, COUNT(*) AS c, "
               f"MIN({vp}) AS mn, MAX({vp}) AS mx FROM {m['src']}{where} "
               f"GROUP BY g{having}")
        key = (pid, sql)
        if key in self.used:
            return None
        self.used.add(key)
        t0 = time.time()
        exp = self.conn(pid).execute(sql).fetchall()
        rows = bsstr.canon_rows(exp)
        args = [p["arg"] for p in preds] + [
            "having=" + f"{a}{op}{val}" for a, op, val in hconds]
        spec = dict(id=None, family=family, pair=pid, dataset=m["label"],
                    sql=sql, cube=dict(id=pid, args=args,
                                       cube=f"cubes/{pid}.ecube",
                                       dict=f"tables/{pid}/d.dict"),
                    nrows=len(rows), gen_ms=round((time.time() - t0) * 1000, 1))
        if len(rows) <= 5000:
            spec["rows"] = rows
        else:
            spec["rows"] = None
            spec["rows_digest"] = bsstr.rows_digest(rows)
        return spec


# ---------------------------------------------------------------------------
# value/name helpers
# ---------------------------------------------------------------------------
def observed(m):
    return m["all_vals"] if m.get("all_vals") else sorted(m["sample_vals"])


def obs_at(m, frac):
    vals = observed(m)
    n = len(vals)
    i = max(0, min(n - 1, int(round(frac * (n - 1)))))
    return vals[i]


def rng_range(m, rng, mode="centered", frac=0.1):
    vals = observed(m)
    n = len(vals)
    if n <= 1:
        return m["vmin"], m["vmax"]
    if mode == "prefix":
        return m["vmin"], vals[max(1, min(n - 1, int(frac * (n - 1))))]
    if mode == "suffix":
        return vals[max(0, min(n - 2, int((1 - frac) * (n - 1))))], m["vmax"]
    if mode == "two_obs":
        i, j = sorted(rng.sample(range(n), 2))
        if j == i:
            j = min(n - 1, i + 1)
        return vals[i], vals[j]
    c = n // 2
    half = max(1, int(frac * n / 2))
    return vals[max(0, c - half)], vals[min(n - 1, c + half)]


def names_of(m):
    return m.get("names") if m.get("names") is not None else m["sample_names"]


def pick_names(m, rng, k):
    pool = names_of(m)
    k = min(k, len(pool))
    return rng.sample(list(pool), k)


def nonexistent(m, rng):
    if m["g_type"] == "int":
        return str(-1000000 - rng.randrange(1000))
    return "ZZZ_NONEXISTENT"


def pred_range(vp, lo=None, hi=None):
    if lo is not None and hi is not None:
        return dict(sql=f"{vp} BETWEEN {lo} AND {hi}", arg=f"range={lo},{hi}")
    if lo is not None:
        return dict(sql=f"{vp} >= {lo}", arg=f"lo={lo}")
    return dict(sql=f"{vp} <= {hi}", arg=f"hi={hi}")


def pred_in(ge, m, names):
    return dict(sql=f"{ge} IN ({lit(names, m['g_type'])})", arg="where_in=" + ",".join(names))


def pred_notin(ge, m, names):
    return dict(sql=f"{ge} NOT IN ({lit(names, m['g_type'])})",
                arg="where_not_in=" + ",".join(names))


def unobserved_inside(m):
    vals = m.get("all_vals")
    if not vals:
        return None
    s = set(vals)
    for i in range(len(vals) - 1):
        if vals[i + 1] - vals[i] > 1:
            return vals[i] + 1
    return None


# ---------------------------------------------------------------------------
# families
# ---------------------------------------------------------------------------
def fam_fixed(ctx, rng):
    out = []
    for pid in ["C1", "C2", "C3", "C4", "C5", "C6", "C9", "C10", "C11"]:
        s = ctx.make("fixed_groupby", pid, [], [])
        if s:
            out.append(s)
    return out


def fam_group_in(ctx, rng):
    out = []
    pairs = ["C1", "C2", "C3", "C4", "C5", "C9", "C10", "C8"]
    for pid in pairs:
        m = ctx.pair(pid)
        pool = names_of(m)
        for k in [1, 2, min(5, len(pool)), len(pool)]:
            names = rng.sample(list(pool), min(k, len(pool)))
            if rng.random() < 0.2:  # one nonexistent name
                names = names + [nonexistent(m, rng)]
            s = ctx.make("group_in", pid, [pred_in(f"({m['g_expr']})", m, names)], [])
            if s:
                out.append(s)
    return out


def fam_group_not_in(ctx, rng):
    out = []
    pairs = ["C1", "C2", "C8", "C4", "C3", "C10", "C5"]
    for pid in pairs:
        m = ctx.pair(pid)
        pool = names_of(m)
        for k in [1, 2, min(4, max(1, len(pool) - 1))]:
            if k >= len(pool):
                continue
            names = rng.sample(list(pool), k)
            s = ctx.make("group_not_in", pid, [pred_notin(f"({m['g_expr']})", m, names)], [])
            if s:
                out.append(s)
    return out


def fam_range_two_sided(ctx, rng):
    out = []
    sel = [0.0001, 0.01, 0.05, 0.1, 0.25, 0.5, 0.9, 0.99, 1.0]
    pairs = ["C2", "C1", "C4", "C5", "C9", "C10", "C11", "C3"]
    i = 0
    for f in sel:
        for pid in pairs[:5]:
            m = ctx.pair(pid)
            mode = "prefix" if f <= 0.05 and i % 2 == 0 else "centered"
            lo, hi = rng_range(m, rng, mode=mode, frac=f)
            s = ctx.make("range_two_sided", pid, [pred_range(f"({m['v_expr']})", lo, hi)], [])
            if s:
                out.append(s)
        i += 1
    # observed-bounds queries on the remaining pairs
    for pid in ["C3", "C1", "C10", "C11", "C2", "C5"]:
        m = ctx.pair(pid)
        lo, hi = rng_range(m, rng, mode="two_obs")
        s = ctx.make("range_two_sided", pid, [pred_range(f"({m['v_expr']})", lo, hi)], [])
        if s:
            out.append(s)
    return out


def fam_range_one_sided(ctx, rng):
    out = []
    pairs = ["C2", "C1", "C3", "C5", "C9", "C10", "C11", "C4"]
    for pid in pairs:
        m = ctx.pair(pid)
        vals = observed(m)
        for frac in [0.05, 0.5, 0.95]:
            v = vals[max(0, min(len(vals) - 1, int(frac * (len(vals) - 1))))]
            if rng.random() < 0.5:
                s = ctx.make("range_one_sided", pid,
                             [pred_range(f"({m['v_expr']})", lo=v)], [])
            else:
                s = ctx.make("range_one_sided", pid,
                             [pred_range(f"({m['v_expr']})", hi=v)], [])
            if s:
                out.append(s)
    return out


def fam_having(ctx, rng):
    out = []
    pairs = ["C1", "C2", "C3", "C4", "C5", "C9", "C10", "C11"]
    qi = 0
    for pid in pairs:
        m = ctx.pair(pid)
        agg = m["agg"]
        for a in A_ORDER:
            q = agg[A_QKEY[a]]
            cands = [x for x in q if x is not None]
            for op in [">=", "<=", ">", "<", "="]:
                if not cands:
                    continue
                thr = cands[qi % len(cands)]
                if op == "=" and rng.random() < 0.5:
                    thr = cands[0]
                if rng.random() < 0.2:
                    if op in (">=", ">"):
                        thr = (agg[A_MAXKEY[a]] or 10 ** 15) + 1
                    elif op in ("<=", "<"):
                        thr = (agg[A_MINKEY[a]] or 0) - 1
                qi += 1
                preds = []
                if rng.random() < 0.6:
                    lo, hi = rng_range(m, rng, mode="centered", frac=rng.choice([0.1, 0.5, 0.99]))
                    preds.append(pred_range(f"({m['v_expr']})", lo, hi))
                s = ctx.make("having", pid, preds, [(a, op, thr)])
                if s:
                    out.append(s)
            if len(out) >= 40:
                return out
    return out


def fam_nullable(ctx, rng):
    out = []
    for pid in ["C1", "C2", "C8"]:
        m = ctx.pair(pid)
        ge, vp = f"({m['g_expr']})", f"({m['v_expr']})"
        pool = names_of(m)
        out.append(ctx.make("nullable_group_keys", pid, [], []))
        lo, hi = rng_range(m, rng, mode="centered", frac=0.5)
        out.append(ctx.make("nullable_group_keys", pid, [pred_range(vp, lo, hi)], []))
        names = rng.sample(list(pool), min(2, len(pool)))
        out.append(ctx.make("nullable_group_keys", pid, [pred_in(ge, m, names)], []))
        out.append(ctx.make("nullable_group_keys", pid, [pred_notin(ge, m, names)], []))
        out.append(ctx.make("nullable_group_keys", pid, [pred_range(vp, lo, hi)],
                            [("sum", ">=", m["agg"]["s_q"][1] or 0)]))
        out.append(ctx.make("nullable_group_keys", pid,
                            [pred_range(vp, lo, hi), pred_notin(ge, m, names)], []))
        out.append(ctx.make("nullable_group_keys", pid,
                            [pred_range(vp, lo, hi), pred_in(ge, m, names)], []))
        # NULL group explicitly targeted through the full-name NOT IN list
        if m.get("names"):
            out.append(ctx.make("nullable_group_keys", pid,
                                [pred_notin(ge, m, m["names"])], []))
    return [s for s in out if s]


def fam_empty(ctx, rng):
    out = []
    for pid in ["C2", "C3", "C5", "C9", "C10", "C11", "C1"]:
        m = ctx.pair(pid)
        vp, ge = f"({m['v_expr']})", f"({m['g_expr']})"
        pool = names_of(m)
        # inverted range
        if m["vmax"] > m["vmin"]:
            s = ctx.make("empty_result", pid,
                         [pred_range(vp, m["vmax"], m["vmin"])], [])
            if s:
                out.append(s)
        # outside domain both sides
        s = ctx.make("empty_result", pid,
                     [pred_range(vp, m["vmax"] + 1000, m["vmax"] + 2000)], [])
        if s:
            out.append(s)
        # IN nonexistent
        s = ctx.make("empty_result", pid,
                     [pred_in(ge, m, [nonexistent(m, rng)])], [])
        if s:
            out.append(s)
        # HAVING impossible
        s = ctx.make("empty_result", pid, [],
                     [("sum", ">=", 10 ** 18)])
        if s:
            out.append(s)
        # NOT IN every group name -> SQL three-valued logic returns no row
        if m.get("names") and len(m["names"]) <= 100:
            s = ctx.make("empty_result", pid,
                         [pred_notin(ge, m, m["names"])], [])
            if s:
                out.append(s)
        # single unobserved value
        un = unobserved_inside(m)
        if un is not None:
            s = ctx.make("empty_result", pid, [pred_range(vp, un, un)], [])
            if s:
                out.append(s)
    return out


def fam_highcard(ctx, rng):
    out = []
    m = ctx.pair("C6")
    vp = f"({m['v_expr']})"
    out.append(ctx.make("high_cardinality_grouping", "C6", [], []))  # 400k groups, digest
    for k in [5, 50, 500]:
        names = pick_names(m, rng, k)
        out.append(ctx.make("high_cardinality_grouping", "C6",
                            [pred_in(f"({m['g_expr']})", m, names)], []))
    for thr in m["agg"]["c_q"] + [m["agg"]["c_max"]]:
        out.append(ctx.make("high_cardinality_grouping", "C6", [], [("count", ">=", thr)]))
    for thr in m["agg"]["s_q"]:
        out.append(ctx.make("high_cardinality_grouping", "C6", [], [("sum", ">=", thr)]))
    lo, hi = rng_range(m, rng, mode="centered", frac=0.5)
    out.append(ctx.make("high_cardinality_grouping", "C6", [pred_range(vp, lo, hi)],
                        [("count", ">=", m["agg"]["c_q"][1])]))
    # C7: 3,000,000 groups; only selective predicates keep the output small
    m = ctx.pair("C7")
    vp = f"({m['v_expr']})"
    for k in [5, 20, 100]:
        names = pick_names(m, rng, k)
        out.append(ctx.make("high_cardinality_grouping", "C7",
                            [pred_in(f"({m['g_expr']})", m, names)], []))
    names = pick_names(m, rng, 30)
    lo, hi = rng_range(m, rng, mode="two_obs")
    out.append(ctx.make("high_cardinality_grouping", "C7",
                        [pred_range(vp, lo, hi), pred_in(f"({m['g_expr']})", m, names)], []))
    lo, hi = rng_range(m, rng, mode="two_obs")
    out.append(ctx.make("high_cardinality_grouping", "C7", [pred_range(vp, lo, hi)],
                        [("count", ">=", 3)]))
    return [s for s in out if s]


def fam_combined(ctx, rng):
    out = []
    pairs = ["C1", "C2", "C4", "C5", "C8", "C9", "C10", "C11"]
    for pid in pairs:
        m = ctx.pair(pid)
        vp, ge = f"({m['v_expr']})", f"({m['g_expr']})"
        pool = names_of(m)
        for j in range(4):
            lo, hi = rng_range(m, rng, mode="centered", frac=rng.choice([0.05, 0.25, 0.75]))
            if j % 2 == 0:
                names = rng.sample(list(pool), min(2, len(pool)))
                gp = pred_in(ge, m, names)
            else:
                k = max(1, min(3, len(pool) - 1))
                names = rng.sample(list(pool), k)
                gp = pred_notin(ge, m, names)
            a = A_ORDER[(j + len(pid)) % 4]
            q = m["agg"][A_QKEY[a]]
            thr = q[j % len(q)] if q and q[j % len(q)] is not None else 0
            op = [">=", "<=", ">", "<"][j % 4]
            s = ctx.make("combined_predicates", pid, [pred_range(vp, lo, hi), gp],
                         [(a, op, thr)])
            if s:
                out.append(s)
    return out


def fam_boundary(ctx, rng):
    out = []
    pairs = ["C2", "C5", "C9", "C11", "C10", "C1"]
    for pid in pairs:
        m = ctx.pair(pid)
        vp = f"({m['v_expr']})"
        vals = observed(m)
        # single-value ranges on observed values
        for frac in [0.0, 0.5, 1.0]:
            v = vals[max(0, min(len(vals) - 1, int(frac * (len(vals) - 1))))]
            s = ctx.make("boundary_values", pid, [pred_range(vp, v, v)], [])
            if s:
                out.append(s)
        # adjacent observed values (inclusive both ends) and one-below/above
        if m.get("adj"):
            a, b = rng.choice(m["adj"])
            for lo, hi in [(a, b), (a - 1, a), (a, a + 1), (a - 1, b + 1)]:
                s = ctx.make("boundary_values", pid, [pred_range(vp, lo, hi)], [])
                if s:
                    out.append(s)
        # domain edges
        for lo, hi in [(m["vmin"] - 1, m["vmin"]), (m["vmax"], m["vmax"] + 1),
                       (m["vmin"], m["vmax"]), (-2 ** 31, 2 ** 31 - 1),
                       (-2 ** 63, 2 ** 63 - 1)]:
            s = ctx.make("boundary_values", pid, [pred_range(vp, lo, hi)], [])
            if s:
                out.append(s)
    return out


def fam_multi_agg(ctx, rng):
    """All four aggregates are projected and checked in every spec; this family
    additionally stresses their joint behaviour: a range plus one HAVING
    predicate on sum / count / min / max (the documented ECUBE01 interface is
    one HAVING predicate per query), and exact-equality predicates on MIN/MAX
    at observed values."""
    out = []
    pairs = ["C1", "C2", "C3", "C10", "C11", "C5", "C9"]
    for pid in pairs:
        m = ctx.pair(pid)
        vp = f"({m['v_expr']})"
        agg = m["agg"]
        for j, a in enumerate(A_ORDER):
            q = [x for x in agg[A_QKEY[a]] if x is not None]
            if not q:
                continue
            thr = q[j % len(q)]
            op = [">=", "<=", ">", "<"][j % 4]
            preds = []
            if j % 2 == 0:
                lo, hi = rng_range(m, rng, mode="centered",
                                   frac=rng.choice([0.05, 0.25, 0.5, 0.9]))
                preds = [pred_range(vp, lo, hi)]
            s = ctx.make("multi_aggregate", pid, preds, [(a, op, thr)])
            if s:
                out.append(s)
        # all four aggregates under one exact-equality predicate on MIN/MAX
        vals = observed(m)
        v = vals[len(vals) // 3]
        for a in ["min", "max"]:
            s = ctx.make("multi_aggregate", pid, [pred_range(vp, m["vmin"], m["vmax"])],
                         [(a, "=", v)])
            if s:
                out.append(s)
    return out


def fam_encoded(ctx, rng, base):
    """Copy selected specs onto the cubes built directly from encoded trees."""
    out = []
    want = {"C1": 4, "C2": 4, "C3": 4}
    for pid, k in want.items():
        picks = [s for s in base if s["pair"] == pid and s.get("rows") is not None][:k]
        if len(picks) < k:
            picks = [s for s in base if s["pair"] == pid][:k]
        for s in picks:
            s2 = dict(s)
            s2["family"] = "encoded_build"
            s2["cube"] = dict(s["cube"])
            s2["cube"]["cube"] = f"cubes/{pid}_enc.ecube"
            s2["note"] = "same SQL/expected as plain build; cube built from the fused encoded tree"
            out.append(s2)
    return out


# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--meta", default=f"{P.BASE}/out/pairs_meta.json")
    ap.add_argument("--out", default=f"{P.BASE}/out/spec_corpus_20261101.json")
    ap.add_argument("--counts-out", default=f"{P.BASE}/out/spec_counts_20261101.json")
    args = ap.parse_args()
    metas = json.load(open(args.meta))
    for pid in ["C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8", "C9", "C10", "C11"]:
        if pid not in metas:
            raise SystemExit(f"missing pair meta: {pid}")
    ctx = Ctx(metas)
    fams = [
        ("fixed_groupby", fam_fixed),
        ("group_in", fam_group_in),
        ("group_not_in", fam_group_not_in),
        ("range_two_sided", fam_range_two_sided),
        ("range_one_sided", fam_range_one_sided),
        ("having", fam_having),
        ("nullable_group_keys", fam_nullable),
        ("empty_result", fam_empty),
        ("high_cardinality_grouping", fam_highcard),
        ("combined_predicates", fam_combined),
        ("boundary_values", fam_boundary),
        ("multi_aggregate", fam_multi_agg),
    ]
    specs = []
    for name, fn in fams:
        rng = random.Random(f"{SEED}:{name}")
        t0 = time.time()
        got = [s for s in fn(ctx, rng) if s]
        for s in got:
            s["id"] = f"{name}-{len(specs):04d}"
            specs.append(s)
        print(f"[gen] {name}: {len(got)} specs ({time.time()-t0:.1f}s)", flush=True)
    enc = fam_encoded(ctx, random.Random(f"{SEED}:encoded"), specs)
    for s in enc:
        s["id"] = f"encoded_build-{len(specs):04d}"
        specs.append(s)
    print(f"[gen] encoded_build: {len(enc)} specs", flush=True)
    families = {}
    for s in specs:
        families.setdefault(s["family"], []).append(s["id"])
    out = dict(seed=SEED, duckdb_version=duckdb.__version__,
               created="2026-11-01", n_specs=len(specs),
               families={k: len(v) for k, v in sorted(families.items())},
               specs=specs)
    json.dump(out, open(args.out, "w"))
    counts = dict(n_specs=len(specs),
                  families={k: len(v) for k, v in sorted(families.items())},
                  n_digest_specs=sum(1 for s in specs if s.get("rows") is None),
                  n_rows_stored=sum(len(s.get("rows") or []) for s in specs))
    json.dump(counts, open(args.counts_out, "w"), indent=1)
    print(json.dumps(counts, indent=1), flush=True)


if __name__ == "__main__":
    main()
