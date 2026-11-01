#!/usr/bin/env python3
"""Second query engine: DataFusion 55.0.0 over the SAME ECUBE02 index.

DataFusion executes the SQL; the index is consumed through a user-defined table
function registered with `SessionContext.register_udtf`:

    SELECT name, sum, count, min, max
    FROM ecube('<cube>', '<dict>', '<spec>')
    ORDER BY name

The UDTF does exactly the reduction the C++ table function does (same block
prefix, same MIN/MAX monotone block search, same NULL-group / HAVING rules), but
in numpy over an mmapped index; DataFusion then owns the planning, the scan of
the 11 emitted rows and the ORDER BY. Reported latency is the complete
`ctx.sql(...).collect()` of the full result.

Protocol (kept separate from the DuckDB protocol, never mixed):
  * index load = mmap + dictionary parse + per-group nonzero-rank arrays,
    done once outside the timed loop;
  * 1 untimed warmup, then REPS timed executions, median and p95;
  * single thread, one persistent SessionContext;
  * `SELECT 42` is measured with the same loop as the DataFusion fixed floor.

Usage:
  datafusion_ecube.py <cube> <dict> <reps> [spec]          single query
  datafusion_ecube.py --panel <cube> <dict> <panel.tsv> <reps>
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import json
import os
import statistics
import sys

import numpy as np
import pyarrow as pa
import pyarrow.dataset as pds

import datafusion
from datafusion import SessionContext, udtf

R_BLOCK = 64
MAGIC2 = b"ECUBE02\n"
MAGIC3 = b"ECUBE03\n"


# --------------------------------------------------------------------------- #
# ECUBE02/ECUBE03 reader (numpy, mmapped).                                     #
# --------------------------------------------------------------------------- #
class Cube:
    def __init__(self, cube_path, dict_path):
        self.raw = np.memmap(cube_path, dtype=np.uint8, mode="r")
        buf = self.raw
        magic = bytes(buf[:8])
        if magic not in (MAGIC2, MAGIC3):
            raise SystemExit(f"bad magic {magic!r}")
        self.format = 2 if magic == MAGIC2 else 3
        # Header: char magic[8]; u64 n,ngroups,nvalues; i64 vmin; u32 dense,
        # has_null, null_group, block_ranks, null_measure; char pad[4];
        n, ngroups, nvalues, vmin, dense, has_null, null_group, block_ranks, null_measure = \
            np.frombuffer(buf, dtype=np.dtype([
                ("n", "<u8"), ("ngroups", "<u8"), ("nvalues", "<u8"), ("vmin", "<i8"),
                ("dense", "<u4"), ("has_null", "<u4"), ("null_group", "<u4"),
                ("block_ranks", "<u4"), ("null_measure", "<u4")]), count=1, offset=8)[0]
        self.n = int(n)
        self.G = int(ngroups)
        self.V = int(nvalues)
        self.vmin = int(vmin)
        self.dense = int(dense)
        self.has_null = int(has_null)
        self.null_group = int(null_group)
        self.R = int(block_ranks)
        self.null_measure = int(null_measure) if self.format >= 3 else 0
        self.nblocks = (self.V + self.R - 1) // self.R
        off = 64
        if self.dense:
            self.distinct = None
        else:
            self.distinct = np.frombuffer(buf, dtype="<i8", count=self.V, offset=off)
            off += self.V * 8
        self.counts = np.frombuffer(buf, dtype="<u4", count=self.G * self.V, offset=off)
        self.counts = self.counts.reshape(self.G, self.V)
        off += self.G * self.V * 4
        self.null_cnt = None
        if self.format >= 3:
            self.null_cnt = np.frombuffer(buf, dtype="<u4", count=self.G, offset=off)
            off += self.G * 4
        self.block_cnt = np.frombuffer(buf, dtype="<u4", count=self.G * (self.nblocks + 1),
                                       offset=off).reshape(self.G, self.nblocks + 1)
        off += self.G * (self.nblocks + 1) * 4
        self.block_sum = np.frombuffer(buf, dtype="<i8", count=self.G * (self.nblocks + 1),
                                       offset=off).reshape(self.G, self.nblocks + 1)
        self.values = (self.distinct.astype(np.int64) if not self.dense
                       else np.arange(self.vmin, self.vmin + self.V, dtype=np.int64))
        self.names = read_dict(dict_path)
        if len(self.names) + (1 if self.has_null else 0) < self.G:
            raise SystemExit("dict short")
        self.names = self.names[:self.G]
        while len(self.names) < self.G:          # dictionary has k entries, G = k (+1 for NULL)
            self.names.append(f"g{len(self.names)}")
        if self.has_null and self.null_group < len(self.names):
            self.names[self.null_group] = "NULL"
        # per-group ascending list of ranks with count > 0 (for MIN/MAX search)
        self.nz = [np.nonzero(self.counts[g])[0] for g in range(self.G)]

    def _edge(self, a, b):
        """Exact (count, sum) over ranks [a,b) inside a rank block, int64."""
        c = self.counts[:, a:b].astype(np.int64)
        v = self.values[a:b]
        return c.sum(axis=1), (c * v).sum(axis=1)

    def reduce(self, spec):
        lo = hi = None
        want_in = want_notin = None
        having = []
        for tok in [t for t in spec.split(";") if t]:
            if tok.startswith("range="):
                a, b = tok[6:].split(",")
                lo, hi = int(a), int(b)
            elif tok.startswith("lo="):
                lo = int(tok[3:])
            elif tok.startswith("hi="):
                hi = int(tok[3:])
            elif tok.startswith("where_in="):
                want_in = set(tok[9:].split(","))
            elif tok.startswith("where_not_in="):
                want_notin = set(tok[13:].split(","))
            elif tok.startswith("having="):
                having.extend(tok[7:].split(","))
            else:
                raise SystemExit(f"bad spec token {tok}")
        a, b = 0, self.V
        if lo is not None or hi is not None:
            if lo is not None and hi is not None and lo > hi:
                a = b = 0
            elif self.dense:
                vmax = self.vmin + self.V - 1
                l = max(self.vmin, lo if lo is not None else self.vmin)
                h = min(vmax, hi if hi is not None else vmax)
                if h < l:
                    a = b = 0
                else:
                    a, b = l - self.vmin, h - self.vmin + 1
            else:
                a = int(np.searchsorted(self.distinct, lo if lo is not None else np.iinfo(np.int64).min))
                b = int(np.searchsorted(self.distinct, hi if hi is not None else np.iinfo(np.int64).max,
                                        side="right"))
            if b < a:
                b = a
        allowed = np.ones(self.G, dtype=bool)
        if want_in is not None:
            allowed = np.array([nm in want_in for nm in self.names])
        if want_notin is not None:
            allowed &= np.array([nm not in want_notin for nm in self.names])
        if self.has_null and self.null_group < self.G and (
                want_in is not None or want_notin is not None):
            allowed[self.null_group] = False
        if self.null_measure and self.null_cnt is not None and lo is None and hi is None:
            nulls = self.null_cnt.astype(np.int64)
        else:
            nulls = np.zeros(self.G, dtype=np.int64)
        cnt = np.zeros(self.G, dtype=np.int64)
        sm = np.zeros(self.G, dtype=np.int64)
        if b > a:
            ba, bb = a // self.R, b // self.R
            if ba == bb:
                cnt, sm = self._edge(a, b)
            else:
                end_first = min((ba + 1) * self.R, b)
                cnt, sm = self._edge(a, end_first)
                if bb > ba + 1:
                    cnt = cnt + (self.block_cnt[:, bb].astype(np.int64) -
                                 self.block_cnt[:, ba + 1].astype(np.int64))
                    sm = sm + (self.block_sum[:, bb] - self.block_sum[:, ba + 1])
                c1, s1 = self._edge(bb * self.R, b)
                cnt = cnt + c1
                sm = sm + s1
        mn = np.full(self.G, 0, dtype=np.int64)
        mx = np.full(self.G, 0, dtype=np.int64)
        for g in range(self.G):
            if cnt[g] == 0 and nulls[g] == 0:
                continue
            if cnt[g] == 0:
                continue
            ranks = self.nz[g]
            i = int(np.searchsorted(ranks, a))
            j = int(np.searchsorted(ranks, b, side="left")) - 1
            mn[g] = self.values[ranks[i]]
            mx[g] = self.values[ranks[j]]
        total = cnt + nulls
        rows = []
        for g in range(self.G):
            if not allowed[g] or total[g] == 0:
                continue
            val_null = cnt[g] == 0
            ok = True
            for h in having:
                if not eval_having(h, int(sm[g]), int(total[g]), int(mn[g]), int(mx[g]), val_null):
                    ok = False
                    break
            if not ok:
                continue
            rows.append((self.names[g],
                         None if val_null else int(sm[g]),
                         int(total[g]),
                         None if val_null else int(mn[g]),
                         None if val_null else int(mx[g])))
        return rows


def read_dict(path):
    d = open(path, "rb").read()
    if len(d) < 52 or d[:8] != b"BSSTR01\n":
        raise SystemExit("dict magic")
    q = 48
    k = int.from_bytes(d[q:q + 4], "little")
    q += 4
    names = []
    for _ in range(k):
        ln = int.from_bytes(d[q:q + 4], "little")
        q += 4
        names.append(d[q:q + ln].decode())
        q += ln
    return names


def eval_having(cond, sm, cnt, mn, mx, val_null):
    for op in (">=", "<=", ">", "<", "="):
        if op in cond:
            lhs, rhs = cond.split(op, 1)
            break
    else:
        return True
    if lhs == "sum":
        if val_null:
            return False
        x = sm
    elif lhs == "count":
        x = cnt
    elif lhs == "min":
        if val_null:
            return False
        x = mn
    elif lhs == "max":
        if val_null:
            return False
        x = mx
    else:
        return True
    v = float(rhs)
    return {">=": x >= v, "<=": x <= v, ">": x > v, "<": x < v, "=": x == v}[op]


SCHEMA = pa.schema([("name", pa.string()), ("sum", pa.int64()), ("count", pa.int64()),
                    ("min", pa.int64()), ("max", pa.int64())])


def rows_to_dataset(rows):
    return pds.dataset(pa.table({
        "name": pa.array([r[0] for r in rows], pa.string()),
        "sum": pa.array([r[1] for r in rows], pa.int64()),
        "count": pa.array([r[2] for r in rows], pa.int64()),
        "min": pa.array([r[3] for r in rows], pa.int64()),
        "max": pa.array([r[4] for r in rows], pa.int64()),
    }, schema=SCHEMA))


def make_udtf(cube):
    def _fn(*args):
        spec = args[2].python_value()
        if hasattr(spec, "as_py"):
            spec = spec.as_py()
        return rows_to_dataset(cube.reduce(spec))
    return udtf(_fn, name="ecube")


SQL = "SELECT name, sum, count, min, max FROM ecube('{c}','{d}','{s}') ORDER BY name"


def bench(ctx, sql, reps, warm=1):
    for _ in range(warm):
        ctx.sql(sql).collect()
    ts = []
    for _ in range(reps):
        t = __import__("time").perf_counter()
        ctx.sql(sql).collect()
        ts.append((__import__("time").perf_counter() - t) * 1000.0)
    ts.sort()
    return {"median_ms": statistics.median(ts), "min_ms": ts[0],
            "p95_ms": ts[min(len(ts) - 1, int(len(ts) * 0.95))], "reps": reps, "samples": ts}


def main():
    argv = sys.argv[1:]
    panel_mode = False
    if argv and argv[0] == "--panel":
        panel_mode = True
        argv = argv[1:]
    cube_path, dict_path = argv[0], argv[1]
    t0 = __import__("time").perf_counter()
    cube = Cube(cube_path, dict_path)
    load_ms = (__import__("time").perf_counter() - t0) * 1000.0

    ctx = SessionContext()
    ctx.register_udtf(make_udtf(cube))
    # DataFusion per-query floor with the same loop shape
    floor = bench(ctx, "SELECT 42 AS x", 50)

    if not panel_mode:
        reps = int(argv[2])
        spec = argv[3] if len(argv) > 3 else ""
        sql = SQL.format(c=cube_path, d=dict_path, s=spec)
        r = ctx.sql(sql).collect()
        res = bench(ctx, sql, reps)
        out = r[0].to_pydict()
        print(json.dumps({"engine": "datafusion", "protocol": "warm_engine_resident",
                          "datafusion_version": datafusion.__version__, "spec": spec,
                          "sql": sql, "index_load_ms": load_ms, "floor_median_ms": floor["median_ms"],
                          "format": cube.format, "groups": cube.G, "values": cube.V,
                          **{k: v for k, v in res.items() if k != "samples"},
                          "rows": len(out["name"]),
                          "result": {k: [None if x is None else str(x) for x in v]
                                     for k, v in out.items()}}))
        return

    from panel_def import read_tsv
    panel = read_tsv(argv[2])
    reps = int(argv[3])
    cases = []
    for case in panel:
        sql = SQL.format(c=cube_path, d=dict_path, s=case["cube_spec"])
        r = ctx.sql(sql).collect()
        res = bench(ctx, sql, reps)
        out = r[0].to_pydict()
        cases.append({"name": case["name"], "spec": case["cube_spec"], "sql": sql,
                      **{k: v for k, v in res.items() if k != "samples"},
                      "rows": len(out["name"]),
                      "result": {k: [None if x is None else str(x) for x in v]
                                 for k, v in out.items()}})
    print(json.dumps({"engine": "datafusion", "protocol": "warm_engine_resident",
                      "datafusion_version": datafusion.__version__,
                      "index_load_ms": load_ms, "floor_median_ms": floor["median_ms"],
                      "format": cube.format, "groups": cube.G, "values": cube.V,
                      "cases": cases}))


if __name__ == "__main__":
    main()
