#!/usr/bin/env python3
"""DuckDB 2.0 single-thread warm baseline for the TPC-H SF2 multi-column cases.

Protocol (identical on every side): one persistent read-only connection,
threads=1, one untimed warmup, then REPS timed `execute()+fetchall()`.
Latency is the median of execute+fetch, result serialisation excluded.

Two SQL shapes per case:
  * sum_count : SUM(l_extendedprice*100), COUNT(*)          -- the protocol used
                by MULTI_PREDICATE_20261101.md (52.5 / 72.6 / 41.8 ms).
  * minmax    : the same plus MIN/MAX, with the price expression cast to BIGINT
                so the returned values are exact integers (used as the oracle).
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import json
import os
import statistics
import sys
import time

import duckdb

CASE_TMPL = (
    "SELECT CASE WHEN l_returnflag='A' THEN 0 WHEN l_returnflag='N' THEN 1 ELSE 2 END AS g,"
    " {agg} COUNT(*) AS c FROM lineitem"
    " WHERE l_extendedprice*100 BETWEEN {vlo} AND {vhi}"
    "   AND l_discount*100 BETWEEN {alo} AND {ahi}"
    "   AND l_quantity BETWEEN {blo} AND {bhi}"
    " GROUP BY 1 ORDER BY 1"
)
AGG_SC = "SUM(l_extendedprice*100) AS s,"
AGG_MM = (
    "CAST(SUM(CAST(ROUND(l_extendedprice*100,0) AS BIGINT)) AS HUGEINT) AS s,"
    " MIN(CAST(ROUND(l_extendedprice*100,0) AS BIGINT)) AS mn,"
    " MAX(CAST(ROUND(l_extendedprice*100,0) AS BIGINT)) AS mx,"
)


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
            cases.append(
                dict(name=p[0], vlo=int(p[1]), vhi=int(p[2]), alo=int(p[3]), ahi=int(p[4]),
                     blo=int(p[5]), bhi=int(p[6]))
            )
    return cases


def timed(con, sql, reps):
    con.execute(sql).fetchall()  # warmup
    ts = []
    rows = None
    for _ in range(reps):
        t = time.perf_counter()
        rows = con.execute(sql).fetchall()
        ts.append((time.perf_counter() - t) * 1e3)
    s = sorted(ts)
    return dict(median_ms=statistics.median(ts), min_ms=s[0],
                p95_ms=s[min(len(s) - 1, int(len(s) * 0.95))],
                mean_ms=statistics.fmean(ts),
                raw_ms=ts), rows


def main():
    db = sys.argv[1]
    cases_path = sys.argv[2]
    out_path = sys.argv[3]
    reps = int(sys.argv[4]) if len(sys.argv) > 4 else 100

    con = duckdb.connect(db, read_only=True)
    con.execute("SET threads=1")
    con.execute("SET preserve_insertion_order=true")
    out = dict(duckdb_version=duckdb.__version__, db=db, reps=reps, protocol="threads=1, warm, execute+fetchall",
               cases={})
    for c in load_cases(cases_path):
        sql_sc = CASE_TMPL.format(agg=AGG_SC, **c)
        sql_mm = CASE_TMPL.format(agg=AGG_MM, **c)
        sc, _ = timed(con, sql_sc, reps)
        mm, rows = timed(con, sql_mm, reps)
        out["cases"][c["name"]] = dict(
            params=[c["vlo"], c["vhi"], c["alo"], c["ahi"], c["blo"], c["bhi"]],
            sql_sum_count=sql_sc, sql_minmax=sql_mm,
            sum_count=sc, minmax=mm,
            rows=[[str(x) for x in r] for r in rows],
        )
        print(f"{c['name']:20s} sum_count median={sc['median_ms']:9.3f} ms "
              f"p95={sc['p95_ms']:9.3f} | minmax median={mm['median_ms']:9.3f} ms "
              f"| rows={len(rows)}")
    json.dump(out, open(out_path, "w"), indent=1)
    print(f"wrote {out_path}")


if __name__ == "__main__":
    main()
