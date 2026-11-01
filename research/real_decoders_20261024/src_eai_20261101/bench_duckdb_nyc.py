#!/usr/bin/env python3
"""DuckDB 2.0 single-thread warm baseline for the NYC-100M grouped all-four query.

Protocol: one persistent read-only connection, threads=1, one untimed warmup,
then REP timed executions of execute()+fetchall(). Latency = execute+fetch.
Emits JSON with median/min/p95 and the actual row counts so selectivity is
derived, not assumed.
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import json
import statistics
import sys
import time

import duckdb

DB = sys.argv[1] if len(sys.argv) > 1 else "nyc_taxi_100m_duckdb/nyc.duckdb"
OUT = sys.argv[2] if len(sys.argv) > 2 else "../../work/runs/duckdb_nyc100.json"
REPS = int(sys.argv[3]) if len(sys.argv) > 3 else 30

SQL = ("SELECT passenger_count AS g, SUM(fare_cents) AS s, COUNT(*) AS c, "
       "MIN(fare_cents) AS mn, MAX(fare_cents) AS mx FROM nyc {w} GROUP BY 1 ORDER BY 1")
CASES = [
    ("fixed_all4", ""),
    ("le_m7000", "WHERE fare_cents <= -7000"),
    ("le_m720", "WHERE fare_cents <= -720"),
    ("le_509", "WHERE fare_cents <= 509"),
    ("le_650", "WHERE fare_cents <= 650"),
    ("le_930", "WHERE fare_cents <= 930"),
    ("le_1350", "WHERE fare_cents <= 1350"),
    ("le_99900", "WHERE fare_cents <= 99900"),
    ("narrow_80_90M", "WHERE fare_cents BETWEEN 80000000 AND 90000000"),
    ("in_1_2", "WHERE passenger_count IN (1,2)"),
    ("having", "WHERE fare_cents <= 99900 GROUP BY 1 HAVING SUM(fare_cents) >= 1e10"),
]


def main():
    con = duckdb.connect(DB, read_only=True)
    con.execute("SET threads=1")
    con.execute("SET preserve_insertion_order=true")
    out = {"duckdb_version": duckdb.__version__, "db": DB, "reps": REPS, "cases": {}}
    for name, where in CASES:
        sql = SQL.format(w=where)
        if name == "having":
            sql = ("SELECT passenger_count AS g, SUM(fare_cents) AS s, COUNT(*) AS c, "
                   "MIN(fare_cents) AS mn, MAX(fare_cents) AS mx FROM nyc "
                   "GROUP BY 1 HAVING SUM(fare_cents) >= 1e10 ORDER BY 1")
        con.execute(sql).fetchall()  # warmup
        ts = []
        rows = None
        for _ in range(REPS):
            t = time.perf_counter()
            rows = con.execute(sql).fetchall()
            ts.append((time.perf_counter() - t) * 1000.0)
        ts_sorted = sorted(ts)
        out["cases"][name] = {
            "sql": sql,
            "median_ms": statistics.median(ts),
            "min_ms": ts_sorted[0],
            "p95_ms": ts_sorted[min(len(ts_sorted) - 1, int(len(ts_sorted) * 0.95))],
            "rows": [[str(x) for x in r] for r in rows],
        }
    json.dump(out, open(OUT, "w"), indent=1)
    for k, v in out["cases"].items():
        print(f"{k:16s} median={v['median_ms']:10.3f} min={v['min_ms']:10.3f} p95={v['p95_ms']:10.3f} rows={len(v['rows'])}")


if __name__ == "__main__":
    main()
