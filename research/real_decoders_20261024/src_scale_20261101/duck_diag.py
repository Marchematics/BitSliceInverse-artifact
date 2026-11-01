#!/usr/bin/env python3
"""DuckDB 2.0 diagnostics for the 10M-group and 1B controls.

Purpose: separate *scan/parquet-decode* cost from *hash-aggregation* cost, and give a
storage-format-free control (in-memory table) for the same query, so the SCALE report
never attributes a DuckDB weakness to the wrong layer.

usage: duck_diag.py [both|10m|1b]
writes receipts/scale_20261101/duckdb_diag.json
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

HOME = _artifact.expand("~")
ROOT = _artifact.ROOT + "/research/real_decoders_20261024"
OUT = ROOT + "/results/scale_20261101"
REC = _artifact.WORK + "/receipts/scale_20261101"


def bench(con, sql, reps=3):
    con.execute(sql).fetchall()
    ts = []
    rows = None
    for _ in range(reps):
        t = time.perf_counter()
        rows = con.execute(sql).fetchall()
        ts.append((time.perf_counter() - t) * 1000.0)
    ts.sort()
    return {"median_ms": round(statistics.median(ts), 2), "min_ms": round(ts[0], 2),
            "reps": reps, "result_rows": len(rows)}


def main():
    what = sys.argv[1] if len(sys.argv) > 1 else "both"
    res = {"duckdb_version": duckdb.__version__, "date": time.strftime("%Y-%m-%dT%H:%M:%S"),
           "note": "diagnostic: scan/decode vs hash-aggregation cost, parquet vs in-memory table"}
    if what in ("both", "10m"):
        p = OUT + "/g10m.parquet"
        d = {}
        for T in (1, 8):
            con = duckdb.connect()
            con.execute("SET threads=%d" % T)
            con.execute("SET preserve_insertion_order=false")
            con.execute("SET memory_limit='120GB'")
            base = "read_parquet('%s')" % p
            d["T%d_parquet" % T] = {
                "count_only": bench(con, "SELECT COUNT(*) FROM " + base),
                "group_count_only": bench(con, "SELECT g, COUNT(*) FROM " + base + " GROUP BY 1"),
                "group_count_sum": bench(con, "SELECT g, SUM(v), COUNT(*) FROM " + base + " GROUP BY 1"),
                "full5": bench(con, "SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM " + base + " GROUP BY 1"),
            }
            con.close()
        # storage-format-free control: in-memory table of the same rows
        con = duckdb.connect()
        con.execute("SET memory_limit='120GB'")
        con.execute("CREATE TABLE t AS SELECT * FROM read_parquet('%s')" % p)
        d["table_bytes"] = con.execute("SELECT estimated_size FROM duckdb_tables() WHERE table_name='t'").fetchone()[0]
        for T in (1, 8):
            con.execute("SET threads=%d" % T)
            d["T%d_table" % T] = {
                "count_only": bench(con, "SELECT COUNT(*) FROM t"),
                "group_count_only": bench(con, "SELECT g, COUNT(*) FROM t GROUP BY 1"),
                "group_count_sum": bench(con, "SELECT g, SUM(v), COUNT(*) FROM t GROUP BY 1"),
                "full5": bench(con, "SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM t GROUP BY 1"),
            }
        con.close()
        res["g10m"] = d
        print(json.dumps(d, indent=1), flush=True)
    if what in ("both", "1b"):
        p = OUT + "/nyc1b.parquet"
        d = {}
        for T in (1, 8):
            con = duckdb.connect()
            con.execute("SET threads=%d" % T)
            con.execute("SET preserve_insertion_order=false")
            con.execute("SET memory_limit='200GB'")
            base = "read_parquet('%s')" % p
            d["T%d_parquet" % T] = {
                "count_only": bench(con, "SELECT COUNT(*) FROM " + base),
                "full5": bench(con, "SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM " + base + " GROUP BY 1"),
            }
            con.close()
        con = duckdb.connect()
        con.execute("SET memory_limit='200GB'")
        con.execute("CREATE TABLE t AS SELECT * FROM read_parquet('%s')" % p)
        for T in (1, 8):
            con.execute("SET threads=%d" % T)
            d["T%d_table" % T] = {
                "count_only": bench(con, "SELECT COUNT(*) FROM t"),
                "full5": bench(con, "SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM t GROUP BY 1"),
            }
        con.close()
        res["nyc1b"] = d
        print(json.dumps(d, indent=1), flush=True)
    os.makedirs(REC, exist_ok=True)
    with open(REC + "/duckdb_diag.json", "w") as f:
        json.dump(res, f, indent=1)
    print("receipt ->", REC + "/duckdb_diag.json")


if __name__ == "__main__":
    main()
