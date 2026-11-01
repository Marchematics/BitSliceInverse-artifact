#!/usr/bin/env python3
"""DuckDB 2.x side of the output/serialization comparison (one measured process).

usage: duck_output.py DB SQL OUT MODE
   MODE = copy_json | copy_parquet | fetch
Prints one JSON line with the internal timings; the parent adds /usr/bin/time RSS.
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import json
import sys
import time

import duckdb

db, sql, out, mode = sys.argv[1:5]
t0 = time.perf_counter()
con = duckdb.connect(db, read_only=True)
con.execute("SET threads=1")
t1 = time.perf_counter()
res = {"mode": mode, "connect_ms": (t1 - t0) * 1000}
if mode == "copy_json":
    t = time.perf_counter()
    con.execute(f"COPY ({sql}) TO '{out}' (FORMAT JSON, ARRAY true)")
    res["write_ms"] = (time.perf_counter() - t) * 1000
elif mode == "copy_parquet":
    t = time.perf_counter()
    con.execute(f"COPY ({sql}) TO '{out}' (FORMAT PARQUET, COMPRESSION zstd)")
    res["write_ms"] = (time.perf_counter() - t) * 1000
elif mode == "fetch":
    t = time.perf_counter()
    rows = con.execute(sql).fetchall()
    res["fetch_ms"] = (time.perf_counter() - t) * 1000
    res["rows"] = len(rows)
    t = time.perf_counter()
    json.dump([[str(x) for x in r] for r in rows], open(out, "w"))
    res["pyjson_ms"] = (time.perf_counter() - t) * 1000
else:
    raise SystemExit("bad mode")
res["total_ms"] = (time.perf_counter() - t0) * 1000
print(json.dumps(res))
