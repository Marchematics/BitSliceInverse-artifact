#!/usr/bin/env python3
"""ClickHouse (embedded 'chdb' 4.4.0, engine 26.9.2.1): same SQL.

ClickHouse has no downloadable static binary on this host (github/DNS blocked), so the
official embedded ClickHouse build shipped on PyPI as ``chdb`` is used. It is the same
ClickHouse execution engine.

  VENV=../../work/bench_baselines/venv/bin/python ; nice -n 5 taskset -c 56 $VENV bench_chdb.py
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import json
import os
import sys
import time

import pyarrow as pa

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as C

import chdb
from chdb.session import Session

FMT = os.environ.get("CHDB_FMT", "Arrow")


def rows_from_result(r):
    if FMT == "Arrow":
        try:
            raw = r.bytes()
            tbl = pa.ipc.open_stream(pa.py_buffer(raw)).read_all()
        except Exception:
            tbl = pa.ipc.open_file(pa.py_buffer(r.bytes())).read_all()
        return [[x["g"], x["s"], x["c"], x["mn"], x["mx"]] for x in tbl.to_pylist()]
    # JSONEachRow fallback
    out = []
    for line in r.bytes().decode().strip().splitlines():
        d = json.loads(line)
        out.append([d["g"], d["s"], d["c"], d["mn"], d["mx"]])
    return out


def main():
    t0 = time.perf_counter()
    s = Session()
    s.query("SET max_threads=1")
    s.query("SET max_block_size=65536")
    s.query("CREATE TABLE nyc ENGINE = Memory AS "
            "SELECT passenger_count, fare_cents FROM file('%s')" % C.PARQUET_PATH)
    load_ms = (time.perf_counter() - t0) * 1000.0
    print("chdb load %.1f ms; engine=%s" % (load_ms, chdb.engine_version), flush=True)

    cases = {}
    for name, where, _label in C.CASES:
        sql = C.sql_for(where)
        try:
            C.warmup(lambda: rows_from_result(s.query(sql, FMT)))
        except Exception as e:
            sql2 = sql.replace("GROUP BY 1", "GROUP BY g")
            print("note: rewrite GROUP BY 1 -> g (%s)" % str(e).splitlines()[0], flush=True)
            sql = sql2
            C.warmup(lambda: rows_from_result(s.query(sql, FMT)))

        def run(_sql=sql):
            return rows_from_result(s.query(_sql, FMT))

        st, rows = C.timed(run)
        cases[name] = (st, rows)
        print("%-12s median=%10.3f ms reps=%d" % (name, st["median_ms"], st["reps"]), flush=True)
    rec = C.make_receipt(
        "clickhouse_chdb", "chdb %s / ClickHouse engine %s" % (".".join(map(str, chdb.chdb_version))
                                                               if isinstance(chdb.chdb_version, tuple)
                                                               else chdb.chdb_version,
                                                               chdb.engine_version),
        "Parquet file loaded into a ClickHouse Memory table (in-memory)", os.path.getsize(C.PARQUET_PATH),
        notes="Official ClickHouse engine via the embedded chdb wheel (server binary not "
              "downloadable: github/DNS blocked). max_threads=1, 3 untimed warm-ups "
              "(first executions are ~2x slower: CH expression JIT/page warm-up: "
              "~1.3 s vs ~0.65 s steady state, measured). Memory table = engine-resident; "
              "result fetched as Arrow IPC stream each rep.",
        cases=cases, load_ms=load_ms)
    rec["threading_check"] = "SET max_threads=1"
    return C.write_receipt(rec, os.path.join(C.RUNS, "clickhouse_chdb.json"))


if __name__ == "__main__":
    print("chdb", chdb.chdb_version, "engine", chdb.engine_version)
    main()
