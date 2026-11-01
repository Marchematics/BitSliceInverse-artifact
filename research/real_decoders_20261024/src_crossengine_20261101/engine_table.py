#!/usr/bin/env python3
"""Per-engine table: engine-resident and cold-request latency, index load time.

Everything here is measured on the pinned NYC-100M ECUBE02 and on the same
engine binaries used by panel.py, so the numbers can be quoted next to the mixed
workload. Cold requests are separate processes timed by this parent; the child's
own internal breakdown is recorded from its stderr/stdout JSON.

Usage: engine_table.py [cube]
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import json
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.environ.get("CE_REPO", _artifact.expand("../../research/real_decoders_20261024"))
DATA = os.path.join(REPO, "results")
WORK = os.environ.get("CE_WORK", _artifact.expand("../../work/crossengine"))
BIN = os.path.join(WORK, "build")
OUT = os.environ.get("CE_OUT", os.path.join(WORK, "runs"))
VENV = _artifact.python()
CUBE = sys.argv[1] if len(sys.argv) > 1 else os.path.join(WORK, "data/nyc100_enc.ecube")
DICT = os.path.join(DATA, "nyc_taxi_100m_prep/Observed/state.dict")
DUCKDB_FILE = os.path.join(DATA, "nyc_taxi_100m_duckdb/nyc.duckdb")
ENC_CUBE = os.path.join(REPO, "src_eai_20261101/enc_cube")
PIN = ["nice", "-n", "5", "taskset", "-c", _artifact.core()]
SPECS = {"fixed_all4": "", "le_930": "hi=930", "in_1_2": "where_in=1,2",
         "having": "having=sum>=1e10", "narrow": "range=80000000,90000000"}


def run(cmd, timeout=1800):
    t = time.perf_counter()
    p = subprocess.run(PIN + cmd, capture_output=True, text=True, timeout=timeout)
    wall = (time.perf_counter() - t) * 1000.0
    if p.returncode != 0:
        raise SystemExit(f"FAILED: {' '.join(cmd)}\n{p.stderr[-800:]}")
    return p, wall


def last_json(text):
    for line in reversed(text.strip().splitlines()):
        line = line.strip()
        if line.startswith("{"):
            return json.loads(line)
    return None


def cold(cmd_builder, reps=20):
    walls = []
    inner = None
    for _ in range(reps):
        p, wall = run(cmd_builder())
        walls.append(wall)
        if inner is None:
            inner = last_json(p.stderr) or last_json(p.stdout)
    walls.sort()
    return {"median_ms": walls[len(walls) // 2], "min_ms": walls[0],
            "p95_ms": walls[min(len(walls) - 1, int(len(walls) * 0.95))], "reps": reps,
            "internal": inner}


def main():
    os.makedirs(OUT, exist_ok=True)
    rec = {"cube": CUBE, "dict": DICT, "duckdb_file": DUCKDB_FILE,
           "pinned_enc_cube": ENC_CUBE, "pin": "nice -n 5 taskset -c 112",
           "specs": SPECS, "engines": {}}

    # ---- index load (mmap + dictionary), warm process ----------------------
    p, _ = run([os.path.join(BIN, "ecube_direct"), "load", CUBE, DICT])
    rec["engines"]["cube_direct_load"] = last_json(p.stdout)
    p, _ = run([os.path.join(BIN, "ecube_duck"), "load", CUBE, DICT])
    rec["engines"]["duckdb_table_function_load"] = last_json(p.stdout)

    # ---- native cube CLI, engine-resident (pinned binary) -----------------
    print("[native] enc_cube bench ...", flush=True)
    native = {}
    for name, spec in SPECS.items():
        cmd = [ENC_CUBE, "bench", CUBE, DICT, os.path.join(OUT, "native_out.json"), "500"]
        if spec:
            cmd.append(spec)
        p, _ = run(cmd)
        native[name] = last_json(p.stderr)
    rec["engines"]["native_enc_cube"] = {"protocol": "warm_in_process", "cases": native}

    # ---- cube through DuckDB, warm unprepared / prepared -------------------
    print("[duckdb] table function bench ...", flush=True)
    for mode, key in (("bench", "duckdb_table_function_warm"),
                      ("benchp", "duckdb_table_function_warm_prepared")):
        cases = {}
        for name, spec in SPECS.items():
            cmd = [os.path.join(BIN, "ecube_duck"), mode, CUBE, DICT, "500"]
            if spec:
                cmd.append(spec)
            p, _ = run(cmd)
            cases[name] = last_json(p.stdout)
        rec["engines"][key] = {"protocol": mode, "cases": cases}

    # ---- DataFusion, warm --------------------------------------------------
    print("[datafusion] bench ...", flush=True)
    cases = {}
    for name, spec in SPECS.items():
        cmd = [VENV, os.path.join(HERE, "datafusion_ecube.py"), CUBE, DICT, "100"]
        if spec:
            cmd.append(spec)
        p, _ = run(cmd)
        cases[name] = last_json(p.stdout)
    rec["engines"]["datafusion_warm"] = {"protocol": "warm_engine_resident", "cases": cases}

    # ---- cold requests: one process per request ---------------------------
    print("[cold] native CLI ...", flush=True)
    cold_native, cold_direct, cold_duck = {}, {}, {}
    for name, spec in SPECS.items():
        def mk_native(spec=spec, name=name):
            c = [ENC_CUBE, "query", CUBE, DICT, os.path.join(OUT, f"cold_native_{name}.json")]
            if spec:
                c.append(spec)
            return c

        def mk_direct(spec=spec, name=name):
            c = [os.path.join(BIN, "ecube_direct"), "query", CUBE, DICT,
                 os.path.join(OUT, f"cold_direct_{name}.json")]
            if spec:
                c.append(spec)
            return c

        def mk_duck(spec=spec, name=name):
            c = [os.path.join(BIN, "ecube_duck"), "query", CUBE, DICT,
                 os.path.join(OUT, f"cold_duck_{name}.json")]
            if spec:
                c.append(spec)
            return c

        cold_native[name] = cold(mk_native)
        cold_direct[name] = cold(mk_direct)
        cold_duck[name] = cold(mk_duck)
    rec["engines"]["native_enc_cube_cold"] = {"protocol": "cold_process", "cases": cold_native}
    rec["engines"]["cube_direct_cold"] = {"protocol": "cold_process", "cases": cold_direct}
    rec["engines"]["duckdb_table_function_cold"] = {"protocol": "cold_process", "cases": cold_duck}

    # ---- engine floors -----------------------------------------------------
    print("[floor] ...", flush=True)
    p, _ = run([os.path.join(BIN, "ecube_duck"), "floor", "300"])
    rec["engines"]["duckdb_floor_unprepared"] = last_json(p.stdout)
    p, _ = run([os.path.join(BIN, "ecube_duck"), "floorp", "300"])
    rec["engines"]["duckdb_floor_prepared"] = last_json(p.stdout)
    p, _ = run([VENV, os.path.join(HERE, "datafusion_ecube.py"), CUBE, DICT, "50", ""])
    df = last_json(p.stdout)
    rec["engines"]["datafusion_floor"] = {"median_ms": df["floor_median_ms"],
                                         "note": "SELECT 42, same loop shape",
                                         "index_load_ms": df["index_load_ms"]}

    # ---- process floor for a trivial C++ binary ---------------------------
    p, wall = run([os.path.join(BIN, "ecube_direct"), "load", CUBE, DICT])
    rec["process_floor"] = {"trivial_load_only_ms": wall}

    path = os.path.join(OUT, "engine_receipt.json")
    with open(path, "w") as f:
        json.dump(rec, f, indent=1)
    print(f"wrote {path}")
    for name in SPECS:
        row = {
            "native_warm_us": native[name]["query_median_ms"] * 1000,
            "duck_warm_us": rec["engines"]["duckdb_table_function_warm"]["cases"][name]["sql_median_ms"] * 1000,
            "duck_warm_p_us": rec["engines"]["duckdb_table_function_warm_prepared"]["cases"][name]["sql_median_ms"] * 1000,
            "df_warm_us": rec["engines"]["datafusion_warm"]["cases"][name]["median_ms"] * 1000,
            "native_cold_ms": cold_native[name]["median_ms"],
            "direct_cold_ms": cold_direct[name]["median_ms"],
            "duck_cold_ms": cold_duck[name]["median_ms"],
        }
        print(f"{name:12s} " + " ".join(f"{k}={v:.2f}" for k, v in row.items()))


if __name__ == "__main__":
    main()
