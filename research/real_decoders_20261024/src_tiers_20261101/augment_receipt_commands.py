#!/usr/bin/env python3
"""Add the exact command lines + core affinity to the follow-up receipts.

The measurement scripts record the numbers and the per-query SQL/specs; this small
script attaches the literal command templates that produced them (the same strings the
drivers run) so a receipt is self-contained. It is idempotent and only edits the three
follow-up receipt JSONs (panel_results.json, nullmeasure_100m.json, output_reps.json)
in receipts/tiers_20261101/ and results/tiers_20261101/ when the latter exists.

usage (from research/real_decoders_20261024):
    python3 src_tiers_20261101/augment_receipt_commands.py
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import json
import os

REPO = os.path.abspath(os.getcwd())
RECEIPT = os.path.join(_artifact.WORK, "receipts", "tiers_20261101")
RESULTS = os.path.join(REPO, "results", "tiers_20261101")
BIN = _artifact.expand(os.environ.get("TIERS_BIN", "../../work/tiers/bin"))
SCRATCH = _artifact.expand(os.environ.get("TIERS_SCRATCH", "../../work/tiers/build"))

PANEL_CMDS = {
    "pin_subprocesses": "nice -n 5 taskset -c 96",
    "pin_driver": "nice -n 5 taskset -c 97",
    "build_nyc100_cube3": f"{BIN}/enc_cube3 build_enc results/nyc_taxi_100m_prep/Observed.encoded "
                          f"results/nyc_taxi_100m_prep/Observed 17 13 {SCRATCH}/nyc100_e3.ecube",
    "build_ig2_65k_cube3": f"{BIN}/enc_cube3 build_enc results/ig2_prep_20261025/Observed.encoded "
                           f"results/ig2_prep_20261025/prepared/Observed 17 13 {SCRATCH}/ig2_65k_e3.ecube",
    "ours_A_D": f"{BIN}/enc_cube3 bench <cube> <state.dict> /dev/null 1000 <spec>  "
                f"(query mode: {BIN}/enc_cube3 query <cube> <state.dict> <out.json> <spec>)",
    "ours_B": f"src_eai_multi_20261101/mec bench {SCRATCH}/tpch2_index.mec 200 "
              f"results/eai_multi_20261101/datasets/tpch2/cases.txt",
    "ours_C": "../../work/clustered/bin/clst_tool query "
              "../../work/clustered/build/clst_g<G>_stats.clst -2147483648 <cut> "
              "<out.json> /dev/null 100",
    "control_duckdb_scan": "duckdb.connect(config={'threads': 1}); "
                            "SELECT COUNT(*) FROM (<same aggregation with WHERE>)  # 100 reps, warm",
    "control_duckdb_mv": "duckdb.connect(config={'threads': 1}); CREATE TEMP TABLE mv AS "
                         "SELECT <g>,SUM(v),COUNT(*),MIN(v),MAX(v) FROM <table> GROUP BY 1; "
                         "SELECT COUNT(*) FROM (SELECT <cols> FROM mv [WHERE ...])  # 100 reps",
    "control_value_index": f"src_encoded_ops_20261030/value_index_v2 {SCRATCH}/nyc100.vidx "
                           f"results/nyc_taxi_100m_prep/Observed/state.dict <out.json> range=<lo>,<hi>"
                           f"   # 100 launches, stderr query_ms",
    "control_bitplane": "src_encoded_ops_20261030/multibits query "
                        f"{SCRATCH}/tpch2_mb.bin <out.json> <vlo> <vhi> <a_lo> <a_hi> <b_lo> <b_hi>"
                        "   # 40 launches, stderr query_ms",
    "control_sorted_index": f"src_eai_multi_20261101/sorted_index query {SCRATCH}/tpch2_ridx.bin "
                            "results/eai_multi_20261101/datasets/tpch2/{g,v,c1,c2}.i64 <out.json> "
                            "<vlo> <vhi> <a_lo> <a_hi> <b_lo> <b_hi>   # 20 launches, stderr query_ms",
}

NULL_CMDS = {
    "pin_subprocesses": "nice -n 5 taskset -c 96",
    "data": "python3 src_tiers_20261101/make_nullmeasure_data.py   # system python3 + numpy/pyarrow",
    "build": f"{BIN}/enc_cube3 build {SCRATCH}/gid_nyc_g10.i32 {SCRATCH}/val_nyc100.i32 "
             f"{SCRATCH}/nyc100_e3_nullmeasure.ecube {SCRATCH}/nyc_null_valid.bits",
    "bench": f"{BIN}/enc_cube3 bench {SCRATCH}/nyc100_e3_nullmeasure.ecube "
             f"results/nyc_taxi_100m_prep/Observed/state.dict /dev/null 1000 <spec>",
    "query": f"{BIN}/enc_cube3 query {SCRATCH}/nyc100_e3_nullmeasure.ecube "
             f"results/nyc_taxi_100m_prep/Observed/state.dict <out.json> <spec>",
    "duckdb_reference": "duckdb.connect(config={'threads': 1}); CREATE TEMP TABLE t AS "
                        f"SELECT g,v FROM read_parquet('{SCRATCH}/nyc_nullmeasure.parquet'); "
                        "<per-case SQL in cases[*].duckdb_sql>",
}

OUTPUT_CMDS = {
    "pin": "nice -n 5 taskset -c 96 (every measured process)",
    "ours_json": f"{BIN}/tiers_tool complete_bench {SCRATCH}/stats_1000000.bin "
                 f"{SCRATCH}/out1m_complete.json json 12",
    "ours_bin": f"{BIN}/tiers_tool complete_bench {SCRATCH}/stats_1000000.bin "
                f"{SCRATCH}/out1m_complete.bin bin 12",
    "duckdb_json": "COPY (SELECT g1000000 AS g, SUM(v), COUNT(*), MIN(v), MAX(v) FROM groups "
                   "GROUP BY 1 ORDER BY 1) TO 'out.json' (FORMAT JSON, ARRAY true)",
    "duckdb_parquet": "COPY (same query) TO 'out.parquet' (FORMAT PARQUET, COMPRESSION zstd)",
    "duckdb_fetch": "con.execute(sql).fetchall() then json.dump(...)  # same group table",
    "driver": "python src_tiers_20261101/run_output_reps.py (spawns duck_output.py per format)",
}


def augment(name, cmds):
    for base in (RECEIPT, RESULTS):
        path = os.path.join(base, name)
        if not os.path.exists(path):
            continue
        d = json.load(open(path))
        d["commands"] = cmds
        json.dump(d, open(path, "w"), indent=1)
        print("augmented", path)


def main():
    augment("panel_results.json", PANEL_CMDS)
    augment("nullmeasure_100m.json", NULL_CMDS)
    augment("output_reps.json", OUTPUT_CMDS)


if __name__ == "__main__":
    main()
