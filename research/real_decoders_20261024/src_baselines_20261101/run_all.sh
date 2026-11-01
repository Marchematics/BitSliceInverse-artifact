#!/usr/bin/env bash
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
# Run every external baseline under the identical protocol on the remote host:
#   nice -n 5 taskset -c 56 <engine>        (single timing core, single-thread settings)
# Each run is wrapped in /usr/bin/time so CPU-time/wall ratio can be checked for
# accidental multi-threading, and stdout/stderr are kept as logs.
#
#   cd ../../research/real_decoders_20261024/src_baselines_20261101
#   nohup bash run_all.sh > ../../work/bench_baselines/logs/run_all.log 2>&1 &
set -uo pipefail
cd "$(dirname "$0")"
CORE=${CORE:-56}
RUNS=../../work/bench_baselines/runs
LOGS=../../work/bench_baselines/logs
VENV=../../work/bench_baselines/venv/bin/python
VOR=python3
SYS=/usr/bin/python3
mkdir -p "$RUNS" "$LOGS"
export BASELINE_REPS=${BASELINE_REPS:-30}

run() {  # run <name> <python> <script> [args...]
    local name=$1 py=$2 script=$3; shift 3
    echo "===== $name  $(date -Is)  $(basename $py) $script $*" | tee -a "$LOGS/run_all.log"
    /usr/bin/time -f "wall=%e user=%U sys=%S rss_kb=%M" \
        -o "$RUNS/$name.time" \
        nice -n 5 taskset -c "$CORE" "$py" "$script" "$@" >> "$LOGS/$name.log" 2>&1
    local rc=$?
    echo "----- $name rc=$rc $(cat "$RUNS/$name.time" 2>/dev/null)" | tee -a "$LOGS/run_all.log"
    return 0
}

# 1) DuckDB 2.0: reference rows + native scan + materialized-view variant
run duckdb20_scan        "$VOR" bench_duckdb.py ref
run duckdb20_scan        "$VOR" bench_duckdb.py scan
run duckdb20_materialized "$VOR" bench_duckdb.py mv

# 2) Arrow (in-memory and Parquet-dataset paths)
run arrow25_acero_groupby "$SYS" bench_arrow.py mem
run arrow25_dataset_parquet "$SYS" bench_arrow.py ds

# 3) Vortex 0.87
run vortex087_scan       "$VOR" bench_vortex.py scan
run vortex087_predecoded "$VOR" bench_vortex.py pre

# 4) Polars 2.0 (single thread via env)
export POLARS_MAX_THREADS=1
run polars20             "$SYS" bench_polars.py
unset POLARS_MAX_THREADS

# 5) DataFusion 55
run datafusion55_parquet "$SYS" bench_datafusion.py

# 6) ClickHouse (embedded chdb)
run clickhouse_chdb      "$VENV" bench_chdb.py

# 7) Daft 0.7.26
export RAYON_NUM_THREADS=1
run daft0726             "$VENV" bench_daft.py
unset RAYON_NUM_THREADS

# 8) Pandas 2.3.3 (classic scan engine)
run pandas233            "$SYS" bench_pandas.py

# 9) Class-2 in-repo baselines: materialized summary + sorted range index
run materialized_summary "$SYS" bench_materialized.py
run range_index_nyc      "$SYS" bench_range_index.py

echo "ALL DONE $(date -Is)" | tee -a "$LOGS/run_all.log"
