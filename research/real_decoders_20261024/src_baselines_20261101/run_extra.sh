#!/usr/bin/env bash
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
# Runs AFTER run_all.sh: stability control + Cabin selection-only baseline.
# Both pinned to the same timing core, serialised after the main suite.
#
#   cd ../../research/real_decoders_20261024/src_baselines_20261101
#   nohup bash run_extra.sh > ../../work/bench_baselines/logs/run_extra.log 2>&1 &
set -uo pipefail
cd "$(dirname "$0")"
CORE=${CORE:-56}
RUNS=../../work/bench_baselines/runs
LOGS=../../work/bench_baselines/logs
VENV=../../work/bench_baselines/venv/bin/python
VOR=python3
SYS=/usr/bin/python3
mkdir -p "$RUNS" "$LOGS"

run() {
    local name=$1 py=$2 script=$3; shift 3
    echo "===== $name  $(date -Is)" | tee -a "$LOGS/run_extra.log"
    /usr/bin/time -f "wall=%e user=%U sys=%S rss_kb=%M" -o "$RUNS/$name.time" \
        nice -n 5 taskset -c "$CORE" "$py" "$script" "$@" >> "$LOGS/$name.log" 2>&1
    echo "----- $name rc=$? $(cat "$RUNS/$name.time" 2>/dev/null)" | tee -a "$LOGS/run_extra.log"
}

# 0) Our own ECUBE01 cube, measured by THIS harness (materialized-aggregate block)
run eai_cube01 "$SYS" bench_eai_cube.py

# 1) 60-rep stability control for DuckDB (and 20-rep Arrow) on the fixed query
STABILITY_REPS=60 run stability_control "$VOR" bench_stability.py

# 2) Cabin (SIGMOD 2024) selection-only baseline, 2 reps/case (index rebuild per process)
CABIN_REPS=2 CORE=$CORE run cabin_sigmod24_scan "$SYS" bench_cabin.py

echo "EXTRA DONE $(date -Is)" | tee -a "$LOGS/run_extra.log"
