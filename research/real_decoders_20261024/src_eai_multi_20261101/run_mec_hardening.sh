#!/usr/bin/env bash
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
# ONE COMMAND for the MEC hardening measurements (multi-core / one budget rule / cold).
#   cd ../../research/real_decoders_20261024
#   bash src_eai_multi_20261101/run_mec_hardening.sh
# Cores 64-79 are pinned for every timed command: nice -n 5 taskset -c 64-79.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
OUT="$ROOT/results/eai_multi_20261101"
PY_DUCK="${PY_DUCK:-python3}"
PIN=(nice -n 5 taskset -c 64-79)

bash "$HERE/build.sh"
mkdir -p "$OUT/mt" "$OUT/budget" "$OUT/cold"
echo "== [1/3] multi-core: build + query 1/4/8/16 threads, DuckDB same-thread control =="
"${PIN[@]}" "$PY_DUCK" "$HERE/bench_mec_hardening.py" --part mt \
  --reps "${REPS:-200}" --reps-nyc "${REPS_NYC:-30}" \
  --duckdb-reps "${DUCK_REPS:-20}" --duckdb-reps-nyc "${DUCK_REPS_NYC:-3}"
echo "== [2/3] MEC01 vs MEC02 under one budget rule =="
"${PIN[@]}" "$PY_DUCK" "$HERE/bench_mec_hardening.py" --part budget
echo "== [3/3] cold complete request (>=20 fresh launches) =="
"${PIN[@]}" "$PY_DUCK" "$HERE/bench_mec_hardening.py" --part cold --launches "${LAUNCHES:-21}"
echo "== receipts: $OUT/{mt,budget,cold}/*.json =="
