#!/usr/bin/env bash
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
# ONE COMMAND: rebuild MEC02, prepare every dataset if needed, and run the whole
# measurement + exactness battery, writing all receipts.
#
#   cd ../../research/real_decoders_20261024
#   bash src_eai_multi_20261101/run_mec.sh
#
# Environment:
#   DATASETS   comma list (default: tpch2,tpch2_null,syn_cluster,nyc3,nyc3b)
#   CORE       pinned core (default 64)          FORCE_PREP=1     regenerate datasets
#   SKIP_BASELINES=1  skip bitplane + sorted-index baselines (MEC vs DuckDB only)
#
# Everything timing-related runs as: nice -n 5 taskset -c $CORE <cmd>
# Output: results/eai_multi_20261101/{MEC_ALL.json, datasets/, <ds>/{plan,build,info,
#         sweep,diff_fuzz}.json}
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
OUT="$ROOT/results/eai_multi_20261101"
DSDIR="$OUT/datasets"
mkdir -p "$OUT"

CORE="${CORE:-64}"
DATASETS="${DATASETS:-tpch2,tpch2_null,syn_cluster,nyc3,nyc3b}"
PY_SYS="${PY_SYS:-python3}"                       # numpy + pyarrow (dataset prep)
PY_DUCK="${PY_DUCK:-python3}"   # duckdb 2.0.0 (oracle + timing)
PIN=(nice -n 5 taskset -c "$CORE")

echo "== [1/3] build binaries =="
bash "$HERE/build.sh"

echo "== [2/3] prepare datasets (skips any with meta.json unless FORCE_PREP=1) =="
MISSING=""
for ds in ${DATASETS//,/ }; do
  if [ ! -f "$DSDIR/$ds/meta.json" ] || [ "${FORCE_PREP:-0}" = "1" ]; then MISSING="$MISSING,$ds"; fi
done
if [ -n "$MISSING" ]; then
  MISSING="${MISSING#,}"
  echo "   preparing: $MISSING"
  "${PIN[@]}" "$PY_SYS" "$HERE/prep_mec_datasets.py" --datasets "$MISSING"
else
  echo "   all datasets present"
fi
KEEP=""
for ds in ${DATASETS//,/ }; do
  if [ -f "$DSDIR/$ds/meta.json" ]; then KEEP="$KEEP,$ds";
  else echo "   !! $ds missing after prep -- dropping it" >&2; fi
done
DATASETS="${KEEP#,}"
echo "   datasets: $DATASETS"

echo "== [3/3] plan/build/benchmark/exactness battery =="
EXTRA=""
if [ "${SKIP_BASELINES:-0}" = "1" ]; then EXTRA="--skip-baselines"; fi
"${PIN[@]}" "$PY_DUCK" "$HERE/run_all_mec.py" --datasets "$DATASETS" --outdir "$OUT" \
  --pin "nice -n 5 taskset -c $CORE" $EXTRA

echo "== done: $OUT/MEC_ALL.json =="
