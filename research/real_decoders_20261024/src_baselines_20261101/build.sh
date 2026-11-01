#!/usr/bin/env bash
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
# Build the two C++ baselines.  Run on the remote host (localhost), never on the Mac.
set -euo pipefail
cd "$(dirname "$0")"
NICE="nice -n 5"
CORE_BUILD=57          # builds are not timing runs: keep them off the timing core 56
mkdir -p ../../work/bench_baselines/data

$NICE taskset -c $CORE_BUILD g++ -O3 -march=native -std=c++17 -o range_index_nyc range_index_nyc.cpp
$NICE taskset -c $CORE_BUILD g++ -O3 -std=c++17 -o mat_summary mat_summary.cpp
$NICE taskset -c $CORE_BUILD g++ -O3 -std=c++17 -o trivial_probe trivial_probe.cpp
echo "built: $(ls -l range_index_nyc mat_summary | wc -l) binaries"
