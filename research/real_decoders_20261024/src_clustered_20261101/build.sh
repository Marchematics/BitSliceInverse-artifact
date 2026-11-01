#!/usr/bin/env bash
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
# Build the clustered stream's tool: clst_tool (CLST01 value-clustered exact
# aggregation). Same compiler flags as the pinned streams (-O3 -march=native).
#
# usage:  bash src_clustered_20261101/build.sh
#         CLST_BIN=$ARTIFACT_ROOT/work/clustered/bin   (default)
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
BIN="${CLST_BIN:-$ARTIFACT_ROOT/work/clustered/bin}"
mkdir -p "$BIN"
g++ -std=c++20 -O3 -march=native -DNDEBUG "$HERE/clst_tool.cpp" -o "$BIN/clst_tool"
echo "clst_tool.cpp sha256: $(sha256sum "$HERE/clst_tool.cpp" | cut -d' ' -f1)"
echo "clst_tool     sha256: $(sha256sum "$BIN/clst_tool" | cut -d' ' -f1)"
echo "clst_tool bytes:      $(stat -c%s "$BIN/clst_tool")"
