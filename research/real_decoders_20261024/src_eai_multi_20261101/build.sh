#!/usr/bin/env bash
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
# Build MEC02 and the sorted row-id range-index baseline.
#   bash build.sh   ->  ./mec  ./sorted_index
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
CXX=${CXX:-g++}
FLAGS="-std=c++20 -O3 -march=native -DNDEBUG -fno-math-errno -Wall -Wextra"
$CXX $FLAGS -o "$HERE/mec" "$HERE/mec.cpp"
$CXX $FLAGS -o "$HERE/sorted_index" "$HERE/sorted_index.cpp"
echo "built:$HERE/mec $HERE/sorted_index"
