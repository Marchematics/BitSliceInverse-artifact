#!/usr/bin/env bash
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
# Build the PRE-FIX ECUBE01 binary: verbatim copy of enc_cube.cpp as pinned on
# 2026-11-01 (sha256 00185d02...), i.e. the source that reproduces the documented
# ECUBE01 receipt (NYC-100M cube sha256 da893447...05fc4, 3,879,696 B).
# Same toolchain / vendored FastLanes primitives as src_eai_20261101/build.sh.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE/.."
FL_INC=${FL_INC:-vendor/src/include}
V=vendor
OUT=${OUT:-../../work/specs/bin}
mkdir -p "$OUT"
g++ -std=c++20 -O3 -march=native -DNDEBUG \
  -I "$V/src/include" -I "$FL_INC" \
  "$HERE/enc_cube_orig.cpp" \
  "$V/src/alp/src/fastlanes_gen_ffor.cpp" \
  "$V/src/alp/src/fastlanes_gen_unffor.cpp" \
  "$V/src/primitives/fls_generated/fallback/scalar_aav_uf1/fallback_scalar_aav_1024_uf1_rsum_src.cpp" \
  "$V/src/primitives/fls_generated/fallback/scalar_aav_uf1/fallback_scalar_aav_1024_uf1_transpose_src.cpp" \
  "$V/src/primitives/fls_generated/fallback/scalar_aav_uf1/fallback_scalar_aav_1024_uf1_unrsum_src.cpp" \
  "$V/src/primitives/fls_generated/fallback/scalar_aav_uf1/fallback_scalar_aav_1024_uf1_untranspose_src.cpp" \
  -o "$OUT/enc_cube_orig"
echo built:"$OUT/enc_cube_orig"
