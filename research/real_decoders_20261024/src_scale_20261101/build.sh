#!/usr/bin/env bash
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
# Build the scale-campaign binaries. cwd may be anywhere; the script resolves itself.
#   cube_scale  : ECUBE02 v2 + parallel build/query + materialized group stats
#   gen_scale   : deterministic 1B-replica and 10M-group dataset generators
# On the remote host the vendored FastLanes tree lives at ../vendor (repo-relative)
# and the FastLanes public headers at vendor/src/include
# (same include set as src_eai_20261101/build.sh, so cube_scale is build-compatible
# with the validated v2 binary).
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE/.."
FL_INC=${FL_INC:-vendor/src/include}
V=vendor
g++ -std=c++20 -O3 -march=native -DNDEBUG -pthread \
  -I "$V/src/include" -I "$FL_INC" \
  "$HERE/cube_scale.cpp" \
  "$V/src/alp/src/fastlanes_gen_ffor.cpp" \
  "$V/src/alp/src/fastlanes_gen_unffor.cpp" \
  "$V/src/primitives/fls_generated/fallback/scalar_aav_uf1/fallback_scalar_aav_1024_uf1_rsum_src.cpp" \
  "$V/src/primitives/fls_generated/fallback/scalar_aav_uf1/fallback_scalar_aav_1024_uf1_transpose_src.cpp" \
  "$V/src/primitives/fls_generated/fallback/scalar_aav_uf1/fallback_scalar_aav_1024_uf1_unrsum_src.cpp" \
  "$V/src/primitives/fls_generated/fallback/scalar_aav_uf1/fallback_scalar_aav_1024_uf1_untranspose_src.cpp" \
  -o "$HERE/cube_scale"
echo "built:$HERE/cube_scale"

g++ -std=c++17 -O3 -march=native -DNDEBUG \
  "$HERE/gen_scale.cpp" -o "$HERE/gen_scale"
echo "built:$HERE/gen_scale"

# ECUBE03 (frozen format) single+parallel build paths: copy of src_eai_20261101/eai_cube3.cpp
# plus build_enc_mt / build_plain_mt. Same vendored deps as src_eai_20261101/build.sh.
g++ -std=c++20 -O3 -march=native -DNDEBUG -pthread \
  -I "$V/src/include" -I "$FL_INC" \
  "$HERE/ecube3_mt.cpp" \
  "$V/src/alp/src/fastlanes_gen_ffor.cpp" \
  "$V/src/alp/src/fastlanes_gen_unffor.cpp" \
  "$V/src/primitives/fls_generated/fallback/scalar_aav_uf1/fallback_scalar_aav_1024_uf1_rsum_src.cpp" \
  "$V/src/primitives/fls_generated/fallback/scalar_aav_uf1/fallback_scalar_aav_1024_uf1_transpose_src.cpp" \
  "$V/src/primitives/fls_generated/fallback/scalar_aav_uf1/fallback_scalar_aav_1024_uf1_unrsum_src.cpp" \
  "$V/src/primitives/fls_generated/fallback/scalar_aav_uf1/fallback_scalar_aav_1024_uf1_untranspose_src.cpp" \
  -o "$HERE/ecube3_mt"
echo "built:$HERE/ecube3_mt"
