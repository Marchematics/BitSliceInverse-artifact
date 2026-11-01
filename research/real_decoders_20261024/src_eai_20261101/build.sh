#!/usr/bin/env bash
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
# Build the EAI prototypes against the vendored FastLanes primitives.
# cwd = research/real_decoders_20261024 (set by the caller).
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE/.."
FL_INC=${FL_INC:-vendor/src/include}
V=vendor
DEPS=("$V/src/alp/src/fastlanes_gen_ffor.cpp"
      "$V/src/alp/src/fastlanes_gen_unffor.cpp"
      "$V/src/primitives/fls_generated/fallback/scalar_aav_uf1/fallback_scalar_aav_1024_uf1_rsum_src.cpp"
      "$V/src/primitives/fls_generated/fallback/scalar_aav_uf1/fallback_scalar_aav_1024_uf1_transpose_src.cpp"
      "$V/src/primitives/fls_generated/fallback/scalar_aav_uf1/fallback_scalar_aav_1024_uf1_unrsum_src.cpp"
      "$V/src/primitives/fls_generated/fallback/scalar_aav_uf1/fallback_scalar_aav_1024_uf1_untranspose_src.cpp")
for pair in "eai_cube2:enc_cube" "eai_cube3:enc_cube3"; do
  src="${pair%%:*}"; out="${pair##*:}"
  g++ -std=c++20 -O3 -march=native -DNDEBUG -I "$V/src/include" -I "$FL_INC" \
    "$HERE/$src.cpp" "${DEPS[@]}" -o "$HERE/$out"
  echo "built:$out"
done
