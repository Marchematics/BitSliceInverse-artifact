#!/usr/bin/env bash
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
# Build the FROZEN ECUBE03 binary from a verbatim copy of the frozen artifact:
#   source eai_cube3.cpp sha256 c93a4530f7b21d5e1a1f174374f5fdd49783a20ad32638b370baabf3e86562f0
# (extracted from the pinned git blob d792ed36..., commit 6a94fee).
# The worktree file has one later edit (encoded-build dense-flag contiguity) that
# does not change any index used by the SPECS corpus (all encoded pairs are
# non-dense); this script builds the frozen blob that the paper cites.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE/.."
FL_INC=${FL_INC:-vendor/src/include}
V=vendor
OUT=${OUT:-../../work/specs/bin}
mkdir -p "$OUT"
g++ -std=c++20 -O3 -march=native -DNDEBUG \
  -I "$V/src/include" -I "$FL_INC" \
  "$HERE/eai_cube3_frozen.cpp" \
  "$V/src/alp/src/fastlanes_gen_ffor.cpp" \
  "$V/src/alp/src/fastlanes_gen_unffor.cpp" \
  "$V/src/primitives/fls_generated/fallback/scalar_aav_uf1/fallback_scalar_aav_1024_uf1_rsum_src.cpp" \
  "$V/src/primitives/fls_generated/fallback/scalar_aav_uf1/fallback_scalar_aav_1024_uf1_transpose_src.cpp" \
  "$V/src/primitives/fls_generated/fallback/scalar_aav_uf1/fallback_scalar_aav_1024_uf1_unrsum_src.cpp" \
  "$V/src/primitives/fls_generated/fallback/scalar_aav_uf1/fallback_scalar_aav_1024_uf1_untranspose_src.cpp" \
  -o "$OUT/enc_cube3_frozen"
echo built:"$OUT/enc_cube3_frozen"
