#!/usr/bin/env bash
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
set -euo pipefail
cd "$(dirname "$0")/.."
g++ -std=c++20 -O3 -march=native -DNDEBUG \
  -I vendor/src/include \
  -I vendor/src/include \
  src_encoded_ops_20261030/generic_eval.cpp \
  vendor/src/alp/src/fastlanes_gen_ffor.cpp \
  vendor/src/alp/src/fastlanes_gen_unffor.cpp \
  vendor/src/primitives/fls_generated/fallback/scalar_aav_uf1/fallback_scalar_aav_1024_uf1_rsum_src.cpp \
  vendor/src/primitives/fls_generated/fallback/scalar_aav_uf1/fallback_scalar_aav_1024_uf1_transpose_src.cpp \
  vendor/src/primitives/fls_generated/fallback/scalar_aav_uf1/fallback_scalar_aav_1024_uf1_unrsum_src.cpp \
  vendor/src/primitives/fls_generated/fallback/scalar_aav_uf1/fallback_scalar_aav_1024_uf1_untranspose_src.cpp \
  -o src_encoded_ops_20261030/generic_eval
