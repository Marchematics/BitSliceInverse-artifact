#!/usr/bin/env bash
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
# Build the tiers stream's tools. Two artifacts:
#   enc_cube    -- the PINNED ECUBE02 binary, compiled here from the pinned source
#                  sha256 99e4dcefa1c9b391781c1583f4259cf0693761973cd6b2de0533ce77d57f1543
#                  (src_eai_20261101/eai_cube2.cpp); the build is byte-identical to
#                  the stream's own binary (sha cc3afecd...), which pins the protocol.
#   gen_groups  -- splitmix64(rowid) % G group-key generator
#   tiers_tool  -- materialized 32 B/group stats + block-summary fallback + JSON/binary
#                  result writers (the structures the router picks when O(G*V) does
#                  not fit the space budget)
#
# usage:  bash src_tiers_20261101/build.sh
#         BIN=$ARTIFACT_ROOT/work/tiers/bin  (default)
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"            # .../research/real_decoders_20261024
cd "$REPO"
BIN="${BIN:-$ARTIFACT_ROOT/work/tiers/bin}"
FL_INC="${FL_INC:-vendor/src/include}"
V=vendor
mkdir -p "$BIN"
DEPS=("$V/src/alp/src/fastlanes_gen_ffor.cpp"
      "$V/src/alp/src/fastlanes_gen_unffor.cpp"
      "$V/src/primitives/fls_generated/fallback/scalar_aav_uf1/fallback_scalar_aav_1024_uf1_rsum_src.cpp"
      "$V/src/primitives/fls_generated/fallback/scalar_aav_uf1/fallback_scalar_aav_1024_uf1_transpose_src.cpp"
      "$V/src/primitives/fls_generated/fallback/scalar_aav_uf1/fallback_scalar_aav_1024_uf1_unrsum_src.cpp"
      "$V/src/primitives/fls_generated/fallback/scalar_aav_uf1/fallback_scalar_aav_1024_uf1_untranspose_src.cpp")

g++ -std=c++20 -O3 -march=native -DNDEBUG -I "$V/src/include" -I "$FL_INC" \
    src_eai_20261101/eai_cube2.cpp "${DEPS[@]}" -o "$BIN/enc_cube"
g++ -std=c++20 -O3 -march=native -DNDEBUG "$HERE/gen_groups.cpp" -o "$BIN/gen_groups"
g++ -std=c++20 -O3 -march=native -DNDEBUG "$HERE/tiers_tool.cpp" -o "$BIN/tiers_tool"

echo "pinned-source-sha: $(sha256sum src_eai_20261101/eai_cube2.cpp | cut -d' ' -f1)"
echo "enc_cube-sha256:   $(sha256sum "$BIN/enc_cube" | cut -d' ' -f1)"
echo "gen_groups-sha256: $(sha256sum "$BIN/gen_groups" | cut -d' ' -f1)"
echo "tiers_tool-sha256: $(sha256sum "$BIN/tiers_tool" | cut -d' ' -f1)"
