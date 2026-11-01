#!/usr/bin/env python3
"""Build the fast-setup adapter variants (word-level NOT NULL validity) on the CPU host."""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parent))
import artifact_paths as _artifact
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent
VENDOR = ROOT / 'vendor/src/include'
GENERATED = [
    ROOT / 'vendor/src/alp/src/fastlanes_gen_ffor.cpp',
    ROOT / 'vendor/src/alp/src/fastlanes_gen_unffor.cpp',
    ROOT / 'vendor/src/primitives/fls_generated/fallback/scalar_aav_uf1/fallback_scalar_aav_1024_uf1_rsum_src.cpp',
    ROOT / 'vendor/src/primitives/fls_generated/fallback/scalar_aav_uf1/fallback_scalar_aav_1024_uf1_transpose_src.cpp',
    ROOT / 'vendor/src/primitives/fls_generated/fallback/scalar_aav_uf1/fallback_scalar_aav_1024_uf1_unrsum_src.cpp',
    ROOT / 'vendor/src/primitives/fls_generated/fallback/scalar_aav_uf1/fallback_scalar_aav_1024_uf1_untranspose_src.cpp',
]
TARGETS = [
    ('src_fullsql_ig2_fast_20261029', 'results/ig2_prep_20261025/adapter_ig2_fast'),
    ('src_fullsql_hatred8_fast_20261030', 'results/hatred8_prep_v5/adapter_hatred8_fast'),
    ('src_fullsql_hatred10_fast_20261030', 'results/hatred10_prep_v1/adapter_hatred10_fast'),
    ('src_fullsql_citymax14_fast_20261030', 'results/citymax14_prep_v3/adapter_citymax14_fast'),
    ('src_fullsql_ig16_direct_fast_20261030', 'results/ig16_prep_v1/adapter_ig16_fast'),
]
for src, out in TARGETS:
    cmd = ['g++', '-std=c++20', '-O3', '-march=native', '-DNDEBUG', '-I', str(VENDOR),
           str(ROOT / src / 'adapter.cpp'), *[str(p) for p in GENERATED], '-o', str(ROOT / out)]
    subprocess.run(cmd, check=True)
    print('built', out)
