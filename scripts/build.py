#!/usr/bin/env python3
"""Build native indexes and experiment tools."""
import argparse
import os
from pathlib import Path
import platform
import shlex
import subprocess

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / 'research/real_decoders_20261024'
OUT = ROOT / 'build'
CXX = shlex.split(os.environ.get('CXX', 'c++'))
FLAGS = ['-std=c++20', '-O3', '-DNDEBUG', '-pthread']
if platform.machine().lower() in {'x86_64', 'amd64'}:
    FLAGS += ['-march=native']

TARGETS = {
    'enc_cube': ('src_eai_20261101/eai_cube2.cpp', True),
    'enc_cube3': ('src_eai_20261101/eai_cube3.cpp', True),
    'ecube3_mt': ('src_scale_20261101/ecube3_mt.cpp', True),
    'cube_scale': ('src_scale_20261101/cube_scale.cpp', True),
    'generic_eval': ('src_encoded_ops_20261030/generic_eval.cpp', True),
    'encode': ('../../scripts/encode.cpp', True),
    'mec': ('src_eai_multi_20261101/mec.cpp', False),
    'sorted_index': ('src_eai_multi_20261101/sorted_index.cpp', False),
    'clst_tool': ('src_clustered_20261101/clst_tool.cpp', False),
    'tiers_tool': ('src_tiers_20261101/tiers_tool.cpp', False),
    'gen_groups': ('src_tiers_20261101/gen_groups.cpp', False),
    'gen_scale': ('src_scale_20261101/gen_scale.cpp', False),
    'range_index': ('src_baselines_20261101/range_index_nyc.cpp', False),
    'mat_summary': ('src_baselines_20261101/mat_summary.cpp', False),
    'ecube_direct': ('src_crossengine_20261101/ecube_direct.cpp', False),
    'ecube_fmt_build': ('src_crossengine_20261101/ecube_fmt_build.cpp', False),
    'multibits': ('src_encoded_ops_20261030/multibits.cpp', False),
    'value_index_build': ('src_encoded_ops_20261030/value_index_build.cpp', True),
    'value_index_v2': ('src_encoded_ops_20261030/value_index_v2.cpp', False),
    'block_summary': ('src_encoded_ops_20261030/block_summary.cpp', True),
}


def run(args):
    subprocess.run([str(a) for a in args], check=True, cwd=SRC)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('targets', nargs='*', metavar='TARGET')
    args = ap.parse_args()
    names = args.targets or list(TARGETS)
    for name in names:
        if name not in TARGETS:
            ap.error(f'Unknown target: {name}')
    OUT.mkdir(exist_ok=True)
    objdir = OUT / 'obj'
    objdir.mkdir(exist_ok=True)
    vendor = SRC / 'vendor'
    includes = ['-I', str(vendor / 'src/include')]
    primitives = [vendor / f'src/alp/src/fastlanes_gen_{x}.cpp'
                  for x in ['ffor', 'unffor']]
    primitives += [vendor / ('src/primitives/fls_generated/fallback/scalar_aav_uf1/'
                             f'fallback_scalar_aav_1024_uf1_{x}_src.cpp')
                   for x in ['rsum', 'transpose', 'unrsum', 'untranspose']]
    objects = []
    if any(TARGETS[n][1] for n in names):
        for p in primitives:
            obj = objdir / (p.stem + '.o')
            if not obj.exists() or obj.stat().st_mtime < p.stat().st_mtime:
                run(CXX + FLAGS + includes + ['-c', p, '-o', obj])
            objects.append(obj)
    for name in names:
        rel, linked = TARGETS[name]
        run(CXX + FLAGS + includes + [SRC / rel] + (objects if linked else []) +
            ['-o', OUT / name])
        print(f'built {name}', flush=True)
    # Paths expected by the original experiment drivers.
    links = {
        'work/specs/bin/enc_cube3_frozen': 'enc_cube3',
        'work/specs/bin/enc_cube_specs': 'enc_cube3',
        'work/tiers/bin/enc_cube': 'enc_cube',
        'work/tiers/bin/enc_cube3': 'enc_cube3',
        'work/tiers/bin/tiers_tool': 'tiers_tool',
        'work/tiers/bin/gen_groups': 'gen_groups',
        'work/clustered/bin/clst_tool': 'clst_tool',
        'work/crossengine/build/ecube_direct': 'ecube_direct',
        'work/crossengine/build/ecube_fmt_build': 'ecube_fmt_build',
    }
    for name, (rel, _) in TARGETS.items():
        if not rel.startswith('../'):
            links[str(Path('research/real_decoders_20261024') / Path(rel).parent / name)] = name
    links['research/real_decoders_20261024/src_baselines_20261101/range_index_nyc'] = 'range_index'
    links['research/real_decoders_20261024/src_baselines_20261101/enc_cube3_frozen'] = 'enc_cube3'
    for rel, name in links.items():
        if not (OUT / name).exists():
            continue
        dst = ROOT / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        if not dst.exists():
            dst.symlink_to(os.path.relpath(OUT / name, dst.parent))


if __name__ == '__main__':
    main()
