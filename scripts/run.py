#!/usr/bin/env python3
"""Run the paper's experiment families from the repository root."""
import argparse
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / 'research/real_decoders_20261024'
EXPERIMENTS = {
    'references': 'src_tiers_20261101/make_ref_tables.py',
    'groups': 'src_tiers_20261101/run_groups.py',
    'output': 'src_tiers_20261101/run_output_reps.py',
    'nulls': 'src_tiers_20261101/run_nullmeasure.py',
    'panel': 'src_tiers_20261101/run_panel.py',
    'amortization': 'src_tiers_20261101/run_amort.py',
    'tiers': 'src_tiers_20261101/run_scale.py',
    'clustered': 'src_clustered_20261101/run_clustered.py',
    'clustered-1b': 'src_clustered_20261101/run_clustered_1b.py',
    'clustered-mt': 'src_clustered_20261101/run_clustered_mt.py',
    'scale': 'src_scale_20261101/run_scale.py',
    'mec': 'src_eai_multi_20261101/run_mec.sh',
    'formats': 'src_crossengine_20261101/run_format.py',
    'engines': 'src_crossengine_20261101/panel.py',
    'router': 'src_router_20261101/router_eval.py',
}


def execute(script, args, env):
    exe = 'bash' if script.suffix == '.sh' else sys.executable
    subprocess.run([exe, str(script), *args], cwd=SRC, env=env, check=True)


def references(env):
    scratch=ROOT/'work/tiers/build'
    scratch.mkdir(parents=True,exist_ok=True)
    n=(scratch/'val_nyc100.i32').stat().st_size//4
    for groups in (1000,100000,1000000):
        out=scratch/f'gid_g{groups}.i32'
        if not out.exists():
            subprocess.run([str(ROOT/'build/gen_groups'),str(n),str(groups),str(out)],check=True)
    execute(SRC/EXPERIMENTS['references'],[],env)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('experiment', choices=list(EXPERIMENTS) + ['specs','fuzz','baselines'])
    ap.add_argument('args', nargs=argparse.REMAINDER)
    args = ap.parse_args()
    env = dict(os.environ)
    env['PYTHONPATH'] = str(SRC) + os.pathsep + env.get('PYTHONPATH','')
    env['BSI_PYTHON'] = sys.executable
    env['PY_DUCK'] = sys.executable
    env['PY_SYS'] = sys.executable
    env['CORE'] = env.get('BSI_CORE', str(min(os.sched_getaffinity(0))) if hasattr(os,'sched_getaffinity') else '0')
    env['CE_REPO'] = str(SRC)
    env['CE_WORK'] = str(ROOT/'work/crossengine')
    env['CE_OUT'] = str(ROOT/'work/crossengine/runs')
    env['CE_CUBE'] = str(ROOT/'work/crossengine/data/fmt_encoded.ecube')
    env['CUBE'] = str(ROOT/'build/enc_cube3')
    for rel in ['specs/out','bench_baselines/data','crossengine/runs','crossengine/data','scale']:
        (ROOT/'work'/rel).mkdir(parents=True,exist_ok=True)
    if args.experiment=='references':
        references(env)
        return
    if args.experiment in {'groups','tiers'} and not (ROOT/'work/tiers/build/cuts.json').exists():
        references(env)
    if args.experiment=='nulls' and not (ROOT/'work/tiers/build/nyc_null_valid.bits').exists():
        execute(SRC/'src_tiers_20261101/make_nullmeasure_data.py',[],env)
    if args.experiment=='scale':
        out=SRC/'results/scale_20261101'
        out.mkdir(parents=True,exist_ok=True)
        plain=SRC/'results/nyc100_plain'
        if any(x in args.args for x in ['ecube3mt','ecube3q']):
            cubes=ROOT/'work/specs/cubes'
            cubes.mkdir(parents=True,exist_ok=True)
            preps=['nyc_taxi_100m_prep','nyc_taxi_2023_prep','tpch_sf2_prep']
            for i,name in enumerate(preps,1):
                target=cubes/f'C{i}.ecube'
                if not target.exists():
                    base=SRC/'results'/name
                    subprocess.run([str(ROOT/'build/enc_cube3'),'build_enc',str(base/'Observed.encoded'),
                        str(base/'Observed'),'17','13',str(target)],check=True)
            if not (ROOT/'work/tiers/build/nyc_null_valid.bits').exists():
                execute(SRC/'src_tiers_20261101/make_nullmeasure_data.py',[],env)
        if any(x in args.args for x in ['build1b','q1b','exact1b','duck1b']):
            if not (out/'nyc1b_gid.i32').exists():
                subprocess.run([str(ROOT/'build/gen_scale'),'replica',str(plain/'passenger.i32'),
                    str(plain/'fare.i32'),str(out/'nyc1b_gid.i32'),str(out/'nyc1b_val.i32'),'10','10'],check=True)
        if any(x in args.args for x in ['build10m','q10m','duck10m','exact10m']):
            if not (out/'g10m_gid.i32').exists():
                subprocess.run([str(ROOT/'build/gen_scale'),'hashgroups',str(plain/'passenger.i32'),
                    str(plain/'fare.i32'),str(out/'g10m_gid.i32'),str(out/'g10m_val.i32'),'10000000','raw'],check=True)
        for stage,stem in [('duck1b','nyc1b'),('duck10m','g10m')]:
            if stage in args.args and not (out/f'{stem}.parquet').exists():
                execute(SRC/'src_scale_20261101/make_parquet.py',
                        [str(out/f'{stem}_gid.i32'),str(out/f'{stem}_val.i32'),str(out/f'{stem}.parquet')],env)
    if args.experiment in {'specs','fuzz'}:
        s = SRC/'src_specs_20261101'
        base = ROOT/'work/specs'
        if args.experiment == 'specs':
            execute(s/'materialize.py', [], env)
            execute(s/'gen_specs.py', [], env)
            execute(s/'run_specs.py',['--binary',str(ROOT/'build/enc_cube3'),
                    '--cube-prefix',str(base/'cubes'),*args.args], env)
        else:
            execute(s/'fuzz.py',['--patched-bin',str(ROOT/'build/enc_cube3'),
                    '--prefix-bin',str(ROOT/'build/enc_cube3'),
                    '--patched-cubes',str(base/'cubes'),'--prefix-cubes',str(base/'cubes'),*args.args],env)
        return
    if args.experiment == 'baselines':
        s = SRC/'src_baselines_20261101'
        jobs = [('bench_duckdb.py',['ref']),('bench_duckdb.py',['scan']),
                ('bench_duckdb.py',['mv']),('bench_range_index.py',[]),
                ('bench_materialized.py',[]),('bench_arrow.py',['mem']),
                ('bench_arrow.py',['ds']),('bench_polars.py',[]),
                ('bench_datafusion.py',[]),('bench_chdb.py',[]),
                ('bench_daft.py',[]),('bench_pandas.py',[]),('bench_vortex.py',['scan'])]
        env['POLARS_MAX_THREADS']='1';env['RAYON_NUM_THREADS']='1'
        for file, extra in jobs:execute(s/file,extra,env)
        return
    extra=args.args
    if args.experiment=='router': extra=[str(SRC/'src_router_20261101/matrix_20261101.csv'),*extra]
    execute(SRC/EXPERIMENTS[args.experiment],extra,env)
    if args.experiment=='mec':
        index=SRC/'results/eai_multi_20261101/tpch2/index.mec'
        target=ROOT/'work/tiers/build/tpch2_index.mec'
        if index.exists() and not target.exists():
            target.parent.mkdir(parents=True,exist_ok=True)
            target.symlink_to(os.path.relpath(index,target.parent))


if __name__=='__main__': main()
