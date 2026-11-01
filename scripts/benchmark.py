#!/usr/bin/env python3
"""Measure fixed and changing-range queries and compare every result with SQL."""
import argparse
import hashlib
import json
from pathlib import Path
import platform
import statistics
import subprocess
import time

import duckdb

from prepare import ROOT, RESULTS
from check import canon, run


def last_json(text):
    return next(json.loads(s) for s in reversed(text.splitlines()) if s.startswith('{'))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('dataset', choices=['ig2','nyc','nyc12','tpch'])
    ap.add_argument('--reps', type=int, default=30)
    ap.add_argument('--core', type=int)
    args = ap.parse_args()
    if args.core is not None:
        import os
        os.sched_setaffinity(0, {args.core})
    bindings = {
        'ig2': ('ig2_prep_20261025/prepared/Observed', ROOT/'work/tiers/build/ig2_65k.parquet', 'state','pop'),
        'nyc': ('nyc_taxi_100m_prep/Observed', RESULTS/'nyc_taxi_100m_arrow/nyc100m.parquet','passenger_count','fare_cents'),
        'nyc12': ('nyc_taxi_2023_prep/Observed', RESULTS/'nyc_taxi_2023_arrow/nyc12m.parquet','passenger_count','fare_cents'),
        'tpch': ('tpch_sf2_prep/Observed', RESULTS/'tpch_sf2_arrow/lineitem.parquet','l_returnflag','CAST(l_extendedprice*100 AS BIGINT)'),
    }
    prep_rel, parquet, group, value = bindings[args.dataset]
    prep = RESULTS/prep_rel
    encoded = prep.parent/'Observed.encoded'
    if args.dataset == 'ig2': encoded = prep.parent.parent/'Observed.encoded'
    out = ROOT/'work/bench'/args.dataset
    out.mkdir(parents=True,exist_ok=True)
    cube = out/'index.ecube'
    t0 = time.perf_counter()
    build = run(ROOT/'build/enc_cube3','build_enc',encoded,prep,17,13,cube)
    wall = time.perf_counter()-t0
    c = duckdb.connect()
    c.execute('SET threads=1')
    c.execute(f'CREATE TABLE t AS SELECT {group} AS g,{value} AS v FROM read_parquet(?)',[str(parquet)])
    cuts = [-7000,-720,509,650,930,1350,99900] if args.dataset.startswith('nyc') else [
        int(x) for x in c.execute('SELECT quantile_disc(v,[0.01,0.1,0.5]) FROM t').fetchone()[0]]
    queries = [('fixed', [], '')] + [(f'le_{cut}',[f'hi={cut}'],f'WHERE v<={cut}') for cut in cuts]
    records=[]
    for name,spec,where in queries:
        sql=f'SELECT g,SUM(v),COUNT(*),MIN(v),MAX(v) FROM t {where} GROUP BY g'
        for _ in range(3): c.execute(sql).fetchall()
        samples=[]
        for _ in range(args.reps):
            t0=time.perf_counter(); expected=c.execute(sql).fetchall(); samples.append((time.perf_counter()-t0)*1000)
        benches=[]
        for _ in range(3):
            end=time.perf_counter()+.5
            while time.perf_counter()<end: pass
            p=run(ROOT/'build/enc_cube3','bench',cube,prep/'state.dict',out/'rows.json',max(200,args.reps),*spec)
            benches.append(last_json(p.stderr))
        assert canon(json.loads((out/'rows.json').read_text())['rows']) == canon(expected),name
        records.append({'case':name,'sql':sql,'exact':True,'duckdb_median_ms':statistics.median(samples),
                        'cube_median_ms':statistics.median(b['query_median_ms'] for b in benches),
                        'cube_p95_ms':statistics.median(b['query_p95_ms'] for b in benches)})
    report={'dataset':args.dataset,'rows':c.execute('SELECT count(*) FROM t').fetchone()[0],
            'duckdb_version':duckdb.__version__,'platform':platform.machine(),'threads':1,
            'build_wall_s':wall,'build':last_json(build.stderr),'index_bytes':cube.stat().st_size,
            'index_sha256':hashlib.sha256(cube.read_bytes()).hexdigest(),'queries':records}
    (out/'results.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))


if __name__ == '__main__': main()
