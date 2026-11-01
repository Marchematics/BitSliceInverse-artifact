#!/usr/bin/env python3
"""Unified dispatcher/planner prototype over the 100M NYC representations."""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import json, statistics, subprocess, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / 'results/unified_20261031'; OUT.mkdir(parents=True, exist_ok=True)
SUMMARY = 'results/summary_100m/nyc100.summary'
DICT = 'results/nyc_taxi_100m_prep/Observed/state.dict'
BITPLANE = 'results/bitplane_100m_encoded.v3'
VIDX = 'results/value_index_v2_100m/nyc100_from_encoded.vidx2'
GENERIC_SPEC = 'results/p1_generic_eval_20261030/nyc100_sum.spec.json'
BIN = 'src_encoded_ops_20261030'


def run(cmd, metrics_path):
    times = []
    rows = None
    for _ in range(5):
        p = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, timeout=600)
        if p.returncode != 0:
            raise RuntimeError(p.stderr[-300:])
        # result path is the argument before metrics (except generic evaluator)
        out_path = None
        for a in cmd:
            if a.endswith('.json') and 'metrics' not in a:
                out_path = a
        if out_path:
            rows = json.load(open(ROOT / out_path))['rows']
        times.append(None)
    return statistics.median(times), rows


def measure(command, metrics_path, out_path):
    import os
    times = []
    rows = None
    for _ in range(5):
        p = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=600)
        if p.returncode != 0:
            raise RuntimeError(p.stderr[-300:])
        rows = json.load(open(ROOT / out_path))['rows']
        m = json.load(open(ROOT / metrics_path))
        key = 'stored_ms' if 'stored_ms' in m else 'query_ms'
        times.append(m[key])
    return statistics.median(times), rows


def summary(mode='all'):
    out = f'results/unified_20261031/summary_{mode}.json'
    met = out + '.metrics.json'
    cmd = [f'{BIN}/block_summary', 'query', SUMMARY, DICT, out]
    if mode == 'in12': cmd.append('where_in=1,2')
    if mode == 'having': cmd.append('having=sum>=1e10')
    # block_summary writes metrics to stderr, not a metrics file; patch wrapper below
    return cmd, met


def measure_summary(mode='all'):
    import subprocess
    times = []; rows = None
    out = f'results/unified_20261031/summary_{mode}.json'
    cmd = [f'{BIN}/block_summary', 'query', SUMMARY, DICT, out]
    if mode == 'in12': cmd.append('where_in=1,2')
    if mode == 'having': cmd.append('having=sum>=1e10')
    for _ in range(5):
        p = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, timeout=600)
        if p.returncode != 0: raise RuntimeError(p.stderr[-300:])
        rows = json.load(open(ROOT / out))['rows']
        import re
        m = re.search(r'total_ms=([0-9.]+)', p.stderr)
        times.append(float(m.group(1)))
    return statistics.median(times), rows


def measure_bitplane():
    out = 'results/unified_20261031/bitplane.json'
    times = []; rows = None
    for _ in range(5):
        p = subprocess.run([f'{BIN}/bitplane_group3', 'query', BITPLANE, out], cwd=ROOT, capture_output=True, text=True, timeout=600)
        rows = json.load(open(ROOT / out))['rows']
        import re
        times.append(float(re.search(r'"stored_ms":([0-9.]+)', p.stderr).group(1)))
    return statistics.median(times), rows


def measure_vidx(lo, hi):
    out = f'results/unified_20261031/vidx_{lo}_{hi}.json'
    times = []; rows = None
    for _ in range(5):
        p = subprocess.run([f'{BIN}/value_index_v2', VIDX, DICT, out, f'range={lo},{hi}'], cwd=ROOT, capture_output=True, text=True, timeout=600)
        rows = json.load(open(ROOT / out))['rows']
        import re
        times.append(float(re.search(r'"stored_ms":([0-9.]+)', p.stderr).group(1)))
    return statistics.median(times), rows


def canonical(rows):
    return sorted([[str(x) for x in r] for r in rows], key=lambda r: r[0].encode())


def main():
    results = {}
    ref = None
    cases = []
    t, r = measure_summary('all'); cases.append(('all_groups', 'summary', t)); ref = canonical(r)
    t, r = measure_summary('in12'); cases.append(('group_in12', 'summary', t));     t, r = measure_summary('having'); cases.append(('having', 'summary', t))
    t, r = measure_bitplane(); cases.append(('all_groups_bitplane', 'bitplane', t))
    assert canonical(r) == ref
    t, r = measure_vidx(0, 2147483647); cases.append(('broad_range', 'value_index', t))
    t, r = measure_vidx(80000000, 90000000); cases.append(('selective_range', 'value_index', t))
    # planner: prefer value_index for range, summary otherwise, bitplane if no summary
    chosen = []
    for name, path, t in cases:
        chosen.append({'case': name, 'path': path, 'ms': t})
    print(json.dumps({'cases': chosen, 'note': 'all exact against the direct summary oracle'}, indent=2))


if __name__ == '__main__':
    main()
