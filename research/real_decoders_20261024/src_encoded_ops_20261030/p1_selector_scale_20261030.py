#!/usr/bin/env python3
"""Large-scale selector validation: auto/per-row/run on 10M, 12M TPC-H and 983k shuffled."""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import json
import statistics
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
EVAL = ROOT / 'src_encoded_ops_20261030/generic_eval'
OUT = ROOT / 'results/p1_selector_scale_20261030'
OUT.mkdir(parents=True, exist_ok=True)
REPS = 10


def canonical(rows):
    return sorted(([str(v) for v in row] for row in rows), key=lambda r: r[0].encode())


def specs():
    out = []
    for p in sorted((ROOT / 'results/p1_tpch_sf2_20261030').glob('*.spec.json')):
        out.append(('tpch_' + p.stem, p))
    for p in sorted((ROOT / 'results/p1_scale10m_20261030').glob('*.spec.json')):
        out.append(('10m_' + p.stem, p))
    # 983k fully shuffled: construct four aggregate specs.
    for op in ('sum', 'count', 'min', 'max'):
        spec = {'encoded': 'results/ig2_scale15shuf_prep/Observed.encoded',
                'prep': 'results/ig2_scale15shuf_prep/Observed', 'dict': 'state.dict',
                'group_id': 17,
                'aggs': ([{'op': op, 'id': 13, 'bias': 2147483648}]
                         if op != 'count' else [{'op': 'count'}]),
                'where_not_in': ['District of Columbia']}
        p = OUT / f'shuf983k_{op}.spec.json'
        p.write_text(json.dumps(spec) + '\n')
        out.append(('shuf983k_' + op, p))
    return out


def main():
    records = []
    for name, spec_path in specs():
        base = json.loads(spec_path.read_text())
        ref = None
        variants = {m: [] for m in ('auto', 'perrow', 'run')}
        for rep in range(REPS):
            order = ['auto', 'perrow', 'run']
            order = order[rep % 3:] + order[:rep % 3]
            for mode in order:
                s = dict(base); s['path'] = mode
                vp = OUT / f'{name}.{mode}.spec.json'; vp.write_text(json.dumps(s) + '\n')
                out = OUT / f'{name}.{mode}.result.json'
                proc = subprocess.run([str(EVAL), str(vp), str(out)], cwd=str(ROOT),
                                      capture_output=True, text=True, timeout=600)
                if proc.returncode != 0: raise RuntimeError(proc.stderr[-300:])
                rows = canonical(json.loads(out.read_text())['rows'])
                if ref is None: ref = rows
                elif rows != ref: raise RuntimeError('mode mismatch ' + name + ' ' + mode)
                variants[mode].append(json.loads((Path(str(out) + '.metrics.json')).read_text())['stored_ms'])
        variants = {m: statistics.median(v) for m, v in variants.items()}
        best = min(variants, key=variants.get)
        rec = {'name': name, 'variants': variants, 'best': best,
               'regret': variants['auto'] / variants[best] - 1.0}
        records.append(rec); print(rec)
    (OUT / 'analysis.json').write_text(json.dumps({
        'specs': len(records), 'auto_optimal': sum(1 for r in records if r['regret'] <= 0.02),
        'auto_within_5pct': sum(1 for r in records if r['regret'] <= 0.05),
        'max_regret': max(r['regret'] for r in records),
        'median_regret': statistics.median([r['regret'] for r in records]),
        'records': records}, indent=2) + '\n')


if __name__ == '__main__':
    main()
