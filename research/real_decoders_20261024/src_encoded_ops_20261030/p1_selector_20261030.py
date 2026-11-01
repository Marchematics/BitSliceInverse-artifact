#!/usr/bin/env python3
"""P1 path-selector study: compare auto/per-row/run-splitting on the 41-spec grid."""

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
SPECS = ROOT / 'results/p1_generic_eval_20261030'
OUT = ROOT / 'results/p1_selector_20261030'
OUT.mkdir(parents=True, exist_ok=True)
REPS = 10


def canonical(rows):
    return sorted(([str(v) for v in row] for row in rows), key=lambda r: r[0].encode())


def main():
    specs = sorted(SPECS.glob('*.spec.json'))
    records = []
    for spec_path in specs:
        spec = json.loads(spec_path.read_text())
        if all(a.get('op') == 'count' for a in spec['aggs']):
            continue  # count-only always uses the flat-count operator; modes are identical
        base = json.loads((SPECS / (spec_path.name[:-10] + '.result.json')).read_text())
        base_rows = canonical(base['rows'])
        variants = {m: [] for m in ('auto', 'perrow', 'run')}
        exact_all = True
        for rep in range(REPS):
            order = ['auto', 'perrow', 'run']
            order = order[(rep % 3):] + order[:(rep % 3)]
            for mode in order:
                s = dict(spec); s['path'] = mode
                vp = OUT / f'{spec_path.stem}.{mode}.spec.json'
                vp.write_text(json.dumps(s) + '\n')
                out = OUT / f'{spec_path.stem}.{mode}.result.json'
                proc = subprocess.run([str(EVAL), str(vp), str(out)], cwd=str(ROOT),
                                      capture_output=True, text=True, timeout=120)
                if proc.returncode != 0:
                    raise RuntimeError(proc.stderr[-300:])
                got = canonical(json.loads(out.read_text())['rows'])
                exact_all = exact_all and got == base_rows
                variants[mode].append(json.loads((Path(str(out) + '.metrics.json')).read_text())['stored_ms'])
        variants = {m: statistics.median(v) for m, v in variants.items()}
        best = min(variants, key=variants.get)
        best_ms = variants[best]
        auto_ms = variants['auto']
        records.append({'spec': spec_path.stem, 'variants': variants, 'best': best,
                        'best_ms': best_ms, 'auto_ms': auto_ms,
                        'regret': auto_ms / best_ms - 1.0,
                        'exact_variants': exact_all})
    optimum = sum(1 for r in records if r['regret'] <= 0.02)
    near = sum(1 for r in records if r['regret'] <= 0.05)
    analysis = {'specs': len(records), 'reps': REPS, 'auto_optimal': optimum,
                'auto_within_5pct': near,
                'max_regret': max(r['regret'] for r in records),
                'median_regret': statistics.median([r['regret'] for r in records]),
                'all_variants_exact': all(r['exact_variants'] for r in records),
                'records': records}
    (OUT / 'analysis.json').write_text(json.dumps(analysis, indent=2) + '\n')
    print(json.dumps({k: v for k, v in analysis.items() if k != 'records'}))


if __name__ == '__main__':
    main()
