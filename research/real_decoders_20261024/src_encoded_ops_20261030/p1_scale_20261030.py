#!/usr/bin/env python3
"""P1 scale check: the generic encoded evaluator on the 983,040-row IG/2 variants."""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import json
import statistics
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
EVAL = ROOT / 'src_encoded_ops_20261030/generic_eval'
OUT = ROOT / 'results/p1_scale_20261030'
OUT.mkdir(parents=True, exist_ok=True)
REPS = 10


def duckdb_rows(db, sql):
    import duckdb
    con = duckdb.connect(str(ROOT / db), read_only=True, config={'threads': '1'})
    rows = [[str(a), str(b) if b is not None else None] for a, b in con.execute(sql).fetchall()]
    con.close()
    return sorted(rows, key=lambda r: r[0].encode())


def main():
    import duckdb
    variants = {
        'ordered': ('ig2_scale15_prep', 'results/ig2_scale15_duckdb/full18.duckdb'),
        'shuffled': ('ig2_scale15shuf_prep', 'results/ig2_scale15shuf_duckdb/full18.duckdb'),
        'block': ('ig2_scale15blk_prep', 'results/ig2_scale15blk_duckdb/full18.duckdb'),
    }
    aggs = [('sum', 'SUM(CAST("POPESTIMATE2014" AS BIGINT))'),
            ('count', 'COUNT(*)'),
            ('min', 'MIN("POPESTIMATE2014")'),
            ('max', 'MAX("POPESTIMATE2014")')]
    results = []
    for name, (prep_tag, db) in variants.items():
        prep = ROOT / 'results' / prep_tag / 'Observed'
        encoded = ROOT / 'results' / prep_tag / 'Observed.encoded'
        for op, expr in aggs:
            spec = {'encoded': str(encoded), 'prep': str(prep), 'dict': 'state.dict',
                    'group_id': 17,
                    'aggs': ([{'op': op, 'id': 13, 'bias': 2147483648}]
                             if op != 'count' else [{'op': 'count'}]),
                    'where_not_in': ['District of Columbia']}
            spec_path = OUT / f'{name}_{op}.spec.json'
            spec_path.write_text(json.dumps(spec) + '\n')
            sql = (f'SELECT "State", {expr} FROM "IGlocations1_1" '
                   f'WHERE "State" NOT IN (\'District of Columbia\') GROUP BY 1')
            expected = duckdb_rows(db, sql)
            out_path = OUT / f'{name}_{op}.result.json'
            proc = subprocess.run([str(EVAL), str(spec_path), str(out_path)], cwd=str(ROOT),
                                  capture_output=True, text=True, timeout=300)
            if proc.returncode != 0:
                results.append({'variant': name, 'op': op, 'status': 'failed',
                                'error': proc.stderr[-400:]}); continue
            got = json.loads(out_path.read_text())
            rows = sorted([[r[0], r[1]] for r in got['rows']], key=lambda r: r[0].encode())
            exact = rows == expected
            times = []
            for _ in range(REPS):
                subprocess.run([str(EVAL), str(spec_path), str(out_path)], cwd=str(ROOT),
                               capture_output=True, text=True, timeout=300, check=True)
                times.append(json.loads((Path(str(out_path) + '.metrics.json')).read_text())['stored_ms'])
            con = duckdb.connect(str(ROOT / db), read_only=True, config={'threads': '1'})
            t0 = time.perf_counter()
            for _ in range(REPS): con.execute(sql).fetchall()
            duck = (time.perf_counter() - t0) * 1000.0 / REPS
            con.close()
            rec = {'variant': name, 'op': op, 'status': 'passed' if exact else 'mismatch',
                   'exact': exact, 'rows': len(rows), 'generic_median_ms': statistics.median(times),
                   'duckdb_warm_median_ms': duck, 'speedup': duck / statistics.median(times)}
            results.append(rec)
            print(rec)
    (OUT / 'analysis.json').write_text(json.dumps({'results': results}, indent=2) + '\n')


if __name__ == '__main__':
    main()
