#!/usr/bin/env python3
"""P1 10M-row scale check: generic encoded evaluator vs warm DuckDB vs native."""

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
NATIVE = ROOT / 'results/ig2_prep_20261025/adapter_ig2_fast'
PREP = ROOT / 'results/ig2_scale10m_prep/Observed'
ENCODED = ROOT / 'results/ig2_scale10m_prep/Observed.encoded'
DB = ROOT / 'results/ig2_scale10m_duckdb/projected.duckdb'
OUT = ROOT / 'results/p1_scale10m_20261030'
OUT.mkdir(parents=True, exist_ok=True)
REPS = 5


def canonical(rows):
    return sorted(([str(v) for v in row] for row in rows), key=lambda r: r[0].encode())


def main():
    import duckdb
    aggs = [('sum', 'SUM(CAST("POPESTIMATE2014" AS BIGINT))'),
            ('count', 'COUNT(*)'),
            ('min', 'MIN("POPESTIMATE2014")'),
            ('max', 'MAX("POPESTIMATE2014")')]
    results = []
    for op, expr in aggs:
        spec = {'encoded': str(ENCODED), 'prep': str(PREP), 'dict': 'state.dict',
                'group_id': 17,
                'aggs': ([{'op': op, 'id': 13, 'bias': 2147483648}]
                         if op != 'count' else [{'op': 'count'}]),
                'where_not_in': ['District of Columbia']}
        sp = OUT / f'{op}.spec.json'; sp.write_text(json.dumps(spec) + '\n')
        sql = ('SELECT "State", ' + expr + ' FROM "IGlocations1_1" '
               'WHERE "State" NOT IN (\'District of Columbia\') GROUP BY 1')
        con = duckdb.connect(str(DB), read_only=True, config={'threads': '1'})
        expected = canonical(con.execute(sql).fetchall())
        times = []
        for _ in range(REPS):
            out = OUT / f'{op}.result.json'
            proc = subprocess.run([str(EVAL), str(sp), str(out)], cwd=str(ROOT),
                                  capture_output=True, text=True, timeout=600)
            if proc.returncode != 0: raise RuntimeError(proc.stderr[-300:])
            times.append(json.loads((Path(str(out) + '.metrics.json')).read_text())['stored_ms'])
        got = canonical(json.loads((OUT / f'{op}.result.json').read_text())['rows'])
        t0 = time.perf_counter()
        for _ in range(REPS): con.execute(sql).fetchall()
        duck = (time.perf_counter() - t0) * 1000.0 / REPS
        con.close()
        # native specialized stored interval
        ntimes = []
        for _ in range(3):
            res = OUT / f'{op}.native.result.json'; met = OUT / f'{op}.native.metrics.json'
            proc = subprocess.run([str(NATIVE), 'query_fullsql', str(ENCODED), str(PREP),
                                   str(PREP / 'query.sql'), 'stream', str(res), '-', str(met)],
                                  cwd=str(ROOT), capture_output=True, text=True, timeout=600)
            if proc.returncode != 0: raise RuntimeError(proc.stderr[-300:])
            m = json.loads(met.read_text())
            ntimes.append(m['setup_ms'] + m['query_ms'] + m['output_write_ms'])
        rec = {'op': op, 'exact': got == expected, 'rows': len(got),
               'generic_median_ms': statistics.median(times),
               'duckdb_warm_median_ms': duck,
               'native_median_ms': statistics.median(ntimes),
               'speedup_vs_duckdb': duck / statistics.median(times)}
        results.append(rec); print(rec)
    (OUT / 'analysis.json').write_text(json.dumps({'results': results}, indent=2) + '\n')


if __name__ == '__main__':
    main()
