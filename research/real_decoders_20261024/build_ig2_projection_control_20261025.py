#!/usr/bin/env python3
"""Projection-matched DuckDB 2.0.0 control for the exact IG/2 query.

The frozen protocol compares the native two-column projected input against a
persisted DuckDB database holding all 18 original columns. This script builds the
fairness control: a persisted DuckDB database that contains exactly the two
columns the original IG/2 SQL references, in the original row order and with the
original declared types, and proves that the unchanged original SQL returns the
identical complete typed result on it.

It does not modify any frozen artifact. It reads each prepared case's frozen
full-18-column database, projects the two required columns into a new database,
verifies every projected typed value against the prepared source rows, and writes
the exact-query result and a receipt beside the new database.
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parent))
import artifact_paths as _artifact
import argparse
import hashlib
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
WS = ROOT.parents[1]
SITE = WS / 'bitslice_external_20261021/deps/site'
DUCK_NATIVE = SITE / '_duckdb.abi3.so'
DUCK_SHA = 'e7cda112749d860a36cb1524ff465cefecdb0acc5c4e2ff4510ecc1b6f6c061a'
COLUMNS = [{'name': 'State', 'type': 'VARCHAR'},
           {'name': 'TEMP(Calculation_8840730155303010)(3219150654)(0)', 'type': 'HUGEINT'}]
ALIAS = COLUMNS[1]['name']
TABLE = 'IGlocations1_1'


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def sha(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for b in iter(lambda: f.read(1 << 20), b''):
            h.update(b)
    return h.hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('run_id')
    parser.add_argument('--source-run', default='run_ig2_specialization_v8')
    args = parser.parse_args()
    require(args.run_id.isidentifier(), 'fresh run id')
    source = ROOT / 'results' / args.source_run
    prepared = ROOT / 'results' / 'ig2_prep_20261025' / 'prepared'
    out = ROOT / 'results' / args.run_id
    out.mkdir(exist_ok=False)
    started = time.time()
    sys.path.insert(0, str(SITE))
    import duckdb
    import _duckdb
    require(duckdb.__version__ == '2.0.0' and Path(_duckdb.__file__).resolve() == DUCK_NATIVE.resolve()
            and sha(DUCK_NATIVE) == DUCK_SHA, 'qualified pinned DuckDB 2.0.0 identity')
    records = []
    for case_dir in sorted(p for p in prepared.iterdir() if p.is_dir()):
        case = case_dir.name
        full = source / f'{case}_duckdb' / 'full18.duckdb'
        require(full.is_file(), 'frozen full-18 database ' + case)
        target = out / (case + '_proj2')
        target.mkdir()
        database = target / 'projected.duckdb'
        build_start = time.perf_counter()
        con = duckdb.connect(str(database), config={'threads': '1', 'preserve_insertion_order': 'true'})
        con.execute(f'ATTACH \'{full}\' AS full18 (READ_ONLY)')
        con.execute(f'CREATE TABLE {TABLE} AS SELECT "State", "POPESTIMATE2014" FROM full18.{TABLE} ORDER BY rowid')
        con.execute('CHECKPOINT')
        con.close()
        build_ms = (time.perf_counter() - build_start) * 1000
        # Verify every projected typed value equals the prepared original source rows.
        raw_rows = (case_dir / 'source_rows.jsonl').read_text(encoding='utf-8')
        source_rows = [line for line in raw_rows.split('\n') if line]
        expected = [(json.loads(line)[17], json.loads(line)[13]) for line in source_rows]
        con = duckdb.connect(str(database), read_only=True, config={'threads': '1', 'preserve_insertion_order': 'true'})
        stored = con.execute(f'SELECT "State", "POPESTIMATE2014" FROM {TABLE}').fetchall()
        require(len(stored) == len(expected), 'projected row count ' + case)
        require(all(a == b for a, b in zip(stored, expected)), 'projected full typed value order ' + case)
        # The unchanged original SQL must return the identical complete typed result.
        sqlpath = case_dir / 'query.sql'
        cursor = con.execute(sqlpath.read_text())
        actual_columns = [{'name': e[0], 'type': str(e[1])} for e in cursor.description]
        require(actual_columns == COLUMNS, 'projected original result aliases/types ' + case)
        rows = cursor.fetchall()
        actual = [[k, str(v) if v is not None else None] for k, v in rows]
        require(len({r[0] for r in actual}) == len(actual), 'unique complete keys ' + case)
        actual.sort(key=lambda r: r[0].encode('utf-8'))
        result = {'columns': actual_columns, 'rows': actual}
        con.close()
        oracle = json.loads((case_dir / 'oracle.json').read_text())
        require(result == oracle, 'projected-DB exact original result ' + case)
        result_path = target / 'result.json'
        result_path.write_text(json.dumps(result, ensure_ascii=False, allow_nan=False) + '\n', encoding='utf-8')
        receipt = {'case': case, 'status': 'passed', 'identity': {'version': duckdb.__version__,
                   'native_module': str(Path(_duckdb.__file__).resolve()), 'native_sha256': sha(DUCK_NATIVE)},
                   'source_full18_database': str(full), 'source_full18_sha256': sha(full),
                   'projected_database': str(database), 'projected_database_bytes': database.stat().st_size,
                   'projected_database_sha256': sha(database), 'projected_columns': ['State', 'POPESTIMATE2014'],
                   'projected_native_types': ['VARCHAR', 'INTEGER'], 'rows': len(stored),
                   'full_value_cells_verified': len(stored) * 2, 'original_row_order_preserved': True,
                   'original_SQL_bytes_unchanged': True, 'complete_result_equals_oracle': True,
                   'result_sha256': sha(result_path), 'build_ms_not_a_timing_result': build_ms,
                   'scope': 'projection-pruned persisted DuckDB control for the same exact SQL; not a full-schema claim'}
        (target / 'build_receipt.json').write_text(json.dumps(receipt, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
        records.append(receipt)
    summary = {'status': 'passed', 'cases': len(records), 'projected_columns': 2,
               'all_complete_results_equal_oracle': True, 'all_original_rows_verified': True,
               'original_SQL_unchanged': True, 'wall_s': time.time() - started,
               'not_a_timing_result': True, 'receipts': [r['case'] for r in records]}
    (out / 'summary.json').write_text(json.dumps(summary, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    print(json.dumps({'status': 'passed', 'cases': len(records)}))


if __name__ == '__main__':
    main()
