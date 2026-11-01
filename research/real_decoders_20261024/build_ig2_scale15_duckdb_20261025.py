#!/usr/bin/env python3
"""Build scaled IG/2 persisted DuckDB controls from the synthetic Arrow input.

Runs under a Python 3.12 environment with duckdb 2.0.0 and pyarrow. Verifies the
native DuckDB module identity against the pinned audit hash, builds a full-18
database and a projection-matched 2-column database, and checks the unchanged
original SQL result against the scaled oracle.
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parent))
import artifact_paths as _artifact
import argparse
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PINNED_NATIVE_SHA = 'e7cda112749d860a36cb1524ff465cefecdb0acc5c4e2ff4510ecc1b6f6c061a'
SQL_SHA = '0bc95092f92bbb2f8652e66c10db8bec5fba88b6d96c5ac23e9650fe27ecca8b'
QUERY_COLUMNS = [{'name': 'State', 'type': 'VARCHAR'},
                 {'name': 'TEMP(Calculation_8840730155303010)(3219150654)(0)', 'type': 'HUGEINT'}]


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
    parser.add_argument('--tag', default='scale15')
    args = parser.parse_args()
    tag = args.tag
    scale_arrow = ROOT / 'results' / f'ig2_{tag}_arrow' / 'Observed_arrow'
    prep = ROOT / 'results' / f'ig2_{tag}_prep' / 'Observed'
    out = ROOT / 'results' / f'ig2_{tag}_duckdb'
    require(sha(prep / 'query.sql') == SQL_SHA, 'pinned original SQL')
    oracle = json.loads((prep / 'oracle.json').read_text(encoding='utf-8'))
    import duckdb
    import pyarrow as pa
    import pyarrow.ipc as ipc
    native = Path(duckdb._duckdb.__file__) if hasattr(duckdb, '_duckdb') else None
    if native is None:
        import _duckdb as native_module
        native = Path(native_module.__file__)
    require(duckdb.__version__ == '2.0.0', 'DuckDB 2.0.0')
    require(sha(native) == PINNED_NATIVE_SHA, 'pinned DuckDB native module identity')
    out.mkdir(exist_ok=True)
    full_db = out / 'full18.duckdb'
    proj_db = out / 'proj2' / 'projected.duckdb'
    proj_db.parent.mkdir(exist_ok=True)
    for path in (full_db, proj_db):
        if path.exists():
            path.unlink()
    full_table = ipc.open_file(str(scale_arrow / 'full18.arrow')).read_all()
    projected = ipc.open_file(str(scale_arrow / 'projected.arrow')).read_all()
    require(full_table.num_rows == 983040 and full_table.num_columns == 18, 'scaled full Arrow shape')
    con = duckdb.connect(str(full_db), config={'threads': '1', 'preserve_insertion_order': 'true'})
    con.register('arrow_source', full_table)
    con.execute('CREATE TABLE "IGlocations1_1" AS SELECT * FROM arrow_source')
    con.execute('CHECKPOINT')
    con.close()
    con = duckdb.connect(str(full_db), read_only=True, config={'threads': '1'})
    require(con.execute('SELECT COUNT(*) FROM "IGlocations1_1"').fetchone()[0] == 983040, 'full rows')
    cursor = con.execute((prep / 'query.sql').read_text(encoding='utf-8'))
    columns = [{'name': entry[0], 'type': str(entry[1])} for entry in cursor.description]
    raw = cursor.fetchall()
    actual = [[row[0], str(row[1])] for row in raw]
    actual.sort(key=lambda row: row[0].encode('utf-8'))
    con.close()
    require(columns == QUERY_COLUMNS, 'full-query aliases/types')
    require({'columns': columns, 'rows': actual} == oracle, 'scaled full-query result equals oracle')
    con = duckdb.connect(str(proj_db), config={'threads': '1', 'preserve_insertion_order': 'true'})
    con.register('arrow_projected', projected)
    con.execute('CREATE TABLE "IGlocations1_1" AS SELECT * FROM arrow_projected')
    con.execute('CHECKPOINT')
    con.close()
    con = duckdb.connect(str(proj_db), read_only=True, config={'threads': '1'})
    cursor = con.execute((prep / 'query.sql').read_text(encoding='utf-8'))
    proj_columns = [{'name': entry[0], 'type': str(entry[1])} for entry in cursor.description]
    proj_rows = sorted(([row[0], str(row[1])] for row in cursor.fetchall()),
                       key=lambda row: row[0].encode('utf-8'))
    con.close()
    require(proj_columns == QUERY_COLUMNS, 'projected-query aliases/types')
    require({'columns': proj_columns, 'rows': proj_rows} == oracle,
            'scaled projected-query result equals oracle')
    summary = {'status': 'passed', 'case': 'Observed', 'rows': 983040, 'scale_factor': 15,
               'duckdb': {'version': duckdb.__version__, 'native': str(native),
                          'native_sha256': sha(native), 'pinned_sha256': PINNED_NATIVE_SHA},
               'full_database': str(full_db), 'full_database_bytes': full_db.stat().st_size,
               'full_database_sha256': sha(full_db),
               'projected_database': str(proj_db), 'projected_database_bytes': proj_db.stat().st_size,
               'projected_database_sha256': sha(proj_db),
               'full_query_result_equals_oracle': True,
               'projected_query_result_equals_oracle': True,
               'pyarrow': pa.__version__,
               'scope': 'synthetic 15x row-replicated scale test; not additional real data'}
    (ROOT / 'results' / f'ig2_{tag}_duckdb_summary.json').write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    print(json.dumps({'status': 'passed', 'full_bytes': full_db.stat().st_size,
                      'proj_bytes': proj_db.stat().st_size}))


if __name__ == '__main__':
    main()
