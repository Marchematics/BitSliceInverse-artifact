#!/usr/bin/env python3
"""Build Apache Arrow IPC control files for the two expansion queries.

This creates the external-family control used in the paper tables: for each
prepared case, a typed Arrow IPC file containing all original columns and one
containing only the projected columns the query references. Files are built from
the pinned PublicBI CSV with the declared DDL types and the literal token `null`
as the only null representation, then verified: row count, projected typed values
against the prepared original rows, and the exact query result against the
prepared oracle.

Runs with the system Python 3 that has pyarrow (the pinned DuckDB 2.0.0 module is
Python-3.12-only, so this script does not import DuckDB at all).
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parent))
import artifact_paths as _artifact
import argparse
import hashlib
import json
import re
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA = Path('../../work/bsi_e2e_20261025/data_sources')

QUERIES = {
    'hatred10': {
        'csv': DATA / 'Hatred/Hatred_1.csv',
        'ddl': ROOT.parent / 'real_decoders_20261025/sources/hatred10/benchmark__Hatred__tables__Hatred_1.table.sql',
        'prepared': ROOT / 'results/hatred10_prep_v1',
        'projected': ['State ', 'Keyword', 'StatePopNum'],
        'result_columns': [{'name': 'State ', 'type': 'VARCHAR'},
                           {'name': 'TEMP(Calculation_-454863522737569789)(2109769841)(0)', 'type': 'HUGEINT'},
                           {'name': 'TEMP(Per100000 (copy))(3451088709)(0)', 'type': 'BIGINT'},
                           {'name': 'TEMP(Per100000 (copy))(3855970114)(0)', 'type': 'BIGINT'}],
    },
    'hatred8': {
        'csv': DATA / 'Hatred/Hatred_1.csv',
        'ddl': ROOT.parent / 'real_decoders_20261025/sources/hatred8/benchmark__Hatred__tables__Hatred_1.table.sql',
        'prepared': ROOT / 'results/hatred8_prep_v5',
        'projected': ['State ', 'Keyword'],
        'result_columns': [{'name': 'State ', 'type': 'VARCHAR'}],
    },
    'citymax14': {
        'csv': DATA / 'CityMaxCapita/CityMaxCapita_1.csv',
        'ddl': ROOT.parent / 'real_decoders_20261025/sources/citymax14/benchmark__CityMaxCapita__tables__CityMaxCapita_1.table.sql',
        'prepared': ROOT / 'results/citymax14_prep_v3',
        'projected': ['City'],
        'result_columns': [{'name': 'City', 'type': 'VARCHAR'},
                           {'name': 'TEMP(Calculation_383650433986449409)(2109769841)(0)', 'type': 'HUGEINT'}],
    },
}


def sha(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for b in iter(lambda: f.read(1 << 20), b''):
            h.update(b)
    return h.hexdigest()


def parse_ddl(text):
    import pyarrow as pa
    columns = []
    for line in text.splitlines():
        m = re.match(r'\s*"([^"]+)"\s+(varchar|integer|smallint|double|decimal)\s*(\(([^)]*)\))?\s*(NOT NULL)?\s*,?\s*$',
                     line, re.IGNORECASE)
        if not m:
            continue
        name, base, _, args, notnull = m.groups()
        base = base.lower()
        if base == 'varchar':
            arrow = pa.string()
        elif base == 'integer':
            arrow = pa.int32()
        elif base == 'smallint':
            arrow = pa.int16()
        elif base == 'double':
            arrow = pa.float64()
        else:
            precision, scale = map(int, args.split(','))
            arrow = pa.decimal128(precision, scale)
        columns.append({'name': name, 'base': base, 'arrow': arrow, 'notnull': bool(notnull)})
    return columns


def read_pinned_csv(csv, columns):
    import pyarrow as pa
    import pyarrow.csv as pcsv
    read_options = pcsv.ReadOptions(column_names=[c['name'] for c in columns], use_threads=False)
    parse_options = pcsv.ParseOptions(delimiter='|')
    convert_options = pcsv.ConvertOptions(column_types={c['name']: c['arrow'] for c in columns},
                                          null_values=['null'], strings_can_be_null=True)
    return pcsv.read_csv(str(csv), read_options=read_options,
                         parse_options=parse_options, convert_options=convert_options)


def projected_rows(prepared_case):
    lines = (prepared_case / 'source_rows.jsonl').read_text(encoding='utf-8').split('\n')
    rows = [json.loads(line) for line in lines if line]
    return rows


def arrow_hatred8(table):
    import pyarrow as pa
    import pyarrow.compute as pc
    if table.num_rows == 0:
        return {'columns': [{'name': 'State ', 'type': 'VARCHAR'}], 'rows': []}
    state = table.column('State ')
    keyword = table.column('Keyword')
    cond = pc.and_(
        pc.or_(pc.and_(pc.greater_equal(state, 'AK'), pc.less_equal(state, 'CT')),
               pc.and_(pc.greater_equal(state, 'DE'), pc.less_equal(state, 'WY'))),
        pc.is_in(keyword, value_set=pa.array(['bimbo', 'hag', 'twat'])))
    filtered = table.filter(cond)
    grouped = filtered.group_by('State ').aggregate([('State ', 'count')])
    rows = []
    for record in grouped.to_pylist():
        key, count = record['State '], record['State _count']
        if 30 <= count <= 100000:
            rows.append([key])
    rows.sort(key=lambda r: r[0].encode('utf-8'))
    return {'columns': [{'name': 'State ', 'type': 'VARCHAR'}], 'rows': rows}


def arrow_hatred10(table):
    import pyarrow as pa
    import pyarrow.compute as pc
    columns = [{'name': 'State ', 'type': 'VARCHAR'},
               {'name': 'TEMP(Calculation_-454863522737569789)(2109769841)(0)', 'type': 'HUGEINT'},
               {'name': 'TEMP(Per100000 (copy))(3451088709)(0)', 'type': 'BIGINT'},
               {'name': 'TEMP(Per100000 (copy))(3855970114)(0)', 'type': 'BIGINT'}]
    if table.num_rows == 0:
        return {'columns': columns, 'rows': []}
    state = table.column('State ')
    keyword = table.column('Keyword')
    cond = pc.and_(
        pc.or_(pc.and_(pc.greater_equal(state, 'AK'), pc.less_equal(state, 'CT')),
               pc.and_(pc.greater_equal(state, 'DE'), pc.less_equal(state, 'WY'))),
        pc.is_in(keyword, value_set=pa.array(['fatass', 'fatso', 'fattie'])))
    filtered = table.filter(cond)
    ones = pa.array([1] * filtered.num_rows, type=pa.int64())
    counted = filtered.append_column('__one', ones)
    grouped = counted.group_by('State ').aggregate([('__one', 'count'), ('StatePopNum', 'max'),
                                                   ('StatePopNum', 'min')])
    rows = []
    for record in grouped.to_pylist():
        if 30 <= record['__one_count'] <= 100000:
            rows.append([record['State '], str(record['__one_count']),
                         str(record['StatePopNum_max']), str(record['StatePopNum_min'])])
    rows.sort(key=lambda r: r[0].encode('utf-8'))
    return {'columns': columns, 'rows': rows}


def arrow_citymax14(table):
    import pyarrow as pa
    columns = [{'name': 'City', 'type': 'VARCHAR'},
               {'name': 'TEMP(Calculation_383650433986449409)(2109769841)(0)', 'type': 'HUGEINT'}]
    if table.num_rows == 0:
        return {'columns': columns, 'rows': []}
    # count_all semantics for SUM(1): count rows, not non-null City values, so the
    # NULL group keeps its 21,554 (observed) row count.
    ones = pa.array([1] * table.num_rows, type=pa.int64())
    counted = table.append_column('__one', ones)
    grouped = counted.group_by('City').aggregate([('__one', 'count')])
    null_count = 0
    named = []
    for record in grouped.to_pylist():
        key, count = record['City'], record['__one_count']
        if key is None:
            null_count = count
        else:
            named.append([key, str(count)])
    named.sort(key=lambda r: r[0].encode('utf-8'))
    rows = ([[None, str(null_count)]] if null_count else []) + named
    return {'columns': columns, 'rows': rows}


IG2_PREPARED = ROOT / 'results/ig2_prep_20261025/prepared'
IG2_CASES = ['Observed', 'MixedTail', 'AbsentLiteral', 'AllExcluded', 'AllNullPayload', 'Endpoints', 'Empty']
IG2_TYPES = ['int64', 'int64', 'int64', 'string', 'int64', 'int64', 'string', 'int64', 'int64',
             'int64', 'int64', 'int64', 'int64', 'int64', 'int64', 'int64', 'int64', 'string']
IG2_NAMES = ['CENSUS2010POP', 'CONCIT', 'COUSUB', 'City', 'County Code', 'ESTIMATESBASE2010',
             'FUNCSTAT', 'Number of Records', 'PLACE', 'POPESTIMATE2010', 'POPESTIMATE2011',
             'POPESTIMATE2012', 'POPESTIMATE2013', 'POPESTIMATE2014', 'PRIMGEO_FLAG', 'SUMLEV',
             'State code', 'State']
IG2_COLUMNS = [{'name': 'State', 'type': 'VARCHAR'},
               {'name': 'TEMP(Calculation_8840730155303010)(3219150654)(0)', 'type': 'HUGEINT'}]


def arrow_ig2(table):
    import pyarrow.compute as pc
    if table.num_rows == 0:
        return {'columns': IG2_COLUMNS, 'rows': []}
    state = table.column('State')
    payload = table.column('POPESTIMATE2014')
    filtered = table.filter(pc.not_equal(state, 'District of Columbia'))
    grouped = filtered.group_by('State').aggregate([('POPESTIMATE2014', 'sum')])
    rows = [[record['State'], None if record['POPESTIMATE2014_sum'] is None
             else str(record['POPESTIMATE2014_sum'])] for record in grouped.to_pylist()]
    rows.sort(key=lambda r: r[0].encode('utf-8'))
    return {'columns': IG2_COLUMNS, 'rows': rows}


def build_ig2(run_id, out):
    import pyarrow as pa
    import pyarrow.ipc as ipc
    require_(IG2_PREPARED.is_dir(), 'IG/2 prepared dir')
    require_(len(IG2_TYPES) == 18 and len(IG2_NAMES) == 18, 'IG/2 schema length')
    receipts = []
    for case in IG2_CASES:
        folder = IG2_PREPARED / case
        oracle = json.loads((folder / 'oracle.json').read_text())
        rows = [json.loads(line) for line in (folder / 'source_rows.jsonl').read_text().split('\n') if line]
        columns = {}
        for ci, (name, kind) in enumerate(zip(IG2_NAMES, IG2_TYPES)):
            values = [row[ci] for row in rows]
            if kind == 'string':
                columns[name] = pa.array(values, type=pa.string())
            else:
                columns[name] = pa.array([None if v is None else int(v) for v in values], type=pa.int64())
        table = pa.table(columns)
        target = out / (case + '_arrow')
        target.mkdir()
        full_path = target / 'full18.arrow'
        with ipc.new_file(str(full_path), table.schema) as sink:
            sink.write_table(table)
        projected = table.select(['State', 'POPESTIMATE2014'])
        proj_path = target / 'projected.arrow'
        with ipc.new_file(str(proj_path), projected.schema) as sink:
            sink.write_table(projected)
        actual = projected.to_pylist()
        for i, row in enumerate(rows):
            require_(actual[i]['State'] == row[17], f'IG/2 projected State {case} row {i}')
            require_(actual[i]['POPESTIMATE2014'] == row[13], f'IG/2 projected POPESTIMATE2014 {case} row {i}')
        result = arrow_ig2(table)
        require_(result == oracle, 'IG/2 Arrow query result equals oracle ' + case)
        receipts.append({'case': case, 'rows': len(rows),
                         'full_arrow': str(full_path), 'full_arrow_bytes': full_path.stat().st_size,
                         'full_arrow_sha256': sha(full_path),
                         'projected_arrow': str(proj_path), 'projected_arrow_bytes': proj_path.stat().st_size,
                         'projected_arrow_sha256': sha(proj_path),
                         'projected_columns': ['State', 'POPESTIMATE2014'],
                         'complete_result_equals_oracle': True,
                         'projected_values_verified': len(rows) * 2})
    summary = {'status': 'passed', 'query': 'ig2', 'cases': len(IG2_CASES),
               'pyarrow_version': pa.__version__, 'python': sys.version.split()[0],
               'full_columns': 18, 'projected_columns': ['State', 'POPESTIMATE2014'],
               'all_complete_results_equal_oracle': True, 'all_projected_values_verified': True,
               'scope': 'external Arrow control files for IG/2 built from the prepared typed rows; not a new measurement of the native method',
               'records': receipts}
    (out / 'summary.json').write_text(json.dumps(summary, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    print(json.dumps({'status': 'passed', 'query': 'ig2', 'cases': len(IG2_CASES), 'pyarrow': pa.__version__}))


def require_(ok, message):
    if not ok:
        raise RuntimeError(message)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('query', choices=sorted([*QUERIES, 'ig2']))
    parser.add_argument('run_id')
    args = parser.parse_args()
    if args.query == 'ig2':
        out = ROOT / 'results' / args.run_id
        out.mkdir(exist_ok=False)
        build_ig2(args.run_id, out)
        return
    spec = QUERIES[args.query]
    prepared = spec['prepared']
    require = lambda ok, msg: (_ for _ in ()).throw(RuntimeError(msg)) if not ok else None
    require(prepared.is_dir(), 'prepared dir present')
    out = ROOT / 'results' / args.run_id
    out.mkdir(exist_ok=False)
    import pyarrow as pa
    import pyarrow.ipc as ipc
    started = time.time()
    schema = parse_ddl(spec['ddl'].read_text(encoding='utf-8'))
    require(len(schema) == 31, 'DDL column count')
    full = read_pinned_csv(spec['csv'], schema)
    cases = sorted(p.name for p in prepared.iterdir() if p.is_dir() and (p / 'oracle.json').is_file())
    require(cases, 'prepared cases')
    receipts = []
    for case in cases:
        folder = prepared / case
        oracle = json.loads((folder / 'oracle.json').read_text())
        rows = projected_rows(folder)
        n = len(rows)
        if case == 'Observed':
            require(full.num_rows == n, 'full row count')
            full_table = full
        else:
            # Fixtures are authored; build the Arrow tables from the same authored
            # rows so every case exercises the identical full-schema contract.
            full_rows = [json.loads(line) for line in
                         (folder / 'source_rows.jsonl').read_text(encoding='utf-8').split('\n') if line]
            cols = {}
            for ci, column in enumerate(schema):
                values = []
                for row in full_rows:
                    value = row[ci]
                    if column['base'] == 'decimal':
                        from decimal import Decimal
                        value = Decimal(str(value))
                    values.append(value)
                cols[column['name']] = pa.array(values, type=column['arrow'])
            full_table = pa.table(cols)
        target = out / (case + '_arrow')
        target.mkdir()
        full_path = target / 'full31.arrow'
        with ipc.new_file(str(full_path), full_table.schema) as sink:
            sink.write_table(full_table)
        projected_names = spec['projected']
        projected_table = full_table.select(projected_names)
        proj_path = target / 'projected.arrow'
        with ipc.new_file(str(proj_path), projected_table.schema) as sink:
            sink.write_table(projected_table)
        # Verify projected typed values against the prepared original rows.
        projected_rows_actual = projected_table.to_pylist()
        require(len(projected_rows_actual) == len(rows), 'projected row count ' + case)
        for i, row in enumerate(rows):
            for j, name in enumerate(projected_names):
                if args.query in ('hatred8', 'hatred10'):
                    expected = row[24 if name == 'State ' else (13 if name == 'Keyword' else 26)]
                else:
                    expected = row[2]
                actual = projected_rows_actual[i][name]
                require(actual == expected, f'projected value {case} {name} row {i}')
        # Verify the exact query result from the Arrow data.
        if args.query == 'hatred8':
            result = arrow_hatred8(full_table)
        elif args.query == 'hatred10':
            result = arrow_hatred10(full_table)
        else:
            result = arrow_citymax14(full_table)
        require(result == oracle, 'Arrow query result equals oracle ' + case)
        receipts.append({'case': case, 'rows': n,
                         'full_arrow': str(full_path), 'full_arrow_bytes': full_path.stat().st_size,
                         'full_arrow_sha256': sha(full_path),
                         'projected_arrow': str(proj_path), 'projected_arrow_bytes': proj_path.stat().st_size,
                         'projected_arrow_sha256': sha(proj_path),
                         'projected_columns': projected_names,
                         'complete_result_equals_oracle': True,
                         'projected_values_verified': n * len(projected_names)})
    summary = {'status': 'passed', 'query': args.query, 'cases': len(cases),
               'pyarrow_version': pa.__version__, 'python': sys.version.split()[0],
               'csv_sha256': sha(spec['csv']), 'ddl_sha256': sha(spec['ddl']),
               'full_columns': 31, 'projected_columns': spec['projected'],
               'all_complete_results_equal_oracle': True,
               'all_projected_values_verified': True,
               'wall_s': time.time() - started,
               'scope': 'external Arrow control files built from pinned CSV with declared DDL types; not a new measurement of the native method',
               'records': receipts}
    (out / 'summary.json').write_text(json.dumps(summary, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    print(json.dumps({'status': 'passed', 'query': args.query, 'cases': len(cases),
                      'pyarrow': pa.__version__}))


if __name__ == '__main__':
    main()
