#!/usr/bin/env python3
"""Frozen exact IG/2 preparation/actual-DuckDB correctness workers.

Authoring is not admission. Execute only through the granted frozen gate. No
network, codec search, query changes, source row dropping or cache persistence
outside declared database artifacts. DuckDB build uses checked full typed rows
with executemany; it is not an independent CSV-parser comparison.
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parent))
import artifact_paths as _artifact
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import struct
import sys
import time

ROOT = Path(__file__).resolve().parent
WS = ROOT.parents[1]
SQL = ROOT / 'source/PublicBI_fullsql/benchmark__IGlocations1__queries__2.sql'
DDL = ROOT / 'source/PublicBI_fullsql/benchmark__IGlocations1__tables__IGlocations1_1.table.sql'
CSV = ROOT / 'input_round3/IGlocations1_1.csv'
SQL_SHA = '0bc95092f92bbb2f8652e66c10db8bec5fba88b6d96c5ac23e9650fe27ecca8b'
DDL_SHA = 'bd070cf6fed9e82b84ea20636b13ab014cb69970892dc452c8039f96611da0bc'
CSV_SHA = '186979a35563c6b6a7e88b21b5385e4ed70d131b852770d0c2423c2a76c2a447'
CSV_BLOB = 'ee275641f7c34fcb258559be100f83cf088b927c'
COHORT_SHA = 'c291344d257095e52cb2995c2374c12c0db6859567aeb85f9a9d794f04b36984'
DUCK_NATIVE = WS / 'bitslice_external_20261021/deps/site/_duckdb.abi3.so'
DUCK_SHA = 'e7cda112749d860a36cb1524ff465cefecdb0acc5c4e2ff4510ecc1b6f6c061a'
ALIAS = 'TEMP(Calculation_8840730155303010)(3219150654)(0)'
COLUMNS = [{'name': 'State', 'type': 'VARCHAR'}, {'name': ALIAS, 'type': 'HUGEINT'}]
CASES = ['Observed', 'MixedTail', 'AbsentLiteral', 'AllExcluded', 'AllNullPayload', 'Endpoints', 'Empty']
DECIMAL = re.compile(r'[+-]?[0-9]+', re.ASCII)


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def sha(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def digest(data):
    return {'bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest(),
            'git_sha1': hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()}


def read_json(path):
    return json.loads(path.read_text(encoding='utf-8'))


def write_json(path, value):
    with path.open('x', encoding='utf-8', newline='\n') as stream:
        json.dump(value, stream, ensure_ascii=False, allow_nan=False, indent=2)
        stream.write('\n')


def write_bytes(path, data):
    with path.open('xb') as stream:
        stream.write(data)
    require(path.read_bytes() == data, 'prepared write/readback')


def bitmap(flags):
    out = bytearray(((len(flags) + 63) // 64) * 8)
    for row, flag in enumerate(flags):
        if flag:
            out[row // 8] |= 1 << (row % 8)
    return bytes(out)


def base_schema():
    require(sha(ROOT / 'COHORT_ROUND3.json') == COHORT_SHA, 'cohort schema identity')
    schema = next(t for t in read_json(ROOT / 'COHORT_ROUND3.json')['tables']
                  if t['name'] == 'IGlocations1')['columns']
    require([c['index'] for c in schema] == list(range(18)), '18 original columns')
    require(schema[17]['name'] == 'State' and schema[13]['name'] == 'POPESTIMATE2014'
            and not schema[17]['nullable'] and not schema[13]['nullable'], 'original query schema')
    require(sha(DDL) == DDL_SHA, 'original DDL identity before source parsing')
    field_pattern = re.compile(r'  "([^"]+)" (integer|smallint|varchar\([0-9]+\))( NOT NULL)?[,]?')
    ddl_fields = [field_pattern.fullmatch(line) for line in DDL.read_text().split('\n')[1:-2]]
    require(len(ddl_fields) == 18 and all(ddl_fields), 'exact original DDL18 field grammar')
    for col, match in zip(schema, ddl_fields):
        require((col['name'], col['type'], col['nullable']) ==
                (match.group(1), match.group(2), match.group(3) is None), 'original DDL/cohort all18 schema equality')
    return schema


def typed_rows(data, schema):
    records = data.split(b'\n')
    if records and records[-1] == b'':
        records.pop()
    rows = []
    for rid, record in enumerate(records):
        fields = record.split(b'|')
        require(len(fields) == 18, f'original field count row {rid}')
        row = []
        for column, raw in zip(schema, fields):
            token = raw.decode('utf-8', errors='strict')
            if token == 'null':
                require(column['nullable'], f'original NOT NULL row {rid} col {column["index"]}')
                row.append(None)
            elif column['type'] in ('integer', 'smallint'):
                require(DECIMAL.fullmatch(token) is not None, 'exact signed decimal grammar')
                value = int(token)
                bits = 32 if column['type'] == 'integer' else 16
                require(-(1 << (bits-1)) <= value < (1 << (bits-1)), 'source signed width')
                row.append(value)
            else:
                require(column['type'].startswith('varchar('), 'declared source field type')
                limit = re.fullmatch(r'varchar\(([0-9]+)\)', column['type'])
                require(limit is not None and len(token) <= int(limit.group(1)), 'declared VARCHAR character bound')
                row.append(token)  # no Unicode normalization, trimming or case conversion
        rows.append(row)
    return rows


def oracle(rows):
    # Independent original-string/signed-value logic. No code ids, bias, lowered
    # predicate, native bitmap, dictionary or engine output is read here.
    groups, selected = {}, []
    for row in rows:
        state, population = row[17], row[13]
        passed = state is not None and state != 'District of Columbia'
        selected.append(passed)
        if not passed:
            continue
        total, seen = groups.get(state, (0, False))
        if population is not None:
            total, seen = total + population, True
        require(-(1 << 127) <= total < (1 << 127), 'oracle signed128 result range')
        groups[state] = (total, seen)
    result = {'columns': COLUMNS, 'rows': [[key, str(groups[key][0]) if groups[key][1] else None]
               for key in sorted(groups, key=lambda key: key.encode('utf-8'))]}
    return result, bitmap(selected)


def fixture_rows(schema, name):
    sizes = dict(MixedTail=1031, AbsentLiteral=65, AllExcluded=63,
                 AllNullPayload=64, Endpoints=1024, Empty=0)
    states = ['District of Columbia', None, '', '\u00e9', 'e\u0301', '\U0001f600', 'a\n"\\', 'alpha', 'line\u2028separator', 'para\u2029separator']
    populations = [None, -(1 << 31), (1 << 31)-1, -1, 0, 1, 17]
    nullable = (True, True)
    if name == 'AbsentLiteral':
        states, populations, nullable = ['alpha', 'beta', 'district of columbia', None], [1, -1, 0], (True, False)
    elif name == 'AllExcluded':
        states, populations, nullable = ['District of Columbia', None], [1, -1], (True, False)
    elif name == 'AllNullPayload':
        states, populations = ['alpha', 'beta', 'District of Columbia', None], [None]
    elif name == 'Endpoints':
        states, populations, nullable = ['alpha', 'beta', 'District of Columbia'], [-(1 << 31), (1 << 31)-1, -1, 0, 1], (False, False)
    elif name == 'Empty':
        nullable = (False, False)
    rows = []
    for i in range(sizes[name]):
        row = [0 if col['type'] in ('integer', 'smallint') else ('X' if col['index'] == 6 else '') for col in schema]
        row[17], row[13] = states[i % len(states)], populations[i % len(populations)]
        rows.append(row)
    return rows, nullable


def reconstruct_prepared(folder, rows, binding):
    data = (folder / 'state.dict').read_bytes()
    require(data[:8] == b'BSSTR01\n' and data[8:48].decode('ascii') == binding, 'dictionary header/source')
    k, = struct.unpack_from('<I', data, 48)
    pos, dictionary = 52, []
    for _ in range(k):
        length, = struct.unpack_from('<I', data, pos)
        pos += 4
        key = data[pos:pos+length].decode('utf-8', errors='strict')
        require(len(data[pos:pos+length]) == length, 'dictionary truncation')
        dictionary.append(key)
        pos += length
    require(pos == len(data) and len(set(dictionary)) == k, 'exact unique dictionary')
    for col in (17, 13):
        values = (folder / f'col{col}.u32').read_bytes()
        valid = (folder / f'col{col}.valid.bitmap').read_bytes()
        require(values == (folder / f'col{col}.biased.u32').read_bytes(), 'stream alias exact')
        require(len(values) == 4 * len(rows) and valid == bitmap([row[col] is not None for row in rows]), 'complete validity')
        for rid, row in enumerate(rows):
            u, = struct.unpack_from('<I', values, 4*rid)
            if row[col] is None:
                require(u == 0, 'canonical null placeholder')
                restored = None
            elif col == 17:
                require(0 <= u - (1 << 31) < k, 'restored code domain')
                restored = dictionary[u - (1 << 31)]
            else:
                restored = u - (1 << 31)
            require(type(restored) is type(row[col]) and restored == row[col], f'full original-value restoration col{col} row{rid}')
    require((folder / 'empty.txt').exists() == (len(rows) == 0), 'empty marker corresponds actual relation')
    return {'status': 'passed', 'rows': len(rows), 'projected_values': 2*len(rows),
            'dictionary_strings_reconstructed_against_original_source': True,
            'complete_validity_and_null_placeholders_checked': True, 'empty_source_checked': True}


def prepare(output):
    require(sha(SQL) == SQL_SHA and sha(DDL) == DDL_SHA, 'pinned literal SQL/DDL')
    data = CSV.read_bytes()
    require(digest(data) == {'bytes': 5453836, 'sha256': CSV_SHA, 'git_sha1': CSV_BLOB}, 'original CSV identity')
    schema = base_schema()
    observed = typed_rows(data, schema)
    require(len(observed) == 65536, 'full original row count')
    output.mkdir(exist_ok=False)
    records = []
    for name in CASES:
        folder = output / name
        folder.mkdir()
        rows, nullable = (observed, (False, False)) if name == 'Observed' else fixture_rows(schema, name)
        require(len(rows) <= 1000000, 'native bounded row count')
        raw_rows = ''.join(json.dumps(row, ensure_ascii=False, separators=(',', ':'))+'\n' for row in rows).encode('utf-8')
        binding = CSV_BLOB if name == 'Observed' else digest(raw_rows)['git_sha1']
        write_bytes(folder / 'source_rows.jsonl', raw_rows)
        write_bytes(folder / 'binding.txt', (binding+'\n').encode('ascii'))
        write_bytes(folder / 'query.sql', SQL.read_bytes())
        ddl = DDL.read_text()
        if nullable[0]:
            ddl = ddl.replace('"State" varchar(20) NOT NULL', '"State" varchar(20)')
        if nullable[1]:
            ddl = ddl.replace('"POPESTIMATE2014" integer NOT NULL', '"POPESTIMATE2014" integer')
        write_bytes(folder / 'schema.sql', ddl.encode())
        dictionary, code = [], {}
        for row in rows:
            if row[17] is not None and row[17] not in code:
                code[row[17]] = len(dictionary)
                dictionary.append(row[17])
        d = bytearray(b'BSSTR01\n'+binding.encode()+struct.pack('<I', len(dictionary)))
        for key in dictionary:
            key_bytes = key.encode('utf-8', errors='strict')
            require(len(key_bytes) <= 1024, 'native dictionary entry bound')
            d.extend(struct.pack('<I', len(key_bytes))+key_bytes)
        require(sum(len(key.encode('utf-8')) for key in dictionary) <= 64*1024*1024, 'dictionary budget')
        write_bytes(folder / 'state.dict', bytes(d))
        write_bytes(folder / 'columns.tsv', ''.join(f'{col} 32 {int(null)} {family} {len(rows)}\n'
                    for col, null, family in [(17, nullable[0], 5), (13, nullable[1], 2)]).encode())
        for col in (17, 13):
            biased = b''.join(struct.pack('<I', 0 if row[col] is None else
                            ((code[row[col]] if col == 17 else row[col]) + (1 << 31))) for row in rows)
            write_bytes(folder / f'col{col}.u32', biased)
            write_bytes(folder / f'col{col}.biased.u32', biased)
            write_bytes(folder / f'col{col}.valid.bitmap', bitmap([row[col] is not None for row in rows]))
        if not rows:
            write_bytes(folder / 'empty.txt', b'1\n')
        expected, selected = oracle(rows)
        write_json(folder / 'oracle.json', expected)
        write_bytes(folder / 'oracle.bitmap', selected)
        restore = reconstruct_prepared(folder, rows, binding)
        write_json(folder / 'RESTORATION_RECEIPT.json', restore)
        record = {'case': name, 'observed_original_query': name == 'Observed', 'rows': len(rows),
                  'state_nullable': nullable[0], 'population_nullable': nullable[1], 'binding': binding,
                  'full_schema_columns': 18, 'projected_columns': [17, 13],
                  'source_csv': str(CSV) if name == 'Observed' else None,
                  'source_csv_sha256': CSV_SHA if name == 'Observed' else None,
                  'source_rows_sha256': sha(folder / 'source_rows.jsonl'),
                  'schema_sha256': sha(folder / 'schema.sql'), 'original_ddl_sha256': DDL_SHA,
                  'original_sql_sha256': SQL_SHA, 'dictionary_cardinality': len(dictionary),
                  'dictionary_bytes': len(d), 'prepared_representation': 'literal UTF8 dictionary + fixed INT32 codes, not FSST/native.fls',
                  'source_schema_variant': 'original exact DDL' if name == 'Observed' else 'declared mathematical fixture DDL',
                  'files': {p.name: digest(p.read_bytes()) for p in sorted(folder.iterdir()) if p.is_file()}}
        write_json(folder / 'case.json', record)
        records.append(record)
    write_json(output / 'PREPARATION_RECEIPT.json', {'status': 'passed', 'cases': records,
        'earlier_all18_fields_machine_validated': True, 'new_projection_frozen_before_inspection': True,
        'fresh_unseen_dataset_claim': False, 'observed_full_original_rows': 65536,
        'fixed_codecs_independent_of_predicate_outcomes': True,
        'full_dictionary_original_value_restoration': True, 'no_timing_claim': True})


def selftest_preparer(output):
    schema = base_schema()
    fields = [b'0' if col['type'] in ('integer', 'smallint') else
              (b'X' if col['index'] == 6 else b'A') for col in schema]
    cases = [('state_not_null', 17, b'null'), ('population_not_null', 13, b'null'),
             ('positive_int32_overflow', 13, b'2147483648'),
             ('negative_int32_underflow', 13, b'-2147483649'),
             ('invalid_utf8', 17, bytes([255])), ('varchar_character_bound', 17, b'x'*21)]
    rejected = []
    for name, column, replacement in cases:
        bad = list(fields)
        bad[column] = replacement
        try:
            typed_rows(b'|'.join(bad)+b'\n', schema)
        except (RuntimeError, UnicodeDecodeError):
            rejected.append(name)
        else:
            raise RuntimeError('missing bounded parser rejection '+name)
    for value in [-(1 << 31), (1 << 31)-1]:
        good = list(fields)
        good[13] = str(value).encode('ascii')
        parsed = typed_rows(b'|'.join(good)+b'\n', schema)
        require(len(parsed) == 1 and parsed[0][13] == value, 'exact valid parser endpoint')
    write_json(output, {'preparer_helper_fixtures': 'PASS', 'rejections': rejected,
        'valid_endpoint_rows': 2, 'observed_values_used': False})


def duckdb_identity():
    require(sha(DUCK_NATIVE) == DUCK_SHA, 'pinned actual DuckDB native identity')
    sys.path.insert(0, str(DUCK_NATIVE.parent))
    import duckdb
    import _duckdb
    require(duckdb.__version__ == '2.0.0' and Path(_duckdb.__file__).resolve() == DUCK_NATIVE.resolve(), 'loaded DuckDB version/file')
    return duckdb, {'version': duckdb.__version__, 'python_module': duckdb.__file__,
        'python_module_sha256': sha(Path(duckdb.__file__)), 'native_module': _duckdb.__file__,
        'native_module_sha256': sha(Path(_duckdb.__file__)), 'cpu_affinity': sorted(os.sched_getaffinity(0)),
        'environment_threads': {key: os.environ.get(key) for key in ['OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS', 'RAYON_NUM_THREADS']}}


def session_settings(con):
    con.execute('SET threads=1')
    con.execute('SET preserve_insertion_order=true')
    settings = {'threads': con.execute("SELECT current_setting('threads')").fetchall()[0][0],
                'default_collation': con.execute("SELECT current_setting('default_collation')").fetchall()[0][0],
                'preserve_insertion_order': con.execute("SELECT current_setting('preserve_insertion_order')").fetchall()[0][0]}
    settings['binary_behavior_probes'] = con.execute("SELECT 'A'='a', 'é'='e', 'é'='é'").fetchall()[0]
    require(settings['binary_behavior_probes'] == (False, False, False), 'case/accent/NFC equality must remain distinct')
    require(settings['threads'] == 1 and settings['default_collation'] in ('', 'binary') and
            settings['preserve_insertion_order'] is True, 'actual single-thread BINARY session')
    return settings


def checked_case(folder):
    meta = read_json(folder / 'case.json')
    require(meta['case'] in CASES and sha(folder / 'query.sql') == SQL_SHA and
            sha(folder / 'schema.sql') == meta['schema_sha256'], 'prepared SQL/schema identity')
    if meta['case'] == 'Observed':
        require(meta['schema_sha256'] == DDL_SHA and meta['state_nullable'] is False and
                meta['population_nullable'] is False and meta['rows'] == 65536, 'observed unchanged original NN schema')
    return meta


def duckdb_build(folder, storage):
    began = time.perf_counter()
    duckdb, identity = duckdb_identity()
    meta = checked_case(folder)
    require(sha(folder / 'source_rows.jsonl') == meta['source_rows_sha256'], 'full typed source identity')
    raw_rows = (folder / 'source_rows.jsonl').read_text(encoding='utf-8')
    require(not raw_rows or raw_rows.endswith('\n'), 'prepared full rows final LF')
    rows = [json.loads(line) for line in raw_rows.split('\n')[:-1]] if raw_rows else []
    require(len(rows) == meta['rows'] and all(len(row) == 18 for row in rows), 'full18 typed vectors')
    source_read_ms = (time.perf_counter()-began)*1000
    storage.mkdir(exist_ok=False)
    dbfile = storage / 'full18.duckdb'
    con = duckdb.connect(str(dbfile))
    settings = session_settings(con)
    insert_start = time.perf_counter()
    con.execute((folder / 'schema.sql').read_text())  # exact original DDL for Observed
    if rows:
        con.execute('BEGIN TRANSACTION')
        con.executemany('INSERT INTO "IGlocations1_1" VALUES ('+','.join(['?']*18)+')', rows)
        con.execute('COMMIT')
    columns = con.execute("PRAGMA table_info('IGlocations1_1')").fetchall()
    schema = base_schema()
    require(len(columns) == len(schema) == 18, 'actual full18 schema cardinality')
    for i, (actual, declared) in enumerate(zip(columns, schema)):
        expected_type = 'VARCHAR' if declared['type'].startswith('varchar(') else declared['type'].upper()
        nullable = meta['state_nullable'] if i == 17 else meta['population_nullable'] if i == 13 else declared['nullable']
        require(actual[0] == i and actual[1] == declared['name'] and actual[2] == expected_type and
                bool(actual[3]) == (not nullable), 'actual all18 column order/name/type/nullability')
    con.execute('CHECKPOINT')
    con.close()
    build_ms = (time.perf_counter()-insert_start)*1000
    con = duckdb.connect(str(dbfile), read_only=True)
    session_settings(con)
    fetched = con.execute('SELECT rowid,* FROM "IGlocations1_1" ORDER BY rowid').fetchall()
    require(len(fetched) == len(rows), 'full engine roundtrip count')
    for i, (actual, expected) in enumerate(zip(fetched, rows)):
        require(actual[0] == i and len(actual) == 19 and
                all(type(a) is type(b) and a == b for a, b in zip(actual[1:], expected)), 'full18 complete typed row roundtrip')
    con.close()
    receipt = {'status': 'passed', 'method': 'duckdb', 'case': meta['case'], 'identity': identity,
        'settings': settings, 'rows': len(rows), 'full_value_cells_verified': 18*len(rows),
        'schema_columns': columns, 'source_read_and_identity_ms': source_read_ms,
        'literal_ddl_insert_checkpoint_ms': build_ms, 'build_save_validate_ms': (time.perf_counter()-began)*1000,
        'ingestion': 'checked full18 typed original rows via executemany; shared source parser, not independent CSV parsing',
        'source_parse_and_native_preparation_cost_separately_in_gate_resource_receipt': True,
        'full18_database_bytes': dbfile.stat().st_size, 'database_sha256': sha(dbfile),
        'source_rows_sha256': meta['source_rows_sha256'], 'original_sql_sha256': SQL_SHA,
        'schema_sha256': meta['schema_sha256'], 'fulltable_vs_projected_compression_ratio_not_defined': True,
        'peak_process_rss_kib': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss, 'no_timing_claim': True}
    write_json(storage / 'build_receipt.json', receipt)


def duckdb_query(folder, storage, sqlpath, resultpath, bitmappath, metricspath):
    start = time.perf_counter()
    duckdb, identity = duckdb_identity()
    meta = checked_case(folder)
    receipt = read_json(storage / 'build_receipt.json')
    require(receipt['status'] == 'passed' and receipt['case'] == meta['case'] and
            receipt['source_rows_sha256'] == meta['source_rows_sha256'] and
            sha(storage / 'full18.duckdb') == receipt['database_sha256'] and sha(sqlpath) == SQL_SHA,
            'engine reload/input identity')
    con = duckdb.connect(str(storage / 'full18.duckdb'), read_only=True)
    settings = session_settings(con)
    setup_ms = (time.perf_counter()-start)*1000
    tick = time.perf_counter()
    cursor = con.execute(sqlpath.read_text())  # original full SQL, no rewritten aggregate or projection
    actual_columns = [{'name': entry[0], 'type': str(entry[1])} for entry in cursor.description]
    raw_rows = cursor.fetchall()
    execute_fetch_ms = (time.perf_counter()-tick)*1000
    require(actual_columns == COLUMNS, 'actual original result aliases/type promotion must match VARCHAR/HUGEINT')
    actual = []
    for key, value in raw_rows:
        require(type(key) is str and (value is None or type(value) is int), 'complete typed engine values')
        key.encode('utf-8', errors='strict')
        require(value is None or -(1 << 127) <= value < (1 << 127), 'actual HUGEINT range')
        actual.append([key, str(value) if value is not None else None])
    require(len({row[0] for row in actual}) == len(actual), 'GROUP BY unique complete keys')
    actual.sort(key=lambda row: row[0].encode('utf-8'))
    result = {'columns': actual_columns, 'rows': actual}
    # JSON file I/O is separate from execute/fetch and complete-worker cost.
    write_json(resultpath, result)
    diagnostic_ms, diagnostic_bytes = 0.0, 0
    if bitmappath != '-':
        diag_start = time.perf_counter()
        selected = con.execute('SELECT rowid, ("State" NOT IN (\'District of Columbia\')) FROM "IGlocations1_1" ORDER BY rowid').fetchall()
        require(len(selected) == meta['rows'] and all(row[0] == i for i, row in enumerate(selected)), 'diagnostic full row ids')
        data = bitmap([row[1] is True for row in selected])
        write_bytes(Path(bitmappath), data)
        diagnostic_bytes = len(data)
        diagnostic_ms = (time.perf_counter()-diag_start)*1000
    con.close()
    write_json(metricspath, {'method': 'duckdb', 'case': meta['case'], 'identity': identity,
        'settings': settings, 'rows': meta['rows'], 'actual_cursor_description': actual_columns,
        'original_engine_row_order_retained': raw_rows,
        'unordered_group_result_canonicalized_only_after_execute': True,
        'setup_ms': setup_ms, 'execute_fetch_ms': execute_fetch_ms,
        'diagnostic_ms_separate': diagnostic_ms, 'diagnostic_enabled': bitmappath != '-',
        'diagnostic_bitmap_bytes': diagnostic_bytes, 'result_bytes': resultpath.stat().st_size,
        'query_worker_does_not_read_source_rows_or_oracle': True,
        'full_worker_ms_before_metrics': (time.perf_counter()-start)*1000,
        'peak_process_rss_kib': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        'no_timing_claim': True})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['prepare', 'selftest_preparer', 'identity', 'duckdb_build', 'duckdb_query'])
    parser.add_argument('paths', nargs='+')
    args = parser.parse_args()
    if args.mode == 'prepare':
        require(len(args.paths) == 1, 'prepare arity')
        prepare(Path(args.paths[0]))
    elif args.mode == 'selftest_preparer':
        require(len(args.paths) == 1, 'preparer selftest arity')
        selftest_preparer(Path(args.paths[0]))
    elif args.mode == 'identity':
        require(len(args.paths) == 1, 'identity arity')
        _, identity = duckdb_identity()
        write_json(Path(args.paths[0]), identity)
    elif args.mode == 'duckdb_build':
        require(len(args.paths) == 2, 'build arity')
        duckdb_build(*map(Path, args.paths))
    else:
        require(len(args.paths) == 6, 'query arity')
        folder, storage, sqlpath, resultpath, bitmappath, metricspath = args.paths
        duckdb_query(Path(folder), Path(storage), Path(sqlpath), Path(resultpath), bitmappath, Path(metricspath))


if __name__ == '__main__':
    main()
