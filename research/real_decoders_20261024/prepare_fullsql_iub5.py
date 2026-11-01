#!/usr/bin/env python3
"""Frozen exact IUB/5 preparation/actual-DuckDB correctness workers.

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
import datetime as dt
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
SQL = ROOT / 'source/PublicBI_fullsql/benchmark__IUBLibrary__queries__5.sql'
DDL = ROOT / 'source/PublicBI_fullsql/benchmark__IUBLibrary__tables__IUBLibrary_1.table.sql'
CSV = ROOT / 'input_round3/IUBLibrary_1.csv'
SQL_SHA = '97d8b7a460d7e02ae40c59626bfbe489a12480db328c72ea5ccc5d2b12e9c73a'
DDL_SHA = '28ad4b0608d4e4042f6b45654ef775fb0ece9341c4b3c4cb59bdd7ed05f97106'
CSV_SHA = '333660e1abbf44ddc6f8216779b425186f6a37028341821630658d68670e856e'
CSV_BLOB = '405d3ce9144d61aff8781f848040a418e3b0bf01'
COHORT_SHA = 'c291344d257095e52cb2995c2374c12c0db6859567aeb85f9a9d794f04b36984'
DUCK_NATIVE = WS / 'bitslice_external_20261021/deps/site/_duckdb.abi3.so'
DUCK_SHA = 'e7cda112749d860a36cb1524ff465cefecdb0acc5c4e2ff4510ecc1b6f6c061a'
ALIAS = 'usr:Number of Records:ok'
COLUMNS = [{'name': 'Sh', 'type': 'VARCHAR'}, {'name': ALIAS, 'type': 'HUGEINT'}]
CASES = ['Observed', 'MixedTail', 'AllSelected', 'AllExcluded', 'AllNullDates', 'Endpoints', 'Empty']
DECIMAL = re.compile(r'[+-]?[0-9]+', re.ASCII)
DATE_CANONICAL = re.compile(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', re.ASCII)
DATE_COLUMNS = [6, 10, 13, 20]
DATE_EPOCH_ORDINAL = 719163


def checked_date(token):
    require(type(token) is str and DATE_CANONICAL.fullmatch(token) is not None,
            'finite canonical CE DATE YYYY-MM-DD grammar')
    value = dt.date(int(token[:4]), int(token[5:7]), int(token[8:10]))
    require(value.isoformat() == token, 'exact canonical DATE roundtrip')
    return value


def civil_days(token):
    # Own Gregorian ordinal; datetime is used independently for validation and
    # restoration, not to choose the fixed interval or encode the day value.
    value = checked_date(token)
    year, month, day = int(token[:4]), int(token[5:7]), int(token[8:10])
    before = [0,31,59,90,120,151,181,212,243,273,304,334]
    y = year-1
    leap = year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)
    ordinal = 365*y+y//4-y//100+y//400+before[month-1]+day+(1 if leap and month>2 else 0)
    days = ordinal-DATE_EPOCH_ORDINAL
    require(days == (value-dt.date(1970,1,1)).days, 'independent Gregorian day encoding')
    return days


def key_order(key):
    return (0, b'') if key is None else (1, key.encode('utf-8', errors='strict'))


def engine_rows(rows, schema):
    return [[checked_date(value) if value is not None and col['type'] == 'date' else value
             for value, col in zip(row, schema)] for row in rows]



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
    return json.loads(path.read_bytes().decode('utf-8',errors='strict'))


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
                  if t['name'] == 'IUBLibrary')['columns']
    require([c['index'] for c in schema] == list(range(27)), '27 original columns')
    require(schema[6]['name'] == 'DateLastCharged' and schema[18]['name'] == 'Sh'
            and not schema[6]['nullable'] and not schema[18]['nullable'], 'original query schema')
    require(sha(DDL) == DDL_SHA, 'original DDL identity before source parsing')
    field_pattern = re.compile(r'  "([^"]+)" (integer|smallint|date|varchar\([0-9]+\))( NOT NULL)?[,]?')
    ddl_fields = [field_pattern.fullmatch(line) for line in DDL.read_bytes().decode('utf-8',errors='strict').split('\n')[1:-2]]
    require(len(ddl_fields) == 27 and all(ddl_fields), 'exact original DDL27 field grammar')
    for col, match in zip(schema, ddl_fields):
        require((col['name'], col['type'], col['nullable']) ==
                (match.group(1), match.group(2), match.group(3) is None), 'original DDL/cohort all27 schema equality')
    require([c['index'] for c in schema if c['type']=='date'] == DATE_COLUMNS, 'all four original DATE positions')
    return schema


def typed_rows(data, schema):
    records = data.split(b'\n')
    if records and records[-1] == b'':
        records.pop()
    rows = []
    for rid, record in enumerate(records):
        fields = record.split(b'|')
        require(len(fields) == 27, f'original field count row {rid}')
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
            elif column['type'] == 'date':
                checked_date(token)
                row.append(token)
            else:
                require(column['type'].startswith('varchar('), 'declared source field type')
                limit = re.fullmatch(r'varchar\(([0-9]+)\)', column['type'])
                require(limit is not None and len(token) <= int(limit.group(1)), 'declared VARCHAR character bound')
                row.append(token)  # no Unicode normalization, trimming or case conversion
        rows.append(row)
    return rows


def oracle(rows):
    # Original civil YEAR predicate and key grouping, independent of codes,
    # own ordinal, interval plans, native bitmaps and actual engine results.
    groups, selected = {}, []
    for row in rows:
        passed = row[6] is not None and checked_date(row[6]).year == 1900
        selected.append(passed)
        if passed:
            groups[row[18]] = groups.get(row[18], 0)+1
    result = {'columns': COLUMNS, 'rows': [[key, str(groups[key])]
               for key in sorted(groups, key=key_order)]}
    return result, bitmap(selected)


def fixture_rows(schema, name):
    sizes = dict(MixedTail=1031, AllSelected=65, AllExcluded=63,
                 AllNullDates=64, Endpoints=1024, Empty=0)
    dates = ['1899-12-31','1900-01-01','1900-02-28','1900-03-01','1900-12-31',
             '1901-01-01','2000-02-29','0001-01-01','9999-12-31',None]
    keys = [None,'','A','a','\u00e9','e\u0301','\U0001f600','\n','"\\','\u2028','\u2029']
    nullable = (True, True)
    if name == 'AllSelected':
        dates, nullable = ['1900-01-01','1900-02-28','1900-03-01','1900-12-31'], (True,True)
    elif name == 'AllExcluded':
        dates = ['1899-12-31','1901-01-01','2000-02-29',None]
    elif name == 'AllNullDates':
        dates = [None]
    elif name == 'Endpoints':
        dates = ['0001-01-01','0001-12-31','1600-02-29','1700-02-28','1700-03-01',
                 '1800-02-28','1800-03-01','1899-12-31','1900-01-01','1900-02-28',
                 '1900-03-01','1900-12-31','1901-01-01','2000-02-28','2000-02-29',
                 '2000-03-01','2100-02-28','2100-03-01','2400-02-29','9999-12-31']
        keys, nullable = ['','A','a','\u00e9','e\u0301','\U0001f600'], (False,False)
    elif name == 'Empty':
        nullable = (False,False)
    rows = []
    for i in range(sizes[name]):
        row = [0 if col['type'] in ('integer','smallint') else
               '1970-01-01' if col['type'] == 'date' else '' for col in schema]
        row[6], row[18] = dates[i % len(dates)], keys[i % len(keys)]
        rows.append(row)
    return rows, nullable


def validate_full_rows(rows, schema, nullable):
    # Validate generated mathematical tuples and reloaded JSONL too; do not send
    # invalid fixture values to an engine merely because the native projection
    # does not reference their columns.
    for row in rows:
        require(len(row)==27,'full typed row cardinality')
        for col,value in zip(schema,row):
            i=col['index']
            allowed_null=nullable[0] if i==6 else nullable[1] if i==18 else col['nullable']
            if value is None:
                require(allowed_null,'full typed row NOT NULL')
            elif col['type'] in ('integer','smallint'):
                bits=32 if col['type']=='integer' else 16
                require(type(value) is int and -(1<<(bits-1)) <= value < (1<<(bits-1)), 'full typed signed range')
            elif col['type']=='date':
                checked_date(value)
            else:
                limit=re.fullmatch(r'varchar\(([0-9]+)\)',col['type'])
                require(type(value) is str and limit is not None and len(value)<=int(limit.group(1)), 'full typed VARCHAR bound')
                value.encode('utf-8',errors='strict')


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
    for col in (6, 18):
        values = (folder / f'col{col}.u32').read_bytes()
        valid = (folder / f'col{col}.valid.bitmap').read_bytes()
        require(values == (folder / f'col{col}.biased.u32').read_bytes(), 'stream alias exact')
        require(len(values) == 4 * len(rows) and valid == bitmap([row[col] is not None for row in rows]), 'complete validity')
        for rid, row in enumerate(rows):
            u, = struct.unpack_from('<I', values, 4*rid)
            if row[col] is None:
                require(u == 0, 'canonical null placeholder')
                restored = None
            elif col == 18:
                require(0 <= u - (1 << 31) < k, 'restored code domain')
                restored = dictionary[u - (1 << 31)]
            else:
                restored = (dt.date(1970,1,1)+dt.timedelta(days=u-(1 << 31))).isoformat()
            require(type(restored) is type(row[col]) and restored == row[col], f'full original-value restoration col{col} row{rid}')
    require((folder / 'empty.txt').exists() == (len(rows) == 0), 'empty marker corresponds actual relation')
    return {'status': 'passed', 'rows': len(rows), 'projected_values': 2*len(rows),
            'dictionary_strings_reconstructed_against_original_source': True,
            'complete_validity_and_null_placeholders_checked': True, 'empty_source_checked': True, 'all_dates_independent_calendar_roundtrip': True}


def prepare(output):
    require(sha(SQL) == SQL_SHA and sha(DDL) == DDL_SHA, 'pinned literal SQL/DDL')
    schema = base_schema()
    data = CSV.read_bytes()
    require(digest(data) == {'bytes':453969,'sha256':CSV_SHA,'git_sha1':CSV_BLOB},'original CSV identity')
    observed = typed_rows(data, schema)
    require(len(observed) == 1795, 'full original row count')
    output.mkdir(exist_ok=False)
    records = []
    for name in CASES:
        folder = output / name
        folder.mkdir()
        rows, nullable = (observed, (False, False)) if name == 'Observed' else fixture_rows(schema, name)
        require(len(rows)<=1000000,'native bounded row count')
        validate_full_rows(rows,schema,nullable)
        raw_rows = ''.join(json.dumps(row, ensure_ascii=False, separators=(',', ':'))+'\n' for row in rows).encode('utf-8')
        binding = CSV_BLOB if name == 'Observed' else digest(raw_rows)['git_sha1']
        write_bytes(folder / 'source_rows.jsonl', raw_rows)
        write_bytes(folder / 'binding.txt', (binding+'\n').encode('ascii'))
        write_bytes(folder / 'query.sql', SQL.read_bytes())
        ddl = DDL.read_bytes().decode('utf-8',errors='strict')
        if nullable[0]:
            ddl = ddl.replace('"DateLastCharged" date NOT NULL', '"DateLastCharged" date')
        if nullable[1]:
            ddl = ddl.replace('"Sh" varchar(2) NOT NULL', '"Sh" varchar(2)')
        write_bytes(folder / 'schema.sql', ddl.encode())
        dictionary, code = [], {}
        for row in rows:
            if row[18] is not None and row[18] not in code:
                code[row[18]] = len(dictionary)
                dictionary.append(row[18])
        d = bytearray(b'BSSTR01\n'+binding.encode()+struct.pack('<I', len(dictionary)))
        for key in dictionary:
            key_bytes = key.encode('utf-8', errors='strict')
            require(len(key_bytes) <= 1024, 'native dictionary entry bound')
            d.extend(struct.pack('<I', len(key_bytes))+key_bytes)
        require(sum(len(key.encode('utf-8')) for key in dictionary) <= 64*1024*1024, 'dictionary budget')
        write_bytes(folder / 'state.dict', bytes(d))
        write_bytes(folder / 'columns.tsv', ''.join(f'{col} 32 {int(null)} {family} {len(rows)}\n'
                    for col, null, family in [(6, nullable[0], 5), (18, nullable[1], 5)]).encode())
        for col in (6, 18):
            biased = b''.join(struct.pack('<I', 0 if row[col] is None else
                            ((code[row[col]] if col == 18 else civil_days(row[col])) + (1 << 31))) for row in rows)
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
                  'date_nullable': nullable[0], 'sh_nullable': nullable[1], 'binding': binding,
                  'full_schema_columns': 27, 'projected_columns': [6, 18],
                  'source_csv': str(CSV) if name == 'Observed' else None,
                  'source_csv_sha256': CSV_SHA if name == 'Observed' else None,
                  'source_rows_sha256': sha(folder / 'source_rows.jsonl'),
                  'schema_sha256': sha(folder / 'schema.sql'), 'original_ddl_sha256': DDL_SHA,
                  'original_sql_sha256': SQL_SHA, 'dictionary_cardinality': len(dictionary),
                  'dictionary_bytes': len(d), 'prepared_representation': 'finite DATE32 + literal UTF8 Sh dictionary + fixed INT32 code/day plainFFOR; not native.fls',
                  'source_schema_variant': 'original exact DDL' if name == 'Observed' else 'declared mathematical fixture DDL',
                  'files': {p.name: digest(p.read_bytes()) for p in sorted(folder.iterdir()) if p.is_file()}}
        write_json(folder / 'case.json', record)
        records.append(record)
    write_json(output / 'PREPARATION_RECEIPT.json', {'status': 'passed', 'cases': records,
        'earlier_all27_fields_machine_read':True,'earlier_date_semantics_validation_not_claimed':True, 'new_projection_frozen_before_inspection': True,
        'fresh_unseen_dataset_claim': False, 'observed_full_original_rows': 1795,
        'fixed_codecs_independent_of_predicate_outcomes': True,
        'full_dictionary_original_value_restoration': True, 'no_timing_claim': True})


def selftest_preparer(output):
    schema = base_schema()
    fields = [b'0' if col['type'] in ('integer','smallint') else
              b'1970-01-01' if col['type'] == 'date' else b'A' for col in schema]
    cases = [('date_not_null',6,b'null'),('sh_not_null',18,b'null'),
             ('positive_int32_overflow',3,b'2147483648'),('negative_int32_underflow',3,b'-2147483649'),
             ('invalid_utf8',18,bytes([255])),('varchar_character_bound',18,b'xxx'),
             ('invalid_1900_leap',6,b'1900-02-29'),('invalid_2100_leap',6,b'2100-02-29'),
             ('zero_year',6,b'0000-01-01'),('year_10000',6,b'10000-01-01'),
             ('infinite_date',6,b'infinity'),('date_noncanonical',6,b'1900-1-01'),
             ('date_trailing_space',6,b'1900-01-01 '),('date_invalid_day',6,b'1900-04-31')]
    rejected = []
    for name,column,replacement in cases:
        bad = list(fields)
        bad[column] = replacement
        try:
            typed_rows(b'|'.join(bad)+b'\n',schema)
        except (RuntimeError,ValueError,UnicodeDecodeError):
            rejected.append(name)
        else:
            raise RuntimeError('missing bounded parser rejection '+name)
    endpoints = ['0001-01-01','9999-12-31','1600-02-29','2000-02-29','1900-01-01','1901-01-01']
    for token in endpoints:
        good = list(fields)
        good[6] = token.encode('ascii')
        parsed = typed_rows(b'|'.join(good)+b'\n',schema)
        days = civil_days(parsed[0][6])
        require((dt.date(1970,1,1)+dt.timedelta(days=days)).isoformat() == token, 'valid DATE endpoint')
    require(civil_days('1900-01-01') == -25567 and civil_days('1901-01-01') == -25202,
            'fixed SQL Gregorian interval constants')
    write_json(output, {'preparer_helper_fixtures':'PASS','rejections':rejected,
        'valid_endpoint_rows':len(endpoints),'observed_values_used':False})


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
    con.execute('SET threads=1').fetchall()
    con.execute('SET preserve_insertion_order=true').fetchall()
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
        require(meta['schema_sha256'] == DDL_SHA and meta['date_nullable'] is False and
                meta['sh_nullable'] is False and meta['rows'] == 1795, 'observed unchanged original NN schema')
    return meta


def duckdb_build(folder, storage):
    began = time.perf_counter()
    duckdb, identity = duckdb_identity()
    meta = checked_case(folder)
    require(sha(folder / 'source_rows.jsonl') == meta['source_rows_sha256'], 'full typed source identity')
    raw_rows = (folder/'source_rows.jsonl').read_bytes().decode('utf-8',errors='strict')
    require(not raw_rows or raw_rows.endswith('\n'), 'prepared full rows final LF')
    rows = [json.loads(line) for line in raw_rows.split('\n')[:-1]] if raw_rows else []
    require(len(rows) == meta['rows'] and all(len(row) == 27 for row in rows), 'full27 typed vectors')
    schema = base_schema()
    validate_full_rows(rows,schema,(meta['date_nullable'],meta['sh_nullable']))
    rows = engine_rows(rows,schema)
    source_read_ms = (time.perf_counter()-began)*1000
    storage.mkdir(exist_ok=False)
    dbfile = storage / 'full27.duckdb'
    con = duckdb.connect(str(dbfile),config={'threads':'1','preserve_insertion_order':'true'})
    settings = session_settings(con)
    insert_start = time.perf_counter()
    con.execute((folder / 'schema.sql').read_bytes().decode('utf-8',errors='strict')).fetchall()  # exact original DDL for Observed
    if rows:
        con.execute('BEGIN TRANSACTION').fetchall()
        con.executemany('INSERT INTO "IUBLibrary_1" VALUES ('+','.join(['?']*27)+')',rows).fetchall()
        con.execute('COMMIT').fetchall()
    columns = con.execute("PRAGMA table_info('IUBLibrary_1')").fetchall()
    schema = base_schema()
    require(len(columns) == len(schema) == 27, 'actual full27 schema cardinality')
    for i, (actual, declared) in enumerate(zip(columns, schema)):
        expected_type = 'VARCHAR' if declared['type'].startswith('varchar(') else declared['type'].upper()
        nullable = meta['date_nullable'] if i == 6 else meta['sh_nullable'] if i == 18 else declared['nullable']
        require(actual[0] == i and actual[1] == declared['name'] and actual[2] == expected_type and
                bool(actual[3]) == (not nullable), 'actual all27 column order/name/type/nullability')
    con.execute('CHECKPOINT').fetchall()
    con.close()
    build_ms = (time.perf_counter()-insert_start)*1000
    con = duckdb.connect(str(dbfile),read_only=True,config={'threads':'1','preserve_insertion_order':'true'})
    session_settings(con)
    fetched = con.execute('SELECT rowid,* FROM "IUBLibrary_1" ORDER BY rowid').fetchall()
    require(len(fetched) == len(rows), 'full engine roundtrip count')
    for i, (actual, expected) in enumerate(zip(fetched, rows)):
        require(actual[0] == i and len(actual) == 28 and
                all(type(a) is type(b) and a == b for a, b in zip(actual[1:], expected)), 'full27 complete typed row roundtrip')
    con.close()
    receipt = {'status': 'passed', 'method': 'duckdb', 'case': meta['case'], 'identity': identity,
        'settings': settings, 'rows': len(rows), 'full_value_cells_verified': 27*len(rows),
        'schema_columns':columns,'date_columns_typed_roundtripped':DATE_COLUMNS,
        'source_read_and_identity_ms':source_read_ms,
        'literal_ddl_insert_checkpoint_ms': build_ms, 'build_save_validate_ms': (time.perf_counter()-began)*1000,
        'ingestion': 'checked full27 typed original rows via executemany; shared source parser, not independent CSV parsing',
        'source_parse_and_native_preparation_cost_separately_in_gate_resource_receipt': True,
        'full27_database_bytes': dbfile.stat().st_size, 'database_sha256': sha(dbfile),
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
            sha(storage / 'full27.duckdb') == receipt['database_sha256'] and sha(sqlpath) == SQL_SHA,
            'engine reload/input identity')
    con = duckdb.connect(str(storage / 'full27.duckdb'),read_only=True,config={'threads':'1','preserve_insertion_order':'true'})
    settings = session_settings(con)
    setup_ms = (time.perf_counter()-start)*1000
    tick = time.perf_counter()
    cursor = con.execute(sqlpath.read_bytes().decode('utf-8',errors='strict'))  # original full SQL, no rewritten aggregate or projection
    actual_columns = [{'name': entry[0], 'type': str(entry[1])} for entry in cursor.description]
    raw_rows = cursor.fetchall()
    execute_fetch_ms = (time.perf_counter()-tick)*1000
    require(actual_columns == COLUMNS, 'actual original result aliases/type promotion must match VARCHAR/HUGEINT')
    actual = []
    for key, value in raw_rows:
        require((key is None or type(key) is str) and type(value) is int and 0 < value < (1 << 127),
                'complete typed nullable-key/non-NULL SUM(1) engine values')
        if key is not None:
            key.encode('utf-8',errors='strict')
        actual.append([key,str(value)])
    require(len({row[0] for row in actual}) == len(actual), 'GROUP BY unique complete keys')
    actual.sort(key=lambda row: key_order(row[0]))
    result = {'columns': actual_columns, 'rows': actual}
    # JSON file I/O is separate from execute/fetch and complete-worker cost.
    write_json(resultpath, result)
    diagnostic_ms, diagnostic_bytes = 0.0, 0
    if bitmappath != '-':
        diag_start = time.perf_counter()
        selected = con.execute('SELECT rowid, (CAST(EXTRACT(YEAR FROM "DateLastCharged") AS BIGINT) = 1900) FROM "IUBLibrary_1" ORDER BY rowid').fetchall()
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
