#!/usr/bin/env python3
"""Build actual Vortex 0.87 control files for the three expansion queries.

Runs under the dedicated Vortex environment (python 3.12, vortex-data 0.87.0,
native sha256 101324d9..., pyarrow 26.0.0, duckdb 2.0.0). For each prepared case
it reads the full-schema persisted DuckDB database, compresses every column and
the projected columns the query references with `vx.compress`, serializes each
compressed array into the frozen VXBANK1 container, reloads and verifies the
values, then executes the query semantics with Arrow on the reloaded columns and
compares to the prepared oracle.

Vortex 0.87 cannot represent a fully-NULL Arrow column; such columns are skipped
in the full-schema control and listed in the receipt (they carry no values).
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parent))
import artifact_paths as _artifact
import hashlib
import importlib.metadata
import json
import pathlib
import struct
from pathlib import Path
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parent
VORTEX_NATIVE_SHA = '101324d9241e33f3beebbb2f347969054c26d3cfbba5f7e45ffff116dabe8228'

QUERIES = {
    'hatred8': {'prep': 'hatred8_prep_v5', 'table': 'Hatred_1',
                'cases': ['Observed', 'Empty', 'Singleton', 'Boundary', 'AllExcluded', 'RangeEndpoints'],
                'projected': ['State ', 'Keyword'],
                'sql_sha': '59bfb39f39631750e8c92e6a810043eb701048ea6ccd383bf41030265e3831c6'},
    'citymax14': {'prep': 'citymax14_prep_v3', 'table': 'CityMaxCapita_1',
                  'cases': ['Observed', 'Empty', 'Singleton', 'AllNullCity', 'MixedNull'],
                  'projected': ['City'],
                  'sql_sha': '9b13c04a23f00ca53d14bd9a6f6a9dc8a751bf74ef79b4f94d9ce470530bdbc4'},
    'hatred10': {'prep': 'hatred10_prep_v1', 'table': 'Hatred_1',
                 'cases': ['Observed', 'Empty', 'Singleton', 'Boundary', 'AllExcluded', 'KeywordNone'],
                 'projected': ['State ', 'Keyword', 'StatePopNum'],
                 'sql_sha': 'de973d94afba5a1fb9c3c472dcb547dd3fd8c1c9fe5e745aeb74294179596364'},
}


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def sha(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for b in iter(lambda: f.read(1 << 20), b''):
            h.update(b)
    return h.hexdigest()


def query_hatred8(table):
    import pyarrow as pa
    import pyarrow.compute as pc
    columns = [{'name': 'State ', 'type': 'VARCHAR'}]
    if table.num_rows == 0:
        return {'columns': columns, 'rows': []}
    state = table.column('State ')
    keyword = table.column('Keyword')
    cond = pc.and_(
        pc.or_(pc.and_(pc.greater_equal(state, 'AK'), pc.less_equal(state, 'CT')),
               pc.and_(pc.greater_equal(state, 'DE'), pc.less_equal(state, 'WY'))),
        pc.is_in(keyword, value_set=pa.array(['bimbo', 'hag', 'twat'])))
    grouped = table.filter(cond).group_by('State ').aggregate([('State ', 'count')])
    rows = [[record['State ']] for record in grouped.to_pylist()
            if 30 <= record['State _count'] <= 100000]
    rows.sort(key=lambda r: r[0].encode('utf-8'))
    return {'columns': columns, 'rows': rows}


def query_citymax14(table):
    import pyarrow as pa
    columns = [{'name': 'City', 'type': 'VARCHAR'},
               {'name': 'TEMP(Calculation_383650433986449409)(2109769841)(0)', 'type': 'HUGEINT'}]
    if table.num_rows == 0:
        return {'columns': columns, 'rows': []}
    ones = pa.array([1] * table.num_rows, type=pa.int64())
    grouped = table.append_column('__one', ones).group_by('City').aggregate([('__one', 'count')])
    null_count = 0
    named = []
    for record in grouped.to_pylist():
        if record['City'] is None:
            null_count = record['__one_count']
        else:
            named.append([record['City'], str(record['__one_count'])])
    named.sort(key=lambda r: r[0].encode('utf-8'))
    rows = ([[None, str(null_count)]] if null_count else []) + named
    return {'columns': columns, 'rows': rows}


def query_hatred10(table):
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
    grouped = filtered.append_column('__one', ones).group_by('State ').aggregate(
        [('__one', 'count'), ('StatePopNum', 'max'), ('StatePopNum', 'min')])
    rows = [[record['State '], str(record['__one_count']), str(record['StatePopNum_max']),
             str(record['StatePopNum_min'])]
            for record in grouped.to_pylist() if 30 <= record['__one_count'] <= 100000]
    rows.sort(key=lambda r: r[0].encode('utf-8'))
    return {'columns': columns, 'rows': rows}


QUERY_FUNCS = {'hatred8': query_hatred8, 'citymax14': query_citymax14, 'hatred10': query_hatred10}


def main():
    require(len(sys.argv) == 3, 'QUERY RUN_ID')
    query = sys.argv[1]
    run_id = sys.argv[2]
    require(query in QUERIES and run_id.isidentifier(), 'query/run id')
    spec = QUERIES[query]
    out = ROOT / 'results' / run_id
    out.mkdir(exist_ok=False)
    import duckdb
    import pyarrow as pa
    import vortex as vx
    from vortex._lib.serde import encode_ipc_array_buffers, decode_ipc_array_buffers
    require(importlib.metadata.version('vortex-data') == '0.87.0', 'pinned Vortex 0.87.0')
    require(sha(pathlib.Path(vx._lib.__file__)) == VORTEX_NATIVE_SHA, 'pinned Vortex native identity')
    require(importlib.metadata.version('duckdb') == '2.0.0', 'pinned DuckDB 2.0.0 for typed source export')
    prepared = ROOT / 'results' / spec['prep']
    require(sha(prepared / 'Observed' / 'query.sql') == spec['sql_sha'], 'pinned original SQL bytes')
    identity = {'vortex_data': importlib.metadata.version('vortex-data'),
                'pyarrow': pa.__version__, 'duckdb': importlib.metadata.version('duckdb'),
                'python': sys.version.split()[0], 'native_module': str(vx._lib.__file__),
                'native_module_sha256': sha(pathlib.Path(vx._lib.__file__))}
    vx.set_worker_threads(1)
    pa.set_cpu_count(1)
    pa.set_io_thread_count(1)
    started = time.time()

    def save_compressed(path, array):
        compressed = vx.compress(vx.array(array))
        ab, db = encode_ipc_array_buffers(compressed)
        with path.open('xb') as f:
            f.write(b'VXBANK1\n')
            for buffers in (ab, db):
                f.write(struct.pack('<I', len(buffers)))
                for b in buffers:
                    f.write(struct.pack('<Q', len(b)))
                    f.write(b)
        return compressed

    def load_compressed(path):
        data = path.read_bytes()
        require(data[:8] == b'VXBANK1\n', 'VXBANK magic')
        off = 8
        groups = []
        for _ in range(2):
            n, = struct.unpack_from('<I', data, off)
            off += 4
            buffer = []
            for _ in range(n):
                size, = struct.unpack_from('<Q', data, off)
                off += 8
                buffer.append(data[off:off + size])
                off += size
            groups.append(buffer)
        require(off == len(data), 'VXBANK suffix')
        return decode_ipc_array_buffers(*groups)

    def to_arrow(array):
        back = array.to_arrow_array()
        return back.cast(pa.string()) if pa.types.is_string_view(back.type) else back

    records = []
    for case in spec['cases']:
        folder = prepared / case
        oracle = json.loads((folder / 'oracle.json').read_text())
        database = Path(str(folder) + '_duckdb/full31.duckdb')
        require(database.is_file(), 'full-schema DuckDB source ' + case)
        con = duckdb.connect(str(database), read_only=True, config={'threads': '1'})
        table = con.sql('SELECT * FROM "' + spec['table'] + '"').to_arrow_table()
        con.close()
        case_meta = json.loads((folder / 'case.json').read_text())
        expected_rows = case_meta.get('rows', len([line for line in (folder / 'source_rows.jsonl')
                                                   .read_text().split('\n') if line]))
        require(table.num_rows == expected_rows, 'typed source row count ' + case)
        target = out / (case + '_vortex')
        full_dir = target / 'full'
        proj_dir = target / 'projected'
        full_dir.mkdir(parents=True)
        proj_dir.mkdir()
        sizes = {'full': 0, 'projected': 0}
        mapping = {'full': {}, 'projected': {}}
        skipped = []
        for index, name in enumerate(table.schema.names):
            array = table.column(index).combine_chunks()
            if array.null_count == len(array) and len(array) > 0:
                skipped.append(name)
                continue
            full_path = full_dir / f'col{index}.vxipc'
            compressed = save_compressed(full_path, array)
            reloaded = load_compressed(full_path)
            require(to_arrow(reloaded).to_pylist() == array.to_pylist(), f'Vortex roundtrip {case} {name}')
            require(reloaded.nbytes == compressed.nbytes, 'compressed size identity')
            sizes['full'] += full_path.stat().st_size
            mapping['full'][index] = name
            if name in spec['projected']:
                proj_path = proj_dir / f'col{index}.vxipc'
                save_compressed(proj_path, array)
                reloaded_proj = load_compressed(proj_path)
                require(to_arrow(reloaded_proj).to_pylist() == array.to_pylist(),
                        f'Vortex projected roundtrip {case} {name}')
                sizes['projected'] += proj_path.stat().st_size
                mapping['projected'][index] = name
        missing_projected = sorted(set(spec['projected']) - set(mapping['projected'].values()))
        # Rebuild the reloaded table for the exact-result check. Cases whose
        # projected key column is entirely NULL cannot be represented by Vortex
        # 0.87 and are recorded as not checked.
        result_checked = False
        if not missing_projected:
            import pyarrow as pa2
            columns = {name: to_arrow(load_compressed(full_dir / f'col{index}.vxipc'))
                       for index, name in mapping['full'].items()}
            reloaded_table = pa2.table(columns)
            result = QUERY_FUNCS[query](reloaded_table)
            require(result == oracle, 'Vortex hybrid query result equals oracle ' + case)
            result_checked = True
        for mode in ('full', 'projected'):
            (target / mode / 'columns.json').write_text(
                json.dumps({str(k): v for k, v in mapping[mode].items()}, indent=2) + '\n', encoding='utf-8')
        records.append({'case': case, 'rows': table.num_rows,
                        'full_dir': str(full_dir), 'full_bytes': sizes['full'],
                        'projected_dir': str(proj_dir), 'projected_bytes': sizes['projected'],
                        'full_columns': len(mapping['full']),
                        'skipped_all_null_columns': skipped,
                        'missing_projected_columns': missing_projected,
                        'projected_columns': spec['projected'],
                        'complete_result_equals_oracle': result_checked,
                        'result_check_note': ('projected key column entirely NULL; not representable in Vortex 0.87'
                                              if missing_projected else 'exact oracle check passed')})
    summary = {'status': 'passed', 'query': query, 'prep_run': spec['prep'],
               'cases': len(spec['cases']), 'identity': identity,
               'vortex_native_sha256': identity['native_module_sha256'],
               'scope': 'actual Vortex 0.87 compressed load + Arrow aggregation hybrid control for an '
                        'expansion query; fully-NULL columns are skipped in the full control',
               'all_complete_results_equal_oracle': True, 'wall_s': time.time() - started,
               'records': records}
    (out / 'summary.json').write_text(json.dumps(summary, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    print(json.dumps({'status': 'passed', 'query': query, 'cases': len(spec['cases'])}))


if __name__ == '__main__':
    main()
