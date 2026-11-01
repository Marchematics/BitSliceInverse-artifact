#!/usr/bin/env python3
"""Build actual Vortex 0.87 control files for the IG/2 external baseline.

Runs under the dedicated Vortex environment (python 3.12, vortex-data 0.87.0,
pyarrow 26.0.0) whose native module sha256 matches the frozen Vortex control
(`101324d9241e33f3beebbb2f347969054c26d3cfbba5f7e45ffff116dabe8228`). For each
prepared IG/2 case this script compresses every original column (full 18-column
control) and the two referenced columns (projected control) with `vx.compress`,
serializes each compressed array into the same VXBANK1 container used by the
frozen control, reloads it, verifies the values, and then executes the IG/2 query
semantics on the reloaded arrays to check the exact prepared oracle.

Vortex 0.87 has no SQL/group-by engine; the aggregation is executed with Arrow
after the compressed load. The result is therefore documented as a **Vortex
compressed-load + Arrow-aggregation hybrid**, not as a pure Vortex query engine.
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
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parent
PREPARED = ROOT / 'results/ig2_prep_20261025/prepared'
CASES = ['Observed', 'MixedTail', 'AbsentLiteral', 'AllExcluded', 'AllNullPayload', 'Endpoints', 'Empty']
TYPES = ['int64', 'int64', 'int64', 'string', 'int64', 'int64', 'string', 'int64', 'int64',
         'int64', 'int64', 'int64', 'int64', 'int64', 'int64', 'int64', 'int64', 'string']
NAMES = ['CENSUS2010POP', 'CONCIT', 'COUSUB', 'City', 'County Code', 'ESTIMATESBASE2010',
         'FUNCSTAT', 'Number of Records', 'PLACE', 'POPESTIMATE2010', 'POPESTIMATE2011',
         'POPESTIMATE2012', 'POPESTIMATE2013', 'POPESTIMATE2014', 'PRIMGEO_FLAG', 'SUMLEV',
         'State code', 'State']
PROJECTED = ['State', 'POPESTIMATE2014']
VORTEX_NATIVE_SHA = '101324d9241e33f3beebbb2f347969054c26d3cfbba5f7e45ffff116dabe8228'
IG2_SQL_SHA = '0bc95092f92bbb2f8652e66c10db8bec5fba88b6d96c5ac23e9650fe27ecca8b'
IG2_COLUMNS = [{'name': 'State', 'type': 'VARCHAR'},
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
    require(len(sys.argv) == 2, 'RUN_ID')
    run_id = sys.argv[1]
    require(run_id.isidentifier(), 'run id')
    out = ROOT / 'results' / run_id
    out.mkdir(exist_ok=False)
    import pyarrow as pa
    import vortex as vx
    from vortex._lib.serde import encode_ipc_array_buffers, decode_ipc_array_buffers
    require(importlib.metadata.version('vortex-data') == '0.87.0', 'pinned Vortex 0.87.0')
    require(sha(pathlib.Path(vx._lib.__file__)) == VORTEX_NATIVE_SHA, 'pinned Vortex native identity')
    require(sha(PREPARED / 'Observed' / 'query.sql') == IG2_SQL_SHA, 'pinned IG/2 SQL bytes')
    identity = {'vortex_data': importlib.metadata.version('vortex-data'),
                'pyarrow': pa.__version__, 'python': sys.version.split()[0],
                'native_module': str(vx._lib.__file__),
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
            buf = []
            for _ in range(n):
                size, = struct.unpack_from('<Q', data, off)
                off += 8
                buf.append(data[off:off + size])
                off += size
            groups.append(buf)
        require(off == len(data), 'VXBANK suffix')
        return decode_ipc_array_buffers(*groups)

    def to_arrow(array):
        back = array.to_arrow_array()
        return back.cast(pa.string()) if pa.types.is_string_view(back.type) else back

    records = []
    for case in CASES:
        folder = PREPARED / case
        oracle = json.loads((folder / 'oracle.json').read_text())
        rows = [json.loads(line) for line in (folder / 'source_rows.jsonl').read_text().split('\n') if line]
        target = out / (case + '_vortex')
        full_dir = target / 'full'
        proj_dir = target / 'projected'
        full_dir.mkdir(parents=True)
        proj_dir.mkdir()
        sizes = {'full': 0, 'projected': 0}
        built = {}
        for ci, (name, kind) in enumerate(zip(NAMES, TYPES)):
            values = [row[ci] for row in rows]
            if kind == 'string':
                array = pa.array(values, type=pa.string())
            else:
                array = pa.array([None if v is None else int(v) for v in values], type=pa.int64())
            full_path = full_dir / f'col{ci}.vxipc'
            compressed = save_compressed(full_path, array)
            reloaded = load_compressed(full_path)
            require(to_arrow(reloaded).to_pylist() == array.to_pylist(),
                    f'Vortex roundtrip {case} {name}')
            require(reloaded.nbytes == compressed.nbytes, 'compressed size identity')
            built[name] = to_arrow(reloaded)
            sizes['full'] += full_path.stat().st_size
            if name in PROJECTED:
                proj_path = proj_dir / f'col{ci}.vxipc'
                save_compressed(proj_path, array)
                reloaded_proj = load_compressed(proj_path)
                require(to_arrow(reloaded_proj).to_pylist() == array.to_pylist(),
                        f'Vortex projected roundtrip {case} {name}')
                sizes['projected'] += proj_path.stat().st_size
        # Exact query result from the reloaded compressed columns (hybrid control).
        table = pa.table(built)
        import pyarrow.compute as pc
        filtered = table.filter(pc.not_equal(table.column('State'), 'District of Columbia'))
        grouped = filtered.group_by('State').aggregate([('POPESTIMATE2014', 'sum')])
        actual = [[record['State'], None if record['POPESTIMATE2014_sum'] is None
                   else str(record['POPESTIMATE2014_sum'])] for record in grouped.to_pylist()]
        actual.sort(key=lambda r: r[0].encode('utf-8'))
        require({'columns': IG2_COLUMNS, 'rows': actual} == oracle,
                'Vortex hybrid query result equals oracle ' + case)
        records.append({'case': case, 'rows': len(rows),
                        'full_dir': str(full_dir), 'full_bytes': sizes['full'],
                        'projected_dir': str(proj_dir), 'projected_bytes': sizes['projected'],
                        'compressed_columns': len(NAMES), 'projected_columns': PROJECTED,
                        'complete_result_equals_oracle': True})
    summary = {'status': 'passed', 'query': 'ig2', 'cases': len(CASES), 'identity': identity,
               'vortex_native_sha256': identity['native_module_sha256'],
               'scope': 'actual Vortex 0.87 compressed load + Arrow aggregation hybrid control for IG/2; '
                        'Vortex 0.87 has no SQL/group-by engine',
               'all_complete_results_equal_oracle': True, 'wall_s': time.time() - started,
               'records': records}
    (out / 'summary.json').write_text(json.dumps(summary, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    print(json.dumps({'status': 'passed', 'query': 'ig2', 'cases': len(CASES),
                      'vortex-data': identity['vortex_data'], 'pyarrow': identity['pyarrow']}))


if __name__ == '__main__':
    main()
