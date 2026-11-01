#!/usr/bin/env python3
"""Build scaled IG/2 Vortex 0.87 controls from the synthetic Arrow input.

Runs under the dedicated Vortex environment. Compresses the full 18 columns and the
two referenced columns of the 16x-replicated case into VXBANK1 containers, verifies
every column roundtrip, and checks the query result computed from the reloaded
compressed columns against the scaled oracle.
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parent))
import artifact_paths as _artifact
import hashlib
import importlib.metadata
import json
import struct
from pathlib import Path

ROOT = Path(__file__).resolve().parent
ARROW = ROOT / 'results/ig2_scale8shuf_arrow/Observed_arrow'
PREP = ROOT / 'results/ig2_scale8shuf_prep/Observed'
OUT = ROOT / 'results/ig2_scale8shuf_vortex/Observed_vortex'
VORTEX_NATIVE_SHA = '101324d9241e33f3beebbb2f347969054c26d3cfbba5f7e45ffff116dabe8228'
SQL_SHA = '0bc95092f92bbb2f8652e66c10db8bec5fba88b6d96c5ac23e9650fe27ecca8b'
NAMES = ['CENSUS2010POP', 'CONCIT', 'COUSUB', 'City', 'County Code', 'ESTIMATESBASE2010',
         'FUNCSTAT', 'Number of Records', 'PLACE', 'POPESTIMATE2010', 'POPESTIMATE2011',
         'POPESTIMATE2012', 'POPESTIMATE2013', 'POPESTIMATE2014', 'PRIMGEO_FLAG', 'SUMLEV',
         'State code', 'State']
PROJECTED = ['State', 'POPESTIMATE2014']
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
    require(sha(PREP / 'query.sql') == SQL_SHA, 'pinned original SQL')
    oracle = json.loads((PREP / 'oracle.json').read_text(encoding='utf-8'))
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.ipc as ipc
    import vortex as vx
    from vortex._lib.serde import encode_ipc_array_buffers, decode_ipc_array_buffers
    require(importlib.metadata.version('vortex-data') == '0.87.0', 'Vortex 0.87.0')
    require(sha(Path(vx._lib.__file__)) == VORTEX_NATIVE_SHA, 'pinned Vortex native identity')
    vx.set_worker_threads(1)
    pa.set_cpu_count(1)
    pa.set_io_thread_count(1)
    table = ipc.open_file(str(ARROW / 'full18.arrow')).read_all()
    require(table.num_rows == 524288 and table.num_columns == 18, 'scaled Arrow full shape')
    OUT.mkdir(parents=True)
    full_dir = OUT / 'full'
    proj_dir = OUT / 'projected'
    full_dir.mkdir(exist_ok=True)
    proj_dir.mkdir(exist_ok=True)

    def compress_save(path, array):
        compressed = vx.compress(vx.array(array))
        ab, db = encode_ipc_array_buffers(compressed)
        with path.open('xb') as f:
            f.write(b'VXBANK1\n')
            for buffers in (ab, db):
                f.write(struct.pack('<I', len(buffers)))
                for b in buffers:
                    f.write(struct.pack('<Q', len(b))); f.write(b)
        return compressed.nbytes

    def reload(path):
        data = path.read_bytes()
        require(data[:8] == b'VXBANK1\n', 'VXBANK magic')
        off = 8
        groups = []
        for _ in range(2):
            n, = struct.unpack_from('<I', data, off); off += 4
            buffers = []
            for _ in range(n):
                size, = struct.unpack_from('<Q', data, off); off += 8
                buffers.append(data[off:off + size]); off += size
            groups.append(buffers)
        require(off == len(data), 'VXBANK suffix')
        decoded = decode_ipc_array_buffers(*groups)
        array = decoded.to_arrow_array()
        return array.cast(pa.string()) if pa.types.is_string_view(array.type) else array

    full_mapping, proj_mapping = {}, {}
    full_bytes = proj_bytes = 0
    reloaded_projected = {}
    for index, name in enumerate(table.schema.names):
        array = table.column(index).combine_chunks()
        path = full_dir / f'col{index}.vxipc'
        full_bytes += compress_save(path, array)
        require(reload(path).equals(array), f'Vortex roundtrip {name}')
        full_mapping[index] = name
        if name in PROJECTED:
            proj_path = proj_dir / f'col{index}.vxipc'
            proj_bytes += compress_save(proj_path, array)
            reloaded = reload(proj_path)
            require(reloaded.equals(array), f'Vortex projected roundtrip {name}')
            proj_mapping[index] = name
            reloaded_projected[name] = reloaded
    require(set(proj_mapping.values()) == set(PROJECTED), 'projected columns present')
    (full_dir / 'columns.json').write_text(json.dumps({str(k): v for k, v in full_mapping.items()},
                                                      indent=2) + '\n', encoding='utf-8')
    (proj_dir / 'columns.json').write_text(json.dumps({str(k): v for k, v in proj_mapping.items()},
                                                      indent=2) + '\n', encoding='utf-8')
    filtered = pa.table(reloaded_projected).filter(
        pc.not_equal(reloaded_projected['State'], 'District of Columbia'))
    grouped = filtered.group_by('State').aggregate([('POPESTIMATE2014', 'sum')])
    actual = [[record['State'], None if record['POPESTIMATE2014_sum'] is None
               else str(record['POPESTIMATE2014_sum'])] for record in grouped.to_pylist()]
    actual.sort(key=lambda row: row[0].encode('utf-8'))
    require({'columns': QUERY_COLUMNS, 'rows': actual} == oracle,
            'Vortex reloaded query result equals scaled oracle')
    summary = {'status': 'passed', 'case': 'Observed', 'rows': 524288, 'scale_factor': 16,
               'vortex_data': importlib.metadata.version('vortex-data'),
               'pyarrow': pa.__version__, 'python': __import__('sys').version.split()[0],
               'native_sha256': sha(Path(vx._lib.__file__)),
               'full_dir': str(full_dir), 'full_compressed_bytes': full_bytes,
               'projected_dir': str(proj_dir), 'projected_compressed_bytes': proj_bytes,
               'reloaded_query_result_equals_oracle': True,
               'scope': 'synthetic 16x row-replicated scale test; Vortex compressed load + Arrow '
                        'aggregation hybrid, not a pure Vortex engine measurement'}
    (ROOT / 'results/ig2_scale8shuf_vortex_summary.json').write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    print(json.dumps({'status': 'passed', 'full_compressed_bytes': full_bytes,
                      'projected_compressed_bytes': proj_bytes}))


if __name__ == '__main__':
    main()
