#!/usr/bin/env python3
"""Build a shuffled 8x row-replicated IG/2 case (524,288 rows) for the order x scale test."""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parent))
import artifact_paths as _artifact
import hashlib
import json
import random
import shutil
import struct
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SOURCE = ROOT / 'results/ig2_prep_20261025/prepared/Observed'
SCALE = 8
SEED = 20261026
PREP_OUT = ROOT / 'results/ig2_scale8shuf_prep/Observed'
ARROW_OUT = ROOT / 'results/ig2_scale8shuf_arrow/Observed_arrow'
IG2_SQL_SHA = '0bc95092f92bbb2f8652e66c10db8bec5fba88b6d96c5ac23e9650fe27ecca8b'
TYPES = ['int64', 'int64', 'int64', 'string', 'int64', 'int64', 'string', 'int64', 'int64',
         'int64', 'int64', 'int64', 'int64', 'int64', 'int64', 'int64', 'int64', 'string']
NAMES = ['CENSUS2010POP', 'CONCIT', 'COUSUB', 'City', 'County Code', 'ESTIMATESBASE2010',
         'FUNCSTAT', 'Number of Records', 'PLACE', 'POPESTIMATE2010', 'POPESTIMATE2011',
         'POPESTIMATE2012', 'POPESTIMATE2013', 'POPESTIMATE2014', 'PRIMGEO_FLAG', 'SUMLEV',
         'State code', 'State']


def sha(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for b in iter(lambda: f.read(1 << 20), b''):
            h.update(b)
    return h.hexdigest()


def load_u32(path):
    data = Path(path).read_bytes()
    return list(struct.unpack('<' + 'I' * (len(data) // 4), data))


def load_bits(path):
    data = Path(path).read_bytes()
    return [(data[i >> 3] >> (i & 7)) & 1 for i in range(len(data) * 8)]


def write_u32(path, values):
    Path(path).write_bytes(struct.pack('<' + 'I' * len(values), *values))


def write_bits(path, bits):
    out = bytearray((len(bits) + 7) // 8)
    for i, bit in enumerate(bits):
        if bit:
            out[i >> 3] |= 1 << (i & 7)
    Path(path).write_bytes(bytes(out))


def main():
    assert sha(SOURCE / 'query.sql') == IG2_SQL_SHA
    oracle = json.loads((SOURCE / 'oracle.json').read_text())
    rows = [json.loads(line) for line in
            (SOURCE / 'source_rows.jsonl').read_text().split('\n') if line]
    assert len(rows) == 65536
    total = len(rows) * SCALE
    order = list(range(total))
    random.Random(SEED).shuffle(order)
    if PREP_OUT.exists():
        shutil.rmtree(PREP_OUT)
    PREP_OUT.mkdir(parents=True)
    ARROW_OUT.mkdir(parents=True, exist_ok=True)
    for name in ('binding.txt', 'query.sql', 'schema.sql', 'state.dict'):
        shutil.copyfile(SOURCE / name, PREP_OUT / name)
    for name in ('col13.u32', 'col13.biased.u32', 'col17.u32', 'col17.biased.u32'):
        rep = load_u32(SOURCE / name) * SCALE
        write_u32(PREP_OUT / name, [rep[i] for i in order])
    for name in ('col13.valid.bitmap', 'col17.valid.bitmap'):
        rep = load_bits(SOURCE / name)[:65536] * SCALE
        write_bits(PREP_OUT / name, [rep[i] for i in order])
    (PREP_OUT / 'columns.tsv').write_text(f'17 32 0 5 {total}\n13 32 0 2 {total}\n',
                                          encoding='ascii')
    with open(PREP_OUT / 'source_rows.jsonl', 'wb') as f:
        for i in order:
            f.write((json.dumps(rows[i % 65536], ensure_ascii=False) + '\n').encode())
    scaled = [[state, None if value is None else str(int(value) * SCALE)]
              for state, value in oracle['rows']]
    (PREP_OUT / 'oracle.json').write_text(
        json.dumps({'columns': oracle['columns'], 'rows': scaled}) + '\n')
    meta = json.loads((SOURCE / 'case.json').read_text())
    meta.update({'rows': total, 'scale_factor': SCALE, 'synthetic_row_replication': True,
                 'row_order': 'shuffled', 'shuffle_seed': SEED, 'source_rows': 65536,
                 'source_case': str(SOURCE)})
    (PREP_OUT / 'case.json').write_text(json.dumps(meta, indent=2) + '\n')
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.ipc as ipc
    all_rows = [rows[i % 65536] for i in order]
    columns = {}
    for ci, (name, kind) in enumerate(zip(NAMES, TYPES)):
        values = [row[ci] for row in all_rows]
        if kind == 'string':
            columns[name] = pa.array(values, type=pa.string())
        else:
            columns[name] = pa.array([None if v is None else int(v) for v in values],
                                     type=pa.int64())
    table = pa.table(columns)
    with ipc.new_file(str(ARROW_OUT / 'full18.arrow'), table.schema) as sink:
        sink.write_table(table)
    proj = table.select(['State', 'POPESTIMATE2014'])
    with ipc.new_file(str(ARROW_OUT / 'projected.arrow'), proj.schema) as sink:
        sink.write_table(proj)
    filtered = table.filter(pc.not_equal(table.column('State'), 'District of Columbia'))
    grouped = filtered.group_by('State').aggregate([('POPESTIMATE2014', 'sum')])
    actual = sorted([[record['State'],
                      None if record['POPESTIMATE2014_sum'] is None
                      else str(record['POPESTIMATE2014_sum'])]
                     for record in grouped.to_pylist()], key=lambda r: r[0].encode())
    expected = {'columns': oracle['columns'], 'rows': scaled}
    assert {'columns': oracle['columns'], 'rows': actual} == expected
    summary = {'status': 'passed', 'case': 'Observed', 'scale_factor': SCALE, 'rows': total,
               'synthetic_row_replication': True, 'row_order': 'shuffled', 'shuffle_seed': SEED,
               'prepared_dir': str(PREP_OUT), 'arrow_dir': str(ARROW_OUT),
               'arrow_full_sha256': sha(ARROW_OUT / 'full18.arrow'),
               'arrow_full_bytes': (ARROW_OUT / 'full18.arrow').stat().st_size,
               'pyarrow': pa.__version__,
               'scope': 'synthetic 8x shuffled replication of the pinned Observed case'}
    (ROOT / 'results/ig2_scale8shuf_inputs_summary.json').write_text(
        json.dumps(summary, indent=2) + '\n')
    print(json.dumps({'status': 'passed', 'rows': total, 'prepared': str(PREP_OUT)}))


if __name__ == '__main__':
    main()
