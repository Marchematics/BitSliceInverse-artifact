#!/usr/bin/env python3
"""Build a synthetic 15x row-replicated IG/2 case for the scale-sensitivity test.

Reads the pinned prepared IG/2 `Observed` case (65,536 rows x 18 typed columns) and
the already-prepared Arrow full/projected files, replicates every row 16 times
(1,048,576 rows) and writes:

  results/ig2_scale15_prep/Observed/          prepared-format case + scaled oracle
  results/ig2_scale15_arrow/Observed_arrow/   Arrow IPC full-18 and projected-2

The replication is synthetic and is labelled as such: the query result is the
original result with every SUM multiplied by 15 (same groups, same order). No
sampling, filtering or value perturbation is performed.

Runs with the system Python that has pyarrow.
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parent))
import artifact_paths as _artifact
import argparse
import hashlib
import json
import random
import shutil
import struct
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SOURCE = ROOT / 'results/ig2_prep_20261025/prepared/Observed'
SCALE = 15
SEED = 20261025
IG2_SQL_SHA = '0bc95092f92bbb2f8652e66c10db8bec5fba88b6d96c5ac23e9650fe27ecca8b'
TYPES = ['int64', 'int64', 'int64', 'string', 'int64', 'int64', 'string', 'int64', 'int64',
         'int64', 'int64', 'int64', 'int64', 'int64', 'int64', 'int64', 'int64', 'string']
NAMES = ['CENSUS2010POP', 'CONCIT', 'COUSUB', 'City', 'County Code', 'ESTIMATESBASE2010',
         'FUNCSTAT', 'Number of Records', 'PLACE', 'POPESTIMATE2010', 'POPESTIMATE2011',
         'POPESTIMATE2012', 'POPESTIMATE2013', 'POPESTIMATE2014', 'PRIMGEO_FLAG', 'SUMLEV',
         'State code', 'State']


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
    parser.add_argument('--shuffle', action='store_true')
    parser.add_argument('--block-shuffle', action='store_true',
                        help='shuffle 64-row blocks instead of individual rows')
    args = parser.parse_args()
    tag = args.tag
    prep_out = ROOT / 'results' / (f'ig2_{tag}_prep') / 'Observed'
    arrow_out = ROOT / 'results' / (f'ig2_{tag}_arrow') / 'Observed_arrow'
    require(SOURCE.is_dir(), 'pinned prepared Observed case')
    require(sha(SOURCE / 'query.sql') == IG2_SQL_SHA, 'pinned IG/2 SQL')
    oracle = json.loads((SOURCE / 'oracle.json').read_text(encoding='utf-8'))
    rows = [json.loads(line) for line in
            (SOURCE / 'source_rows.jsonl').read_text(encoding='utf-8').split('\n') if line]
    require(len(rows) == 65536, 'source row count')
    n = len(rows) * SCALE
    if prep_out.exists():
        shutil.rmtree(prep_out)
    prep_out.mkdir(parents=True)
    arrow_out.mkdir(parents=True, exist_ok=True)

    # ---- prepared-format case ----
    shutil.copyfile(SOURCE / 'binding.txt', prep_out / 'binding.txt')
    shutil.copyfile(SOURCE / 'query.sql', prep_out / 'query.sql')
    shutil.copyfile(SOURCE / 'schema.sql', prep_out / 'schema.sql')
    shutil.copyfile(SOURCE / 'state.dict', prep_out / 'state.dict')
    indices = list(range(n))
    if args.shuffle:
        random.Random(SEED).shuffle(indices)
    elif args.block_shuffle:
        block = 64
        blocks = [list(range(start, min(n, start + block))) for start in range(0, n, block)]
        random.Random(SEED).shuffle(blocks)
        indices = [i for group in blocks for i in group]
    for name in ('col13.u32', 'col13.biased.u32', 'col17.u32', 'col17.biased.u32'):
        base = (SOURCE / name).read_bytes()
        values = list(struct.unpack('<' + 'I' * 65536, base))
        out = bytearray(4 * n)
        struct.pack_into('<' + 'I' * n, out, 0, *[values[i % 65536] for i in indices])
        with open(prep_out / name, 'wb') as f:
            f.write(out)
    for name in ('col13.valid.bitmap', 'col17.valid.bitmap'):
        data = (SOURCE / name).read_bytes()
        with open(prep_out / name, 'wb') as f:
            for _ in range(SCALE):
                f.write(data)
    (prep_out / 'columns.tsv').write_text(f'17 32 0 5 {n}\n13 32 0 2 {n}\n', encoding='ascii')
    all_rows = [rows[i % len(rows)] for i in indices]
    with open(prep_out / 'source_rows.jsonl', 'wb') as f:
        for row in all_rows:
            f.write((json.dumps(row, ensure_ascii=False) + '\n').encode('utf-8'))
    scaled_rows = []
    for state, total in oracle['rows']:
        scaled_rows.append([state, None if total is None else str(int(total) * SCALE)])
    scaled_oracle = {'columns': oracle['columns'], 'rows': scaled_rows}
    (prep_out / 'oracle.json').write_text(
        json.dumps(scaled_oracle, ensure_ascii=False) + '\n', encoding='utf-8')
    case_meta = json.loads((SOURCE / 'case.json').read_text(encoding='utf-8'))
    case_meta.update({'rows': n, 'scale_factor': SCALE, 'synthetic_row_replication': True,
                      'row_order': ('block_shuffled' if args.block_shuffle else 'shuffled') if (args.shuffle or args.block_shuffle) else 'repeated',
                      'shuffle_seed': SEED if (args.shuffle or args.block_shuffle) else None,
                      'block_shuffle': bool(args.block_shuffle),
                      'source_case': str(SOURCE), 'source_rows': len(rows),
                      'note': 'each original row repeated 15 times; sums scale exactly by 15'})
    (prep_out / 'case.json').write_text(json.dumps(case_meta, indent=2, ensure_ascii=False) + '\n',
                                        encoding='utf-8')

    # ---- Arrow full and projected files ----
    import pyarrow as pa
    import pyarrow.ipc as ipc
    if 'all_rows' not in locals():
        all_rows = [rows[i % len(rows)] for i in indices]
    columns = {}
    for ci, (name, kind) in enumerate(zip(NAMES, TYPES)):
        values = [row[ci] for row in all_rows]
        if kind == 'string':
            columns[name] = pa.array(values, type=pa.string())
        else:
            columns[name] = pa.array([None if v is None else int(v) for v in values], type=pa.int64())
    table = pa.table(columns)
    full_path = arrow_out / 'full18.arrow'
    with ipc.new_file(str(full_path), table.schema) as sink:
        sink.write_table(table)
    projected = table.select(['State', 'POPESTIMATE2014'])
    proj_path = arrow_out / 'projected.arrow'
    with ipc.new_file(str(proj_path), projected.schema) as sink:
        sink.write_table(projected)
    import pyarrow.compute as pc
    filtered = table.filter(pc.not_equal(table.column('State'), 'District of Columbia'))
    grouped = filtered.group_by('State').aggregate([('POPESTIMATE2014', 'sum')])
    actual = [[record['State'], None if record['POPESTIMATE2014_sum'] is None
               else str(record['POPESTIMATE2014_sum'])] for record in grouped.to_pylist()]
    actual.sort(key=lambda r: r[0].encode('utf-8'))
    require({'columns': oracle['columns'], 'rows': actual} == scaled_oracle,
            'scaled Arrow result equals scaled oracle')

    summary = {
        'status': 'passed', 'case': 'Observed', 'scale_factor': SCALE, 'rows': n,
        'synthetic_row_replication': True,
        'source_prepared': str(SOURCE), 'tag': tag, 'shuffle': bool(args.shuffle or args.block_shuffle), 'block_shuffle': bool(args.block_shuffle),
                      'shuffle_seed': SEED if (args.shuffle or args.block_shuffle) else None,
        'source_files_sha256': {name: sha(SOURCE / name) for name in
                                ('col13.u32', 'col17.u32', 'binding.txt', 'query.sql', 'oracle.json')},
        'prepared_dir': str(prep_out), 'prepared_oracle_sha256': sha(prep_out / 'oracle.json'),
        'arrow_dir': str(arrow_out),
        'arrow_full_sha256': sha(full_path), 'arrow_full_bytes': full_path.stat().st_size,
        'arrow_projected_sha256': sha(proj_path), 'arrow_projected_bytes': proj_path.stat().st_size,
        'pyarrow': pa.__version__,
        'scope': 'synthetic scale-sensitivity input; rows are exact 16x replication of the pinned '
                 'Observed case, not additional real data'}
    (ROOT / 'results' / f'ig2_{tag}_inputs_summary.json').write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    print(json.dumps({'status': 'passed', 'rows': n, 'prepared': str(prep_out),
                      'arrow_full_bytes': full_path.stat().st_size}))


if __name__ == '__main__':
    main()
