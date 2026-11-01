#!/usr/bin/env python3
"""Build a synthetic 16x row-replicated IG/2 case for the scale-sensitivity test.

Reads the pinned prepared IG/2 `Observed` case (65,536 rows x 18 typed columns) and
the already-prepared Arrow full/projected files, replicates every row 16 times
(1,048,576 rows) and writes:

  results/ig2_scale8_prep/Observed/          prepared-format case + scaled oracle
  results/ig2_scale8_arrow/Observed_arrow/   Arrow IPC full-18 and projected-2

The replication is synthetic and is labelled as such: the query result is the
original result with every SUM multiplied by 16 (same groups, same order). No
sampling, filtering or value perturbation is performed.

Runs with the system Python that has pyarrow.
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parent))
import artifact_paths as _artifact
import hashlib
import json
import shutil
import struct
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SOURCE = ROOT / 'results/ig2_prep_20261025/prepared/Observed'
SCALE = 8
PREP_OUT = ROOT / 'results/ig2_scale8_prep/Observed'
ARROW_OUT = ROOT / 'results/ig2_scale8_arrow/Observed_arrow'
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
    require(SOURCE.is_dir(), 'pinned prepared Observed case')
    require(sha(SOURCE / 'query.sql') == IG2_SQL_SHA, 'pinned IG/2 SQL')
    oracle = json.loads((SOURCE / 'oracle.json').read_text(encoding='utf-8'))
    rows = [json.loads(line) for line in
            (SOURCE / 'source_rows.jsonl').read_text(encoding='utf-8').split('\n') if line]
    require(len(rows) == 65536, 'source row count')
    n = len(rows) * SCALE
    if PREP_OUT.exists():
        shutil.rmtree(PREP_OUT)
    PREP_OUT.mkdir(parents=True)
    ARROW_OUT.mkdir(parents=True, exist_ok=True)

    # ---- prepared-format case ----
    shutil.copyfile(SOURCE / 'binding.txt', PREP_OUT / 'binding.txt')
    shutil.copyfile(SOURCE / 'query.sql', PREP_OUT / 'query.sql')
    shutil.copyfile(SOURCE / 'schema.sql', PREP_OUT / 'schema.sql')
    shutil.copyfile(SOURCE / 'state.dict', PREP_OUT / 'state.dict')
    for name in ('col13.u32', 'col13.biased.u32', 'col17.u32', 'col17.biased.u32',
                 'col13.valid.bitmap', 'col17.valid.bitmap'):
        data = (SOURCE / name).read_bytes()
        with open(PREP_OUT / name, 'wb') as f:
            for _ in range(SCALE):
                f.write(data)
    (PREP_OUT / 'columns.tsv').write_text(f'17 32 0 5 {n}\n13 32 0 2 {n}\n', encoding='ascii')
    with open(PREP_OUT / 'source_rows.jsonl', 'wb') as f:
        for _ in range(SCALE):
            for row in rows:
                f.write((json.dumps(row, ensure_ascii=False) + '\n').encode('utf-8'))
    scaled_rows = []
    for state, total in oracle['rows']:
        scaled_rows.append([state, None if total is None else str(int(total) * SCALE)])
    scaled_oracle = {'columns': oracle['columns'], 'rows': scaled_rows}
    (PREP_OUT / 'oracle.json').write_text(
        json.dumps(scaled_oracle, ensure_ascii=False) + '\n', encoding='utf-8')
    case_meta = json.loads((SOURCE / 'case.json').read_text(encoding='utf-8'))
    case_meta.update({'rows': n, 'scale_factor': SCALE, 'synthetic_row_replication': True,
                      'source_case': str(SOURCE), 'source_rows': len(rows),
                      'note': 'each original row repeated 16 times; sums scale exactly by 16'})
    (PREP_OUT / 'case.json').write_text(json.dumps(case_meta, indent=2, ensure_ascii=False) + '\n',
                                        encoding='utf-8')

    # ---- Arrow full and projected files ----
    import pyarrow as pa
    import pyarrow.ipc as ipc
    all_rows = rows * SCALE
    columns = {}
    for ci, (name, kind) in enumerate(zip(NAMES, TYPES)):
        values = [row[ci] for row in all_rows]
        if kind == 'string':
            columns[name] = pa.array(values, type=pa.string())
        else:
            columns[name] = pa.array([None if v is None else int(v) for v in values], type=pa.int64())
    table = pa.table(columns)
    full_path = ARROW_OUT / 'full18.arrow'
    with ipc.new_file(str(full_path), table.schema) as sink:
        sink.write_table(table)
    projected = table.select(['State', 'POPESTIMATE2014'])
    proj_path = ARROW_OUT / 'projected.arrow'
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
        'source_prepared': str(SOURCE),
        'source_files_sha256': {name: sha(SOURCE / name) for name in
                                ('col13.u32', 'col17.u32', 'binding.txt', 'query.sql', 'oracle.json')},
        'prepared_dir': str(PREP_OUT), 'prepared_oracle_sha256': sha(PREP_OUT / 'oracle.json'),
        'arrow_dir': str(ARROW_OUT),
        'arrow_full_sha256': sha(full_path), 'arrow_full_bytes': full_path.stat().st_size,
        'arrow_projected_sha256': sha(proj_path), 'arrow_projected_bytes': proj_path.stat().st_size,
        'pyarrow': pa.__version__,
        'scope': 'synthetic scale-sensitivity input; rows are exact 16x replication of the pinned '
                 'Observed case, not additional real data'}
    (ROOT / 'results/ig2_scale8_inputs_summary.json').write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    print(json.dumps({'status': 'passed', 'rows': n, 'prepared': str(PREP_OUT),
                      'arrow_full_bytes': full_path.stat().st_size}))


if __name__ == '__main__':
    main()
