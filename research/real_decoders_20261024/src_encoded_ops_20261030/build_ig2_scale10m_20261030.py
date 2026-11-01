#!/usr/bin/env python3
"""Build a 10M-row synthetic IG/2 prepared case + projected Parquet control."""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import json
import shutil
import struct
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
SOURCE = ROOT / 'results/ig2_prep_20261025/prepared/Observed'
OUT = ROOT / 'results/ig2_scale10m_prep/Observed'
PARQUET = ROOT / 'results/ig2_scale10m_arrow/projected.parquet'
SCALE = 153
n = 65536 * SCALE


def load_dict(path):
    data = path.read_bytes()
    assert data[:8] == b'BSSTR01\n'
    p = 48
    (k,) = struct.unpack_from('<I', data, p); p += 4
    out = []
    for _ in range(k):
        (ln,) = struct.unpack_from('<I', data, p); p += 4
        out.append(data[p:p + ln].decode()); p += ln
    return out


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    PARQUET.parent.mkdir(parents=True, exist_ok=True)
    for name in ('binding.txt', 'query.sql', 'schema.sql', 'state.dict'):
        shutil.copyfile(SOURCE / name, OUT / name)
    for name in ('col13.u32', 'col13.biased.u32', 'col17.u32', 'col17.biased.u32'):
        base = np.fromfile(SOURCE / name, dtype=np.uint32)
        assert base.size == 65536
        np.tile(base, SCALE).tofile(OUT / name)
    for name in ('col13.valid.bitmap', 'col17.valid.bitmap'):
        data = (SOURCE / name).read_bytes()
        with open(OUT / name, 'wb') as f:
            for _ in range(SCALE): f.write(data)
    (OUT / 'columns.tsv').write_text(f'17 32 0 5 {n}\n13 32 0 2 {n}\n', encoding='ascii')
    oracle = json.loads((SOURCE / 'oracle.json').read_text())
    rows = [[state, None if total is None else str(int(total) * SCALE)] for state, total in oracle['rows']]
    (OUT / 'oracle.json').write_text(json.dumps({'columns': oracle['columns'], 'rows': rows}) + '\n')
    case = json.loads((SOURCE / 'case.json').read_text())
    case.update({'rows': n, 'scale_factor': SCALE, 'synthetic_row_replication': True,
                 'row_order': 'repeated', 'shuffle_seed': None, 'block_shuffle': False,
                 'source_case': str(SOURCE), 'source_rows': 65536,
                 'note': 'each original row repeated 153 times; sums scale exactly by 153'})
    (OUT / 'case.json').write_text(json.dumps(case, indent=2) + '\n')
    # projected Parquet control: decoded State names + POPESTIMATE2014
    dict_names = load_dict(SOURCE / 'state.dict')
    state_codes = np.fromfile(SOURCE / 'col17.u32', dtype=np.uint32) - np.uint32(0x80000000)
    pop = np.fromfile(SOURCE / 'col13.u32', dtype=np.uint32).astype(np.int64) - (1 << 31)
    state_codes = np.tile(state_codes, SCALE)
    pop = np.tile(pop, SCALE)
    names = [dict_names[int(c)] for c in state_codes]
    table = pa.table({'State': pa.array(names, type=pa.string()),
                      'POPESTIMATE2014': pa.array(pop.astype(np.int32), type=pa.int32())})
    pq.write_table(table, PARQUET, compression='snappy')
    print(json.dumps({'status': 'built', 'rows': n, 'prep': str(OUT), 'parquet': str(PARQUET)}))


if __name__ == '__main__':
    main()
