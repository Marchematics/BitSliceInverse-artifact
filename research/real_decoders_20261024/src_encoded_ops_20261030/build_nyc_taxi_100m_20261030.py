#!/usr/bin/env python3
"""Build a 100M-row real NYC taxi prepared case from 2023-01..2025-09 parquet."""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import glob
import hashlib
import json
import struct
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / 'results/nyc_taxi_100m_raw'
OUT = ROOT / 'results/nyc_taxi_100m_prep/Observed'
PARQUET = ROOT / 'results/nyc_taxi_100m_arrow/nyc100m.parquet'
BINDING = hashlib.sha256(b'nyc-taxi-100m').hexdigest()[:40].encode()
ROWS = 100_000_000


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    PARQUET.parent.mkdir(parents=True, exist_ok=True)
    pc_parts, valid_parts, fare_parts = [], [], []
    for path in sorted(glob.glob(str(SRC / '*.parquet'))):
        t = pq.read_table(path, columns=['passenger_count', 'fare_amount'])
        p = t.column('passenger_count')
        nullmask = pc.is_null(p).to_numpy(zero_copy_only=False)
        arr = p.cast(pa.float64()).to_numpy(zero_copy_only=False)
        v = (~nullmask) & (~np.isnan(arr))
        arr = np.nan_to_num(arr, nan=0.0).astype(np.int64)
        fare = t.column('fare_amount').cast(pa.float64()).to_numpy(zero_copy_only=False)
        pc_parts.append(arr); valid_parts.append(v); fare_parts.append(fare)
        if sum(len(x) for x in pc_parts) >= ROWS:
            break
    pc_int = np.concatenate(pc_parts)[:ROWS]
    valid = np.concatenate(valid_parts)[:ROWS]
    fare = np.concatenate(fare_parts)[:ROWS]
    n = len(pc_int)
    cents_np = np.trunc(fare * 100.0).astype(np.int64)
    values = sorted({int(v) for v in pc_int[valid]})
    index = {v: i for i, v in enumerate(values)}
    codes = np.array([index.get(int(v), 0) for v in pc_int], dtype=np.uint32)
    bias = np.uint32(0x80000000)
    (OUT / 'binding.txt').write_bytes(BINDING)
    (OUT / 'columns.tsv').write_text(f'17 32 1 5 {n}\n13 32 0 2 {n}\n', encoding='ascii')
    group_raw = (codes + bias).astype(np.uint32)
    group_raw[~valid] = 0
    group_raw.tofile(OUT / 'col17.u32'); group_raw.tofile(OUT / 'col17.biased.u32')
    (cents_np + (1 << 31)).astype(np.uint32).tofile(OUT / 'col13.u32')
    (cents_np + (1 << 31)).astype(np.uint32).tofile(OUT / 'col13.biased.u32')
    words = (n + 63) // 64
    gvalid = np.full(words, 0xFFFFFFFFFFFFFFFF, dtype=np.uint64)
    bits = valid.astype(np.uint64)
    for w in range(words):
        chunk = bits[w * 64:(w + 1) * 64]
        word = 0
        for k, b in enumerate(chunk):
            if b: word |= (1 << k)
        gvalid[w] = word
    if n % 64:
        gvalid[-1] &= (1 << (n % 64)) - 1
    gvalid.tofile(OUT / 'col17.valid.bitmap')
    np.full(words, 0xFFFFFFFFFFFFFFFF, dtype=np.uint64).tofile(OUT / 'col13.valid.bitmap')
    d = bytearray(b'BSSTR01\n' + BINDING + struct.pack('<I', len(values)))
    for v in values:
        b = str(v).encode()
        d += struct.pack('<I', len(b)) + b
    (OUT / 'state.dict').write_bytes(d)
    out_table = pa.table({'passenger_count': pa.array(pc_int, type=pa.int64(), mask=~valid),
                          'fare_cents': pa.array(cents_np, type=pa.int64())})
    pq.write_table(out_table, PARQUET, compression='snappy')
    (OUT / 'case.json').write_text(json.dumps({'rows': n, 'source': 'NYC yellow taxi 2023-01..2025-09',
                                               'group': 'passenger_count', 'value': 'fare_amount cents'}) + '\n')
    print(json.dumps({'status': 'built', 'rows': n, 'groups': values, 'nulls': int((~valid).sum()),
                      'prep': str(OUT), 'parquet': str(PARQUET)}))


if __name__ == '__main__':
    main()
