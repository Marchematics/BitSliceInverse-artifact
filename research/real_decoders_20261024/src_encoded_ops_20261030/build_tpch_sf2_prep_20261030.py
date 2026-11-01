#!/usr/bin/env python3
"""Build a prepared 11,997,996-row TPC-H SF2 lineitem case (returnflag group, price)."""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import hashlib
import json
import struct
from pathlib import Path

import numpy as np
import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / 'results/tpch_sf2_arrow/lineitem.parquet'
OUT = ROOT / 'results/tpch_sf2_prep/Observed'
BINDING = hashlib.sha256(b'tpch-sf2-lineitem').hexdigest()[:40].encode()


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    table = pq.read_table(SRC)
    flags = table.column('l_returnflag').cast('string')
    price = pc.cast(table.column('l_extendedprice').cast('float64'), 'float64')
    cents = pc.cast(pc.round(pc.multiply(price, 100.0), ndigits=0), 'int64')
    flags_np = flags.to_numpy(zero_copy_only=False)
    cents_np = cents.to_numpy(zero_copy_only=False).astype(np.int64)
    names = sorted({str(x) for x in flags_np})
    codes = np.array([names.index(str(x)) for x in flags_np], dtype=np.uint32)
    n = len(codes)
    bias = np.uint32(0x80000000)
    (OUT / 'binding.txt').write_bytes(BINDING)
    (OUT / 'columns.tsv').write_text(f'17 32 0 5 {n}\n13 32 0 2 {n}\n', encoding='ascii')
    (codes + bias).astype(np.uint32).tofile(OUT / 'col17.u32')
    (codes + bias).astype(np.uint32).tofile(OUT / 'col17.biased.u32')
    (cents_np + (1 << 31)).astype(np.uint32).tofile(OUT / 'col13.u32')
    (cents_np + (1 << 31)).astype(np.uint32).tofile(OUT / 'col13.biased.u32')
    words = (n + 63) // 64
    valid = np.full(words, 0xFFFFFFFFFFFFFFFF, dtype=np.uint64)
    if n % 64:
        valid[-1] = (1 << (n % 64)) - 1
    valid.tofile(OUT / 'col17.valid.bitmap')
    valid.tofile(OUT / 'col13.valid.bitmap')
    d = bytearray(b'BSSTR01\n' + BINDING + struct.pack('<I', len(names)))
    for s in names:
        b = s.encode()
        d += struct.pack('<I', len(b)) + b
    (OUT / 'state.dict').write_bytes(d)
    oracle = {'columns': ['l_returnflag', 'sum_cents'], 'rows': []}
    for name in names:
        sel = codes == names.index(name)
        oracle['rows'].append([name, str(int(cents_np[sel].sum()))])
    (OUT / 'oracle.json').write_text(json.dumps(oracle) + '\n')
    (OUT / 'case.json').write_text(json.dumps({'rows': n, 'source': 'TPC-H SF2 lineitem',
                                               'group': 'l_returnflag', 'value': 'l_extendedprice cents'}) + '\n')
    print(json.dumps({'status': 'built', 'rows': n, 'groups': names, 'prep': str(OUT)}))


if __name__ == '__main__':
    main()
