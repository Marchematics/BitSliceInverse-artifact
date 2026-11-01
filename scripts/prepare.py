#!/usr/bin/env python3
"""Download public inputs and prepare encoded columns and SQL controls."""
import argparse
import csv
import hashlib
import json
from pathlib import Path
import struct
import subprocess
import urllib.request

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / 'research/real_decoders_20261024'
RESULTS = SRC / 'results'
WORK = ROOT / 'work'
DATA_REV = 'e5fab9e21e0037a6a3f54633c73a3524d08fe8f3'


def download(url, path, sha256=None):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        tmp = path.with_suffix(path.suffix + '.part')
        with urllib.request.urlopen(url, timeout=120) as inp, tmp.open('wb') as out:
            while data := inp.read(1 << 20):
                out.write(data)
        tmp.replace(path)
    if sha256 and hashlib.sha256(path.read_bytes()).hexdigest() != sha256:
        raise ValueError(f'Checksum mismatch: {path.name}')
    return path


def bitmap(valid):
    out = np.zeros(((len(valid) + 63) // 64) * 8, dtype=np.uint8)
    packed = np.packbits(np.asarray(valid, dtype=np.uint8), bitorder='little')
    out[:len(packed)] = packed
    return out.tobytes()


def write_prep(folder, groups, values, names, binding, group_nullable=True, value_valid=None):
    folder.mkdir(parents=True, exist_ok=True)
    groups = np.asarray(groups, dtype='<i4')
    values = np.asarray(values, dtype='<i4')
    n = len(groups)
    assert n == len(values)
    vg = groups >= 0
    vv = np.ones(n, dtype=bool) if value_valid is None else np.asarray(value_valid, dtype=bool)
    (folder / 'binding.txt').write_text(binding)
    (folder / 'columns.tsv').write_text(
        f'17 32 {int(group_nullable)} 5 {n}\n13 32 {int(not vv.all())} 2 {n}\n')
    data = bytearray(b'BSSTR01\n' + binding.encode() + struct.pack('<I', len(names)))
    for name in names:
        b = str(name).encode()
        data += struct.pack('<I', len(b)) + b
    (folder / 'state.dict').write_bytes(data)
    for col, arr, valid in [(17, groups, vg), (13, values, vv)]:
        biased = (arr.astype(np.int64) + 2**31).astype('<u4')
        biased[~valid] = 0
        biased.tofile(folder / f'col{col}.u32')
        biased.tofile(folder / f'col{col}.biased.u32')
        (folder / f'col{col}.valid.bitmap').write_bytes(bitmap(valid))
    groups.tofile(folder / 'g.i32')
    values.tofile(folder / 'v.i32')
    (folder / 'v.valid').write_bytes(bitmap(vv))
    if n == 0:
        (folder / 'empty.txt').touch()


def encode(folder, output):
    subprocess.run([ROOT / 'build/encode', 'build_round3', folder, output], check=True)


def database(parquet, path, table):
    import duckdb
    path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(path)) as con:
        con.execute('SET threads=1')
        con.execute(f'CREATE OR REPLACE TABLE {table} AS SELECT * FROM read_parquet(?)', [str(parquet)])


def ig2(replicas):
    raw = download(
        f'https://raw.githubusercontent.com/azimafroozeh/FastLanes_Data/{DATA_REV}/'
        'public_bi/tables/IGlocations1/IGlocations1_1/IGlocations1_1.csv',
        WORK / 'raw/IGlocations1_1.csv',
        '186979a35563c6b6a7e88b21b5385e4ed70d131b852770d0c2423c2a76c2a447')
    rows = list(csv.reader(raw.open(), delimiter='|', quoting=csv.QUOTE_NONE))
    assert len(rows) == 65536
    states = [r[17] for r in rows]
    names = list(dict.fromkeys(states))
    lookup = {n: i for i, n in enumerate(names)}
    g = np.tile(np.asarray([lookup[s] for s in states], dtype='<i4'), replicas)
    v = np.tile(np.asarray([int(r[13]) for r in rows], dtype='<i4'), replicas)
    key = {1: 'ig2_prep_20261025', 15: 'ig2_scale15_prep', 153: 'ig2_scale10m_prep'}.get(
        replicas, f'ig2_rep{replicas}_prep')
    base = RESULTS / key
    prep = base / ('prepared/Observed' if replicas == 1 else 'Observed')
    write_prep(prep, g, v, names, 'ee275641f7c34fcb258559be100f83cf088b927c', False)
    encode(prep, base / 'Observed.encoded')
    out = WORK / 'tiers/build'
    out.mkdir(parents=True, exist_ok=True)
    tag = {1: '65k', 15: '983k', 153: '10m'}.get(replicas, str(len(g)))
    pq.write_table(pa.table({'state': np.tile(states, replicas), 'pop': v}),
                   out / f'ig2_{tag}.parquet', compression='snappy')
    print(json.dumps({'dataset': 'ig2', 'rows': len(g), 'replicas': replicas}))


def nyc(rows):
    paths = []
    n = 0
    raw = RESULTS / ('nyc_taxi_100m_raw' if rows == 100_000_000 else 'nyc_taxi_2023')
    for year in range(2023, 2026):
        for month in range(1, 13):
            if (year, month) > (2025, 9):
                break
            name = f'yellow_tripdata_{year}-{month:02d}.parquet'
            p = download(f'https://d37ci6vzurychx.cloudfront.net/trip-data/{name}', raw / name)
            paths.append(p)
            n += pq.ParquetFile(p).metadata.num_rows
            if n >= rows:
                break
        if n >= rows:
            break
    assert n >= rows
    parts = []
    left = rows
    for p in paths:
        t = pq.read_table(p, columns=['passenger_count', 'fare_amount']).slice(0, left)
        parts.append(t)
        left -= t.num_rows
        if not left:
            break
    t = pa.concat_tables(parts)
    passenger = t['passenger_count'].cast('float64').to_numpy(zero_copy_only=False)
    valid = ~np.isnan(passenger)
    pg = np.nan_to_num(passenger, nan=0).astype(np.int64)
    fare = t['fare_amount'].cast('float64').to_numpy(zero_copy_only=False)
    cents = np.trunc(fare * 100).astype('<i4')
    names = sorted(set(int(x) for x in pg[valid]))
    g = np.searchsorted(names, pg).astype('<i4')
    g[~valid] = -1
    key = 'nyc_taxi_100m' if rows == 100_000_000 else 'nyc_taxi_2023'
    prep = RESULTS / f'{key}_prep/Observed'
    binding = hashlib.sha256(b'nyc-taxi-100m' if rows == 100_000_000 else b'nyc-taxi-2023-12m').hexdigest()[:40]
    write_prep(prep, g, cents, [str(x) for x in names], binding)
    encode(prep, prep.parent / 'Observed.encoded')
    tag = 'nyc100m' if rows == 100_000_000 else 'nyc12m'
    parq = RESULTS / f'{key}_arrow/{tag}.parquet'
    parq.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table({'passenger_count': pa.array(pg, mask=~valid), 'fare_cents': cents.astype('int64')})
    pq.write_table(table, parq, compression='snappy')
    with pa.OSFile(str(parq.with_suffix('.arrow')), 'wb') as f:
        with pa.ipc.new_file(f, table.schema) as writer:
            writer.write_table(table)
    database(parq, RESULTS / f'{key}_duckdb/nyc.duckdb', 'nyc')
    if rows == 100_000_000:
        plain = RESULTS / 'nyc100_plain'
        plain.mkdir(exist_ok=True)
        pg.astype('<i4')[...].tofile(plain / 'passenger.i32')
        # Plain cube group keys use -1 for NULL, original passenger values otherwise.
        plain_g = pg.astype('<i4')
        plain_g[~valid] = -1
        plain_g.tofile(plain / 'passenger.i32')
        cents.tofile(plain / 'fare.i32')
        tiers = WORK / 'tiers/build'
        tiers.mkdir(parents=True, exist_ok=True)
        cents.tofile(tiers / 'val_nyc100.i32')
        plain_g.tofile(tiers / 'gid_nyc_g10.i32')
    print(json.dumps({'dataset': 'nyc', 'rows': rows}))


def tpch(sf):
    import duckdb
    db = RESULTS / 'tpch_sf2_lineitem_duckdb/lineitem.duckdb'
    db.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db)) as con:
        con.execute('SET threads=1')
        con.execute('INSTALL tpch; LOAD tpch')
        if not con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name='lineitem'").fetchone()[0]:
            con.execute(f'CALL dbgen(sf={sf})')
        parq = RESULTS / 'tpch_sf2_arrow/lineitem.parquet'
        parq.parent.mkdir(parents=True, exist_ok=True)
        con.execute("COPY (SELECT l_returnflag, l_extendedprice FROM lineitem) TO ? (FORMAT PARQUET, COMPRESSION SNAPPY)", [str(parq)])
        t = con.execute('SELECT * FROM lineitem ORDER BY rowid').fetch_arrow_table()
    flags = np.asarray(t['l_returnflag'].to_pylist())
    names = sorted(set(flags))
    g = np.searchsorted(names, flags).astype('<i4')
    cents = pc.cast(pc.round(pc.multiply(t['l_extendedprice'].cast('float64'), 100.), ndigits=0), 'int64').to_numpy(zero_copy_only=False).astype('<i4')
    prep = RESULTS / 'tpch_sf2_prep/Observed'
    write_prep(prep, g, cents, names, hashlib.sha256(b'tpch-sf2-lineitem').hexdigest()[:40], False)
    encode(prep, prep.parent / 'Observed.encoded')
    out = RESULTS / 'multibits_tpch'
    out.mkdir(exist_ok=True)
    arrays = {'g': g, 'p': cents,
              'd': np.rint(t['l_discount'].cast('float64').to_numpy(zero_copy_only=False) * 100),
              'q': t['l_quantity'].cast('int64').to_numpy(zero_copy_only=False),
              't': np.rint(t['l_tax'].cast('float64').to_numpy(zero_copy_only=False) * 100)}
    for k, a in arrays.items():
        np.asarray(a, dtype='<i8').tofile(out / f'{k}.i64')
    print(json.dumps({'dataset': 'tpch', 'sf': sf, 'rows': len(g)}))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('dataset', choices=['ig2', 'nyc', 'tpch'])
    ap.add_argument('--replicas', type=int, default=1)
    ap.add_argument('--rows', type=int, default=100_000_000)
    ap.add_argument('--sf', type=float, default=2)
    args = ap.parse_args()
    RESULTS.mkdir(parents=True, exist_ok=True)
    WORK.mkdir(exist_ok=True)
    (WORK / 'runs').mkdir(exist_ok=True)
    (WORK / 'data').mkdir(exist_ok=True)
    if not (WORK / 'data/results').exists():
        (WORK / 'data/results').symlink_to('../../research/real_decoders_20261024/results', target_is_directory=True)
    if not (WORK / 'repo').exists():
        (WORK / 'repo').symlink_to('..', target_is_directory=True)
    if args.dataset == 'ig2': ig2(args.replicas)
    elif args.dataset == 'nyc': nyc(args.rows)
    else: tpch(args.sf)


if __name__ == '__main__':
    main()
