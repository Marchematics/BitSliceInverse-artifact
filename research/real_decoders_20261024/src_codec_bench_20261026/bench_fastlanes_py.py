#!/usr/bin/env python3
"""FastLanes (PVLDB 2025) kernel/file-format driver using the official pyfastlanes wheel.

Runs under an environment with `pyfastlanes` installed. For each panel int32 column it
writes a single-column CSV + schema.json, encodes it to a .fls file, decodes it back to
CSV, validates the decoded values, and reports bytes/value plus encode and decode wall
times over repetitions.

API limitation: the official Python binding exposes `read_csv -> to_fls` and
`read_fls -> to_csv`; the decode interval therefore includes CSV materialization (the
native interval includes typed-result serialization instead). This is a format/kernel
comparison, not a SQL query comparison.
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import json
import os
import statistics
import struct
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parent
IG2 = REPO / 'results/ig2_prep_20261025/prepared/Observed'
IG16 = REPO / 'results/ig16_prep_v1/Observed'
OUT = REPO / 'results' / 'codec_bench_20261026'
SCHEMA_TYPES = ['integer', 'int32', 'int', 'bigint', 'int64']


def read_u32(path):
    data = Path(path).read_bytes()
    return list(struct.unpack('<' + 'I' * (len(data) // 4), data))


def export_columns():
    OUT.mkdir(parents=True, exist_ok=True)
    inputs = OUT / 'inputs'
    inputs.mkdir(exist_ok=True)
    columns = {
        'popest2014': [int(v) - 0x80000000 for v in read_u32(IG2 / 'col13.u32')],
        'state_code': [int(v) - 0x80000000 for v in read_u32(IG2 / 'col17.u32')],
        'city_code': [int(v) - 0x80000000 for v in read_u32(IG16 / 'col3.u32')],
    }
    result = {}
    for name, values in columns.items():
        binary = inputs / f'{name}.i32'
        binary.write_bytes(struct.pack('<' + 'i' * len(values), *values))
        result[name] = {'values': len(values), 'binary': str(binary)}
    return result


def make_csv_dir(directory, values, type_name):
    directory.mkdir(parents=True, exist_ok=True)
    (directory / 'data.csv').write_text(''.join(f'{v}\n' for v in values), encoding='ascii')
    (directory / 'schema.json').write_text(json.dumps(
        {'columns': [{'name': 'col', 'type': type_name, 'nullability': 'NOT NULL'}]}) + '\n',
        encoding='utf-8')


def read_csv_values(path):
    values = []
    with open(path, encoding='ascii') as f:
        for line in f:
            field = line.strip().split('|')[0]
            if field == '':
                continue
            values.append(int(field))
    return values


def main():
    reps = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    import pyfastlanes
    columns = export_columns()
    summary = {'status': 'failed', 'codec': 'fastlanes_py', 'kind': 'kernel',
               'pyfastlanes_version': getattr(pyfastlanes, '__version__', 'unknown'),
               'reps': reps, 'runs': [],
               'notes': 'official pyfastlanes binding; encode is to_fls after CSV parse, '
                        'decode is read_fls + to_csv, so the decode interval includes CSV '
                        'materialization; format/kernel comparison only'}
    try:
        for name, info in columns.items():
            binary = Path(info['binary'])
            data = binary.read_bytes()
            values = list(struct.unpack('<' + 'i' * (len(data) // 4), data))
            work = OUT / 'fastlanes_py'
            work.mkdir(exist_ok=True)
            selected = None
            for type_name in SCHEMA_TYPES:
                csv_dir = work / f'csv_{type_name.replace("(", "_").replace(")", "")}'
                make_csv_dir(csv_dir, values, type_name)
                fls_path = work / f'{name}.fls'
                try:
                    conn = pyfastlanes.connect()
                    conn.inline_footer().read_csv(str(csv_dir)).to_fls(str(fls_path))
                    selected = (type_name, csv_dir, fls_path)
                    break
                except Exception:
                    continue
            if selected is None:
                summary['runs'].append({'codec': 'fastlanes_py', 'column': name,
                                        'result': {'error': 'no accepted schema type'}})
                continue
            type_name, csv_dir, fls_path = selected
            encode_ms, decode_ms, sizes = [], [], []
            for _ in range(reps):
                conn = pyfastlanes.connect()
                conn.inline_footer().read_csv(str(csv_dir))
                t0 = time.perf_counter()
                conn.to_fls(str(fls_path))
                t1 = time.perf_counter()
                reader = pyfastlanes.connect().read_fls(str(fls_path))
                decoded = work / f'{name}.decoded.csv'
                reader.to_csv(str(decoded))
                t2 = time.perf_counter()
                encode_ms.append((t1 - t0) * 1000.0)
                decode_ms.append((t2 - t1) * 1000.0)
                sizes.append(fls_path.stat().st_size)
            decoded_values = read_csv_values(decoded)
            exact = decoded_values == values
            enc = statistics.median(encode_ms)
            dec = statistics.median(decode_ms)
            n = len(values)
            summary['runs'].append({'codec': 'fastlanes_py', 'column': name,
                                    'result': {'bytes_per_value': sizes[-1] / n,
                                               'encode_ns_per_value': enc * 1e6 / n,
                                               'decode_ns_per_value': dec * 1e6 / n,
                                               'encode_median_ms': enc,
                                               'decode_median_ms': dec,
                                               'decoded_exact': exact,
                                               'schema_type': type_name,
                                               'values': n,
                                               'fls_bytes': sizes[-1]}})
        summary['status'] = 'passed'
    except Exception as exc:  # noqa: BLE001
        summary['error'] = type(exc).__name__ + ': ' + str(exc)
    (OUT / 'fastlanes_py_summary.json').write_text(json.dumps(summary, indent=2) + '\n',
                                                   encoding='utf-8')
    print(json.dumps({'status': summary['status'], 'runs': len(summary['runs']),
                      'error': summary.get('error')}))
    if summary['status'] != 'passed':
        raise SystemExit(1)


if __name__ == '__main__':
    main()
