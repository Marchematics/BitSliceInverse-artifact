#!/usr/bin/env python3
"""Prepare the IG/16 (splitpart/trim group-by) case for a native exact bridge.

Reads the pinned IGlocations1_1 CSV, computes the original query's result with the
reference dialect semantics (splitpart(City, ' ', 1) then trim over the literal
character set that MonetDB/DuckDB applied for the escaped trim argument), builds the
projected prepared case for column 3 (City, dictionary family 5) and writes the
oracle. This is a projected one-column case; no other column is needed.
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parent))
import artifact_paths as _artifact
import hashlib
import json
import struct
from pathlib import Path

ROOT = Path('../../work/bsi_e2e_20261025/repo/research/real_decoders_20261024')
CSV = Path('../../work/bsi_e2e_20261025/data_sources/IGlocations1/IGlocations1_1.csv')
SQLFILE = Path('../../work/bsi_e2e_20261025/data_sources/IGlocations1/benchmark__IGlocations1__queries__16.sql')
IG2_PREP = ROOT / 'results/ig2_prep_20261025/prepared/Observed'
OUT = ROOT / 'results/ig16_prep_v1/Observed'
TRIM_CHARS = ' \\tx0bfr'


def sha(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for b in iter(lambda: f.read(1 << 20), b''):
            h.update(b)
    return h.hexdigest()


def splitpart(city):
    inside = city.split(' ', 1)[0]
    return inside.strip(TRIM_CHARS)


def main():
    rows = []
    with open(CSV, encoding='utf-8', errors='strict') as f:
        for line in f:
            rows.append(line.rstrip('\n').split('|'))
    cities = [row[3] for row in rows]
    n = len(cities)
    assert n == 65536, n
    binding = (IG2_PREP / 'binding.txt').read_text(encoding='ascii').strip()
    dictionary = sorted(set(cities), key=lambda s: s.encode('utf-8'))
    codes = {value: index for index, value in enumerate(dictionary)}
    if OUT.exists():
        import shutil
        shutil.rmtree(OUT)
    OUT.mkdir(parents=True)
    (OUT / 'binding.txt').write_text(binding + '\n', encoding='ascii')
    (OUT / 'query.sql').write_bytes(SQLFILE.read_bytes())
    (OUT / 'columns.tsv').write_text(f'3 32 0 5 {n}\n', encoding='ascii')
    with open(OUT / 'city.dict', 'wb') as f:
        f.write(b'BSSTR01\n')
        f.write(binding.encode('ascii').ljust(40, b'\0')[:40])
        f.write(struct.pack('<I', len(dictionary)))
        for value in dictionary:
            data = value.encode('utf-8')
            f.write(struct.pack('<I', len(data)))
            f.write(data)
    biased = [codes[value] + 0x80000000 for value in cities]
    payload = struct.pack('<' + 'I' * n, *biased)
    (OUT / 'col3.u32').write_bytes(payload)
    (OUT / 'col3.biased.u32').write_bytes(payload)
    (OUT / 'col3.valid.bitmap').write_bytes(b'\xff' * ((n + 7) // 8))
    tokens = sorted({splitpart(city) for city in cities}, key=lambda s: s.encode('utf-8'))
    oracle = {'columns': [{'name': 'City - Split 1', 'type': 'VARCHAR'}],
              'rows': [[token] for token in tokens]}
    (OUT / 'oracle.json').write_text(json.dumps(oracle, ensure_ascii=False) + '\n', encoding='utf-8')
    src = ROOT / 'src_fullsql_ig16_direct'
    src.mkdir(exist_ok=True)
    (src / 'ig16_sql.inc').write_text(json.dumps(SQLFILE.read_text(encoding='utf-8')) + '\n',
                                      encoding='ascii')
    case = {'case': 'Observed', 'original_query': True, 'rows': n,
            'projected_columns': [3], 'binding': binding,
            'source_csv': str(CSV), 'source_csv_sha256': sha(CSV),
            'original_sql_sha256': sha(SQLFILE),
            'trim_character_set': TRIM_CHARS,
            'note': 'one-column projected case for the splitpart/trim group-by query; '
                    'result values are the distinct trimmed first tokens ordered by bytes'}
    (OUT / 'case.json').write_text(json.dumps(case, indent=2, ensure_ascii=False) + '\n',
                                   encoding='utf-8')
    print(json.dumps({'status': 'prepared', 'rows': n, 'dictionary': len(dictionary),
                      'tokens': len(tokens), 'prep': str(OUT)}))


if __name__ == '__main__':
    main()
