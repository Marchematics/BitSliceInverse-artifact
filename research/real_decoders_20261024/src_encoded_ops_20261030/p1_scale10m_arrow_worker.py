#!/usr/bin/env python3
"""Arrow IPC baseline for the 10M P1 scale check (read + filter + group-by + output)."""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import json
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.ipc as ipc


def main():
    arrow_path, op, out_path = sys.argv[1], sys.argv[2], sys.argv[3]
    t0 = time.perf_counter()
    with open(arrow_path, 'rb') as f:
        table = ipc.open_file(f).read_all()
    t1 = time.perf_counter()
    table = table.filter(pc.not_equal(table['State'], 'District of Columbia'))
    agg = {'sum': 'sum', 'count': 'count', 'min': 'min', 'max': 'max'}[op]
    if op == 'count':
        grouped = table.group_by('State').aggregate([('POPESTIMATE2014', 'count')])
        value_col = grouped.column_names[1]
    else:
        grouped = table.group_by('State').aggregate([('POPESTIMATE2014', agg)])
        value_col = grouped.column_names[1]
    states = grouped.column('State').to_pylist()
    values = grouped.column(value_col).to_pylist()
    rows = sorted(([str(s), str(v)] for s, v in zip(states, values)), key=lambda r: r[0].encode())
    payload = json.dumps({'rows': rows})
    Path(out_path).write_text(payload + '\n')
    t2 = time.perf_counter()
    Path(out_path + '.metrics.json').write_text(json.dumps({
        'codec': 'arrow_ipc', 'op': op, 'read_ms': (t1 - t0) * 1000,
        'execute_ms': (t2 - t1) * 1000, 'stored_ms': (t2 - t0) * 1000,
        'rows': len(rows)}) + '\n')
    print(out_path)


if __name__ == '__main__':
    main()
