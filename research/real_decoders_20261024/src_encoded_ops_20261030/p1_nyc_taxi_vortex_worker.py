#!/usr/bin/env python3

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import json, sys, time
from pathlib import Path
import pyarrow as pa, pyarrow.compute as pc, vortex

path, op, out_path = sys.argv[1], sys.argv[2], sys.argv[3]
t0 = time.perf_counter()
table = vortex.open(path).to_arrow().read_all()
t1 = time.perf_counter()
table = table.filter(pc.is_valid(table['passenger_count']))
agg = {'sum': 'sum', 'count': 'count', 'min': 'min', 'max': 'max'}[op]
grouped = table.group_by('passenger_count').aggregate([('fare_cents', agg)])
states = grouped.column('passenger_count').to_pylist()
values = grouped.column(grouped.column_names[1]).to_pylist()
rows = sorted(([str(s), str(v)] for s, v in zip(states, values)), key=lambda r: r[0].encode())
Path(out_path).write_text(json.dumps({'rows': rows}) + '\n')
t2 = time.perf_counter()
Path(out_path + '.metrics.json').write_text(json.dumps({'codec': 'vortex', 'op': op,
    'read_ms': (t1 - t0) * 1000, 'execute_ms': (t2 - t1) * 1000,
    'stored_ms': (t2 - t0) * 1000, 'rows': len(rows)}) + '\n')
