#!/usr/bin/env python3

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import json, sys, time
from pathlib import Path
import pyarrow as pa, pyarrow.compute as pc, pyarrow.ipc as ipc

path, op, out_path = sys.argv[1], sys.argv[2], sys.argv[3]
t0 = time.perf_counter()
with open(path, 'rb') as f:
    table = ipc.open_file(f).read_all()
t1 = time.perf_counter()
price = pc.cast(table.column('l_extendedprice').cast(pa.float64()), pa.float64())
cents = pc.cast(pc.round(pc.multiply(price, 100.0), ndigits=0), pa.int64())
table = table.append_column('cents', cents)
agg = {'sum': 'sum', 'count': 'count', 'min': 'min', 'max': 'max'}[op]
grouped = table.group_by('l_returnflag').aggregate([('cents', agg)])
states = grouped.column('l_returnflag').to_pylist()
values = grouped.column(grouped.column_names[1]).to_pylist()
rows = sorted(([str(s), str(v)] for s, v in zip(states, values)), key=lambda r: r[0].encode())
Path(out_path).write_text(json.dumps({'rows': rows}) + '\n')
t2 = time.perf_counter()
Path(out_path + '.metrics.json').write_text(json.dumps({'codec': 'arrow_ipc', 'op': op,
    'read_ms': (t1 - t0) * 1000, 'execute_ms': (t2 - t1) * 1000,
    'stored_ms': (t2 - t0) * 1000, 'rows': len(rows)}) + '\n')
