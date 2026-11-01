#!/usr/bin/env python3
"""DataFusion second-engine integration over exact block-summary entries."""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import json, struct, sys, time
import pyarrow as pa
from datafusion import SessionContext

summary = sys.argv[1]; dict_path = sys.argv[2]
b = open(summary, "rb").read()
nblocks, ngroups, rows = struct.unpack_from("<QQQ", b, 8)
d = open(dict_path, "rb").read(); q = 48
k = struct.unpack_from("<I", d, q)[0]; q += 4
names = []
for _ in range(k):
    l = struct.unpack_from("<I", d, q)[0]; q += 4
    names.append(d[q:q + l].decode()); q += l
ent = struct.Struct("<qqiiB"); stride = 32; off = 80
n, s, c, mn, mx = [], [], [], [], []
for _bi in range(nblocks):
    for g in range(ngroups):
        sv, cv, miv, mav, has = ent.unpack_from(b, off); off += stride
        if not has or g >= len(names):
            continue
        n.append(names[g]); s.append(sv); c.append(cv); mn.append(miv); mx.append(mav)
t = pa.table({"name": pa.array(n), "sum": pa.array(s, type=pa.int64()),
              "count": pa.array(c, type=pa.int64()), "min": pa.array(mn, type=pa.int32()),
              "max": pa.array(mx, type=pa.int32())})
ctx = SessionContext(); ctx.register_record_batches("blocks", [t.to_batches()])
sql = ("SELECT name, SUM(sum) AS s, SUM(count) AS c, MIN(min) AS mn, MAX(max) AS mx "
       "FROM blocks GROUP BY name HAVING SUM(sum)>=1e10 ORDER BY name")
times = []
for _ in range(5):
    t0 = time.perf_counter(); r = ctx.sql(sql).collect(); times.append((time.perf_counter() - t0) * 1000)
out = r[0].to_pydict()
print(json.dumps({"rows": len(out["name"]), "times_ms": times,
                  "median_ms": sorted(times)[len(times)//2],
                  "result": out, "entries": len(n)}))
