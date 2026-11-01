#!/usr/bin/env python3
"""Convert plain little-endian int32 columns (gid, val) to a parquet file for the
DuckDB 2.0 control.  gid == -1 encodes SQL NULL for the group key (the same
sentinel the plain-column cube build uses); everything else is written verbatim,
so DuckDB and the cube see identical bytes.

usage: make_parquet.py GID.i32 VAL.i32 OUT.parquet [nrows] [cols]
  cols: "gv"      -> columns g (int32, nullable), v (int32)       [default]
        "gv2"     -> columns g (int32, nullable), v (int32),
                     v_real (int32 = same values, for the 10M-group real-V control)
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import sys
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

CHUNK = 20_000_000


def main():
    gid_path, val_path, out_path = sys.argv[1], sys.argv[2], sys.argv[3]
    nrows = int(sys.argv[4]) if len(sys.argv) > 4 else None
    mode = sys.argv[5] if len(sys.argv) > 5 else "gv"
    g = np.memmap(gid_path, dtype=np.int32, mode="r")
    v = np.memmap(val_path, dtype=np.int32, mode="r")
    n = min(len(g), len(v))
    if nrows and nrows > 0:
        n = min(n, nrows)
    fields = [pa.field("g", pa.int32(), nullable=True), pa.field("v", pa.int32(), nullable=False)]
    if mode == "gv2":
        fields.append(pa.field("v_real", pa.int32(), nullable=False))
    schema = pa.schema(fields)
    writer = pq.ParquetWriter(out_path, schema, compression="zstd", use_dictionary=True)
    written = 0
    for off in range(0, n, CHUNK):
        take = min(CHUNK, n - off)
        gc = np.asarray(g[off:off + take])
        vc = np.asarray(v[off:off + take])
        mask = gc < 0
        cols = [pa.array(gc, type=pa.int32(), mask=mask), pa.array(vc, type=pa.int32())]
        if mode == "gv2":
            cols.append(pa.array(vc, type=pa.int32()))
        batch = pa.record_batch(cols, schema=schema)
        writer.write_batch(batch)
        written += take
        if (off // CHUNK) % 10 == 0:
            print("wrote %d/%d" % (written, n), flush=True)
    writer.close()
    print("parquet rows=%d file=%s" % (written, out_path), flush=True)


if __name__ == "__main__":
    main()
