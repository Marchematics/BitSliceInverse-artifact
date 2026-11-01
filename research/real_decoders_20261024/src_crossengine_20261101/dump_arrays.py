#!/usr/bin/env python3
"""Decode one columnar source file into the two int32 columns the pinned cube
builder consumes, so that Parquet and Arrow IPC can be fed to the *same* builder
as the fused encoded tree and the raw int32 columns.

One decoded pass, no row materialisation beyond the two output arrays:

  pass 1  group column -> distinct valid values (sorted) = the rank map
  pass 2  emit gid[i] = rank(passenger_count) or -1 for NULL, val[i] = fare_cents
          straight into a preallocated int32 buffer; plus an optional uint64
          validity bitmap for the value column (ECUBE03 null_measure)

Both passes stream record batches, so peak RSS is one batch per column.

Usage:
  dump_arrays.py <parquet|arrow> <src> <out_dir> [--group passenger_count]
                 [--value fare_cents] [--rows N]
Writes <out_dir>/gid.i32, <out_dir>/val.i32, <out_dir>/val.valid.bitmap (only
when the value column has NULLs), <out_dir>/dump.json
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import argparse
import json
import os
import time

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


def iter_batches(source, kind, columns, batch_rows=1 << 20):
    if kind == "parquet":
        pf = pq.ParquetFile(source)
        for b in pf.iter_batches(batch_size=batch_rows, columns=columns):
            yield b
    elif kind == "arrow":
        with pa.ipc.open_file(source) as r:
            for i in range(r.num_record_batches):
                yield r.get_batch(i).select(columns)
    else:
        raise SystemExit(f"unknown kind {kind}")


def null_count(batch, col):
    return batch.column(col).null_count


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("kind", choices=["parquet", "arrow"])
    ap.add_argument("src")
    ap.add_argument("out_dir")
    ap.add_argument("--group", default="passenger_count")
    ap.add_argument("--value", default="fare_cents")
    ap.add_argument("--rows", type=int, default=100_000_000)
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    cols = [args.group, args.value]

    t0 = time.perf_counter()
    # ---- pass 1: distinct valid group values (the dictionary) ----------------
    uniq = set()
    for b in iter_batches(args.src, args.kind, cols):
        g = b.column(cols.index(args.group))
        g = g.drop_null()
        uniq.update(pc.unique(g).to_pylist())
    values = sorted(uniq)
    rank = {v: i for i, v in enumerate(values)}
    t_distinct = time.perf_counter()

    # exact output size: rows may be capped
    n_total = 0
    for b in iter_batches(args.src, args.kind, cols):
        n_total += b.num_rows
        if n_total >= args.rows:
            break
    n = min(n_total, args.rows)

    gid_path = os.path.join(args.out_dir, "gid.i32")
    val_path = os.path.join(args.out_dir, "val.i32")
    gid = np.memmap(gid_path, dtype=np.int32, mode="w+", shape=(n,))
    val = np.memmap(val_path, dtype=np.int32, mode="w+", shape=(n,))
    words = (n + 63) // 64
    # validity bitmap: bit = 1 when the value payload is present
    vbits = np.full(words, np.uint64(0xFFFFFFFFFFFFFFFF), dtype=np.uint64)
    pos = 0
    val_nulls = 0
    for b in iter_batches(args.src, args.kind, cols):
        if pos >= n:
            break
        take = min(b.num_rows, n - pos)
        g = b.column(cols.index(args.group)).slice(0, take)
        v = b.column(cols.index(args.value)).slice(0, take)

        # group -> rank; NULL -> -1 (the builder's null-group convention)
        gv = g.to_numpy(zero_copy_only=False)
        gnull = pc.is_null(g).to_numpy(zero_copy_only=False)
        gidx = np.full(take, -1, dtype=np.int32)
        ok = ~gnull
        if ok.any():
            gnum = gv[ok].astype(np.int64)
            code = np.searchsorted(np.array(values, dtype=np.int64), gnum)
            gidx[ok] = code.astype(np.int32)
        gid[pos:pos + take] = gidx

        vn = pc.is_null(v).to_numpy(zero_copy_only=False) if v.null_count else None
        vv = v.to_numpy(zero_copy_only=False)
        val[pos:pos + take] = vv.astype(np.int32)
        if vn is not None and vn.any():
            val_nulls += int(vn.sum())
            for i in np.nonzero(vn)[0]:
                bit = pos + int(i)
                vbits[bit >> 6] &= ~np.uint64(1 << (bit & 63))
        pos += take
    gid.flush(); val.flush()
    t_dump = time.perf_counter()
    have_bitmap = val_nulls > 0
    if have_bitmap:
        if n % 64:
            vbits[-1] &= np.uint64((1 << (n % 64)) - 1)
        with open(os.path.join(args.out_dir, "val.valid.bitmap"), "wb") as f:
            f.write(vbits.tobytes())

    info = {
        "kind": args.kind,
        "src": args.src,
        "rows": n,
        "groups": len(values),
        "group_values": values,
        "value_nulls": val_nulls,
        "distinct_ms": (t_distinct - t0) * 1000.0,
        "dump_ms": (t_dump - t_distinct) * 1000.0,
        "total_ms": (t_dump - t0) * 1000.0,
        "gid_bytes": os.path.getsize(gid_path),
        "val_bytes": os.path.getsize(val_path),
        "valid_bitmap": have_bitmap,
    }
    with open(os.path.join(args.out_dir, "dump.json"), "w") as f:
        json.dump(info, f, indent=1)
    print(json.dumps(info))


if __name__ == "__main__":
    main()
