#!/usr/bin/env python3
"""100M nullable-measure column for the ECUBE03 NULL-measure campaign (tiers stream).

Creates, from the frozen NYC-100M prepared columns (decoded with the EAI code bias):
  gid_nyc_g10.i32         passenger_count rank as i32; the NULL group is -2^31 (negative)
                          so the cube build maps it to the NULL group, exactly as before
  nyc_null_valid.bits     12,500,000 B validity bitmap, bit i = 1 when row i has a value
                          (LSB-first), deterministic mask:
                            NULL if splitmix64(rowid) % 100 < 5   (5% random NULLs)
                            OR passenger_count rank == 9         (one all-NULL group)
  nyc_nullmeasure.parquet (g BIGINT nullable, v BIGINT nullable) DuckDB reference with
                          identical NULLs, so COUNT(*)/SUM/MIN/MAX semantics can be checked
Also emits nyc_nullmeasure_meta.json with the per-group valid/NULL counts and file shas.

Run from research/real_decoders_20261024 with the SYSTEM python3 (numpy + pyarrow):
    python3 src_tiers_20261101/make_nullmeasure_data.py
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import hashlib
import json
import os

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

REPO = os.path.abspath(os.getcwd())
PREP = os.path.join(REPO, "results/nyc_taxi_100m_prep/Observed")
SCRATCH = _artifact.expand(os.environ.get("TIERS_SCRATCH", "../../work/tiers/build"))
BIAS = 1 << 31


def sha(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def splitmix64(x):
    z = x + np.uint64(0x9E3779B97F4A7C15)
    z = (z ^ (z >> np.uint64(30))) * np.uint64(0xBF58476D1CE4E5B9)
    z = (z ^ (z >> np.uint64(27))) * np.uint64(0x94D049BB133111EB)
    return z ^ (z >> np.uint64(31))


def main():
    os.makedirs(SCRATCH, exist_ok=True)
    g = (np.fromfile(os.path.join(PREP, "col17.u32"), dtype=np.uint32).astype(np.int64) - BIAS).astype(np.int64)
    v = (np.fromfile(os.path.join(PREP, "col13.u32"), dtype=np.uint32).astype(np.int64) - BIAS).astype(np.int32)
    n = len(g)
    assert n == 100_000_000, n
    rowid = np.arange(n, dtype=np.uint64)
    null_mask = (splitmix64(rowid) % np.uint64(100) < np.uint64(5)) | (g == 9)
    valid = ~null_mask
    del rowid
    print(f"rows={n} nulls={int(null_mask.sum())} ({null_mask.mean()*100:.3f}%) "
          f"group9_rows={int((g==9).sum())}", flush=True)

    gpath = os.path.join(SCRATCH, "gid_nyc_g10.i32")
    g.astype(np.int32).tofile(gpath)
    bpath = os.path.join(SCRATCH, "nyc_null_valid.bits")
    np.packbits(valid, bitorder="little").tofile(bpath)
    print("bitmap bytes", os.path.getsize(bpath), flush=True)

    pqt = os.path.join(SCRATCH, "nyc_nullmeasure.parquet")
    gnull = g < 0
    tbl = pa.table({
        "g": pa.array(g, type=pa.int64(), mask=gnull),
        "v": pa.array(v.astype(np.int64), type=pa.int64(), mask=null_mask),
    })
    pq.write_table(tbl, pqt, compression="zstd")
    print("parquet bytes", os.path.getsize(pqt), flush=True)

    # per-group valid / NULL counts for the receipt (independent of the cube build)
    gn = np.where(gnull, 10, g)
    per = {}
    for gi in range(11):
        m = gn == gi
        per[str(gi)] = {"rows": int(m.sum()), "valid": int((m & valid).sum()),
                        "null": int((m & null_mask).sum())}
    meta = {
        "rows": int(n), "null_fraction": float(null_mask.mean()),
        "null_total": int(null_mask.sum()),
        "mask": "valid = not (splitmix64(rowid) % 100 < 5 or passenger_count_rank == 9); "
                "bit i LSB-first in nyc_null_valid.bits",
        "files": {k: {"path": p, "sha256": sha(p), "bytes": os.path.getsize(p)}
                  for k, p in (("gid", gpath), ("validity", bpath), ("parquet", pqt))},
        "per_group": per,
        "all_null_group": "9",
        "source": {"col17_sha256": sha(os.path.join(PREP, "col17.u32")),
                   "col13_sha256": sha(os.path.join(PREP, "col13.u32"))},
    }
    json.dump(meta, open(os.path.join(SCRATCH, "nyc_nullmeasure_meta.json"), "w"), indent=1)
    print(json.dumps({k: meta[k] for k in ("rows", "null_total", "null_fraction")}, indent=1))


if __name__ == "__main__":
    main()
