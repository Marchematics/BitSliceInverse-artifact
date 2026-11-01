#!/usr/bin/env python3
"""Tiny fixtures for the ECUBE03 parallel-build parity tests (scale stream).

Writes int32 plain columns for the *dense* path (contiguous observed value
domain -> ECUBE03 header dense=1) and, optionally, a validity bitmap for the
NULL-measure path:

  dense  : gid[i] = i % groups, val[i] = vmin + (i % span)   -> span == V -> dense
  valid  : bit i = 1 / 0 alternating pattern when --nulls is given

usage: make_dense_fixture.py OUTDIR [n=2000000] [groups=3] [vmin=-5] [span=10] [--nulls]

Outputs OUTDIR/dense_gid.i32, OUTDIR/dense_val.i32, and with --nulls
OUTDIR/dense_valid.bits (LSB-first bitmap).
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import os
import sys

import numpy as np

with_nulls = "--nulls" in sys.argv
pos = [a for a in sys.argv[1:] if not a.startswith("--")]
out = pos[0]
n = int(pos[1]) if len(pos) > 1 else 2_000_000
groups = int(pos[2]) if len(pos) > 2 else 3
vmin = int(pos[3]) if len(pos) > 3 else -5
span = int(pos[4]) if len(pos) > 4 else 10
os.makedirs(out, exist_ok=True)
i = np.arange(n, dtype=np.int64)
gid = (i % groups).astype(np.int32)
val = (vmin + (i % span)).astype(np.int32)
gid.tofile(os.path.join(out, "dense_gid.i32"))
val.tofile(os.path.join(out, "dense_val.i32"))
assert len(np.unique(val)) == span and val.max() - val.min() + 1 == span
if with_nulls:
    # every 10th row has a NULL value payload
    valid = ((i % 10) != 0).astype(np.uint8)
    packed = np.packbits(valid, bitorder="little")
    packed.tofile(os.path.join(out, "dense_valid.bits"))
    print("nulls:", int(n - valid.sum()), "of", n)
print("fixture rows=%d groups=%d V=%d dense=1" % (n, groups, span))
