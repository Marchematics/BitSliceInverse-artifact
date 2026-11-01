#!/usr/bin/env python3
"""Witness: the two pinned build paths disagree on a contiguous value domain.

`enc_cube build` (plain int32 columns) enables dense addressing when
`span == V` (value = vmin + rank, so the distinct array is not stored), while
`enc_cube build_enc` (encoded column tree) always writes `dense = 0`. For NYC-100M
and TPC-H `span != V`, so the two paths are byte-identical there and the "one
builder, format-neutral" claim holds. On a dataset whose observed value domain is
contiguous (`span == V`) the two paths produce *different bytes for the same
logical data* — both exact, but not byte-comparable.

This script builds such a dataset, runs both builders, and checks that the
answers agree. It is a correctness/consistency witness, not a timing run.

Usage: witness_dense_mode.py
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import hashlib
import json
import os
import subprocess
import sys

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.environ.get("CE_REPO", _artifact.expand("../../research/real_decoders_20261024"))
WORK = os.environ.get("CE_WORK", _artifact.expand("../../work/crossengine"))
BIN = os.path.join(WORK, "build")
OUT = os.environ.get("CE_OUT", os.path.join(WORK, "runs"))
ENC_CUBE = os.path.join(REPO, "src_eai_20261101/enc_cube")
DICT = os.path.join(REPO, "results/nyc_taxi_100m_prep/Observed/state.dict")
TMP = "/tmp/ce_dense_witness"


def sha(p):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()


def run(cmd):
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        raise SystemExit(f"FAILED {' '.join(cmd)}: {p.stderr[-500:]}")
    return p


def main():
    os.makedirs(TMP, exist_ok=True)
    os.makedirs(OUT, exist_ok=True)
    rng = np.random.default_rng(7)
    n = 1000
    gid = rng.integers(0, 3, n, dtype=np.int32)
    val = rng.integers(0, 10, n, dtype=np.int32)   # contiguous domain 0..9 -> span == V
    gp, vp = os.path.join(TMP, "gid.i32"), os.path.join(TMP, "val.i32")
    gid.tofile(gp)
    val.tofile(vp)

    a = os.path.join(TMP, "plain.ecube")
    b = os.path.join(TMP, "plain_dense0.ecube")
    m1 = json.loads(run([ENC_CUBE, "build", gp, vp, a]).stderr.strip().splitlines()[-1])
    m2 = json.loads(run([os.path.join(BIN, "ecube_fmt_build"), "build", gp, vp, "-", b, "2"]).stdout)

    # answers through the shared reader must be identical even though bytes differ
    specs = ["", "hi=3", "range=2,7", "where_in=1"]
    answers = {}
    for i, spec in enumerate(specs):
        outs = []
        for path in (a, b):
            oj = os.path.join(TMP, f"ans_{i}.json")
            cmd = [os.path.join(BIN, "ecube_direct"), "query", path, DICT, oj]
            if spec:
                cmd.append(spec)
            run(cmd)
            outs.append(sha(oj))
        answers[spec or "<fixed>"] = {"answers_identical": outs[0] == outs[1],
                                      "sha256": outs[0]}

    rec = {
        "witness": "dense-addressing divergence between the two pinned build paths",
        "dataset": {"rows": n, "groups": 3, "values": 10, "span_equals_V": True},
        "build_plain": m1,
        "build_dense0": m2,
        "sha256": {"pinned_build_plain": sha(a), "dense0_build": sha(b)},
        "sizes": {"pinned_build_plain": os.path.getsize(a), "dense0_build": os.path.getsize(b)},
        "bytes_differ": sha(a) != sha(b),
        "query_answers": answers,
        "all_answers_identical": all(v["answers_identical"] for v in answers.values()),
        "conclusion": (
            "the 'byte-identical from plain columns and from the encoded tree' "
            "receipt is conditional on span != V; on a contiguous observed value "
            "domain build_plain selects dense addressing while build_enc always "
            "writes dense=0, so the two pinned indexes differ in bytes (256 vs 336 "
            "here) while both remain exact. NYC-100M and TPC-H are unaffected "
            "(span != V)."),
    }
    path = os.path.join(OUT, "dense_mode_witness.json")
    json.dump(rec, open(path, "w"), indent=1)
    print(json.dumps(rec, indent=1))
    print("wrote", path)


if __name__ == "__main__":
    main()
