#!/usr/bin/env python3
"""Rebuild the plain-column cube for every pair with a chosen enc_cube binary.
Used to give the *unpatched* ECUBE01 binary its own cube set, so a pre-fix
differential run measures build-time bugs too (the dense-mode bug is a build bug).

Usage: build_cubes.py --binary PATH --out-dir DIR [--only C1,F5]
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import argparse
import hashlib
import json
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pairs as P


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--binary", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--only", default="")
    ap.add_argument("--out", default="")
    args = ap.parse_args()
    os.makedirs(args.out_dir, exist_ok=True)
    ids = [p["id"] for p in P.CORPUS_PAIRS + P.FUZZ_PAIRS]
    if args.only:
        want = set(args.only.split(","))
        ids = [i for i in ids if i in want]
    out = dict(binary=args.binary, binary_sha256=sha256_file(args.binary), cubes={})
    for pid in ids:
        d = f"{P.BASE}/tables/{pid}"
        dst = f"{args.out_dir}/{pid}.ecube"
        p = subprocess.run([args.binary, "build", f"{d}/g.i32", f"{d}/v.i32", dst],
                           capture_output=True, text=True)
        if p.returncode != 0:
            raise SystemExit(f"{pid}: {p.stderr[-400:]}")
        info = json.loads(p.stderr.strip().splitlines()[-1])
        out["cubes"][pid] = dict(path=dst, sha256=sha256_file(dst),
                                 bytes=os.path.getsize(dst), build_info=info)
        print(pid, info.get("dense"), out["cubes"][pid]["bytes"], flush=True)
    if args.out:
        json.dump(out, open(args.out, "w"), indent=1)
        print("wrote", args.out)


if __name__ == "__main__":
    main()
