#!/usr/bin/env python3
"""Build the cube directly from the fused encoded tree for the pairs that have
one (C1 NYC-100M, C2 NYC-12M, C3 TPC-H SF2) and compare it byte-for-byte with
the cube built from materialised int32 columns.  This is the format-neutrality
receipt for the spec corpus: the same SQL must be exact on both builds.

Usage: verify_identity.py [--binary ...] [--out ...]
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

DATA = P.DATA
BASE = P.BASE


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--binary", default=f"{BASE}/bin/enc_cube_specs")
    ap.add_argument("--meta", default=f"{BASE}/out/pairs_meta.json")
    ap.add_argument("--out", default=f"{BASE}/out/encoded_identity.json")
    ap.add_argument("--cubes-dir", default=f"{BASE}/cubes")
    args = ap.parse_args()
    metas = json.load(open(args.meta))
    out = dict(binary=args.binary, binary_sha256=sha256_file(args.binary), pairs={})
    for pid, (prep, gid, vid) in P.ENC_PREPS.items():
        enc = f"{DATA}/{prep}/Observed.encoded"
        pre = f"{DATA}/{prep}/Observed"
        dst = f"{args.cubes_dir}/{pid}_enc.ecube"
        p = subprocess.run([args.binary, "build_enc", enc, pre, str(gid), str(vid), dst],
                           capture_output=True, text=True)
        if p.returncode != 0:
            raise SystemExit(f"{pid} build_enc failed: {p.stderr[-600:]}")
        info = json.loads(p.stderr.strip().splitlines()[-1])
        plain = f"{args.cubes_dir}/{pid}.ecube"
        sp, se = sha256_file(plain), sha256_file(dst)
        out["pairs"][pid] = dict(prep=prep, encoded=enc, gid=gid, vid=vid,
                                 build_info=info,
                                 plain_cube=plain, plain_sha256=sp,
                                 enc_cube=dst, enc_sha256=se,
                                 plain_bytes=os.path.getsize(plain),
                                 enc_bytes=os.path.getsize(dst),
                                 byte_identical=(sp == se),
                                 rows_match=(info.get("rows") == metas[pid]["n"]),
                                 groups_match=(info.get("groups") == metas[pid]["ngroups"] + (
                                     1 if metas[pid]["has_null"] else 0)),
                                 values_match=(info.get("values") == metas[pid]["nvalues"]))
        metas[pid]["enc_cube_sha256"] = se
        print(pid, "byte_identical=", sp == se, "plain=", sp[:16], "enc=", se[:16],
              flush=True)
    json.dump(metas, open(args.meta, "w"), indent=1)
    json.dump(out, open(args.out, "w"), indent=1)
    print("wrote", args.out)


if __name__ == "__main__":
    main()
