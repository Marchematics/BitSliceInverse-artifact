#!/usr/bin/env python3
"""Execute every corpus spec against ECUBE01 and compare as an exact multiset
with the DuckDB-computed expected rows.  Report pass/fail per family and per
pair; emit a JSON receipt with the binary/corpus hashes and failure witnesses.

Usage: run_specs.py [--binary PATH] [--out FILE] [--tag NAME] [--only FAMILY]
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import argparse
import collections
import hashlib
import json
import os
import platform
import subprocess
import sys
import time

import duckdb

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pairs as P
import bsstr

BASE = P.BASE


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def run_cube(binary, spec, tmp_out, cube_prefix=None):
    cube = f"{BASE}/{spec['cube']['cube']}"
    if cube_prefix:
        cube = f"{cube_prefix}/{os.path.basename(spec['cube']['cube'])}"
    dic = f"{BASE}/{spec['cube']['dict']}"
    cmd = [binary, "query", cube, dic, tmp_out] + list(spec["cube"]["args"])
    t0 = time.perf_counter()
    p = subprocess.run(cmd, capture_output=True, text=True)
    ms = (time.perf_counter() - t0) * 1000.0
    if p.returncode != 0:
        return None, ms, p.stderr[-500:]
    try:
        rows = json.load(open(tmp_out))["rows"]
    except Exception as e:
        return None, ms, f"bad output: {e}"
    return rows, ms, None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--binary", default=f"{BASE}/bin/enc_cube_specs")
    ap.add_argument("--corpus", default=f"{BASE}/out/spec_corpus_20261101.json")
    ap.add_argument("--out", default=f"{BASE}/out/spec_receipt_patched.json")
    ap.add_argument("--tag", default="patched")
    ap.add_argument("--only", default="")
    ap.add_argument("--cube-prefix", default="")
    ap.add_argument("--max-witness", type=int, default=200)
    args = ap.parse_args()
    corpus = json.load(open(args.corpus))
    specs = corpus["specs"]
    if args.only:
        want = set(args.only.split(","))
        specs = [s for s in specs if s["family"] in want]
    tmp_out = f"{BASE}/out/_spec_rows_{os.getpid()}.json"
    fam = collections.defaultdict(lambda: dict(specs=0, exact=0, fail=0, ms=[]))
    pair = collections.defaultdict(lambda: dict(specs=0, exact=0, fail=0))
    failures = []
    t0 = time.time()
    for i, s in enumerate(specs):
        rows, ms, err = run_cube(args.binary, s, tmp_out, args.cube_prefix or None)
        f = fam[s["family"]]
        f["specs"] += 1
        f["ms"].append(ms)
        pair[s["pair"]]["specs"] += 1
        ok = False
        detail = None
        if rows is None:
            detail = {"error": err}
        else:
            if s.get("rows") is not None:
                got = sorted(rows)
                ok = got == sorted(s["rows"])
                if not ok:
                    detail = {"expected_n": len(s["rows"]), "got_n": len(rows),
                              "expected_head": s["rows"][:20], "got_head": got[:20]}
            else:
                dg = bsstr.digest_cube_rows(rows)
                ok = dg == s["rows_digest"] and len(rows) == s["nrows"]
                if not ok:
                    detail = {"expected_n": s["nrows"], "got_n": len(rows),
                              "expected_digest": s["rows_digest"], "got_digest": dg}
        if ok:
            f["exact"] += 1
            pair[s["pair"]]["exact"] += 1
        else:
            f["fail"] += 1
            pair[s["pair"]]["fail"] += 1
            if len(failures) < args.max_witness:
                failures.append(dict(id=s["id"], family=s["family"], pair=s["pair"],
                                     sql=s["sql"], cube_args=s["cube"]["args"],
                                     detail=detail))
        if (i + 1) % 50 == 0:
            print(f"  {i+1}/{len(specs)} done ({time.time()-t0:.1f}s)", flush=True)
    for f in fam.values():
        f["median_ms"] = round(sorted(f["ms"])[len(f["ms"]) // 2], 3) if f["ms"] else None
        del f["ms"]
    receipt = dict(tag=args.tag,
                   binary=args.binary, binary_sha256=sha256_file(args.binary),
                   corpus=args.corpus, corpus_sha256=sha256_file(args.corpus),
                   duckdb_version=duckdb.__version__,
                   host=platform.node(), core_pin="taskset -c 72, nice -n 5",
                   cube_prefix=args.cube_prefix or f"{BASE}/cubes",
                   n_specs=len(specs),
                   n_exact=sum(v["exact"] for v in fam.values()),
                   n_fail=sum(v["fail"] for v in fam.values()),
                   started=time.strftime("%Y-%m-%dT%H:%M:%S"),
                   wall_s=round(time.time() - t0, 1),
                   families={k: v for k, v in sorted(fam.items())},
                   pairs={k: v for k, v in sorted(pair.items())},
                   failures=failures)
    json.dump(receipt, open(args.out, "w"), indent=1)
    os.unlink(tmp_out) if os.path.exists(tmp_out) else None
    print(json.dumps(dict(n_specs=receipt["n_specs"], n_exact=receipt["n_exact"],
                          n_fail=receipt["n_fail"],
                          families={k: (v["specs"], v["exact"]) for k, v in receipt["families"].items()},
                          wall_s=receipt["wall_s"]), indent=1))


if __name__ == "__main__":
    main()
