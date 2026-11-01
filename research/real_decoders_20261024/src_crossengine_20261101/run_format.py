#!/usr/bin/env python3
"""Cross-format build receipt: the identical ECUBE02 index from four inputs.

Paths:
  encoded  fused encoded column tree   -> pinned `enc_cube build_enc`
  plain    raw int32 columns           -> pinned `enc_cube build`
  parquet  results/.../nyc100m.parquet -> dump_arrays.py + ecube_fmt_build
  arrow    results/.../nyc100m.arrow   -> dump_arrays.py + ecube_fmt_build

All four must produce the same sha256. The raw column dump from Parquet/Arrow is
also compared byte-for-byte with the pinned `.i32` columns, which isolates "the
decoder produced the same column" from "the builder is deterministic".

ECUBE03 (the NULL-measure extension that appeared on the host during this run) is
built for the encoded path with the lead's `enc_cube3` and for Parquet/Arrow with
ecube_fmt_build; NYC has no NULL measure so the only difference from ECUBE02 is
the 44-byte null_measure section.

Usage: run_format.py [--out DIR]
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
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.environ.get("CE_REPO", _artifact.expand("../../research/real_decoders_20261024"))
DATA = os.path.join(REPO, "results")
WORK = os.environ.get("CE_WORK", _artifact.expand("../../work/crossengine"))
BIN = os.path.join(WORK, "build")
OUTDIR = os.environ.get("CE_OUT", os.path.join(WORK, "runs"))
FDIR = os.path.join(WORK, "data")
VENV = _artifact.python()

ENCODED = os.path.join(DATA, "nyc_taxi_100m_prep/Observed.encoded")
PREP = os.path.join(DATA, "nyc_taxi_100m_prep/Observed")
DICT = os.path.join(PREP, "state.dict")
PARQUET = os.path.join(DATA, "nyc_taxi_100m_arrow/nyc100m.parquet")
ARROW = os.path.join(DATA, "nyc_taxi_100m_arrow/nyc100m.arrow")
PLAIN_GID = os.path.join(DATA, "nyc100_plain/passenger.i32")
PLAIN_VAL = os.path.join(DATA, "nyc100_plain/fare.i32")
ENC_CUBE = os.path.join(REPO, "src_eai_20261101/enc_cube")
ENC_CUBE3 = os.path.join(REPO, "src_eai_20261101/enc_cube3")
PIN = ["nice", "-n", "5", "taskset", "-c", _artifact.core()]
GID_ID, VAL_ID = "17", "13"


def sha(path, chunk=1 << 22):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def run(cmd, timeout=3600):
    t = time.perf_counter()
    p = subprocess.run(PIN + cmd, capture_output=True, text=True, timeout=timeout)
    wall = (time.perf_counter() - t) * 1000.0
    if p.returncode != 0:
        raise SystemExit(f"FAILED({' '.join(cmd)}):\n{p.stderr[-1200:]}")
    meta = None
    for stream in (p.stderr, p.stdout):
        for line in reversed(stream.strip().splitlines()):
            line = line.strip()
            if line.startswith("{"):
                try:
                    meta = json.loads(line)
                    break
                except json.JSONDecodeError:
                    pass
        if meta is not None:
            break
    return {"cmd": " ".join(cmd), "wall_ms": wall, "meta": meta, "stdout": p.stdout}


def main():
    os.makedirs(OUTDIR, exist_ok=True)
    os.makedirs(FDIR, exist_ok=True)
    rec = {"inputs": {"encoded": ENCODED, "prep": PREP, "parquet": PARQUET, "arrow": ARROW,
                      "plain_gid": PLAIN_GID, "plain_val": PLAIN_VAL},
           "pinned_enc_cube": ENC_CUBE, "enc_cube_sha256": sha(ENC_CUBE),
           "builds": {}}
    out = {}

    # ---- 1. encoded tree (pinned builder) ----------------------------------
    print("[encoded] enc_cube build_enc ...", flush=True)
    p = os.path.join(FDIR, "fmt_encoded.ecube")
    rec["builds"]["encoded"] = run([ENC_CUBE, "build_enc", ENCODED, PREP, GID_ID, VAL_ID, p])
    out["encoded"] = p

    # ---- 2. plain int32 columns (pinned builder) ---------------------------
    print("[plain] enc_cube build ...", flush=True)
    p = os.path.join(FDIR, "fmt_plain.ecube")
    rec["builds"]["plain"] = run([ENC_CUBE, "build", PLAIN_GID, PLAIN_VAL, p])
    out["plain"] = p

    # ---- 3./4. Parquet and Arrow IPC through the added decode path ---------
    for kind, src in (("parquet", PARQUET), ("arrow", ARROW)):
        d = os.path.join(FDIR, f"dump_{kind}")
        print(f"[{kind}] dump_arrays ...", flush=True)
        r = run([VENV, os.path.join(HERE, "dump_arrays.py"), kind, src, d])
        rec["builds"][kind] = r
        dump = json.load(open(os.path.join(d, "dump.json")))
        rec["builds"][kind]["dump"] = dump
        rec["builds"][kind]["gid_sha256"] = sha(os.path.join(d, "gid.i32"))
        rec["builds"][kind]["val_sha256"] = sha(os.path.join(d, "val.i32"))
        for fmt in ("2", "3"):
            p = os.path.join(FDIR, f"fmt_{kind}{'' if fmt == '2' else '_v3'}.ecube")
            print(f"[{kind}] ecube_fmt_build (ECUBE0{fmt}) ...", flush=True)
            rec["builds"].setdefault(f"cube_{kind}_v{fmt}", run(
                [os.path.join(BIN, "ecube_fmt_build"), "build", os.path.join(d, "gid.i32"),
                 os.path.join(d, "val.i32"), "-", p, fmt]))
            out[f"{kind}_v{fmt}"] = p

    # ---- 5. ECUBE03 encoded path (lead's enc_cube3) ------------------------
    if os.path.exists(ENC_CUBE3):
        print("[encoded] enc_cube3 build_enc (ECUBE03) ...", flush=True)
        p3 = os.path.join(FDIR, "fmt_encoded_v3.ecube")
        rec["builds"]["encoded_v3"] = run([ENC_CUBE3, "build_enc", ENCODED, PREP, GID_ID, VAL_ID, p3])
        out["encoded_v3"] = p3
        rec["enc_cube3_sha256"] = sha(ENC_CUBE3)
    # ECUBE02 pipeline through the same format builder, for a like-for-like
    # encoded-vs-parquet comparison of the added builder itself.
    rec["plain_column_equality"] = {
        "pinned_gid_sha256": sha(PLAIN_GID),
        "pinned_val_sha256": sha(PLAIN_VAL),
        "parquet_gid_equal": None, "parquet_val_equal": None,
        "arrow_gid_equal": None, "arrow_val_equal": None,
    }
    for kind in ("parquet", "arrow"):
        d = os.path.join(FDIR, f"dump_{kind}")
        rec["plain_column_equality"][f"{kind}_gid_equal"] = (
            rec["builds"][kind]["gid_sha256"] == rec["plain_column_equality"]["pinned_gid_sha256"])
        rec["plain_column_equality"][f"{kind}_val_equal"] = (
            rec["builds"][kind]["val_sha256"] == rec["plain_column_equality"]["pinned_val_sha256"])

    # ---- shas and equality -------------------------------------------------
    shas = {k: sha(v) for k, v in out.items()}
    rec["sha256"] = shas
    rec["sizes"] = {k: os.path.getsize(v) for k, v in out.items()}
    groups = {
        "ECUBE02": [k for k in shas if k in ("encoded", "plain", "parquet_v2", "arrow_v2")],
        "ECUBE03": [k for k in shas if k in ("encoded_v3", "parquet_v3", "arrow_v3")],
    }
    rec["equal_across_formats"] = {}
    for name, keys in groups.items():
        present = [k for k in keys if k in shas]
        rec["equal_across_formats"][name] = {
            "members": present,
            "sha256_set": sorted({shas[k] for k in present}),
            "identical": len({shas[k] for k in present}) == 1 and len(present) > 1,
        }

    # ---- query the resulting indexes: must be identical -------------------
    print("[verify] identical query answers on every index ...", flush=True)
    specs = ["", "hi=-720", "hi=930", "range=650,1350", "where_in=1,2", "where_not_in=0,1",
             "having=sum>=1e10", "range=80000000,90000000"]
    answers, checks = {}, {}
    for key, path in out.items():
        per = {}
        for i, spec in enumerate(specs):
            oj = os.path.join(OUTDIR, f"fmt_{key}_{i}.json")
            cmd = [os.path.join(BIN, "ecube_direct"), "query", path, DICT, oj]
            if spec:
                cmd.append(spec)
            run(cmd)
            per[spec or "<fixed>"] = sha(oj)
        answers[key] = per
    base = answers.get("encoded", {})
    for key, per in answers.items():
        checks[key] = {"answers_equal_to_encoded": per == base,
                       "n_specs": len(per)}
    rec["query_answers"] = {"specs": [s or "<fixed>" for s in specs], "sha_by_index": answers,
                            "all_equal_to_encoded": all(v["answers_equal_to_encoded"]
                                                        for v in checks.values())}
    rec["index_load"] = {}
    for key, path in out.items():
        r = run([os.path.join(BIN, "ecube_direct"), "load", path, DICT])
        rec["index_load"][key] = json.loads(r["stdout"])

    path = os.path.join(OUTDIR, "format_receipt.json")
    with open(path, "w") as f:
        json.dump(rec, f, indent=1)
    print(json.dumps({"sha256": shas, "equal": rec["equal_across_formats"],
                      "columns": rec["plain_column_equality"],
                      "query_identical": rec["query_answers"]["all_equal_to_encoded"]},
                     indent=1))
    print("wrote", path)


if __name__ == "__main__":
    main()
