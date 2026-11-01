#!/usr/bin/env python3
"""Build MEC02_MANIFEST.json: sha256 + bytes of every receipt plus the headline
roll-up, so the document's numbers can be traced to a file.

  python3 make_manifest.py <receipts_dir> <MEC_ALL.json>
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import hashlib
import json
import os
import sys

rdir, all_path = sys.argv[1], sys.argv[2]
all_ds = json.load(open(all_path))["datasets"]


def sha(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


files = {}
for root, _, names in os.walk(rdir):
    for n in sorted(names):
        if n.endswith((".json", ".txt", ".md")):
            p = os.path.join(root, n)
            files[os.path.relpath(p, rdir)] = dict(bytes=os.path.getsize(p), sha256=sha(p))

ds = {}
for n, d in all_ds.items():
    ds[n] = dict(
        rows=d["meta"]["rows"], G=d["info"]["G"], mode=d["info"].get("mode"),
        BP=d["info"]["BP"], W=d["info"]["W"], count_width=d["info"]["count_width"],
        source_parquet_bytes=d["storage"]["source_parquet_bytes"],
        mec_bytes=d["storage"]["mec_bytes"],
        mec_over_source=d["storage"]["mec_over_source"],
        footprint_ratio=d["storage"]["footprint_ratio"],
        build_ms=d["build"]["build_ms"],
        exact_all=d["exact_all"], fuzz=d["fuzz"],
        mec_sc_us={k: v["mec_sc_us"] for k, v in d["cases"].items()},
        mec_full_us={k: v["mec_full_us"] for k, v in d["cases"].items()},
        duckdb_sc_ms={k: v["duckdb_sc_ms"] for k, v in d["cases"].items()},
        speedup_vs_duckdb={k: round(v["speedup"]["vs_duckdb_sc"], 1)
                           for k, v in d["cases"].items()},
        speedup_vs_bitplane={k: (round(v["speedup"]["vs_bitplane"], 1)
                                 if v["speedup"]["vs_bitplane"] else None)
                             for k, v in d["cases"].items()},
        speedup_vs_sorted_index={k: (round(v["speedup"]["vs_sorted_index"], 1)
                                     if v["speedup"]["vs_sorted_index"] else None)
                                 for k, v in d["cases"].items()},
    )

man = dict(
    cube="MEC02 exact multi-column conjunction cube",
    doc="research/real_decoders_20261025/MULTICOL_CUBE_20261101.md",
    source="research/real_decoders_20261024/src_eai_multi_20261101/",
    host="localhost (ssh localhost), AMD EPYC 9554, single pinned core 64, nice -n 5",
    duckdb="2.0.0 (threads=1, warm persistent connection)",
    date="2026-11-01",
    datasets=ds,
    files=files,
)
out = os.path.join(rdir, "MEC02_MANIFEST.json")
json.dump(man, open(out, "w"), indent=1)
print("wrote", out, len(files), "files")
