#!/usr/bin/env python3
"""Reference-table preparation for the tiers stream (system python3 + numpy + pyarrow).

Run from research/real_decoders_20261024 (the repo dir that holds results/):

    python3 src_tiers_20261101/make_ref_tables.py

Writes into $TIERS_SCRATCH (default ../../work/tiers/build):
  val_nyc100.i32          decoded NYC-100M value column (i32; the plain column is
                          stored biased by 2^31 exactly like the EAI encoded domain)
  ig2_65k.parquet         (state VARCHAR, pop BIGINT) for the 65,536-row IG2 slice
  ig2_983k.parquet        (state VARCHAR, pop BIGINT)
  ig2_10m.parquet         (state VARCHAR, pop BIGINT)
  nyc_groups.parquet      (v BIGINT, g1000 INT32, g100k INT32, g1m INT32) for the
                          group-cardinality tiers; gid = splitmix64(rowid) % G
                          written by the C++ gen_groups binary (same bytes both sides)
  cuts.json               p10/p50 value cuts + measured row fractions per dataset

No DuckDB here: the reference queries run in python3 (duckdb 2.0.0).
The plain Observed columns carry the EAI code bias (decode: u32 - 2^31); the cube's
build_enc path applies the same bias internally (CODE_BIAS in eai_cube2.cpp), so both
sides see identical values. This is asserted by the exactness checks in run_scale.py.
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import json
import os
import struct
import sys

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

SCRATCH = _artifact.expand(os.environ.get("TIERS_SCRATCH", "../../work/tiers/build"))
BIAS = 1 << 31


def load_dict(path):
    d = open(path, "rb").read()
    assert d[:8] == b"BSSTR01\n", (path, d[:8])
    off = 48
    (k,) = struct.unpack_from("<I", d, off)
    off += 4
    names = []
    for _ in range(k):
        (ln,) = struct.unpack_from("<I", d, off)
        off += 4
        names.append(d[off:off + ln].decode())
        off += ln
    return names


def col(prep, name):
    a = np.fromfile(os.path.join(prep, name), dtype=np.uint32).astype(np.int64) - BIAS
    return a


def write_parquet(path, cols):
    tbl = pa.table(cols)
    pq.write_table(tbl, path, compression="zstd")
    return os.path.getsize(path)


def cuts_for(v, name):
    p10 = int(np.quantile(v, 0.10, method="lower"))
    p50 = int(np.quantile(v, 0.50, method="lower"))
    return {
        "p10": p10,
        "p50": p50,
        "frac_le_p10": float((v <= p10).mean()),
        "frac_le_p50": float((v <= p50).mean()),
    }


def main():
    os.makedirs(SCRATCH, exist_ok=True)
    results = "results"
    cuts = {"decode": "u32_payload - 2^31 (EAI CODE_BIAS)", "datasets": {}}

    # --- NYC 100M decoded value column (plain i32, used by tiers_tool) -------
    prep = os.path.join(results, "nyc_taxi_100m_prep", "Observed")
    v = col(prep, "col13.u32").astype(np.int64)
    g = col(prep, "col17.u32")
    v.astype(np.int32).tofile(os.path.join(SCRATCH, "val_nyc100.i32"))
    cuts["datasets"]["nyc100"] = cuts_for(v, "nyc100")
    cuts["datasets"]["nyc100"]["rows"] = int(len(v))
    cuts["datasets"]["nyc100"]["V"] = int(np.unique(v).size)
    cuts["datasets"]["nyc100"]["G"] = 10
    del g

    # --- NYC 12M / TPC-H cuts (reference tables already exist as DuckDB/parquet)
    prep12 = os.path.join(results, "nyc_taxi_2023_prep", "Observed")
    v12 = col(prep12, "col13.u32")
    cuts["datasets"]["nyc12"] = cuts_for(v12, "nyc12")
    cuts["datasets"]["nyc12"]["rows"] = int(len(v12))
    cuts["datasets"]["nyc12"]["V"] = int(np.unique(v12).size)
    cuts["datasets"]["nyc12"]["G"] = 10
    del v12
    prest = os.path.join(results, "tpch_sf2_prep", "Observed")
    vt = col(prest, "col13.u32")
    cuts["datasets"]["tpch"] = cuts_for(vt, "tpch")
    cuts["datasets"]["tpch"]["rows"] = int(len(vt))
    cuts["datasets"]["tpch"]["V"] = int(np.unique(vt).size)
    cuts["datasets"]["tpch"]["G"] = 3
    del vt

    # --- IG2 slices: state VARCHAR + population BIGINT -----------------------
    ig2 = [
        ("ig2_65k", "ig2_prep_20261025/prepared/Observed"),
        ("ig2_983k", "ig2_scale15_prep/Observed"),
        ("ig2_10m", "ig2_scale10m_prep/Observed"),
    ]
    for name, p in ig2:
        prep = os.path.join(results, p)
        names = load_dict(os.path.join(prep, "state.dict"))
        gx = col(prep, "col17.u32")
        vx = col(prep, "col13.u32")
        state = np.array(names, dtype=object)[gx]
        path = os.path.join(SCRATCH, name + ".parquet")
        size = write_parquet(path, {"state": pa.array(state, type=pa.string()),
                                    "pop": pa.array(vx, type=pa.int64())})
        cuts["datasets"][name] = cuts_for(vx, name)
        cuts["datasets"][name].update({
            "rows": int(len(vx)),
            "V": int(np.unique(vx).size),
            "G": len(names),
            "parquet": path,
            "parquet_bytes": size,
            "sql": "SELECT state, SUM(pop), COUNT(*), MIN(pop), MAX(pop) FROM t "
                   "WHERE state NOT IN ('District of Columbia') GROUP BY 1",
        })
        print(f"wrote {path} rows={len(vx)} V={np.unique(vx).size}", flush=True)
        del state, gx, vx

    # --- group-cardinality tiers: gid files (C++) + value column -------------
    gidfiles = {G: os.path.join(SCRATCH, f"gid_g{G}.i32")
                for G in (1000, 100000, 1000000)}
    cols = {"v": pa.array(v, type=pa.int64())}
    for G, gp in gidfiles.items():
        if not os.path.exists(gp):
            sys.exit(f"missing {gp}: run src_tiers_20261101/gen_groups 100000000 {G} {gp}")
        gg = np.fromfile(gp, dtype=np.int32).astype(np.int32)
        assert len(gg) == len(v)
        cols[f"g{G}"] = pa.array(gg, type=pa.int32())
        cuts["datasets"][f"g{G}"] = {"G": G, "rows": int(len(gg))}
        del gg
    path = os.path.join(SCRATCH, "nyc_groups.parquet")
    size = write_parquet(path, cols)
    print(f"wrote {path} rows={len(v)} bytes={size}", flush=True)

    cuts["scratch"] = SCRATCH
    with open(os.path.join(SCRATCH, "cuts.json"), "w") as f:
        json.dump(cuts, f, indent=1)
    print(json.dumps(cuts, indent=1)[:2000])


if __name__ == "__main__":
    main()
