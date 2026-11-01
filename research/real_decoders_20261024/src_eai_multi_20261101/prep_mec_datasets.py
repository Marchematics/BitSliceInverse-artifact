#!/usr/bin/env python3
"""MEC dataset preparation (run with the SYSTEM python3: numpy + pyarrow).

For every dataset we write, from ONE in-memory array set:
  <dir>/g.i64  v.i64  c1.i64  c2.i64   raw little-endian int64 columns for MEC
  <dir>/data.parquet                   the same four columns for the DuckDB oracle
  <dir>/cases.txt                      the case set for this dataset
  <dir>/meta.json                      shape, nulls, domain, source bytes

Group column convention: g >= 0 is a real group value, g < 0 means a NULL group
key (MEC gives it its own group, like ECUBE02).  Predicate columns v/c1/c2 must be
non-NULL: rows with NULL in any of them are dropped and the count is reported
(DuckDB applies the same IS NOT NULL filter on the oracle table, which is built
from the already-filtered columns, so both sides see one table).

Datasets
  tpch2        TPC-H SF2 lineitem:      g=returnflag, v=price,     c1=discount, c2=quantity
  tpch2_tax    TPC-H SF2, 2nd family:   g=returnflag, v=price,     c1=discount, c2=tax
  tpch2_null   TPC-H SF2 with 5% NULL returnflag (NULL-group semantics)
  syn_cluster  synthetic 12M, one run with ~9.6M rows (>65,535 -> uint32 planes)
               and 5% NULL group keys
  nyc3         100M NYC yellow-taxi 2023:  g=passenger_count (NULL kept),
               v=fare_cents, c1=payment_type, c2=RatecodeID
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import argparse
import glob
import json
import os

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = _artifact.expand("../../research/real_decoders_20261024")
RES = os.path.join(ROOT, "results")
OUT = os.path.join(RES, "eai_multi_20261101", "datasets")
MB = 1 << 20


def write_ds(name, g, v, c1, c2, cases, meta):
    d = os.path.join(OUT, name)
    os.makedirs(d, exist_ok=True)
    for nm, arr in (("g", g), ("v", v), ("c1", c1), ("c2", c2)):
        np.ascontiguousarray(arr, dtype="<i8").tofile(os.path.join(d, nm + ".i64"))
    tbl = pa.table({
        "g": pa.array(g.astype("int64"), mask=(g < 0)) if (g < 0).any()
             else pa.array(g.astype("int64")),
        "v": pa.array(v.astype("int64")),
        "c1": pa.array(c1.astype("int64")),
        "c2": pa.array(c2.astype("int64")),
    })
    pq.write_table(tbl, os.path.join(d, "data.parquet"), compression="snappy")
    with open(os.path.join(d, "cases.txt"), "w") as f:
        f.write(cases)
    meta = dict(meta)
    meta.update(
        name=name, dir=d, rows=int(len(g)),
        v_min=int(v.min()), v_max=int(v.max()),
        c1_distinct=int(len(np.unique(c1))), c2_distinct=int(len(np.unique(c2))),
        g0_distinct=int(len(np.unique(g[g >= 0]))),
        null_group_rows=int((g < 0).sum()),
        raw_columns_bytes=int(4 * len(g) * 8),
        parquet_bytes=os.path.getsize(os.path.join(d, "data.parquet")),
    )
    json.dump(meta, open(os.path.join(d, "meta.json"), "w"), indent=1)
    print(f"{name:14s} rows={meta['rows']:>10d} v=[{meta['v_min']},{meta['v_max']}] "
          f"G0={meta['g0_distinct']} D1={meta['c1_distinct']} D2={meta['c2_distinct']} "
          f"nulls={meta['null_group_rows']} parquet={meta['parquet_bytes']/MB:.1f}MB",
          flush=True)


def rd(name):
    return np.fromfile(os.path.join(RES, "multibits_tpch", name), dtype="<i8")


# ---------------------------------------------------------------- case sets --
def standard_cases(v, c1, c2, extra=""):
    """Quantile-driven case set for an arbitrary (v, c1, c2) dataset."""
    q = np.quantile(v, [0.01, 0.05, 0.25, 0.50, 0.75, 0.90, 0.99, 0.999]).astype(np.int64)
    vmin, vmax = int(v.min()), int(v.max())
    span = vmax - vmin + 1
    c1u = np.unique(c1)
    c2u = np.unique(c2)
    c1lo, c1hi = int(c1u[0]), int(c1u[-1])
    c2lo, c2hi = int(c2u[0]), int(c2u[-1])
    mid1 = int(c1u[len(c1u) // 2])
    mid2 = int(c2u[len(c2u) // 2])
    lines = [
        "# name v_lo v_hi c1_lo c1_hi c2_lo c2_hi  (all inclusive; generated from data quantiles)",
        f"all_full          {vmin} {vmax} {c1lo} {c1hi} {c2lo} {c2hi}",
        f"v_mid             {q[2]} {q[4]} {c1lo} {c1hi} {c2lo} {c2hi}",
        f"v_top             {q[5]} {vmax} {c1lo} {c1hi} {c2lo} {c2hi}",
        f"v_narrow          {q[3]} {int(q[3] + span // 1000)} {c1lo} {c1hi} {c2lo} {c2hi}",
        f"cross_selective   {q[3]} {q[4]} {c1lo} {mid1} {c2lo} {mid2}",
        f"cross_tight       {q[2]} {q[3]} {mid1} {mid1} {mid2} {mid2}",
        f"empty_domain      {vmax + 1} {vmax + 100000} {c1lo} {c1hi} {c2lo} {c2hi}",
        f"empty_cross       {q[6]} {q[7]} {c1lo} {c1lo} {c2hi} {c2hi}",
        f"point             {q[3]} {q[3]} {c1lo} {c1hi} {c2lo} {c2hi}",
        f"needle            {q[5]} {int(q[5] + 1000)} {c1lo} {c1hi} {c2lo} {c2hi}",
    ]
    return "\n".join(lines) + extra + "\n"


# ------------------------------------------------------------------ tpch --
def load_tpch():
    """Prepared columns (g, p, d, q).  The only projected TPC-H parquet on the host
    carries l_returnflag + l_extendedprice, so those two are cross-checked against
    the prepared .i64 files; discount/quantity come from the original prep step and
    are used as-is (documented in meta.json)."""
    import pyarrow.compute as pc
    g, p, d, q = rd("g.i64"), rd("p.i64"), rd("d.i64"), rd("q.i64")
    src = os.path.join(RES, "tpch_sf2_arrow", "lineitem.parquet")
    tbl = pq.read_table(src, columns=["l_extendedprice", "l_returnflag"])
    price_chk = pc.cast(pc.round(pc.multiply(pc.cast(tbl.column("l_extendedprice"), "float64"),
                                             100.0), ndigits=0), "int64").to_numpy(
                                                 zero_copy_only=False).astype(np.int64)
    flags = np.asarray([str(x) for x in tbl.column("l_returnflag").to_pylist()])
    order = sorted(set(flags))
    g_chk = np.array([order.index(x) for x in flags], dtype=np.int64)
    if not np.array_equal(price_chk, p):
        raise SystemExit("prepared price column mismatch")
    if not np.array_equal(g_chk, g):
        raise SystemExit("prepared group column mismatch")
    print(f"cross-check ok: price+group == {os.path.basename(src)} "
          f"({tbl.num_rows} rows, {os.path.getsize(src)} B); discount/quantity from prep",
          flush=True)
    return g, p, d, q, None


TPCH_CASES = """# TPC-H SF2 lineitem cases (the three documented ones kept verbatim, plus 7 more)
# name             v_lo        v_hi      c1_lo c1_hi c2_lo c2_hi
broad                 0      2000000     0    100   1    50
selective       1000000      2000000     0      5   1    20
empty           5000000      6000000     0      1   1    10
all_full              0     20000000     0    100   1    50
price_top_decile 9000000    20000000     0    100   1    50
quant_narrow          0     20000000     0    100  20    30
needle          1000000      1001000     0    100   1    50
mid_band        2000000      5000000     2      7  10    40
disc_single           0     20000000     5      5   1    50
disc_quant_tight 3000000     6000000     2      4   5    15
"""


def ds_tpch2():
    g, p, d, q, tax = load_tpch()
    del tax
    write_ds("tpch2", g, p, d, q, TPCH_CASES,
             dict(family="tpch-sf2-price",
                  columns=dict(g="l_returnflag A/N/R -> 0/1/2",
                               v="CAST(ROUND(l_extendedprice*100,0) AS BIGINT)",
                               c1="l_discount*100", c2="l_quantity"),
                  source="results/tpch_sf2_arrow/lineitem.parquet (projected cols) + "
                         "results/multibits_tpch/*.i64",
                  source_parquet_bytes=os.path.getsize(
                      os.path.join(RES, "multibits_tpch", "lineitem.parquet"))))
    rng = np.random.default_rng(20261101)
    gn = g.copy()
    mask = rng.random(len(gn)) < 0.05
    gn[mask] = -1
    write_ds("tpch2_null", gn, p, d, q, TPCH_CASES,
             dict(family="tpch-sf2-null-group",
                  columns=dict(g="l_returnflag with 5% NULL (encoded -1)", v="price cents",
                               c1="l_discount*100", c2="l_quantity"),
                  null_mask="deterministic numpy default_rng(20261101) < 0.05",
                  source="same rows as tpch2"))


# -------------------------------------------------------------- synthetic --
def ds_syn_cluster():
    n = 12_000_000
    rng = np.random.default_rng(20261101)
    g = np.empty(n, dtype=np.int64)
    c1 = np.empty(n, dtype=np.int64)
    c2 = np.empty(n, dtype=np.int64)
    # 70% in one cell (g=0,c1=0,c2=1); 25% in a NULL-group cluster; 5% spread out
    n_a, n_b = int(0.70 * n), int(0.25 * n)
    n_c = n - n_a - n_b
    g[:n_a] = 0
    c1[:n_a] = 0
    c2[:n_a] = 1
    g[n_a:n_a + n_b] = -1          # NULL group, own cluster -> one run with 3M rows
    c1[n_a:n_a + n_b] = 0
    c2[n_a:n_a + n_b] = 1
    s = n_a + n_b
    g[s:] = rng.integers(0, 3, n_c)
    c1[s:] = rng.integers(0, 11, n_c)
    c2[s:] = rng.integers(1, 51, n_c)
    v = rng.integers(90_099, 10_489_951, n, dtype=np.int64)
    # the clustered cells get a correlated wide v so ranges are meaningful
    v[:n_a] = rng.integers(3_000_000, 10_489_951, n_a, dtype=np.int64)
    cases = """# synthetic clustered dataset: cell (c1=0,c2=1) holds 70% of the rows in ONE run
# name           v_lo       v_hi  c1_lo c1_hi c2_lo c2_hi
all_full            0   10489951    0     10    1    50
one_huge_run        0   10489951    0      0    1     1
huge_run_window     3000000 6000000 0      0    1     1
spread_rows         0   10489951    1     10    2    50
mid_band       3000000   7000000    3      7   10    40
point          5000000   5000000    0     10    1    50
needle         9000000   9001000    0     10    1    50
empty_cross    9000000   9500000    2      2   40    40
empty_domain  20000000  20001000    0     10    1    50
null_group_scan     0   10489951    0      0    1     1
"""
    write_ds("syn_cluster", g, v, c1, c2, cases,
             dict(family="synthetic-skewed",
                  design="70% of rows in run (g=0,c1=0,c2=1); 25% in the NULL-group cluster; "
                         "flat v",
                  source="generated numpy default_rng(20261101)"))


# -------------------------------------------------------------------- nyc --
def ds_nyc3():
    files = sorted(glob.glob(os.path.join(RES, "nyc_taxi_100m_raw", "yellow_tripdata_*.parquet")))
    gs, vs, c1s, c2s = [], [], [], []
    kept = dropped = 0
    for fp in files:
        t = pq.read_table(fp, columns=["fare_amount", "passenger_count", "payment_type",
                                       "RatecodeID"])
        fare = t.column("fare_amount").to_numpy(zero_copy_only=False).astype(np.float64)
        pax = t.column("passenger_count").to_numpy(zero_copy_only=False).astype(np.float64)
        pay = t.column("payment_type").to_numpy(zero_copy_only=False).astype(np.float64)
        rate = t.column("RatecodeID").to_numpy(zero_copy_only=False).astype(np.float64)
        ok = ~(np.isnan(fare) | np.isnan(pay) | np.isnan(rate))
        gs.append(np.where(np.isnan(pax), -1, np.round(pax)).astype(np.int64)[ok])
        vs.append(np.round(fare * 100).astype(np.int64)[ok])
        c1s.append(np.round(pay).astype(np.int64)[ok])
        c2s.append(np.round(rate).astype(np.int64)[ok])
        kept += int(ok.sum())
        dropped += int((~ok).sum())
    g = np.concatenate(gs)
    v = np.concatenate(vs)
    c1 = np.concatenate(c1s)
    c2 = np.concatenate(c2s)
    # fold the extreme tail (dollar amounts no real query touches) is NOT allowed:
    # exactness needs the full domain, so keep everything.
    cases = standard_cases(v, c1, c2)
    write_ds("nyc3", g, v, c1, c2, cases,
             dict(family="nyc-taxi-3-predicate",
                  columns=dict(g="passenger_count (NULL kept as its own group)",
                               v="round(fare_amount*100) cents",
                               c1="payment_type", c2="RatecodeID"),
                  rows_dropped_null_predicate=dropped,
                  source="results/nyc_taxi_100m_raw/yellow_tripdata_*.parquet (33 files)"))


def ds_nyc_family2():
    """Second query family on the same 100M trips: different group key, different wide
    column and a much larger c2 domain (265 pickup locations), which stresses the dense
    grid (cells = 11 * 265) rather than the ordered column."""
    files = sorted(glob.glob(os.path.join(RES, "nyc_taxi_100m_raw", "yellow_tripdata_*.parquet")))
    gs, vs, c1s, c2s = [], [], [], []
    dropped = 0
    for fp in files:
        t = pq.read_table(fp, columns=["total_amount", "passenger_count", "payment_type",
                                       "PULocationID"])
        tot = t.column("total_amount").to_numpy(zero_copy_only=False).astype(np.float64)
        pax = t.column("passenger_count").to_numpy(zero_copy_only=False).astype(np.float64)
        pay = t.column("payment_type").to_numpy(zero_copy_only=False).astype(np.float64)
        pul = t.column("PULocationID").to_numpy(zero_copy_only=False).astype(np.float64)
        ok = ~(np.isnan(tot) | np.isnan(pax) | np.isnan(pay) | np.isnan(pul))
        gs.append(np.round(pay).astype(np.int64)[ok])
        vs.append(np.round(tot * 100).astype(np.int64)[ok])
        c1s.append(np.round(pax).astype(np.int64)[ok])
        c2s.append(np.round(pul).astype(np.int64)[ok])
        dropped += int((~ok).sum())
    write_ds("nyc3b", np.concatenate(gs), np.concatenate(vs), np.concatenate(c1s),
             np.concatenate(c2s), standard_cases(np.concatenate(vs), np.concatenate(c1s),
                                                 np.concatenate(c2s)),
             dict(family="nyc-taxi-family2",
                  columns=dict(g="payment_type", v="round(total_amount*100) cents",
                               c1="passenger_count", c2="PULocationID (265 values)"),
                  rows_dropped_null_predicate=dropped,
                  source="results/nyc_taxi_100m_raw/yellow_tripdata_*.parquet (33 files)"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datasets", default="tpch2,tpch2_null,syn_cluster,nyc3,nyc3b")
    args = ap.parse_args()
    want = [x.strip() for x in args.datasets.split(",") if x.strip()]
    os.makedirs(OUT, exist_ok=True)
    if any(w.startswith("tpch2") for w in want):
        ds_tpch2()
    if "syn_cluster" in want:
        ds_syn_cluster()
    if "nyc3" in want:
        ds_nyc3()
    if "nyc3b" in want:
        ds_nyc_family2()
    print("prep done ->", OUT)


if __name__ == "__main__":
    main()
