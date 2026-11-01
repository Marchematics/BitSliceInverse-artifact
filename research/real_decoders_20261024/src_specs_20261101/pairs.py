"""Dataset / column-pair definitions for the SPECS exactness workstream (2026-11-01).

Two groups:
  CORPUS_PAIRS  - large real datasets, reference SQL runs on the original DuckDB
                  database files (NYC 100M, NYC 12M, TPC-H SF2 12M).
  FUZZ_PAIRS    - 1M-row narrow tables materialised once in a local DuckDB file
                  (deterministic reservoir samples of the same sources) plus
                  handcrafted adversarial tables.  The reference engine runs on
                  these narrow tables; the cube is built from the same rows.

Every pair reduces to a logical table with exactly two columns (g, v):
  g : group key, non-NULL values rendered as canonical strings
  v : int64 measure, no NULLs, must fit in int32
The cube is built from int32 exports of (dense-rank(g), v).
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
DATA = _artifact.CODE + "/results"
BASE = _artifact.WORK + "/specs"
REPO = _artifact.ROOT

NYC100 = f"{DATA}/nyc_taxi_100m_duckdb/nyc.duckdb"
NYC2023 = f"{DATA}/nyc_taxi_2023_duckdb/nyc.duckdb"
TPCH = f"{DATA}/tpch_sf2_lineitem_duckdb/lineitem.duckdb"
EVAL = f"{BASE}/tables/eval.duckdb"

EPX = "CAST(l_extendedprice*100 AS BIGINT)"   # TPC-H extended price, cents
QTY = "CAST(l_quantity*100 AS BIGINT)"        # TPC-H quantity, cents
TAX = "CAST(l_tax*100 AS BIGINT)"             # TPC-H tax, cents

CORPUS_PAIRS = [
    dict(id="C1", label="nyc100_passenger_fare", db=NYC100, src="nyc",
         g_expr="passenger_count", v_expr="fare_cents", g_type="int",
         note="NYC 100M rows; 9,606,243 NULL group keys; 21,084 distinct values; non-dense"),
    dict(id="C2", label="nyc2023_passenger_fare", db=NYC2023, src="nyc",
         g_expr="passenger_count", v_expr="fare_cents", g_type="int",
         note="NYC 12M rows; 236,179 NULL group keys; non-dense"),
    dict(id="C3", label="tpch_returnflag_price", db=TPCH, src="lineitem",
         g_expr="l_returnflag", v_expr=EPX, g_type="str",
         note="TPC-H SF2 12M; 3 groups; 984,297 distinct values; non-dense"),
    dict(id="C4", label="tpch_shipmode_price", db=TPCH, src="lineitem",
         g_expr="l_shipmode", v_expr=EPX, g_type="str",
         note="TPC-H SF2; 7 string groups with spaces (REG AIR)"),
    dict(id="C5", label="tpch_linenumber_qty", db=TPCH, src="lineitem",
         g_expr="l_linenumber", v_expr=QTY, g_type="int",
         note="TPC-H SF2; 7 groups; value cents of l_quantity"),
    dict(id="C6", label="tpch_partkey_qty_highcard", db=TPCH, src="lineitem",
         g_expr="l_partkey", v_expr=QTY, g_type="int",
         note="TPC-H SF2; 400,000 groups (high cardinality), V=50"),
    dict(id="C7", label="tpch_orderkey_linenumber", db=TPCH, src="lineitem",
         g_expr="l_orderkey", v_expr="l_linenumber", g_type="int",
         note="TPC-H SF2; 3,000,000 groups (high cardinality); V=7; dense value domain"),
    dict(id="C8", label="tpch_nullable_shipmode", db=TPCH, src="lineitem",
         g_expr="CASE WHEN l_orderkey % 97 = 0 THEN NULL ELSE l_shipmode END",
         v_expr=EPX, g_type="str",
         note="TPC-H SF2; synthetic nullable group key (1/97 rows NULL)"),
    dict(id="C9", label="tpch_shipinstruct_tax", db=TPCH, src="lineitem",
         g_expr="l_shipinstruct", v_expr=TAX, g_type="str",
         note="TPC-H SF2; 4 string groups; V=9 contiguous cents -> dense path"),
    dict(id="C10", label="tpch_negative_group", db=TPCH, src="lineitem",
         g_expr="l_orderkey % 5 - 2", v_expr=EPX, g_type="int",
         note="TPC-H SF2; group values -2..2 (negative group names)"),
    dict(id="C11", label="tpch_gappy_dense", db=TPCH, src="lineitem",
         g_expr="l_partkey % 1000", v_expr="7*(l_partkey % 100)", g_type="int",
         note="ADVERSARIAL: V=100, observed span 694 <= V*8=800 so the original "
              "build_plain heuristic turns dense mode on, but the observed set "
              "(multiples of 7 in 0..693) is gappy -> exercises the dense-rank bug"),
]

# encoded-tree build parameters for the pairs that have a fused encoded tree
# (column ids from <prep>/Observed/columns.tsv: 17 = group, 13 = value)
ENC_PREPS = {
    "C1": ("nyc_taxi_100m_prep", 17, 13),
    "C2": ("nyc_taxi_2023_prep", 17, 13),
    "C3": ("tpch_sf2_prep", 17, 13),
}

# ---------------------------------------------------------------------------
# fuzz narrow tables (1,000,000 rows unless noted), created in EVAL
# ---------------------------------------------------------------------------
FUZZ_TABLES = [
    ("f_nyc1m", "SELECT passenger_count AS g, fare_cents AS v FROM src_nyc.nyc "
                "USING SAMPLE 1000000 ROWS (reservoir, 42)",
     "NYC 2023 12M reservoir sample; NULL group keys; non-dense"),
    ("f_ret", "SELECT l_returnflag AS g, " + EPX + " AS v FROM src_tpch.lineitem "
              "USING SAMPLE 1000000 ROWS (reservoir, 43)",
     "TPC-H returnflag; 3 groups; non-dense"),
    ("f_ship", "SELECT l_shipmode AS g, " + EPX + " AS v FROM src_tpch.lineitem "
               "USING SAMPLE 1000000 ROWS (reservoir, 44)",
     "TPC-H shipmode; string groups with spaces"),
    ("f_linenum", "SELECT l_linenumber AS g, l_linenumber AS v FROM src_tpch.lineitem "
                  "USING SAMPLE 1000000 ROWS (reservoir, 45)",
     "dense value domain 1..7 (valid dense path)"),
    ("f_gappy", "SELECT l_partkey % 1000 AS g, 7*(l_partkey % 100) AS v FROM src_tpch.lineitem "
                "USING SAMPLE 1000000 ROWS (reservoir, 46)",
     "ADVERSARIAL gappy observed domain (V=100, span 694), dense heuristic fires"),
    ("f_neg", "SELECT l_shipmode AS g, -" + EPX + " AS v FROM src_tpch.lineitem "
              "USING SAMPLE 1000000 ROWS (reservoir, 47)",
     "negative values; non-dense"),
    ("f_single", "SELECT l_partkey % 5 AS g, 1234 AS v FROM src_tpch.lineitem "
                 "USING SAMPLE 1000000 ROWS (reservoir, 48)",
     "single observed value (V=1, dense valid)"),
    ("f_wide", "SELECT l_partkey % 100 AS g, " + EPX + " AS v FROM src_tpch.lineitem "
               "USING SAMPLE 1000000 ROWS (reservoir, 49)",
     "100 groups, wide value domain (~400k distinct)"),
    ("f_nullg", "SELECT CASE WHEN l_orderkey % 101 = 0 THEN NULL ELSE l_shipmode END AS g, "
                + EPX + " AS v FROM src_tpch.lineitem USING SAMPLE 1000000 ROWS (reservoir, 50)",
     "synthetic nullable string group key"),
    ("f_highcard", "SELECT l_orderkey % 50000 AS g, l_linenumber AS v FROM src_tpch.lineitem "
                   "USING SAMPLE 1000000 ROWS (reservoir, 51)",
     "~50k groups; dense value domain 1..7"),
    ("f_onerow", "SELECT 'a' AS g, 7 AS v FROM range(1) t(i)",
     "one-row table (edge: n=1, G=1, V=1)"),
    ("f_small", "SELECT CASE WHEN i % 11 = 0 THEN NULL ELSE i % 5 END AS g, "
                "CASE WHEN i % 1000 = 0 THEN 2147483647 WHEN i % 1000 = 1 THEN -2147483648 "
                "ELSE CAST(i % 37 AS BIGINT) - 18 END AS v FROM range(2000) t(i)",
     "handcrafted 2000 rows: INT32_MIN/MAX values, NULL group, negative values"),
]

FUZZ_PAIRS = [
    dict(id="F1", table="f_nyc1m", g_type="int", label="nyc1m"),
    dict(id="F2", table="f_ret", g_type="str", label="tpch_retflag"),
    dict(id="F3", table="f_ship", g_type="str", label="tpch_shipmode"),
    dict(id="F4", table="f_linenum", g_type="int", label="dense_linenumber"),
    dict(id="F5", table="f_gappy", g_type="int", label="gappy_dense"),
    dict(id="F6", table="f_neg", g_type="str", label="negative_values"),
    dict(id="F7", table="f_single", g_type="int", label="single_value"),
    dict(id="F8", table="f_wide", g_type="int", label="wide_domain"),
    dict(id="F9", table="f_nullg", g_type="str", label="nullable_group"),
    dict(id="F10", table="f_highcard", g_type="int", label="highcard_50k"),
    dict(id="F11", table="f_small", g_type="int", label="edge_small"),
    dict(id="F12", table="f_onerow", g_type="str", label="one_row"),
]
_fuzz_notes = {tid: note for tid, _, note in FUZZ_TABLES}
for p in FUZZ_PAIRS:
    p["db"] = EVAL
    p["src"] = p["table"]
    p["g_expr"] = "g"
    p["v_expr"] = "v"
    p["note"] = _fuzz_notes[p["table"]]
