#!/usr/bin/env python3
"""Single definition of the mixed-workload panel, shared by every engine.

Each case is expressed three ways so that the *same* logical query can be issued
through the cube (spec grammar), through a native DuckDB scan (WHERE/HAVING
clause on the `nyc` table), and through a materialized view (HAVING as a WHERE
predicate on the materialized aliases s/c/mn/mx).

Columns of panel.tsv:  name <TAB> cube_spec <TAB> scan_where <TAB> scan_having

The scan SQL is
    SELECT passenger_count g, SUM(fare_cents) s, COUNT(*) c,
           MIN(fare_cents) mn, MAX(fare_cents) mx
    FROM nyc [WHERE <scan_where>] GROUP BY 1 [HAVING <scan_having>] ORDER BY 1
and the cube spec is the same predicate set pushed into the index.
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import os

PANEL = [
    # name        cube spec                                     scan WHERE                                    scan HAVING
    ("fixed",     "",                                          "",                                           ""),
    ("le_m720",   "hi=-720",                                   "fare_cents <= -720",                         ""),
    ("le_930",    "hi=930",                                    "fare_cents <= 930",                          ""),
    ("range_mid", "range=650,1350",                            "fare_cents BETWEEN 650 AND 1350",            ""),
    ("in_1_2",    "where_in=1,2",                              "passenger_count IN (1,2)",                   ""),
    ("notin_0_1", "where_not_in=0,1",                          "passenger_count NOT IN (0,1)",               ""),
    ("having_sum", "having=sum>=1e10",                         "",                                           "s >= 1e10"),
    ("multi",     "where_in=1,2;range=0,1000;having=count>=1000",
                  "passenger_count IN (1,2) AND fare_cents BETWEEN 0 AND 1000", "c >= 1000"),
    ("narrow",    "range=80000000,90000000",                   "fare_cents BETWEEN 80000000 AND 90000000",    ""),
]

COLUMNS = ("name", "cube_spec", "scan_where", "scan_having")


def write_tsv(path):
    with open(path, "w") as f:
        for row in PANEL:
            assert len(row) == 4, row
            f.write("\t".join(row) + "\n")
    return path


def read_tsv(path):
    out = []
    with open(path) as f:
        for line in f:
            line = line.rstrip("\n")
            if not line or line.startswith("#"):
                continue
            fld = line.split("\t")
            while len(fld) < 4:
                fld.append("")
            out.append(dict(zip(COLUMNS, fld)))
    return out


SCAN_SQL = ("SELECT passenger_count AS g, SUM(fare_cents) AS s, COUNT(*) AS c, "
            "MIN(fare_cents) AS mn, MAX(fare_cents) AS mx FROM nyc {where} GROUP BY 1 {having}")


def scan_sql(case, order_by=True):
    w = ("WHERE " + case["scan_where"] + " ") if case["scan_where"] else ""
    h = ("HAVING " + case["scan_having"]) if case["scan_having"] else ""
    sql = SCAN_SQL.format(where=w, having=h)
    return sql + (" ORDER BY 1" if order_by else "")


def mv_sql(case, mv):
    q = f"SELECT g, s, c, mn, mx FROM {mv}"
    if case["scan_having"]:
        q += " WHERE " + case["scan_having"]
    return q + " ORDER BY g"


if __name__ == "__main__":
    import sys
    out = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(__file__), "panel.tsv")
    write_tsv(out)
    print(f"wrote {out} ({len(PANEL)} cases)")
    for c in read_tsv(out):
        print(f"  {c['name']:12s} cube[{c['cube_spec']}] scan[{scan_sql(c)}]")
