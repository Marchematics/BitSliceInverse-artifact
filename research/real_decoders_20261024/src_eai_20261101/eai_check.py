#!/usr/bin/env python3
"""Exactness check: compare one ECUBE01 query against DuckDB 2.0 on the same data.

usage: eai_check.py CUBE INDEX DICT CUBE_OUT DB TABLE GROUP_COL VALUE_COL SCALE [spec...]

SCALE converts the stored integer payload back to the SQL value unit (100 for
TPC-H DECIMAL(15,2) cents, 1 for NYC fare_cents). Specs use the stored integer
unit; the reference SQL multiplies the column by SCALE so the predicates and the
projected aggregates are compared in the identical unit.
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import json
import subprocess
import sys

import duckdb

cube, index, dic, cube_out, db, table, gcol, vcol, scale = sys.argv[1:10]
scale = int(scale)
spec = sys.argv[10:]

con = duckdb.connect(db, read_only=True)
con.execute("SET threads=1")
where = []
have = []
for s in spec:
    if s.startswith("range="):
        lo, hi = s[6:].split(",")
        where.append(f"({vcol}*{scale}) BETWEEN {lo} AND {hi}")
    elif s.startswith("hi="):
        where.append(f"({vcol}*{scale}) <= {s[3:]}")
    elif s.startswith("lo="):
        where.append(f"({vcol}*{scale}) >= {s[3:]}")
    elif s.startswith("where_in="):
        vals = [v for v in s[9:].split(",")]
        quoted = ",".join(v if v.isdigit() else "'" + v.replace("'", "''") + "'" for v in vals)
        where.append(f"{gcol} IN ({quoted})")
    elif s.startswith("where_not_in="):
        vals = [v for v in s[13:].split(",")]
        quoted = ",".join(v if v.isdigit() else "'" + v.replace("'", "''") + "'" for v in vals)
        where.append(f"{gcol} NOT IN ({quoted})")
    elif s.startswith("having="):
        have.append(s[7:])
def xlate(cond):
    for k in ("sum", "count", "min", "max"):
        if cond.startswith(k):
            rest = cond[len(k):]
            agg = {"sum": f"SUM({vcol}*{scale})", "count": "COUNT(*)",
                   "min": f"MIN({vcol}*{scale})", "max": f"MAX({vcol}*{scale})"}[k]
            return agg + rest
    return cond


sql = (f"SELECT {gcol}, CAST(SUM({vcol}*{scale}) AS BIGINT), COUNT(*), "
       f"CAST(MIN({vcol}*{scale}) AS BIGINT), CAST(MAX({vcol}*{scale}) AS BIGINT) FROM {table} "
       + ("WHERE " + " AND ".join(where) + " " if where else "")
       + "GROUP BY 1 "
       + ("HAVING " + " AND ".join(xlate(h) for h in have) + " " if have else "")
       + "ORDER BY 1")
rows = con.execute(sql).fetchall()
subprocess.run([cube, "query", index, dic, cube_out] + spec, check=True, capture_output=True,
               text=True)
cube_rows = json.load(open(cube_out))["rows"]


def norm(x):
    x = str(x)
    return "None" if x in ("None", "NULL", "null") else x


a = sorted([[norm(r[0])] + [str(v) for v in r[1:]] for r in cube_rows])
b = sorted([[norm(r[0])] + [str(v) for v in r[1:]] for r in rows])
print("SQL:", sql)
if a == b:
    print(f"PASS rows={len(a)}")
else:
    print(f"FAIL cube={len(a)} duckdb={len(b)}")
    for row in b:
        if row not in a:
            print("  only duckdb:", row)
    for row in a:
        if row not in b:
            print("  only cube  :", row)
    sys.exit(1)
