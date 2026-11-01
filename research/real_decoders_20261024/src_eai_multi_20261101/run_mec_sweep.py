#!/usr/bin/env python3
"""MEC01 sweep: run every case on MEC, cross-check exactness against DuckDB 2.0,
re-time the bitplane path and emit one receipt with all three engines.

Latency protocol
  MEC      : one warm process, 1 untimed warmup + REPS timed `mec_query`,
             median and p95 of the in-process query (index mmap'd once, outside
             the timed region).  Reported twice:
               sc   = SUM/COUNT only     -> operand-set match for the bitplane
                                           path and for the SUM/COUNT DuckDB SQL
               full = SUM/COUNT/MIN/MAX
  DuckDB   : produced by bench_duckdb_mec.py (threads=1, warm, execute+fetchall),
             again twice: SUM/COUNT only, and SUM/COUNT/MIN/MAX.
  bitplane : the existing `multibits` CLI re-run BITPLANE_REPS times, using its
             own internal query_ms (timer starts before the mmap; SUM/COUNT only).
  MEC CLI  : the MEC CLI re-run MEC_CLI_REPS times in `sc` mode -- the same
             process-per-query measurement boundary as the bitplane CLI, so the
             page-fault cost of touching a cold 34 MB index is included.

Exactness: each case's full grouped result (g, SUM, COUNT, MIN, MAX) is compared
element-wise with DuckDB's; the bitplane path is compared on (g, SUM, COUNT).
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import argparse
import hashlib
import json
import os
import statistics
import subprocess
import sys


def load_cases(path):
    cases = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            p = line.split()
            if len(p) != 7:
                continue
            cases.append(dict(name=p[0], v=[int(p[1]), int(p[2])], a=[int(p[3]), int(p[4])],
                              b=[int(p[5]), int(p[6])]))
    return cases


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def as_int(x):
    s = str(x)
    if s in ("None", "NULL", "null"):
        return None
    if s.endswith(".00"):
        s = s[:-3]
    try:
        return int(s)
    except ValueError:
        return int(round(float(s)))


def canon(rows):
    return sorted(tuple(as_int(x) for x in r) for r in rows)


def stats(vals):
    s = sorted(vals)
    return dict(median_ms=statistics.median(vals), min_ms=s[0],
                p95_ms=s[min(len(s) - 1, int(len(s) * 0.95))],
                mean_ms=statistics.fmean(vals), reps=len(vals), raw_ms=vals)


def run_loop(cmd, reps, parse):
    out = []
    for _ in range(reps):
        p = subprocess.run(cmd, capture_output=True, text=True)
        if p.returncode != 0:
            raise SystemExit(f"failed ({cmd[0]}): {p.stderr[-400:]}")
        out.append(parse(p))
    return stats(out), " ".join(cmd)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mec", required=True)
    ap.add_argument("--index", required=True)
    ap.add_argument("--cases", required=True)
    ap.add_argument("--duckdb-json", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--reps", type=int, default=100)
    ap.add_argument("--bitplane", default="")
    ap.add_argument("--bitplane-index", default="")
    ap.add_argument("--bitplane-reps", type=int, default=50)
    ap.add_argument("--mec-cli-reps", type=int, default=50)
    ap.add_argument("--source-bytes", type=int, default=0)
    ap.add_argument("--raw-columns-bytes", type=int, default=0)
    ap.add_argument("--build-json", default="")
    ap.add_argument("--bitplane-build-json", default="")
    ap.add_argument("--pin", default="")
    args = ap.parse_args()

    cases = load_cases(args.cases)
    duck = json.load(open(args.duckdb_json))

    p = subprocess.run([args.mec, "bench", args.index, str(args.reps), args.cases],
                       capture_output=True, text=True)
    if p.returncode != 0:
        raise SystemExit(f"mec bench failed: {p.stderr[-800:]}")
    mec = json.loads(p.stdout)
    mec_info = json.loads(subprocess.run([args.mec, "info", args.index], capture_output=True,
                                         text=True).stdout)
    build = json.load(open(args.build_json)) if args.build_json else {}
    bp_build = {}
    if args.bitplane_build_json and os.path.exists(args.bitplane_build_json):
        try:
            bp_build = json.load(open(args.bitplane_build_json))
        except Exception:
            bp_build = {}

    out = dict(
        protocol=dict(
            mec=(f"warm process, 1 warmup + {args.reps} timed queries; median/p95 of the "
                 "in-process query; index mmap'd once outside the timed region"),
            mec_cli=(f"{args.mec_cli_reps}x `mec query ... sc`: process per query, timer covers "
                     "mmap + query (same boundary as the bitplane CLI)"),
            duckdb=duck.get("protocol"),
            bitplane=f"{args.bitplane_reps}x CLI, internal query_ms",
            note="all ends exclude result serialisation",
            pin=(args.pin or "nice -n 5 taskset -c 64"),
            cases_file=os.path.abspath(args.cases),
        ),
        artifacts=dict(
            mec_index=dict(path=args.index, bytes=os.path.getsize(args.index),
                           sha256=sha256(args.index)),
            mec_binary=os.path.abspath(args.mec),
            mec_binary_sha256=sha256(args.mec),
            cases=os.path.abspath(args.cases),
            duckdb_json=os.path.abspath(args.duckdb_json),
            bitplane_index=(dict(path=args.bitplane_index,
                                 bytes=os.path.getsize(args.bitplane_index),
                                 sha256=sha256(args.bitplane_index))
                            if args.bitplane_index else None),
        ),
        storage=dict(
            mec_bytes=mec_info["bytes"],
            mec_sections=dict(cumc=mec_info["offsets"]["cumc"],
                              cumsum=mec_info["offsets"]["cumsum"],
                              offs=mec_info["offsets"]["offs"],
                              header=mec_info["offsets"]["header"]),
            mec_shape=dict(rows=mec_info["rows"], G=mec_info["G"], D1=mec_info["D1"],
                           D2=mec_info["D2"], cells=mec_info["cells"], W=mec_info["W"],
                           BP=mec_info["BP"]),
            source_bytes=args.source_bytes or None,
            raw_columns_bytes=args.raw_columns_bytes or None,
            bitplane_bytes=(os.path.getsize(args.bitplane_index) if args.bitplane_index else None),
        ),
        build=dict(mec=build, bitplane=bp_build),
        cases={},
    )
    exact_all = True
    print(f"{'case':20s} {'MEC sc':>9s} {'p95':>8s} {'MEC full':>9s} {'MECcli':>9s} "
          f"{'bitplane':>9s} {'duck sc':>9s} {'duck mm':>9s} {'x duck':>9s} {'x bp':>9s} exact")
    for c in cases:
        name = c["name"]
        d = duck["cases"][name]
        m = mec["cases"][name]
        sc = m["sc"]
        params = c["v"] + c["a"] + c["b"]
        # DuckDB's minmax SQL projects (g, SUM, MIN, MAX, COUNT); reorder to
        # (g, SUM, COUNT, MIN, MAX) so both sides are compared element-wise.
        duck_full = [(r[0], r[1], r[4], r[2], r[3]) for r in d["rows"]]
        duck_sc = [(r[0], r[1], r[4]) for r in d["rows"]]
        mec_full = [(r["g"], r["sum"], r["count"], r["min"], r["max"]) for r in m["rows"]]
        exact_full = canon(duck_full) == canon(mec_full)
        bits = dict(rows_vs_duckdb=exact_full)

        rec = dict(
            params=dict(v=c["v"], a=c["a"], b=c["b"]),
            mec=dict(median_us=m["median_us"], p05_us=m["p05_us"], p95_us=m["p95_us"],
                     min_us=m["min_us"], max_us=m["max_us"], mean_us=m["mean_us"],
                     rows=m["rows"]),
            mec_sc=dict(median_us=sc["median_us"], p05_us=sc["p05_us"], p95_us=sc["p95_us"],
                        min_us=sc["min_us"], max_us=sc["max_us"], mean_us=sc["mean_us"]),
            duckdb=dict(sum_count=dict(median_ms=d["sum_count"]["median_ms"],
                                       min_ms=d["sum_count"]["min_ms"],
                                       p95_ms=d["sum_count"]["p95_ms"],
                                       raw_ms=d["sum_count"]["raw_ms"],
                                       sql=d["sql_sum_count"]),
                        minmax=dict(median_ms=d["minmax"]["median_ms"],
                                    min_ms=d["minmax"]["min_ms"],
                                    p95_ms=d["minmax"]["p95_ms"],
                                    raw_ms=d["minmax"]["raw_ms"],
                                    sql=d["sql_minmax"]),
                        rows=d["rows"]),
            exact=bits,
        )
        cli_ms = None
        if args.mec_cli_reps > 0:
            cli, cli_cmd = run_loop(
                [args.mec, "query", args.index] + [str(x) for x in params] + ["sc"],
                args.mec_cli_reps,
                lambda pr: json.loads(pr.stderr.strip().splitlines()[-1])["query_ms"])
            rec["mec_cli"] = dict(median_ms=cli["median_ms"], min_ms=cli["min_ms"],
                                  p95_ms=cli["p95_ms"], raw_ms=cli["raw_ms"], cmd=cli_cmd)
            cli_ms = cli["median_ms"]
        bp_ms = None
        if args.bitplane:
            tmp = "/tmp/_mec_bp_rows.json"
            bp, bp_cmd = run_loop(
                [args.bitplane, "query", args.bitplane_index, tmp] + [str(x) for x in params],
                args.bitplane_reps,
                lambda pr: json.loads(pr.stderr.strip().splitlines()[-1])["query_ms"])
            blob = json.load(open(tmp))
            bp_rows = blob["rows"] if isinstance(blob, dict) else blob
            rec["bitplane"] = dict(median_ms=bp["median_ms"], min_ms=bp["min_ms"],
                                   p95_ms=bp["p95_ms"], raw_ms=bp["raw_ms"], cmd=bp_cmd)
            rec["exact"]["rows_vs_bitplane"] = canon(bp_rows) == canon(duck_sc)
            bp_ms = bp["median_ms"]

        rec["speedup"] = dict(
            vs_duckdb_sum_count=d["sum_count"]["median_ms"] / (sc["median_us"] / 1000.0),
            vs_duckdb_minmax=d["minmax"]["median_ms"] / (m["median_us"] / 1000.0),
            vs_bitplane=(bp_ms / (sc["median_us"] / 1000.0)) if bp_ms else None,
            vs_duckdb_sum_count_cli=(d["sum_count"]["median_ms"] / cli_ms) if cli_ms else None,
            vs_bitplane_cli=(bp_ms / cli_ms) if (bp_ms and cli_ms) else None,
        )
        exact_all = exact_all and exact_full and rec["exact"].get("rows_vs_bitplane", True)
        out["cases"][name] = rec
        print(f"{name:20s} {sc['median_us']:8.2f}u {sc['p95_us']:7.2f}u "
              f"{m['median_us']:8.2f}u {((cli_ms * 1000) if cli_ms else float('nan')):8.1f}u "
              f"{(bp_ms if bp_ms else float('nan')):8.2f}m "
              f"{d['sum_count']['median_ms']:8.2f}m {d['minmax']['median_ms']:8.2f}m "
              f"{rec['speedup']['vs_duckdb_sum_count']:8.1f}x "
              f"{(rec['speedup']['vs_bitplane'] or float('nan')):8.1f}x "
              f"{'yes' if exact_full else 'NO'}")

    def rng(key):
        vals = [out["cases"][c["name"]]["speedup"][key] for c in cases
                if out["cases"][c["name"]]["speedup"].get(key)]
        return [min(vals), max(vals)] if vals else [None, None]

    bb = out["storage"]["bitplane_bytes"]
    out["summary"] = dict(
        exact_all=exact_all,
        mec_sc_median_us={c["name"]: out["cases"][c["name"]]["mec_sc"]["median_us"] for c in cases},
        mec_full_median_us={c["name"]: out["cases"][c["name"]]["mec"]["median_us"] for c in cases},
        mec_cli_median_ms={c["name"]: out["cases"][c["name"]]["mec_cli"]["median_ms"]
                           for c in cases if "mec_cli" in out["cases"][c["name"]]},
        speedup_vs_duckdb_sum_count=rng("vs_duckdb_sum_count"),
        speedup_vs_duckdb_minmax=rng("vs_duckdb_minmax"),
        speedup_vs_bitplane=rng("vs_bitplane"),
        speedup_vs_bitplane_cli=rng("vs_bitplane_cli"),
        mec_bytes=out["storage"]["mec_bytes"],
        mec_bytes_over_source=(out["storage"]["mec_bytes"] / args.source_bytes
                               if args.source_bytes else None),
        mec_bytes_over_raw_columns=(out["storage"]["mec_bytes"] / args.raw_columns_bytes
                                    if args.raw_columns_bytes else None),
        bitplane_bytes=bb,
        bitplane_bytes_over_source=((bb / args.source_bytes)
                                    if (args.source_bytes and bb) else None),
    )
    json.dump(out, open(args.out, "w"), indent=1)
    print(json.dumps(out["summary"], indent=1))
    print(f"wrote {args.out}")
    if not exact_all:
        sys.exit(3)


if __name__ == "__main__":
    main()
