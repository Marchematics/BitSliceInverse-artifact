#!/usr/bin/env python3
"""Run-to-run dispersion for MEC01: N independent `mec bench` processes, then
report per-case median/spread for both operand sets (SUM/COUNT and MIN/MAX)."""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import argparse
import json
import statistics
import subprocess


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mec", required=True)
    ap.add_argument("--index", required=True)
    ap.add_argument("--cases", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--reps", type=int, default=200)
    args = ap.parse_args()

    runs = []
    cmds = []
    for _ in range(args.runs):
        cmd = [args.mec, "bench", args.index, str(args.reps), args.cases]
        p = subprocess.run(cmd, capture_output=True, text=True)
        if p.returncode != 0:
            raise SystemExit(f"mec bench failed: {p.stderr[-400:]}")
        runs.append(json.loads(p.stdout))
        cmds.append(" ".join(cmd))

    names = list(runs[0]["cases"].keys())
    out = dict(runs=args.runs, reps_per_run=args.reps, cmds=cmds, cases={})
    for n in names:
        sc = [r["cases"][n]["sc"]["median_us"] for r in runs]
        full = [r["cases"][n]["median_us"] for r in runs]
        out["cases"][n] = dict(
            sc_medians_us=sc, sc_median_us=statistics.median(sc),
            sc_spread_pct=(max(sc) - min(sc)) / statistics.median(sc) * 100.0,
            full_medians_us=full, full_median_us=statistics.median(full),
            full_spread_pct=(max(full) - min(full)) / statistics.median(full) * 100.0,
        )
        print(f"{n:20s} sc {statistics.median(sc):8.2f}us spread {out['cases'][n]['sc_spread_pct']:5.1f}%"
              f"   full {statistics.median(full):8.2f}us spread "
              f"{out['cases'][n]['full_spread_pct']:5.1f}%")
    json.dump(out, open(args.out, "w"), indent=1)
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
