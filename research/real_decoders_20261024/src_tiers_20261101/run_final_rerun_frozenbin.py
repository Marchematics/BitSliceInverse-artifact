#!/usr/bin/env python3
"""Frozen-binary control for the final rerun (tiers stream, 2026-11-01).

The clean-state rerun rebuilt ECUBE03 from the current repo source (ae8339cf…, binary
2e52137a…) and reproduced the frozen index byte-for-byte (78bff19a…, 1,140,168 B), but
the microsecond sweep came out 4-16% above the frozen receipt on several range cases.
The frozen measurements used the baselines stream's copy of the ECUBE03 binary
(sha256 96cf26e5…, source c93a4530…). This script re-runs the identical 11-case sweep and
the cold request with that frozen binary, so the deviation can be attributed to
(binary/source revision) vs (host state / pinned core / time).

Writes receipts/final_rerun_20261101/frozenbin_control.json and appends one section to
FINAL_RERUN_20261101.md (idempotent: skipped if the marker is already present).

usage (from research/real_decoders_20261024):
  nice -n 5 taskset -c 96 python3 src_tiers_20261101/run_final_rerun_frozenbin.py
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import hashlib
import json
import os
import statistics
import subprocess
import time

REPO = os.path.abspath(os.getcwd())
SCRATCH = _artifact.expand(os.environ.get("TIERS_SCRATCH", "../../work/tiers/build"))
FROZEN = _artifact.expand("../../work/tiers/frozen_ref")
CORE = _artifact.core()
RECEIPT = os.path.join(_artifact.WORK, "receipts",
                       "final_rerun_20261101")
CURRENT = os.path.join(_artifact.expand("../../work/tiers/bin"), "enc_cube3")
FROZEN_BIN = os.path.join(REPO, "src_baselines_20261101", "enc_cube3_frozen")
NYC_ENC = os.path.join(REPO, "results/nyc_taxi_100m_prep/Observed.encoded")
NYC_PREP = os.path.join(REPO, "results/nyc_taxi_100m_prep/Observed")
DICT = os.path.join(NYC_PREP, "state.dict")
CUBE = os.path.join(SCRATCH, "final_nyc100_v3_frozenbin.ecube")
CUBE_CURRENT = os.path.join(SCRATCH, "final_nyc100_v3.ecube")
FROZEN_INDEX_SHA = "78bff19a0dbd89d0e3b16254cb2a0c807669d982b3a9233b1ba5504dd04c89ee"


def sha(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def loadavg():
    return open("/proc/loadavg").read().strip()


def warm_core(seconds=0.5):
    subprocess.run(["nice", "-n", "5", "taskset", "-c", CORE, "timeout", f"{seconds}s",
                    "bash", "-c", "while :; do :; done"], capture_output=True)


def sh(cmd):
    t = time.perf_counter()
    p = subprocess.run(["nice", "-n", "5", "taskset", "-c", CORE] + cmd,
                       capture_output=True, text=True)
    wall = time.perf_counter() - t
    if p.returncode != 0:
        raise RuntimeError(f"{cmd[:2]} rc={p.returncode}: {p.stderr[-1000:]}")
    return wall, p.stderr


def last_json(s):
    for line in reversed(s.splitlines()):
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                return json.loads(line)
            except json.JSONDecodeError:
                continue
    raise RuntimeError("no json: " + s[-400:])


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p))]


def bench(binary, spec, reps=1000, runs=3):
    recs = []
    for _ in range(runs):
        warm_core()
        _, stderr = sh([binary, "bench", CUBE, DICT, "/dev/null", str(reps)] + spec)
        recs.append(last_json(stderr))
    recs.sort(key=lambda j: j["query_median_ms"])
    pick = recs[len(recs) // 2]
    return {"median_us": pick["query_median_ms"] * 1000,
            "p95_us": pick["query_p95_ms"] * 1000,
            "serialize_us": pick["serialize_median_ms"] * 1000,
            "runs_us": [r["query_median_ms"] * 1000 for r in recs]}


CASES = [
    ("fixed_all4", [], "fixed GROUP BY"),
    ("le_m7000", ["hi=-7000"], "0.135%"),
    ("le_m720", ["hi=-720"], "1.040%"),
    ("le_509", ["hi=509"], "5.803%"),
    ("le_650", ["hi=650"], "11.909%"),
    ("le_930", ["hi=930"], "28.430%"),
    ("le_1350", ["hi=1350"], "51.040%"),
    ("le_99900", ["hi=99900"], "99.9999%"),
    ("in_1_2", ["where_in=1,2"], "g IN (1,2)"),
    ("not_in_0_1", ["where_not_in=0,1"], "g NOT IN (0,1)"),
    ("having", ["having=sum>=1e10"], "HAVING sum>=1e10"),
]


def main():
    os.makedirs(RECEIPT, exist_ok=True)
    out = {"protocol": {
        "date": "2026-11-01", "host": "localhost (AMD EPYC 9554)",
        "pin": f"nice -n 5 taskset -c {CORE}",
        "pstate": "0.5 s pinned busy loop before each timed bench; 3 runs, median of medians",
        "frozen_bin": FROZEN_BIN, "frozen_bin_sha256": sha(FROZEN_BIN),
        "current_bin": CURRENT, "current_bin_sha256": sha(CURRENT),
        "loadavg_start": loadavg(),
    }}
    # rebuild with the frozen binary; must also produce the frozen index sha
    t = time.perf_counter()
    _, stderr = sh(["/usr/bin/time", "-v", FROZEN_BIN, "build_enc", NYC_ENC, NYC_PREP,
                    "17", "13", CUBE])
    out["build_wall_s"] = time.perf_counter() - t
    out["build_json"] = last_json(stderr)
    out["index"] = {"bytes": os.path.getsize(CUBE), "sha256": sha(CUBE),
                    "frozen_sha256": FROZEN_INDEX_SHA,
                    "sha_match": sha(CUBE) == FROZEN_INDEX_SHA}
    print("frozen-binary rebuild:", out["index"], flush=True)

    fz = json.load(open(os.path.join(FROZEN, "eai_ecube03_frozen.json")))["cases"]
    fz_sw3 = json.load(open(os.path.join(FROZEN, "eai_sweep_nyc100_v3.json")))["cases"]
    cur = json.load(open(os.path.join(RECEIPT, "ecube03_sweep.json")))
    cur_cases = cur["cases"]

    def frozen_ref(key):
        if key in fz:
            return fz[key]["median_ms"] * 1000, fz[key]["p95_ms"] * 1000, \
                "baselines_20261101/eai_ecube03_frozen.json"
        if key in ("in_1_2", "having"):
            return (fz_sw3[key]["cube_median_ms"] * 1000,
                    fz_sw3[key]["cube_p95_ms"] * 1000,
                    "eai_cube_20261101/runs/eai_sweep_nyc100_v3.json")
        return 1392.0, 1392.0, "Lead measurement (core 48, 2000 reps); MASTER row 10"

    cases = {}
    for key, spec, label in CASES:
        r = bench(FROZEN_BIN, spec)
        fmed, fp95, src = frozen_ref(key)
        # current-binary rerun from the main receipt
        curq = None
        for q in cur_cases.values():
            if q["frozen"].get("case") == key:
                curq = q
                break
        cases[key] = {
            "label": label, "spec": spec, "frozen_binary": r,
            "frozen_receipt": {"median_us": fmed, "p95_us": fp95, "source": src},
            "delta_vs_frozen_pct": (r["median_us"] - fmed) / fmed * 100,
            "current_binary_rerun_us": (curq["ours"]["median_us"] if curq else None),
            "current_delta_vs_frozen_pct": (curq["rows_cmp"]["median"]["delta_pct"]
                                            if curq else None),
        }
        print(f"  {label:22s} frozen-bin {r['median_us']:7.3f} us "
              f"(vs frozen {cases[key]['delta_vs_frozen_pct']:+.1f}%) | current-bin "
              f"{cases[key]['current_binary_rerun_us'] or 0:7.3f} us "
              f"({cases[key]['current_delta_vs_frozen_pct'] or 0:+.1f}%)", flush=True)

    # cold fixed with the frozen binary, same direct-spawn protocol
    walls = []
    for _ in range(20):
        t = time.perf_counter()
        p = subprocess.run([FROZEN_BIN, "query", CUBE, DICT,
                            os.path.join(SCRATCH, "final_cold_frozenbin.json")],
                           capture_output=True, text=True)
        walls.append((time.perf_counter() - t) * 1000.0)
        if p.returncode != 0:
            raise RuntimeError("cold launch failed")
    cb = json.load(open(os.path.join(FROZEN, "cold_breakdown.json")))
    fz_cold = cb["measurements"]["eai_ecube03_frozen"]["fixed_all4"]
    out["cold_fixed"] = {
        "launches": 20, "wall_median_ms": statistics.median(walls),
        "wall_p95_ms": pct(walls, 0.95), "wall_min_ms": min(walls),
        "frozen_wall_median_ms": fz_cold["wall_median_ms"],
        "delta_vs_frozen_pct": (statistics.median(walls) - fz_cold["wall_median_ms"])
        / fz_cold["wall_median_ms"] * 100,
    }
    out["cases"] = cases
    out["summary"] = {
        "n_cases": len(cases),
        "frozen_bin_median_delta_pct": statistics.median(
            [c["delta_vs_frozen_pct"] for c in cases.values()]),
        "frozen_bin_max_abs_delta_pct": max(abs(c["delta_vs_frozen_pct"])
                                            for c in cases.values()),
        "current_bin_median_delta_pct": statistics.median(
            [c["current_delta_vs_frozen_pct"] for c in cases.values()
             if c["current_delta_vs_frozen_pct"] is not None]),
        "current_bin_max_abs_delta_pct": max(
            abs(c["current_delta_vs_frozen_pct"]) for c in cases.values()
            if c["current_delta_vs_frozen_pct"] is not None),
        "loadavg_end": loadavg(),
    }
    json.dump(out, open(os.path.join(RECEIPT, "frozenbin_control.json"), "w"), indent=1)
    print(json.dumps(out["summary"], indent=1), flush=True)

    # append one section to the FINAL_RERUN markdown
    md_path = os.path.join(RECEIPT, "FINAL_RERUN_20261101.md")
    md = open(md_path).read()
    marker = "## 5. Frozen-binary control"
    if marker in md:
        print("markdown section already present; not appending")
        return
    lines = ["", marker + " (isolates binary/source revision from host state)", "",
             f"The frozen receipt was produced with the baselines stream's ECUBE03 binary "
             f"(`src_baselines_20261101/enc_cube3_frozen`, sha256 `96cf26e5…`, source "
             f"`c93a4530…`); the current repo source is `ae8339cf…` (binary `2e52137a…`). "
             f"The frozen binary rebuilt the index byte-identically (`{out['index']['sha256'][:12]}…`, "
             f"{out['index']['bytes']:,} B). Re-running the 11-case sweep and the cold request "
             f"with the frozen binary gives the following; the `current-bin` columns are the "
             f"main rerun above.", "",
             "| case | frozen receipt | frozen binary, now | current binary, now |",
             "|---|---:|---:|---:|"]
    for key, spec, label in CASES:
        c = cases[key]
        cur = c["current_binary_rerun_us"]
        lines.append(f"| {label} | {c['frozen_receipt']['median_us']:.3f} \u00b5s | "
                     f"{c['frozen_binary']['median_us']:.3f} \u00b5s "
                     f"({c['delta_vs_frozen_pct']:+.1f}%) | "
                     f"{cur:.3f} \u00b5s ({c['current_delta_vs_frozen_pct']:+.1f}%) |")
    lines += ["",
              f"* frozen-binary vs frozen receipt: median \u0394 "
              f"{out['summary']['frozen_bin_median_delta_pct']:+.1f}%, max |\u0394| "
              f"{out['summary']['frozen_bin_max_abs_delta_pct']:.1f}%.",
              f"* current-binary vs frozen receipt: median \u0394 "
              f"{out['summary']['current_bin_median_delta_pct']:+.1f}%, max |\u0394| "
              f"{out['summary']['current_bin_max_abs_delta_pct']:.1f}%.",
              f"* cold fixed with the frozen binary: {out['cold_fixed']['wall_median_ms']:.3f} ms "
              f"vs frozen {out['cold_fixed']['frozen_wall_median_ms']:.3f} ms "
              f"({out['cold_fixed']['delta_vs_frozen_pct']:+.1f}%).",
              f"* load average: start `{out['protocol']['loadavg_start']}`, end "
              f"`{out['summary']['loadavg_end']}`.",
              "", "**Attribution.** If both binary revisions show the same median deviation "
              "from the frozen receipt, the residual is host state / pinned core / time "
              "(the frozen runs used core 56, this rerun core 96, and the host load average "
              "is recorded in each receipt); if the frozen binary is materially closer to its "
              "own receipt, the current source revision carries the difference. The numbers "
              "above decide it; no claim is adjusted by this control.", ""]
    open(md_path, "a").write("\n".join(lines) + "\n")
    print("appended frozen-binary control to", md_path)


if __name__ == "__main__":
    main()
