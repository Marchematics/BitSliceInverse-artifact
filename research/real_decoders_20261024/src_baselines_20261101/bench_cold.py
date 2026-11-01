#!/usr/bin/env python3
"""Cold complete-request breakdown for the two ECUBE versions (quiet-window diagnosis).

The Lead's question: the first frozen-ECUBE02 measurement showed a cold process request of
8.0-11.9 ms, while ECUBE01 was 1.36-1.46 ms and the in-process setup/query/serialize are
0.05 ms / 0.9-1.6 us.  Is that real (v2 regression) or contaminated?

Method: interleaved launches of both binaries on the pinned timing core, so any host-load
drift hits both versions equally.  For each launch we record
  * wall bracket from Python (subprocess + fork/exec + dynamic link + process + exit),
  * the binary's own in-process metrics (setup = open/mmap/header, query, serialize,
    output write, stored = in-process total),
  * rusage deltas for the child: minor/major page faults, user and sys CPU time.
We also launch (a) each binary with no arguments (arg-parse exit: exec + link + exit with no
index work) and (b) a trivial C++ binary, to separate startup from index payload.

  nice -n 5 taskset -c 56 /usr/bin/python3 bench_cold.py
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import json
import os
import re
import resource
import statistics
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as C

S = os.path.dirname(os.path.abspath(__file__))
RUNS = C.RUNS
V1_BIN = os.path.join(S, "enc_cube_v1")
V2_BIN = os.path.join(C.REPO, "src_eai_20261101/enc_cube")          # frozen (pinned source)
V1_IDX = _artifact.expand("../../work/runs/nyc100_plain.ecube")  # sha d46d2b90..., 2,951,956 B
V2_IDX = _artifact.expand("../../work/runs/nyc100_v2.ecube")     # sha c878dba2..., 1,140,124 B
V3_BIN = os.path.join(C.REPO, "src_eai_20261101/enc_cube3")          # frozen (pinned source)
V3_IDX = _artifact.expand("../../work/runs/nyc100_v3.ecube")     # sha 78bff19a..., 1,140,168 B
DICT = os.path.join(C.DATA, "nyc_taxi_100m_prep/Observed/state.dict")
TRIVIAL = os.path.join(S, "trivial_probe")
LAUNCHES = int(os.environ.get("COLD_LAUNCHES", "20"))
CASES = ["fixed_all4", "le_1350", "le_m7000"]
VER = {"eai_ecube01": (V1_BIN, V1_IDX, 2_951_956),
       "eai_ecube02_frozen": (V2_BIN, V2_IDX, 1_140_124),
       "eai_ecube03_frozen": (V3_BIN, V3_IDX, 1_140_168)}


def spec_for(name):
    return [] if name == "fixed_all4" else ["hi=%d" % C.THRESHOLDS[name]]


def parse_metrics(stderr):
    m = re.findall(r"\{.*\}", stderr or "")
    if not m:
        return {}
    try:
        return json.loads(m[-1])
    except Exception:
        return {}


def run_once(cmd, out=None):
    r0 = resource.getrusage(resource.RUSAGE_CHILDREN)
    t0 = time.perf_counter()
    p = subprocess.run(cmd, capture_output=True, text=True)
    wall_ms = (time.perf_counter() - t0) * 1000.0
    r1 = resource.getrusage(resource.RUSAGE_CHILDREN)
    return {
        "wall_ms": wall_ms,
        "rc": p.returncode,
        "minflt": r1.ru_minflt - r0.ru_minflt,
        "majflt": r1.ru_majflt - r0.ru_majflt,
        "child_user_ms": (r1.ru_utime - r0.ru_utime) * 1000.0,
        "child_sys_ms": (r1.ru_stime - r0.ru_stime) * 1000.0,
        "metrics": parse_metrics(p.stderr),
    }


def med(vals):
    return statistics.median(vals) if vals else float("nan")


def stored_of(m):
    """In-process total: v1 prints stored_ms; v2 prints only setup+serialize+output."""
    if not m:
        return float("nan")
    if "stored_ms" in m:
        return m["stored_ms"]
    return (m.get("setup_ms", 0.0) + m.get("serialize_median_ms", 0.0)
            + m.get("output_ms", 0.0))


def summarize(rows):
    return {
        "launches": len(rows),
        "wall_median_ms": med([r["wall_ms"] for r in rows]),
        "wall_min_ms": min(r["wall_ms"] for r in rows),
        "wall_p95_ms": sorted(r["wall_ms"] for r in rows)[int(0.95 * len(rows)) - 1],
        "minflt_median": med([r["minflt"] for r in rows]),
        "majflt_median": med([r["majflt"] for r in rows]),
        "child_user_median_ms": med([r["child_user_ms"] for r in rows]),
        "child_sys_median_ms": med([r["child_sys_ms"] for r in rows]),
        "stored_median_ms": med([stored_of(r["metrics"]) for r in rows]),
        "setup_median_ms": med([r["metrics"].get("setup_ms", float("nan")) for r in rows]),
        "query_median_us": med([r["metrics"].get("query_median_ms", float("nan")) * 1000
                                for r in rows]),
        "serialize_median_us": med([r["metrics"].get("serialize_median_ms", float("nan")) * 1000
                                    for r in rows]),
        "output_median_ms": med([r["metrics"].get("output_ms", float("nan")) for r in rows]),
        "raw": rows,
    }


def main():
    if not os.path.exists(TRIVIAL):
        raise SystemExit("missing trivial_probe (build it in build.sh)")
    load_before = open("/proc/loadavg").read().strip()
    print("loadavg before:", load_before, flush=True)
    out = {"window": {"loadavg_before": load_before, "host": os.uname().nodename,
                      "launches_per_measurement": LAUNCHES,
                      "pinned": "nice -n 5 taskset -c 56 (inherited by children)",
                      "binaries": {"eai_ecube01": {"path": V1_BIN, "index": V1_IDX},
                                   "eai_ecube02_frozen": {"path": V2_BIN, "index": V2_IDX}},
                      "interleaving": "one launch per version per round, cases cycled"},
           "measurements": {}}

    # A) trivial binary + no-arg usage launches: startup floors
    triv = [run_once([TRIVIAL]) for _ in range(LAUNCHES)]
    out["measurements"]["trivial_cpp_binary"] = summarize(triv)
    print("trivial      wall=%.3f ms minflt=%.1f" % (
        out["measurements"]["trivial_cpp_binary"]["wall_median_ms"],
        out["measurements"]["trivial_cpp_binary"]["minflt_median"]), flush=True)
    for name, (b, idx, _sz) in VER.items():
        usage = [run_once([b]) for _ in range(10)]
        out["measurements"][name + "_usage_exit"] = summarize(usage)
        print("%-20s usage-exit wall=%.3f ms" % (name, med([r["wall_ms"] for r in usage])), flush=True)

    # B) interleaved query launches for both versions, cases cycled
    per = {name: {c: [] for c in CASES} for name in VER}
    for _ in range(LAUNCHES):
        for case in CASES:
            spec = spec_for(case)
            for name, (b, idx, _sz) in VER.items():
                o = os.path.join(RUNS, "cold_%s_%s.out.json" % (name, case))
                per[name][case].append(run_once([b, "query", idx, DICT, o] + spec))
    for name in VER:
        out["measurements"][name] = {}
        for case in CASES:
            sm = summarize(per[name][case])
            out["measurements"][name][case] = sm
            print("%-20s %-12s wall=%7.3f stored=%6.3f setup=%6.3f minflt=%5.1f" % (
                name, case, sm["wall_median_ms"], sm["stored_median_ms"],
                sm["setup_median_ms"], sm["minflt_median"]), flush=True)
    load_after = open("/proc/loadavg").read().strip()
    out["window"]["loadavg_after"] = load_after
    print("loadavg after:", load_after, flush=True)

    # C) derived breakdown, median of the per-case medians
    out["breakdown"] = {}
    for name in VER:
        cases = out["measurements"][name]
        wall = med([cases[c]["wall_median_ms"] for c in CASES])
        stored = med([cases[c]["stored_median_ms"] for c in CASES])
        usage = out["measurements"][name + "_usage_exit"]["wall_median_ms"]
        startup = wall - stored
        out["breakdown"][name] = {
            "wall_median_ms": wall,
            "exec_link_exit_plus_wrapper_ms": startup,
            "in_process_stored_ms": stored,
            "in_process_setup_ms": med([cases[c]["setup_median_ms"] for c in CASES]),
            "in_process_query_us": med([cases[c]["query_median_us"] for c in CASES]),
            "in_process_serialize_us": med([cases[c]["serialize_median_us"] for c in CASES]),
            "in_process_output_write_ms": med([cases[c]["output_median_ms"] for c in CASES]),
            "minflt_median": med([cases[c]["minflt_median"] for c in CASES]),
            "usage_exit_wall_ms": usage,
            "index_bytes": VER[name][2],
        }
        print("BREAKDOWN %-20s wall=%.3f startup=%.3f inproc=%.3f (setup %.3f + ser %.3f us + write %.3f ms) minflt=%.1f" % (
            name, wall, startup, stored, out["breakdown"][name]["in_process_setup_ms"],
            out["breakdown"][name]["in_process_serialize_us"],
            out["breakdown"][name]["in_process_output_write_ms"],
            out["breakdown"][name]["minflt_median"]), flush=True)
    # D) controlled contention: reproduce the original 8-12 ms observation by keeping a
    # competing runnable process ON the timing core while launching the frozen binary.
    if os.environ.get("COLD_CONTENTION", "0") == "1":
        busy = subprocess.Popen(["nice", "-n", "5", "taskset", "-c", _artifact.core(), "python3", "-c",
                                 "import time\nt0=time.time()\nwhile time.time()-t0<6: pass"])
        time.sleep(0.3)
        cont = [run_once([V2_BIN, "query", V2_IDX, DICT,
                          os.path.join(RUNS, "cold_contention.out.json")] + spec_for("fixed_all4"))
                for _ in range(10)]
        busy.wait()
        sm = summarize(cont)
        out["controlled_contention"] = {
            "what": "10 launches of the frozen ECUBE02 binary while a busy loop shares core 56",
            "wall_median_ms": sm["wall_median_ms"],
            "wall_min_ms": sm["wall_min_ms"],
            "wall_p95_ms": sm["wall_p95_ms"],
            "stored_median_ms": sm["stored_median_ms"],
            "minflt_median": sm["minflt_median"],
        }
        print("CONTENTION frozen wall median=%.3f min=%.3f p95=%.3f (inproc %.3f)" % (
            sm["wall_median_ms"], sm["wall_min_ms"], sm["wall_p95_ms"], sm["stored_median_ms"]),
            flush=True)
    json.dump(out, open(os.path.join(RUNS, "cold_breakdown.json"), "w"), indent=1)
    print("receipt ->", os.path.join(RUNS, "cold_breakdown.json"))


if __name__ == "__main__":
    main()
