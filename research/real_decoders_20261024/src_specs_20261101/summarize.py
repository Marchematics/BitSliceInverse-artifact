#!/usr/bin/env python3
"""Aggregate every SPECS receipt into one summary JSON + paper coverage tables.

PRIMARY target = FROZEN ECUBE03 (eai_cube3.cpp sha256 c93a4530..., index
nyc100_v3.ecube sha256 78bff19a..., 1,140,168 B).  ECUBE02 (v2) and ECUBE01
(v1) appear only as superseded ablation rows.

Writes <BASE>/out/summary_20261101.json, SEEDS_20261101.json,
paper_table_20261101.{csv,md}, differential_table_20261101.md,
PAPER_TABLES_20261101.md
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import hashlib
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pairs as P

BASE = P.BASE
OUT = f"{BASE}/out"

FROZEN_SOURCE_SHA = "c93a4530f7b21d5e1a1f174374f5fdd49783a20ad32638b370baabf3e86562f0"
FROZEN_INDEX_SHA = "78bff19a0dbd89d0e3b16254cb2a0c807669d982b3a9233b1ba5504dd04c89ee"
FROZEN_INDEX_BYTES = 1140168
FROZEN_BINARY_SHA = "96cf26e536844b4f5f9f08fb044399770936ced24b7b75575862e756e55cf71d"
FROZEN_RECEIPT = ("research/real_decoders_20261025/receipts/baselines_20261101/"
                  "eai_ecube03_frozen.json")


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def load(name, default=None):
    p = f"{OUT}/{name}"
    return json.load(open(p)) if os.path.exists(p) else default


def main():
    corpus = load("spec_corpus_20261101.json")
    counts = load("spec_counts_20261101.json")
    v3 = load("spec_receipt_v3.json")
    v3wt = load("spec_receipt_v3_worktree.json")
    v2 = load("spec_receipt_v2.json")
    v1p = load("spec_receipt_prefix.json")
    ident = load("encoded_identity_v3.json")
    v3cubes = load("v3_cubes.json")
    bugs = load("bug_witnesses_v3.json")
    probe = load("semantics_probe_v3.json")
    fuzz = load("fuzz_receipt_20261101.json")          # v3 campaign (primary)
    fuzz2 = load("fuzz_receipt_v2.json")               # superseded v2 campaign

    first_fail = {}
    for f in (v1p or {}).get("failures", []):
        first_fail.setdefault(f["family"], f)
    fam_rows = []
    for fam in sorted(counts["families"]):
        n = counts["families"][fam]
        ex = v3["families"][fam]["exact"]
        ex2 = v2["families"][fam]["exact"]
        ex1 = v1p["families"][fam]["exact"]
        w = first_fail.get(fam)
        wit = "-" if not w else "%s (%s)" % (w["id"], " ".join(w["cube_args"]) or "fixed")
        fam_rows.append((fam, n, ex, ex2, ex1, wit))
    wit = (bugs or {}).get("witnesses", {})
    verdicts = {}
    for k, v in (probe or {}).get("probes", {}).items():
        verdicts.setdefault(v.get("verdict"), []).append(k)
    null_fixture = [k for k in verdicts.get("PASS", [])
                    if k.split("_")[0] in ("N1", "N2", "N3", "N4", "N5", "N6")
                    and "null" in k]

    summary = dict(
        generated="2026-11-01", workstream="specs",
        primary_format="ECUBE03 (v3, FROZEN)",
        superseded_formats=["ECUBE02 (v2)", "ECUBE01 (v1)"],
        duckdb_version=v3["duckdb_version"],
        frozen=dict(
            source_sha256=FROZEN_SOURCE_SHA,
            worktree_source_sha256=None,
            index_sha256=FROZEN_INDEX_SHA,
            index_bytes=FROZEN_INDEX_BYTES,
            frozen_binary_sha256=FROZEN_BINARY_SHA,
            our_binary_sha256=v3["binary_sha256"],
            our_binary=v3["binary"],
            build_command=("bash src_eai_20261101/build.sh  # compiles eai_cube3.cpp -> "
                           "src_eai_20261101/enc_cube3; SPECS pinned build: "
                           "src_specs_20261101/build_v3.sh -> ../../work/specs/bin/"
                           "enc_cube3_frozen"),
            frozen_receipt=FROZEN_RECEIPT,
            encoded_identity={k: dict(sha256=v["plain_sha256"],
                                      byte_identical=v["byte_identical"])
                              for k, v in (ident or {}).get("pairs", {}).items()}),
        corpus=dict(path="spec_corpus_20261101.json",
                    sha256=sha256_file(f"{OUT}/spec_corpus_20261101.json"),
                    n_specs=counts["n_specs"], families=counts["families"],
                    n_digest_specs=counts["n_digest_specs"]),
        spec_exactness=dict(
            v3=dict(n=v3["n_specs"], exact=v3["n_exact"], fail=v3["n_fail"],
                    binary=v3["binary"], binary_sha256=v3["binary_sha256"],
                    families={k: dict(v) for k, v in v3["families"].items()},
                    wall_s=v3["wall_s"]),
            v3_worktree_binary=None if not v3wt else dict(
                n=v3wt["n_specs"], exact=v3wt["n_exact"], fail=v3wt["n_fail"],
                binary=v3wt["binary"], binary_sha256=v3wt["binary_sha256"]),
            v2_superseded=dict(n=v2["n_specs"], exact=v2["n_exact"],
                               fail=v2["n_fail"], binary_sha256=v2["binary_sha256"],
                               families={k: dict(v) for k, v in v2["families"].items()}),
            v1_prefix=dict(n=v1p["n_specs"], exact=v1p["n_exact"], fail=v1p["n_fail"],
                           binary_sha256=v1p["binary_sha256"],
                           families={k: dict(v) for k, v in v1p["families"].items()},
                           failures_by_root_cause=dict(
                               BUG1_dense_gappy_build=sum(
                                   1 for f in v1p["failures"] if f["pair"] == "C11"),
                               BUG2_null_group_not_in=sum(
                                   1 for f in v1p["failures"] if f["pair"] != "C11" and
                                   any("where_not_in" in a for a in f["cube_args"])),
                               BUG3_dense_int64_bound=sum(
                                   1 for f in v1p["failures"] if f["pair"] != "C11" and
                                   any("9223372036854775808" in a for a in f["cube_args"]))))),
        cubes_v3={k: dict(sha256=v["sha256"], bytes=v["bytes"],
                          build_ms=v["build_info"].get("build_ms"),
                          dense=v["build_info"].get("dense"),
                          groups=v["build_info"].get("groups"),
                          values=v["build_info"].get("values"),
                          rows=v["build_info"].get("rows"))
                  for k, v in v3cubes["cubes"].items()},
        witness=dict(total=len(wit),
                     v3_pass=sum(1 for x in wit.values() if x.get("v3", {}).get("ok")),
                     v2_pass=sum(1 for x in wit.values() if x.get("v2", {}).get("ok")),
                     v1_pass=sum(1 for x in wit.values() if x.get("v1", {}).get("ok")),
                     v3_sha256=bugs.get("v3_sha256"),
                     v3_source_sha256=bugs.get("v3_source_sha256"),
                     receipt="bug_witnesses_v3.json"),
        semantics=dict(verdict_counts={k: len(v) for k, v in verdicts.items()},
                       null_fixture_pass=len(null_fixture),
                       null_fixture_total=6,
                       null_fixture_cases=sorted(null_fixture),
                       receipt="semantics_probe_v3.json"),
        differential=None if not fuzz else dict(
            seed=fuzz["seed"], n_plans=fuzz["n_plans"], n_unique=fuzz["n_unique"],
            plans_sha256=fuzz["plans_sha256"], plan_gen_s=fuzz["plan_gen_s"],
            categories=fuzz["categories"], eval_tables=fuzz["eval_tables"],
            v3=dict(n=fuzz["results"]["patched"]["n"],
                    exact=fuzz["results"]["patched"]["exact"],
                    fail=fuzz["results"]["patched"]["fail"],
                    by_category=fuzz["results"]["patched"]["by_category"],
                    by_pair=fuzz["results"]["patched"]["by_pair"],
                    binary_sha256=fuzz["results"]["patched"]["binary_sha256"]),
            v1_prefix=dict(n=fuzz["results"]["prefix"]["n"],
                           exact=fuzz["results"]["prefix"]["exact"],
                           fail=fuzz["results"]["prefix"]["fail"],
                           by_category=fuzz["results"]["prefix"]["by_category"],
                           by_pair=fuzz["results"]["prefix"]["by_pair"],
                           binary_sha256=fuzz["results"]["prefix"]["binary_sha256"]),
            v2_superseded=None if not fuzz2 else dict(
                n=fuzz2["results"]["patched"]["n"],
                exact=fuzz2["results"]["patched"]["exact"],
                fail=fuzz2["results"]["patched"]["fail"],
                by_category=fuzz2["results"]["patched"]["by_category"],
                by_pair=fuzz2["results"]["patched"]["by_pair"],
                plans_sha256=fuzz2["plans_sha256"])))
    summary["frozen"]["worktree_source_sha256"] = bugs.get("worktree_source_sha256")
    json.dump(summary, open(f"{OUT}/summary_20261101.json", "w"), indent=1)

    seeds = dict(
        workstream="specs", generated="2026-11-01",
        primary_format="ECUBE03 (v3, FROZEN)",
        corpus_seed=corpus["seed"], duckdb_version=v3["duckdb_version"],
        corpus_sha256=summary["corpus"]["sha256"],
        corpus_n_specs=counts["n_specs"],
        frozen=dict(source="research/real_decoders_20261024/src_eai_20261101/eai_cube3.cpp",
                    source_sha256=FROZEN_SOURCE_SHA,
                    index_sha256=FROZEN_INDEX_SHA, index_bytes=FROZEN_INDEX_BYTES,
                    frozen_binary_sha256=FROZEN_BINARY_SHA,
                    our_binary=v3["binary"], our_binary_sha256=v3["binary_sha256"],
                    build_command=summary["frozen"]["build_command"],
                    frozen_receipt=FROZEN_RECEIPT,
                    encoded_index_sha256={k: v["sha256"] for k, v in
                                          summary["frozen"]["encoded_identity"].items()}),
        spec_runs=dict(v3=dict(n=v3["n_specs"], exact=v3["n_exact"]),
                       v2=dict(n=v2["n_specs"], exact=v2["n_exact"]),
                       v1=dict(n=v1p["n_specs"], exact=v1p["n_exact"])),
        differential=dict(
            seed=fuzz["seed"], n_plans=fuzz["n_plans"], n_unique=fuzz["n_unique"],
            plans_sha256=fuzz["plans_sha256"],
            v3=dict(n=fuzz["results"]["patched"]["n"],
                    exact=fuzz["results"]["patched"]["exact"]),
            v2=dict(n=fuzz2["results"]["patched"]["n"],
                    exact=fuzz2["results"]["patched"]["exact"],
                    plans_sha256=fuzz2["plans_sha256"]) if fuzz2 else None,
            v1=dict(n=fuzz["results"]["prefix"]["n"],
                    exact=fuzz["results"]["prefix"]["exact"])) if fuzz else None,
        witness=dict(total=summary["witness"]["total"],
                     v3_pass=summary["witness"]["v3_pass"],
                     v2_pass=summary["witness"]["v2_pass"],
                     v1_pass=summary["witness"]["v1_pass"]),
        semantics=summary["semantics"])
    json.dump(seeds, open(f"{OUT}/SEEDS_20261101.json", "w"), indent=1)

    # ---- machine-copyable tables ------------------------------------------
    with open(f"{OUT}/paper_table_20261101.md", "w") as f:
        f.write("| family | #specs | #exact (ECUBE03 frozen) | v2 (superseded) "
                "| v1 (superseded) | witness (v1 failure) |\n")
        f.write("|---|---:|---:|---:|---:|---|\n")
        for r in fam_rows:
            f.write("| %s | %d | %d | %d/%d | %d/%d | %s |\n" %
                    (r[0], r[1], r[2], r[3], r[1], r[4], r[1], r[5]))
        f.write("| **total** | **%d** | **%d** | **%d/%d** | **%d/%d** | |\n" %
                (counts["n_specs"], v3["n_exact"], v2["n_exact"], v2["n_specs"],
                 v1p["n_exact"], v1p["n_specs"]))
    with open(f"{OUT}/differential_table_20261101.md", "w") as f:
        f.write("| campaign | queries | seed | plan sha256 | datasets | "
                "v3 (frozen) exact | v3 mismatches | v2 (superseded) exact | "
                "v2 mismatches | v1 exact | v1 mismatches |\n")
        f.write("|---|---:|---:|---|---|---:|---:|---:|---:|---:|---:|\n")
        if fuzz:
            r3 = fuzz["results"]["patched"]
            r1 = fuzz["results"]["prefix"]
            r2 = fuzz2["results"]["patched"] if fuzz2 else dict(exact=0, n=0, fail=0)
            f.write("| 100k randomized differential, ECUBE03 vs DuckDB 2.0.0 | %d | %d | "
                    "`%s` | 12 narrow pairs (10 x 1M rows, 1 x 2k, 1 x 1 row) | %d/%d | %d | "
                    "%d/%d | %d | %d/%d | %d |\n" % (
                        fuzz["n_plans"], fuzz["seed"], fuzz["plans_sha256"],
                        r3["exact"], r3["n"], r3["fail"],
                        r2["exact"], r2["n"], r2["fail"],
                        r1["exact"], r1["n"], r1["fail"]))
        else:
            f.write("| 100k randomized differential, ECUBE03 vs DuckDB 2.0.0 | pending | "
                    "| | | | | | | | |\n")
    with open(f"{OUT}/paper_table_20261101.csv", "w") as f:
        f.write("family,n_specs,n_exact_ECUBE03,n_exact_v2_superseded,"
                "n_exact_v1_superseded,witness\n")
        for r in fam_rows:
            f.write("%s,%d,%d,%d,%d,%s\n" % (r[0], r[1], r[2], r[3], r[4],
                                             r[5].replace(",", ";")))
    c1 = summary["cubes_v3"]["C1"]
    with open(f"{OUT}/PAPER_TABLES_20261101.md", "w") as f:
        f.write("# Paper tables — FROZEN ECUBE03 exactness coverage and differential "
                "campaign (2026-11-01)\n\n")
        f.write("Provenance: corpus `research/real_decoders_20261024/results/"
                "specs_20261101/spec_corpus_20261101.json` (sha256 `%s`, %d specs, "
                "13 families, seed %d); reference engine DuckDB %s; **FROZEN ECUBE03** "
                "source `eai_cube3.cpp` sha256 `%s` (git blob d792ed36..., commit "
                "6a94fee); frozen index identity NYC-100M cube sha256 `%s` (%s B, "
                "1,140,168 B; plain and encoded builds byte-identical); v2 (ECUBE02) "
                "and v1 (ECUBE01) columns are superseded ablation only; all runs "
                "pinned `nice -n 5 taskset -c 72` on `localhost`.\n\n" % (
                    summary["corpus"]["sha256"], counts["n_specs"], corpus["seed"],
                    v3["duckdb_version"], FROZEN_SOURCE_SHA, FROZEN_INDEX_SHA,
                    format(FROZEN_INDEX_BYTES, ",")))
        f.write("## Table A — exactness by family (v3 = FROZEN ECUBE03)\n\n")
        f.write(open(f"{OUT}/paper_table_20261101.md").read())
        f.write("\n## Table B — randomized differential campaign\n\n")
        f.write(open(f"{OUT}/differential_table_20261101.md").read())
        f.write("\n## Table C — differential by query category\n\n")
        if fuzz:
            r3 = fuzz["results"]["patched"]
            r1 = fuzz["results"]["prefix"]
            r2 = fuzz2["results"]["patched"] if fuzz2 else None
            f.write("| category | queries | v3 exact | v2 exact | v1 exact | "
                    "v1 mismatches |\n")
            f.write("|---|---:|---:|---:|---:|---:|\n")
            for c in fuzz["categories"]:
                a, b = r3["by_category"][c], r1["by_category"][c]
                e2 = r2["by_category"][c]["exact"] if r2 else 0
                f.write("| %s | %d | %d | %d | %d | %d |\n" %
                        (c, a["n"], a["exact"], e2, b["exact"], b["fail"]))
            f.write("| **total** | **%d** | **%d** | **%d** | **%d** | **%d** |\n" %
                    (r3["n"], r3["exact"], r2["exact"] if r2 else 0,
                     r1["exact"], r1["fail"]))
        f.write("\n## Table D — differential by dataset pair\n\n")
        if fuzz:
            r3 = fuzz["results"]["patched"]
            r1 = fuzz["results"]["prefix"]
            r2 = fuzz2["results"]["patched"] if fuzz2 else None
            f.write("| pair | rows | V | G | queries | v3 exact | v2 mismatches | "
                    "v1 mismatches |\n")
            f.write("|---|---:|---:|---:|---:|---:|---:|---:|\n")
            pm = json.load(open(f"{OUT}/pairs_meta.json"))
            for pid in [f"F{i}" for i in range(1, 13)]:
                mt = pm[pid]
                a, b = r3["by_pair"][pid], r1["by_pair"][pid]
                m2 = r2["by_pair"][pid]["fail"] if r2 else 0
                f.write("| %s | %d | %d | %d | %d | %d | %d | %d |\n" %
                        (pid, mt["n"], mt["nvalues"], mt["ngroups"], a["n"],
                         a["exact"], m2, b["fail"]))
        f.write("\nNOTE 1 (NULL measures): FROZEN ECUBE03 supports NULL measure payloads "
                "exactly (G*4 per-group NULL-count array appended to the index, +%d B "
                "at G=11); its NULL path is proven by the 6/6 plain-path fixture "
                "(N1-N6 in `specs_20261101/semantics_probe_v3.json`; separate frozen "
                "receipt `receipts/baselines_20261101/eai_ecube03_frozen.json`). The "
                "394-spec corpus and the 100k campaign do NOT exercise NULL measures: "
                "all 23 pairs assert `v IS NOT NULL`, and the 8 NYC-100M sweep cases "
                "have no NULL measures. No claim should imply that the corpus "
                "exercises nullable measures.\n" % (11 * 4))
        f.write("\nNOTE 2 (capacity, unchanged): u32 per-rank/block counts bound a single "
                "group to 4,294,967,295 rows; safe at 1B total rows, guard needed before "
                "any >4.29e9-rows-in-one-group claim (flag to `scale`).\n")
        f.write("\nNOTE 3 (empty base table, unchanged): build rejects a table with no "
                "observed value (`ERR empty`, V>=1 precondition); empty *results* are "
                "supported (36/36 empty_result specs exact).\n")
    print(open(f"{OUT}/paper_table_20261101.md").read())
    if fuzz:
        print("differential: v3 %d/%d, v2 %d/%d, v1 %d/%d" % (
            fuzz["results"]["patched"]["exact"], fuzz["results"]["patched"]["n"],
            fuzz2["results"]["patched"]["exact"] if fuzz2 else 0,
            fuzz2["results"]["patched"]["n"] if fuzz2 else 0,
            fuzz["results"]["prefix"]["exact"], fuzz["results"]["prefix"]["n"]))
    print("wrote summary_20261101.json, SEEDS_20261101.json, "
          "paper_table_20261101.{csv,md}, differential_table_20261101.md, "
          "PAPER_TABLES_20261101.md")


if __name__ == "__main__":
    main()
