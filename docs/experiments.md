# Experiments

Run commands from the repository root with the virtual environment activated.
The full suite uses Linux x86-64, GNU time, and `taskset`. Large-scale runs use
128 GB RAM and 200 GB of disk space. Set `BSI_CORE` to select a timing core.

## Data and main queries

```bash
python scripts/prepare.py nyc --rows 100000000
python scripts/benchmark.py nyc
python scripts/prepare.py nyc --rows 12000000
python scripts/benchmark.py nyc12
python scripts/prepare.py tpch --sf 2
python scripts/benchmark.py tpch
python scripts/prepare.py ig2 --replicas 15
python scripts/prepare.py ig2 --replicas 153
```

NYC preparation takes the first rows in month order from the
[TLC yellow taxi files](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page),
starting in January 2023. Fare values use `trunc(fare_amount * 100)`.
TPC-H uses DuckDB's `tpch` generator. Public BI uses the pinned
[FastLanes Data snapshot](https://github.com/azimafroozeh/FastLanes_Data/tree/e5fab9e21e0037a6a3f54633c73a3524d08fe8f3/public_bi).

## Index layouts, scale, and output

```bash
DATASETS=tpch2 python scripts/run.py mec
python scripts/run.py references
python scripts/run.py tiers
python scripts/run.py groups
python scripts/run.py clustered
python scripts/run.py output
python scripts/run.py nulls
python scripts/run.py amortization
python scripts/run.py panel
python scripts/run.py scale build100 q100m build1b q1b exact1b duck1b ecube3mt ecube3q fit
python scripts/run.py scale build10m q10m duck10m exact10m
python scripts/run.py clustered-1b
python scripts/run.py clustered-mt
python scripts/run.py router
```

Run `groups` before the clustered and output experiments. Billion-row inputs
use ten replicas of NYC-100M; synthetic group domains retain the fare values.
Microsecond benchmarks use a 0.5-second core warmup and report the median of
three runs. Each comparison checks the complete grouped result.

## Correctness and external engines

```bash
python scripts/check.py --queries 1000
python scripts/run.py specs
python scripts/run.py fuzz --n 100000
pip install -r requirements-baselines.txt
python scripts/run.py baselines
python scripts/build_engines.py
python scripts/run.py formats
python scripts/run.py engines
```

The specification corpus and randomized campaign use the prepared NYC and TPC-H
tables. External harnesses record the installed engine versions in their results.
The frozen run used DuckDB 2.0.0 and DataFusion 55.0.0; its measurements and
individual query results are retained in `research/real_decoders_20261025/receipts/`.

## Results and plots

```bash
python scripts/plots.py
python scripts/verify.py
```

Plot source data and SVG/PNG outputs are written to `work/plots/`.
FastLanes primitives are pinned at `f0edc1020a538f1f8098640fce8347c9ac247a0d`;
their license is retained in the vendor directory.
