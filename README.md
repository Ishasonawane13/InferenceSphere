# InfluenceSphere

InfluenceSphere is a reproducible Spark and Streamlit platform for large-scale
social-network intelligence. It cleans a directed social graph, computes
centrality and community metrics, mines frequent patterns, clusters users, and
exposes persisted results through a dashboard.

## Problem statement

Large social graphs contain useful signals about influential users, communities,
local structure, and recurring neighborhoods, but their scale makes
single-machine exploratory scripts unreliable. InfluenceSphere provides a
distributed preprocessing and algorithm workflow with explicit sampling,
restartability, evaluation metadata, and honest pending/unavailable states.

## Objectives

- Validate and clean raw edge data deterministically.
- Compute PageRank, HITS, label propagation, triangles, SON, and CURE.
- Preserve output schemas and execution metadata for auditability.
- Benchmark graph scales without fabricating unrun measurements.
- Provide a dashboard that reads persisted real artifacts only.

## Dataset

The project targets the SNAP LiveJournal social-network dataset:

> Jure Leskovec and Andrej Krevl. SNAP Datasets: Stanford Large Network
> Dataset Collection. 2014. <https://snap.stanford.edu/data/>

The LiveJournal graph is approximately 4.8 million users and 69 million
directed edges. Download it with `scripts/download_data.py`; raw and generated
data are intentionally ignored by Git.

## Technology stack

- Python 3.12
- Apache Spark / PySpark 3.5.8
- GraphFrames 0.12.3
- Parquet, CSV, and JSON artifacts
- Streamlit and Plotly
- pandas, NumPy, and PyArrow
- Docker or WSL2 for reliable Windows Spark/Parquet execution

## Architecture

```text
raw SNAP edge list
        |
        v
data loading and preprocessing
        |
        +--> directed / undirected Parquet
        |
        v
graph metrics and algorithm runners
        |
        +--> data/results/*.parquet
        +--> data/results/*.json / *.csv
        |
        v
evaluation, validation, benchmarking
        |
        v
Streamlit dashboard
```

## Algorithms

| Algorithm | Implementation type | Graph / strategy |
|---|---|---|
| PageRank | GraphFrames built-in | Directed graph, configurable iterations |
| HITS | Custom | Distributed DataFrame iterations with L2 normalization |
| Label Propagation | GraphFrames built-in | Undirected graph; nondeterminism documented |
| Triangle count | GraphFrames built-in plus custom metrics | Exact triangle count and clustering statistics |
| SON | Custom | Two-phase RDD Apriori with proportional local thresholds |
| CURE | Custom | Distributed feature assembly, seeded sample, driver-side hierarchical CURE |

No approximate triangle mode is currently claimed: triangle results are labeled
`mode: exact` by the algorithm artifact.

## Folder structure

```text
config.yaml              Spark, path, algorithm, and preflight defaults
src/
  preprocessing.py       Edge validation and Parquet preparation
  graph/                 GraphFrames builders and graph metrics
  algorithms/            Six algorithm implementations
  evaluation/             Comparisons, benchmarks, and schema validation
scripts/                 Download, preprocessing, algorithm, evaluation, pipeline CLIs
dashboard/               Streamlit app, pages, loaders, and Plotly helpers
tests/                   Focused algorithm, pipeline, evaluation, and dashboard tests
data/                    Local raw, processed, and result artifacts (Git-ignored)
outputs/                 Tables, reports, and charts (Git-ignored)
```

## Installation

From a clean checkout:

```bash
python -m venv .venv
# Windows PowerShell:
.venv\Scripts\Activate.ps1
# Linux/macOS:
# source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Install Java 17 and use Docker/WSL2 on Windows when native Hadoop filesystem
support prevents Spark Parquet writes. Confirm the environment:

```bash
python --version
java -version
python -m unittest discover -s tests -p "test_*.py"
```

## Configuration

Edit `config.yaml` or set the supported environment variables:

- `INFLUENCESPHERE_PROJECT_ROOT`
- `SPARK_MASTER`
- `SPARK_APP_NAME`
- `SPARK_DRIVER_MEMORY`
- `SPARK_SHUFFLE_PARTITIONS`
- `GRAPHFRAMES_PACKAGE`

The preflight check runs before heavy Spark jobs and validates memory, disk, and
CPU requirements.

## Exact run commands

Download and preprocess:

```bash
python scripts/download_data.py
python scripts/preprocess_data.py
```

Run the restartable pipeline:

```bash
python -m scripts.run_pipeline --all
python -m scripts.run_pipeline --algorithm pagerank --iterations 10
python -m scripts.run_pipeline --all --force --spark-conf spark.sql.shuffle.partitions=8
python -m scripts.run_pipeline --all --benchmark
```

Generate evaluations and validate schemas:

```bash
python scripts/run_evaluation.py
```

Run the dashboard:

```bash
streamlit run dashboard/app.py
```

For Docker/WSL2, execute the same commands from the project environment where
Java, Spark, GraphFrames, and the mounted repository are available.

## Evaluation and result interpretation

Algorithm outputs are stored below `data/results/`. Evaluation JSON files
contain runtimes, counts, parameters, and algorithm-specific diagnostics.
`outputs/tables/comparisons.json` compares available results and marks missing
comparisons as `pending`. `data/results/benchmark/scalability_benchmark.json`
records deterministic graph sizes and keeps algorithm measurements as
`pending` when the benchmark scaffold did not execute them.

PageRank and HITS rankings are relative scores, not causal influence. LPA
communities can vary between runs and require domain validation. Triangle
statistics are exact for the processed graph. CURE reports the number of users
actually clustered and does not claim that unsampled users were clustered.

## Limitations and future work

- The approximately 4.8M-node / 69M-edge LiveJournal graph can exceed the
  memory available on a single development machine.
- Native Windows Spark may require Docker or WSL2 for Hadoop filesystem support.
- LPA is nondeterministic and may be unstable without domain validation.
- SON uses deterministic basket/itemset caps to control memory and runtime.
- CURE clusters a seeded sample by default; optional assignment is separately
  reported and is not equivalent to clustering the sample.
- The current benchmark scaffold records pending algorithm timings rather than
  inventing them.
- Future work includes multi-node deployment, richer temporal graphs, robust
  LPA stability analysis, distributed CURE merging, and completed benchmark
  execution on provisioned hardware.

## Testing

Run all tests:

```bash
python -m pytest -q
```

On native Windows, Spark/GraphFrames integration tests are skipped when the
required Hadoop runtime is unavailable; pure algorithm, dashboard, schema, and
pipeline restartability tests still run. Run the integration tests in Docker
or WSL2 to execute the full synthetic Spark pipeline.

## References

- Leskovec, J. and Krevl, A. (2014), SNAP Datasets:
  <https://snap.stanford.edu/data/>
- Page, L. et al. (1999), The PageRank Citation Ranking.
- Kleinberg, J. (1999), Authoritative Sources in a Hyperlinked Environment.
- Raghavan, U. N., Albert, R., and Kumara, S. (2007), Near linear time
  algorithm to detect community structures in large-scale networks.
- Guha, S., Rastogi, R., and Shim, K. (1998), CURE: An Efficient Clustering
  Algorithm for Large Databases.
- Spark GraphFrames documentation:
  <https://graphframes.io/>
