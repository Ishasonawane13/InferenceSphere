# Installation

## Pinned compatibility set

This project uses:

| Component | Version |
| --- | --- |
| Python | 3.12.3 |
| Java (JDK) | 17.0.18 |
| Apache Spark / PySpark | 3.5.8 |
| GraphFrames Python client | `graphframes-py==0.12.3` |
| GraphFrames Spark package | `io.graphframes:graphframes-spark3_2.12:0.12.3` |

PySpark 3.5.8 uses Scala 2.12 and requires Java 17. GraphFrames 0.12.3's
Spark 3 / Scala 2.12 artifact targets Spark 3.5.8. Spark resolves it and its
runtime dependencies from Maven Central when the smoke test starts. Do not
substitute the older `graphframes:graphframes` Spark Packages coordinate.

## Clean install (WSL2 Ubuntu on Windows)

Use WSL2 for local Spark on Windows. The native Windows Spark launcher also
requires Hadoop Windows utilities (`winutils.exe`); WSL2 avoids that extra
platform-specific setup.

1. In Ubuntu, install Python 3.12, its virtual-environment support, and JDK
   17 if they are not already installed. Use Python 3.12.3 and JDK 17.0.18
   for the pinned baseline:

   ```bash
   sudo apt-get update
   sudo apt-get install -y python3.12 python3.12-venv openjdk-17-jdk
   ```

2. From the project directory (for a Windows checkout, `/mnt/c/...`), confirm
   `python3.12 --version` and `java -version` match the pinned baseline.
   Create and activate a virtual environment:

   ```bash
   python3.12 -m venv .venv
   source .venv/bin/activate
   ```

3. Install the pinned Python dependencies:

   ```bash
   python -m pip install -r requirements.txt
   ```

4. Copy `.env.example` to `.env` and customize local paths if necessary. The
   `.env` file is intentionally ignored by Git.
5. Run the GraphFrames and PageRank smoke test:

   ```bash
   python scripts/smoke_test_graphframes.py
   ```

The first run downloads the GraphFrames JVM package and dependencies, so it
requires access to Maven Central. A successful run prints the Spark version
and PageRank values for a five-vertex in-memory graph. These values validate
the installation only; they are not results for the SNAP dataset.

## Docker

Docker is an alternative to installing Spark's Windows-specific Hadoop
utilities. Start Docker Desktop, then run these commands from the project
directory:

```powershell
docker build -t influencesphere-smoke .
docker run --rm influencesphere-smoke
```

The first build downloads the Python dependencies. The first container run
also downloads the GraphFrames JVM package from Maven Central.

## Part 2 configuration test

The configured Spark session and hardware preflight can be checked from the
project root with:

```bash
python -m scripts.test_spark_session
```

The preflight reports warnings for low available memory, CPU capacity, or
free disk space; it does not prevent a run from starting.

## Part 4 preprocessing

Run preprocessing from the project root after the raw text file is available:

```bash
python scripts/preprocess_data.py --sample-percent 10
```

This writes cleaned directed and undirected Parquet data under
`data/processed/`, an optional deterministic sample under `data/samples/`, and
JSON/CSV statistics under `outputs/reports/`. Existing outputs are skipped;
use `--force` to rebuild them. On Windows, run the command in WSL2 or Docker
because native Spark Parquet commits require Hadoop filesystem support.

## Part 5 graph metrics

After preprocessing, graph builders load the directed or symmetric undirected
Parquet data as GraphFrames. Metrics are computed with distributed Spark
aggregations:

```python
from src.graph.graph_builder import build_directed_graph
from src.graph.graph_metrics import calculate_graph_metrics

graph = build_directed_graph(spark)
calculate_graph_metrics(graph, config.paths.results)
```

The command writes `data/results/degrees` and
`data/results/graph_summary.json`. Degree aggregation remains keyed and
distributed by vertex, so high-degree adjacency lists are not collected on the
driver. Symmetric undirected input edges are counted as one logical edge for
density and summary counts.

## Part 6 PageRank

Run PageRank on the cleaned directed graph from the project root:

```bash
python -m scripts.run_pagerank
```

The default reset probability is `0.15` (damping factor `0.85`) and the
configured maximum iteration count is used. Results are written under
`data/results/`: the full `pagerank` Parquet table, `top_100.csv`,
`top_1000.csv`, and `pagerank_evaluation.json`. The evaluation records runtime,
degree-based top-100 overlap, and stability between half and full iteration
counts.

## Part 7 HITS

Run the distributed custom HITS implementation:

```bash
python -m scripts.run_hits
```

Each iteration computes authority and hub sums with DataFrame joins and
aggregations, then L2-normalizes both score vectors. Iteration results use
local checkpoints to truncate lineage. Outputs are written to
`data/results/hits` and `data/results/hits_evaluation.json`; the evaluation
includes convergence deltas, runtime, and top-100 overlap/Spearman comparisons
with PageRank when its output is available.

## Part 8 label propagation

Run GraphFrames Label Propagation on the symmetrized undirected graph:

```bash
python -m scripts.run_label_propagation
```

Outputs include `data/results/communities`, a bounded dashboard sample under
`data/results/community_sample/`, and
`data/results/label_propagation_evaluation.json`. Modularity and community
statistics use Spark aggregations. LPA is non-deterministic: repeated runs may
produce different labels or unstable communities, which are not necessarily
meaningful without domain validation.

## Part 9 triangle counting

Run exact GraphFrames triangle counting on the undirected graph:

```bash
python -m scripts.run_triangle_count
```

The command writes per-user `triangles` Parquet data and
`triangle_count_stats.json`, including local clustering coefficients, average
clustering, global transitivity, runtime, and a high-degree feasibility warning.
The installed GraphFrames version is used in exact mode; no approximate mode is
claimed or silently substituted. Exact triangle counting can require
substantial memory for high-degree vertices.

## Part 10 SON

Run SON over each source user's outgoing-neighbor transaction:

```bash
python -m scripts.run_son --min-support 0.01 --max-itemset-size 3
```

Repeat `--min-support` to run multiple thresholds. SON uses partition-local
Apriori in phase 1 and global candidate counting in phase 2. Basket size is
deterministically truncated after sorted neighbor deduplication, and globally
infrequent singleton items are prefiltered by default. Results are written as
`frequent_itemsets` Parquet, `frequent_itemsets.csv`, and `son_evaluation.json`
under a threshold-specific directory in `data/results/`.

## Part 11 CURE

Run sampled CURE clustering over graph-derived features:

```bash
python -m scripts.run_cure --clusters 2 --representatives 5 --alpha 0.2 --sample-size 1000
```

Feature assembly is distributed in Spark and uses in/out/total degree,
PageRank, HITS, and triangle metrics. Missing algorithm outputs are filled
with zero, then features receive `log1p` and standard scaling. Only the
deterministic seeded sample is clustered on the driver with real CURE
representative-point merging; the output JSON reports the exact number of
users
clustered and does not claim that unsampled users were clustered.

## Part 3 dataset download

Preview the dataset download without downloading data:

```bash
python scripts/download_data.py --dry-run
```

The actual download is intentionally separate because the dataset is large:

```bash
python scripts/download_data.py
```

The command keeps `soc-LiveJournal1.txt.gz` in `data/raw/` and extracts
`soc-LiveJournal1.txt` beside it. Interrupted HTTP downloads resume from a
`.part` file when supported by the server; completed archives are skipped.

`setuptools` is pinned to provide the `distutils` compatibility module used by
PySpark 3.5 on Python 3.12.

Official compatibility and installation details:
<https://graphframes.io/02-quick-start/01-installation.html>
