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
