# Installation

## Pinned compatibility set

This project uses:

| Component | Version |
| --- | --- |
| Python | 3.12.4 |
| Java (JDK) | 17.0.12 |
| Apache Spark / PySpark | 3.5.8 |
| GraphFrames Python client | `graphframes-py==0.12.3` |
| GraphFrames Spark package | `io.graphframes:graphframes-spark3_2.12:0.12.3` |

PySpark 3.5.8 uses Scala 2.12. The Maven coordinate above is the GraphFrames
Spark 3 / Scala 2.12 artifact; Spark resolves it and its runtime dependencies
from Maven Central when the smoke test starts. Do not substitute the older
`graphframes:graphframes` Spark Packages coordinate.

## Clean install (Windows PowerShell)

1. Install Python 3.12.4 and a Java 17 JDK (17.0.12), then open a new
   PowerShell window. Confirm `py -3.12 --version` and `java -version`.
2. From the project root, create and activate a virtual environment:

   ```powershell
   py -3.12 -m venv .venv
   .\.venv\Scripts\Activate.ps1
   ```

3. Install the pinned Python dependencies:

   ```powershell
   python -m pip install -r requirements.txt
   ```

4. Copy `.env.example` to `.env` and customize local paths if necessary.
   The `.env` file is intentionally ignored by Git.
5. Run the GraphFrames and PageRank smoke test:

   ```powershell
   python scripts\smoke_test_graphframes.py
   ```

The first run downloads the GraphFrames JVM package and dependencies, so it
requires access to Maven Central. A successful run prints the Spark version
and PageRank values for a five-vertex in-memory graph. These values validate
the installation only; they are not results for the SNAP dataset.

Official compatibility and installation details:
<https://graphframes.io/02-quick-start/01-installation.html>
