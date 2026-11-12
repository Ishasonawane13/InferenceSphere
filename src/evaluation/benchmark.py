"""Deterministic graph-size scalability benchmarks with explicit run statuses."""

from __future__ import annotations

import csv
import json
import logging
import time
from dataclasses import replace
from pathlib import Path
from typing import Any, Callable

from graphframes import GraphFrame
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, pmod, xxhash64

from src.config import AppConfig, load_config
from src.evaluation.validation import BENCHMARK_FIELDS
from src.utils.preflight import get_system_resources


LOGGER = logging.getLogger(__name__)
DEFAULT_PERCENTAGES = (1, 5, 10, 25, 100)
ALGORITHMS = ("pagerank", "hits", "label_propagation", "triangle_count", "son", "cure")


def pending_benchmark_records(
    percentages: tuple[int, ...] = DEFAULT_PERCENTAGES,
    *,
    full_allowed: bool = False,
) -> list[dict[str, Any]]:
    """Create honest pending rows for requested scales before any run is executed."""
    records = []
    for percent in percentages:
        if percent == 100 and not full_allowed:
            status, reason = "skipped", "Full-data preflight requirements were not met."
        else:
            status, reason = "pending", "Benchmark has not been executed."
        for algorithm in ALGORITHMS:
            records.append({
                "sample_percent": percent,
                "vertices": None,
                "edges": None,
                "algorithm": algorithm,
                "status": status,
                "execution_seconds": None,
                "parameters": {},
                "result_count": None,
                "memory_bytes": None,
                "error": reason,
            })
    return records


def _sample_edges(edges: DataFrame, percent: int, seed: int) -> DataFrame:
    if percent == 100:
        return edges
    threshold = percent * 100
    return edges.filter(
        pmod(xxhash64(col("src"), col("dst"), lit(seed)), lit(10000)) < threshold
    )


def _write_records(records: list[dict[str, Any]], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "scalability_benchmark.json").write_text(
        json.dumps(records, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    with (output_dir / "scalability_benchmark.csv").open(
        "w", newline="", encoding="utf-8"
    ) as output:
        writer = csv.DictWriter(output, fieldnames=BENCHMARK_FIELDS)
        writer.writeheader()
        writer.writerows(
            {
                **row,
                "parameters": json.dumps(row["parameters"], allow_nan=False),
            }
            for row in records
        )


def _benchmark_one(
    name: str,
    action: Callable[[], int],
    parameters: dict[str, Any],
    base: dict[str, Any],
) -> dict[str, Any]:
    row = {**base, "algorithm": name, "parameters": parameters}
    started = time.perf_counter()
    try:
        row["result_count"] = int(action())
        row["execution_seconds"] = time.perf_counter() - started
        row["status"] = "complete"
    except Exception as error:
        LOGGER.exception("%s benchmark failed at %s%% sample", name, base["sample_percent"])
        row["execution_seconds"] = time.perf_counter() - started
        row["status"] = "failed"
        row["error"] = f"{type(error).__name__}: {error}"
    return row


def _run_algorithm(
    name: str,
    spark: SparkSession,
    directed: GraphFrame,
    undirected: GraphFrame,
    sample_edges: DataFrame,
    config: AppConfig,
    output_dir: Path,
    percent: int,
) -> int:
    from src.algorithms.cure import run_cure
    from src.algorithms.hits import run_hits
    from src.algorithms.label_propagation import run_label_propagation
    from src.algorithms.pagerank import run_pagerank
    from src.algorithms.son import run_son
    from src.algorithms.triangle_count import run_triangle_count

    if name == "pagerank":
        run_pagerank(directed, output_dir, config=config, max_iterations=config.algorithm.pagerank_max_iterations)
        return spark.read.parquet(str(output_dir / "pagerank")).count()
    if name == "hits":
        run_hits(directed, output_dir, config=config)
        return spark.read.parquet(str(output_dir / "hits")).count()
    if name == "label_propagation":
        run_label_propagation(undirected, output_dir, config=config)
        return spark.read.parquet(str(output_dir / "communities")).count()
    if name == "triangle_count":
        run_triangle_count(undirected, output_dir, config=config)
        return spark.read.parquet(str(output_dir / "triangles")).count()
    if name == "son":
        sample_data_dir = output_dir / "input_data"
        directed_path = sample_data_dir / "cleaned_directed_edges"
        sample_edges.write.mode("overwrite").parquet(str(directed_path))
        sample_config = replace(
            config,
            paths=replace(config.paths, data_processed=sample_data_dir),
        )
        report = run_son(
            spark,
            output_dir,
            config=sample_config,
            min_support=0.01,
            max_itemset_size=3,
        )
        return int(report["frequent_itemset_count"])
    if name == "cure":
        benchmark_root = output_dir.parent
        rank_path, hits_path, triangle_path = (
            benchmark_root / "pagerank" / "pagerank",
            benchmark_root / "hits" / "hits",
            benchmark_root / "triangle_count" / "triangles",
        )
        sample_count = min(
            config.algorithm.cure_sample_size,
            directed.vertices.count(),
        )
        if sample_count <= config.algorithm.cure_k:
            raise ValueError(
                "The sampled graph has too few users for the configured CURE k"
            )
        return run_cure(
            directed,
            spark.read.parquet(str(rank_path)),
            spark.read.parquet(str(hits_path)),
            spark.read.parquet(str(triangle_path)),
            output_dir / "cure",
            config=config,
            sample_size=sample_count,
        )["users_clustered"]
    raise ValueError(f"Unknown benchmark algorithm: {name}")


def run_scalability_benchmark(
    spark: SparkSession,
    config: AppConfig | None = None,
    *,
    percentages: tuple[int, ...] = DEFAULT_PERCENTAGES,
    output_dir: Path | None = None,
) -> list[dict[str, Any]]:
    """Run each algorithm on deterministic edge samples; gate full data by preflight."""
    config = config or load_config()
    edge_path = config.paths.data_processed / "cleaned_directed_edges"
    if not edge_path.is_dir():
        raise FileNotFoundError(f"Cleaned edge data not found: {edge_path}")
    if any(percent <= 0 or percent > 100 for percent in percentages):
        raise ValueError("Sample percentages must be between 1 and 100")
    resources = get_system_resources(config.paths.project_root)
    full_allowed = (
        resources.memory_gb >= config.preflight.minimum_memory_gb
        and resources.free_disk_gb >= config.preflight.minimum_free_disk_gb
        and resources.cpu_cores >= config.preflight.minimum_cpu_cores
    )
    rows = [
        row
        for row in pending_benchmark_records(
            percentages,
            full_allowed=full_allowed,
        )
        if row["status"] == "skipped"
    ]
    source = spark.read.parquet(str(edge_path)).select("src", "dst").cache()
    source.count()
    try:
        for percent in percentages:
            if percent == 100 and not full_allowed:
                continue
            sample = _sample_edges(source, percent, config.algorithm.random_seed).dropDuplicates(["src", "dst"]).cache()
            edge_count = sample.count()
            vertices = (
                sample.select(col("src").alias("id"))
                .union(sample.select(col("dst").alias("id")))
                .distinct()
            )
            vertex_count = vertices.count()
            base = {
                "sample_percent": percent,
                "vertices": vertex_count,
                "edges": edge_count,
                "status": "pending",
                "execution_seconds": None,
                "result_count": None,
                "memory_bytes": None,
                "error": None,
            }
            undirected_edges = (
                sample.select("src", "dst")
                .union(sample.select(col("dst").alias("src"), col("src").alias("dst")))
                .dropDuplicates(["src", "dst"])
            )
            directed = GraphFrame(vertices, sample)
            undirected_vertices = (
                undirected_edges.select(col("src").alias("id"))
                .union(undirected_edges.select(col("dst").alias("id")))
                .distinct()
            )
            undirected = GraphFrame(undirected_vertices, undirected_edges)
            for name in ALGORITHMS:
                destination = Path(output_dir or config.paths.tables) / "benchmark_runs" / f"{percent}pct" / name
                parameters = {
                    "seed": config.algorithm.random_seed,
                    "sample_percent": percent,
                    "max_iterations": config.algorithm.pagerank_max_iterations,
                }
                if name == "son":
                    parameters.update({"min_support": 0.01, "max_itemset_size": 3})
                if name == "cure":
                    parameters.update({
                        "k": config.algorithm.cure_k,
                        "c": config.algorithm.cure_representatives,
                        "alpha": config.algorithm.cure_alpha,
                        "sample_size": config.algorithm.cure_sample_size,
                    })
                rows.append(_benchmark_one(
                    name,
                    lambda name=name, destination=destination: _run_algorithm(
                        name, spark, directed, undirected, sample, config, destination, percent
                    ),
                    parameters,
                    base,
                ))
            sample.unpersist()
    finally:
        source.unpersist()
    _write_records(rows, Path(output_dir or config.paths.tables))
    return rows


def run_configured_benchmark(
    config: AppConfig | None = None,
    *,
    percentages: tuple[int, ...] = DEFAULT_PERCENTAGES,
) -> list[dict[str, Any]]:
    config = config or load_config()
    from src.spark_session import create_spark_session

    resources = get_system_resources(config.paths.project_root)
    full_allowed = (
        resources.memory_gb >= config.preflight.minimum_memory_gb
        and resources.free_disk_gb >= config.preflight.minimum_free_disk_gb
        and resources.cpu_cores >= config.preflight.minimum_cpu_cores
    )
    pending = pending_benchmark_records(
        percentages,
        full_allowed=full_allowed,
    )
    _write_records(
        pending,
        config.paths.tables,
    )
    spark = create_spark_session(config)
    try:
        return run_scalability_benchmark(spark, config, percentages=percentages)
    finally:
        spark.stop()
