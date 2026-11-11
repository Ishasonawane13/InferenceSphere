"""Exact GraphFrames triangle counting and clustering metrics."""

from __future__ import annotations

import itertools
import json
import logging
import time
from pathlib import Path
from typing import Any

from graphframes import GraphFrame
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    col,
    lit,
    max as spark_max,
    sum as spark_sum,
    when,
)

from src.config import AppConfig, load_config
from src.graph.graph_builder import build_undirected_graph
from src.utils.helpers import ensure_directories


LOGGER = logging.getLogger("influencesphere.triangle_count")


def _triangle_metrics(triangles: DataFrame, degrees: DataFrame) -> DataFrame:
    return (
        triangles.select(
            col("id").alias("user_id"),
            col("count").alias("triangle_count"),
        )
        .join(
            degrees.select(
                col("id").alias("user_id"),
                col("degree"),
            ),
            "user_id",
        )
        .withColumn(
            "clustering_coefficient",
            when(
                col("degree") >= 2,
                lit(2.0) * col("triangle_count")
                / (col("degree") * (col("degree") - lit(1.0))),
            ).otherwise(lit(0.0)),
        )
        .select("user_id", "triangle_count", "clustering_coefficient")
    )


def validate_against_bruteforce(
    graph: GraphFrame,
    triangle_metrics: DataFrame,
) -> bool:
    """Validate per-user counts by collecting only a deliberately small graph."""
    edges = {
        tuple(sorted((int(row["src"]), int(row["dst"]))))
        for row in graph.edges.select("src", "dst").collect()
    }
    nodes = sorted(
        {
            int(row["id"])
            for row in graph.vertices.select("id").collect()
        }
    )
    expected = {node: 0 for node in nodes}
    for left, middle, right in itertools.combinations(nodes, 3):
        if {
            (left, middle),
            (left, right),
            (middle, right),
        }.issubset(edges):
            expected[left] += 1
            expected[middle] += 1
            expected[right] += 1
    actual = {
        int(row["user_id"]): int(row["triangle_count"])
        for row in triangle_metrics.select("user_id", "triangle_count").collect()
    }
    return actual == expected


def run_triangle_count(
    graph: GraphFrame,
    output_dir: Path,
    *,
    config: AppConfig | None = None,
    max_degree_warning: int = 10000,
) -> dict[str, Any]:
    """Run exact triangle counting and persist per-user metrics and stats."""
    config = config or load_config()
    if max_degree_warning <= 0:
        raise ValueError("max_degree_warning must be positive")
    output_dir = Path(output_dir)
    ensure_directories(output_dir)
    started = time.perf_counter()

    degree_stats = graph.degrees.agg(spark_max("degree").alias("maximum_degree")).first()
    maximum_degree = int(degree_stats["maximum_degree"] or 0)
    warning = None
    if maximum_degree >= max_degree_warning:
        warning = (
            f"Maximum degree {maximum_degree} reaches the warning threshold "
            f"{max_degree_warning}; exact triangle counting may require substantial memory."
        )
        LOGGER.warning(warning)

    triangles = graph.triangleCount()
    metrics = _triangle_metrics(triangles, graph.degrees)
    metrics.write.mode("overwrite").parquet(str(output_dir / "triangles"))

    triangle_sum = metrics.select(spark_sum("triangle_count").alias("triangle_sum")).first()
    total_triangles = float(triangle_sum["triangle_sum"] or 0.0) / 3.0
    averages = metrics.agg(avg("clustering_coefficient").alias("average")).first()
    connected_triples = graph.degrees.select(
        (col("degree") * (col("degree") - lit(1.0)) / lit(2.0)).alias("triples")
    ).agg(spark_sum("triples").alias("total")).first()["total"] or 0.0
    transitivity = (
        float(3.0 * total_triangles / connected_triples)
        if connected_triples
        else 0.0
    )
    stats: dict[str, Any] = {
        "mode": "exact",
        "total_triangles": total_triangles,
        "average_clustering_coefficient": float(averages["average"] or 0.0),
        "global_transitivity": transitivity,
        "maximum_degree": maximum_degree,
        "feasibility_warning": warning,
        "execution_seconds": time.perf_counter() - started,
    }
    (output_dir / "triangle_count_stats.json").write_text(
        json.dumps(stats, indent=2) + "\n",
        encoding="utf-8",
    )
    return stats


def run_configured_triangle_count(
    output_dir: Path | None = None,
    *,
    config: AppConfig | None = None,
) -> dict[str, Any]:
    """Build the configured undirected graph and run exact triangle counting."""
    config = config or load_config()
    from src.spark_session import create_spark_session

    spark = create_spark_session(config)
    try:
        graph = build_undirected_graph(spark, config)
        return run_triangle_count(graph, output_dir or config.paths.results, config=config)
    finally:
        spark.stop()
