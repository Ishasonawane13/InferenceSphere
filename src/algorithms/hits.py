"""Distributed HITS implementation using Spark DataFrame aggregations."""

from __future__ import annotations

import json
import math
import time
from pathlib import Path
from typing import Any

from graphframes import GraphFrame
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    abs as spark_abs,
    col,
    max as spark_max,
    row_number,
    sum as spark_sum,
)
from pyspark.sql.window import Window

from src.config import AppConfig, load_config
from src.graph.graph_builder import build_directed_graph
from src.utils.helpers import ensure_directories


def _normalize(scores: DataFrame, column: str) -> DataFrame:
    norm_row = scores.select(spark_sum(col(column) * col(column)).alias("sum_sq")).first()
    norm = math.sqrt(float(norm_row["sum_sq"] or 0.0))
    if norm == 0.0:
        return scores
    return scores.withColumn(column, col(column) / norm)


def _initial_scores(vertices: DataFrame) -> DataFrame:
    from pyspark.sql.functions import lit

    return vertices.select("id").withColumn("hub_score", lit(1.0)).withColumn(
        "authority_score", lit(1.0)
    )


def _iterate(edges: DataFrame, scores: DataFrame) -> DataFrame:
    authority = (
        edges.join(scores.select(col("id").alias("src"), "hub_score"), "src")
        .groupBy("dst")
        .agg(spark_sum("hub_score").alias("authority_score"))
        .withColumnRenamed("dst", "id")
    )
    updated = scores.select("id").join(authority, "id", "left").fillna(
        0.0, ["authority_score"]
    )
    hub = (
        edges.join(
            updated.select(col("id").alias("dst"), "authority_score"),
            "dst",
        )
        .groupBy("src")
        .agg(spark_sum("authority_score").alias("hub_score"))
        .withColumnRenamed("src", "id")
    )
    updated = updated.join(hub, "id", "left").fillna(0.0, ["hub_score"])
    return _normalize(_normalize(updated, "authority_score"), "hub_score")


def _convergence(previous: DataFrame, current: DataFrame) -> float:
    row = (
        previous.select("id", "hub_score", "authority_score")
        .alias("previous")
        .join(current.select("id", "hub_score", "authority_score").alias("current"), "id")
        .select(
            spark_abs(col("previous.hub_score") - col("current.hub_score")).alias(
                "hub_delta"
            ),
            spark_abs(
                col("previous.authority_score") - col("current.authority_score")
            ).alias("authority_delta"),
        )
        .agg(
            spark_max("hub_delta").alias("hub"),
            spark_max("authority_delta").alias("authority"),
        )
        .first()
    )
    return max(float(row["hub"] or 0.0), float(row["authority"] or 0.0))


def _ranked(scores: DataFrame) -> DataFrame:
    hub_window = Window.orderBy(col("hub_score").desc(), col("id"))
    authority_window = Window.orderBy(col("authority_score").desc(), col("id"))
    return scores.withColumn("hub_rank", row_number().over(hub_window)).withColumn(
        "authority_rank", row_number().over(authority_window)
    ).select(
        col("id").alias("user_id"),
        "hub_score",
        "authority_score",
        "hub_rank",
        "authority_rank",
    )


def _top_overlap(left: DataFrame, left_rank: str, right: DataFrame, right_rank: str, k: int) -> float:
    if k <= 0:
        return 0.0
    overlap = left.filter(col(left_rank) <= k).select("user_id").join(
        right.filter(col(right_rank) <= k).select("user_id"), "user_id"
    ).count()
    return float(overlap / k)


def _spearman(
    left: DataFrame,
    left_rank: str,
    right: DataFrame,
    right_rank: str,
) -> float | None:
    joined = left.select("user_id", col(left_rank).alias("left_rank")).join(
        right.select("user_id", col(right_rank).alias("right_rank")), "user_id"
    )
    n = joined.count()
    if n < 2:
        return None
    sum_squared = joined.select(
        spark_sum((col("left_rank") - col("right_rank")) ** 2).alias("sum_squared")
    ).first()["sum_squared"]
    return float(1.0 - (6.0 * sum_squared) / (n * (n * n - 1)))


def run_hits(
    graph: GraphFrame,
    output_dir: Path,
    *,
    config: AppConfig | None = None,
    max_iterations: int | None = None,
    tolerance: float = 1e-6,
) -> dict[str, Any]:
    """Run HITS and persist scores, convergence history, and evaluation."""
    config = config or load_config()
    max_iterations = max_iterations or config.algorithm.pagerank_max_iterations
    if max_iterations <= 0 or tolerance <= 0:
        raise ValueError("max_iterations and tolerance must be positive")
    output_dir = Path(output_dir)
    ensure_directories(output_dir)
    started = time.perf_counter()
    scores = _initial_scores(graph.vertices).localCheckpoint(eager=True)
    convergence: list[dict[str, Any]] = []
    for iteration in range(1, max_iterations + 1):
        updated = _iterate(graph.edges, scores).localCheckpoint(eager=True)
        delta = _convergence(scores, updated)
        convergence.append({"iteration": iteration, "delta": delta})
        scores.unpersist()
        scores = updated
        if delta <= tolerance:
            break

    ranked = _ranked(scores).cache()
    node_count = ranked.count()
    ranked.write.mode("overwrite").parquet(str(output_dir / "hits"))
    elapsed = time.perf_counter() - started

    evaluation: dict[str, Any] = {
        "node_count": node_count,
        "iterations": len(convergence),
        "max_iterations": max_iterations,
        "tolerance": tolerance,
        "execution_seconds": elapsed,
        "convergence": convergence,
    }
    pagerank_path = output_dir / "pagerank"
    if pagerank_path.is_dir():
        pagerank = graph.vertices.sparkSession.read.parquet(str(pagerank_path))
        k = min(100, node_count)
        evaluation["pagerank_top_100_overlap_hubs"] = _top_overlap(
            ranked, "hub_rank", pagerank, "rank", k
        )
        evaluation["pagerank_top_100_overlap_authorities"] = _top_overlap(
            ranked, "authority_rank", pagerank, "rank", k
        )
        evaluation["pagerank_spearman_hubs"] = _spearman(
            ranked, "hub_rank", pagerank, "rank"
        )
        evaluation["pagerank_spearman_authorities"] = _spearman(
            ranked, "authority_rank", pagerank, "rank"
        )
    (output_dir / "hits_evaluation.json").write_text(
        json.dumps(evaluation, indent=2) + "\n",
        encoding="utf-8",
    )
    ranked.unpersist()
    return evaluation


def run_configured_hits(
    output_dir: Path | None = None,
    *,
    config: AppConfig | None = None,
    max_iterations: int | None = None,
    tolerance: float = 1e-6,
) -> dict[str, Any]:
    """Build the configured directed graph and run HITS."""
    config = config or load_config()
    from src.spark_session import create_spark_session

    spark = create_spark_session(config)
    try:
        graph = build_directed_graph(spark, config)
        return run_hits(
            graph,
            output_dir or config.paths.results,
            config=config,
            max_iterations=max_iterations,
            tolerance=tolerance,
        )
    finally:
        spark.stop()
