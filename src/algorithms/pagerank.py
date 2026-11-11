"""Run GraphFrames PageRank and evaluate ranking quality and stability."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Iterable

from graphframes import GraphFrame
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    row_number,
)
from pyspark.sql.window import Window

from src.config import AppConfig, load_config
from src.graph.graph_builder import build_directed_graph
from src.utils.helpers import ensure_directories


def _ranked_vertices(vertices: DataFrame) -> DataFrame:
    ranking_window = Window.orderBy(col("pagerank").desc(), col("user_id"))
    return vertices.select("user_id", "pagerank").withColumn(
        "rank",
        row_number().over(ranking_window),
    )


def _top_k_overlap(
    pagerank: DataFrame,
    degree: DataFrame,
    k: int,
) -> float:
    if k <= 0:
        return 0.0
    pagerank_top = pagerank.filter(col("rank") <= k).select("user_id")
    degree_window = Window.orderBy(col("total_degree").desc(), col("user_id"))
    degree_top = (
        degree.withColumn("degree_rank", row_number().over(degree_window))
        .filter(col("degree_rank") <= k)
        .select("user_id")
    )
    overlap = pagerank_top.join(degree_top, "user_id", "inner").count()
    return float(overlap / k)


def _rank_overlap(left: DataFrame, right: DataFrame, k: int) -> float:
    if k <= 0:
        return 0.0
    left_top = left.filter(col("rank") <= k).select("user_id")
    right_top = right.filter(col("rank") <= k).select("user_id")
    overlap = left_top.join(right_top, "user_id", "inner").count()
    return float(overlap / k)


def _degree_table(graph: GraphFrame) -> DataFrame:
    vertices = graph.vertices.select("id")
    in_degree = graph.inDegrees.select(
        col("id"), col("inDegree").alias("in_degree")
    )
    out_degree = graph.outDegrees.select(
        col("id"), col("outDegree").alias("out_degree")
    )
    return (
        vertices.join(in_degree, "id", "left")
        .join(out_degree, "id", "left")
        .fillna(0, ["in_degree", "out_degree"])
        .select(
            col("id").alias("user_id"),
            "in_degree",
            "out_degree",
            (col("in_degree") + col("out_degree")).alias("total_degree"),
        )
    )


def _run_once(
    graph: GraphFrame,
    reset_probability: float,
    max_iterations: int | None,
    tolerance: float | None,
) -> tuple[DataFrame, float]:
    started = time.perf_counter()
    options: dict[str, Any] = {"resetProbability": reset_probability}
    if tolerance is not None:
        options["tol"] = tolerance
    elif max_iterations is not None:
        options["maxIter"] = max_iterations
    pagerank = graph.pageRank(**options).run()
    elapsed = time.perf_counter() - started
    ranked = _ranked_vertices(
        pagerank.vertices.select(
            col("id").alias("user_id"),
            col("pagerank"),
        )
    )
    return ranked, elapsed


def run_pagerank(
    graph: GraphFrame,
    output_dir: Path,
    *,
    config: AppConfig | None = None,
    max_iterations: int | None = None,
    tolerance: float | None = None,
    stability_iterations: Iterable[int] | None = None,
) -> dict[str, Any]:
    """Run PageRank, persist ranked outputs, and return evaluation metadata."""
    config = config or load_config()
    if max_iterations is None:
        max_iterations = config.algorithm.pagerank_max_iterations
    if max_iterations <= 0:
        raise ValueError("max_iterations must be positive")
    if tolerance is not None and tolerance <= 0:
        raise ValueError("tolerance must be positive")

    output_dir = Path(output_dir)
    ensure_directories(output_dir)
    reset_probability = config.algorithm.pagerank_reset_probability
    ranked, elapsed = _run_once(
        graph,
        reset_probability,
        max_iterations,
        tolerance,
    )
    ranked = ranked.cache()
    ranked.count()

    ranked.write.mode("overwrite").parquet(str(output_dir / "pagerank"))
    for limit, name in ((100, "top_100.csv"), (1000, "top_1000.csv")):
        ranked.filter(col("rank") <= limit).orderBy("rank").coalesce(1).write.mode(
            "overwrite"
        ).option("header", True).csv(str(output_dir / name))

    degree = _degree_table(graph).cache()
    node_count = degree.count()
    overlap_k = min(100, node_count)
    degree_overlap = _top_k_overlap(ranked, degree, overlap_k)

    stability_runs: list[dict[str, Any]] = []
    previous: DataFrame | None = None
    requested_iterations = list(
        stability_iterations
        if stability_iterations is not None
        else (max(1, max_iterations // 2), max_iterations)
    )
    for iterations in sorted(set(requested_iterations)):
        if iterations <= 0:
            raise ValueError("stability_iterations must contain positive values")
        if iterations == max_iterations and tolerance is None:
            current = ranked
            duration = elapsed
        else:
            current, duration = _run_once(
                graph,
                reset_probability,
                iterations,
                None,
            )
            current = current.cache()
            current.count()
        run_info: dict[str, Any] = {
            "max_iterations": iterations,
            "execution_seconds": duration,
        }
        if previous is not None:
            run_info["top_100_overlap_with_previous"] = _rank_overlap(
                current, previous, min(100, node_count)
            )
        stability_runs.append(run_info)
        previous = current

    summary = {
        "node_count": node_count,
        "reset_probability": reset_probability,
        "damping_factor": 1.0 - reset_probability,
        "max_iterations": max_iterations,
        "tolerance": tolerance,
        "execution_seconds": elapsed,
        "degree_top_100_overlap": degree_overlap,
        "stability_runs": stability_runs,
        "outputs": {
            "full_parquet": str(output_dir / "pagerank"),
            "top_100_csv": str(output_dir / "top_100.csv"),
            "top_1000_csv": str(output_dir / "top_1000.csv"),
        },
    }
    (output_dir / "pagerank_evaluation.json").write_text(
        json.dumps(summary, indent=2) + "\n",
        encoding="utf-8",
    )
    ranked.unpersist()
    degree.unpersist()
    return summary


def run_configured_pagerank(
    output_dir: Path | None = None,
    *,
    config: AppConfig | None = None,
) -> dict[str, Any]:
    """Build the configured directed graph and run PageRank."""
    config = config or load_config()
    from src.spark_session import create_spark_session

    spark = create_spark_session(config)
    try:
        graph = build_directed_graph(spark, config)
        return run_pagerank(graph, output_dir or config.paths.results, config=config)
    finally:
        spark.stop()
