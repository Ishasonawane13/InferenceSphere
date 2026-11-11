"""GraphFrames label propagation and distributed community evaluation."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from graphframes import GraphFrame
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count,
    lit,
    rand,
    sum as spark_sum,
)

from src.config import AppConfig, load_config
from src.graph.graph_builder import build_undirected_graph
from src.utils.helpers import ensure_directories


def _modularity(graph: GraphFrame, communities: DataFrame) -> float:
    physical_edges = graph.edges.count()
    if physical_edges == 0:
        return 0.0
    degrees = graph.degrees.select(
        col("id"),
        col("degree").alias("vertex_degree"),
    )
    labeled = communities.select(
        col("user_id").alias("id"),
        "community_id",
    )
    same_community = (
        graph.edges.join(
            degrees.select(col("id").alias("src"), col("vertex_degree").alias("src_degree")),
            "src",
        )
        .join(
            degrees.select(col("id").alias("dst"), col("vertex_degree").alias("dst_degree")),
            "dst",
        )
        .join(labeled.select(col("id").alias("src"), col("community_id").alias("src_community")), "src")
        .join(labeled.select(col("id").alias("dst"), col("community_id").alias("dst_community")), "dst")
        .filter(col("src_community") == col("dst_community"))
    )
    contribution = same_community.select(
        lit(1.0)
        - (
            col("src_degree") * col("dst_degree") / lit(float(physical_edges))
        )
        .alias("contribution")
    ).agg(spark_sum("contribution").alias("total")).first()["total"]
    return float((contribution or 0.0) / physical_edges)


def _write_dashboard_sample(
    graph: GraphFrame,
    communities: DataFrame,
    output_dir: Path,
    *,
    max_nodes: int,
    max_edges: int,
) -> None:
    sample_dir = output_dir / "community_sample"
    nodes = (
        communities.orderBy(rand(seed=42))
        .limit(max_nodes)
        .select("user_id", "community_id", "community_size")
    )
    sample_edges = (
        graph.edges.join(
            nodes.select(col("user_id").alias("src")),
            "src",
        )
        .join(nodes.select(col("user_id").alias("dst")), "dst")
        .limit(max_edges)
    )
    nodes.write.mode("overwrite").parquet(str(sample_dir / "nodes"))
    sample_edges.write.mode("overwrite").parquet(str(sample_dir / "edges"))


def run_label_propagation(
    graph: GraphFrame,
    output_dir: Path,
    *,
    config: AppConfig | None = None,
    max_iterations: int | None = None,
    sample_max_nodes: int = 1000,
    sample_max_edges: int = 5000,
) -> dict[str, Any]:
    """Run LPA and persist communities, statistics, modularity, and a sample."""
    config = config or load_config()
    max_iterations = max_iterations or config.algorithm.pagerank_max_iterations
    if max_iterations <= 0 or sample_max_nodes <= 0 or sample_max_edges <= 0:
        raise ValueError("iteration and sample limits must be positive")

    output_dir = Path(output_dir)
    ensure_directories(output_dir)
    started = time.perf_counter()
    labels = graph.labelPropagation(maxIter=max_iterations).run()
    communities = labels.select(
        col("id").alias("user_id"),
        col("label").alias("community_id"),
    )
    sizes = communities.groupBy("community_id").agg(
        count("*").alias("community_size")
    )
    communities = communities.join(sizes, "community_id")
    communities.write.mode("overwrite").parquet(str(output_dir / "communities"))

    community_count = sizes.count()
    size_distribution = [
        {"community_size": int(row["community_size"]), "community_count": int(row["community_count"])}
        for row in sizes.groupBy("community_size")
        .agg(count("*").alias("community_count"))
        .orderBy("community_size")
        .collect()
    ]
    largest = [
        {
            "community_id": int(row["community_id"]),
            "community_size": int(row["community_size"]),
        }
        for row in sizes.orderBy(col("community_size").desc(), col("community_id"))
        .limit(10)
        .collect()
    ]
    _write_dashboard_sample(
        graph,
        communities,
        output_dir,
        max_nodes=sample_max_nodes,
        max_edges=sample_max_edges,
    )
    evaluation = {
        "node_count": communities.count(),
        "community_count": community_count,
        "size_distribution": size_distribution,
        "top_10_largest": largest,
        "modularity": _modularity(graph, communities),
        "max_iterations": max_iterations,
        "execution_seconds": time.perf_counter() - started,
        "warning": (
            "Label Propagation is non-deterministic; communities may be unstable "
            "and are not necessarily meaningful without domain validation."
        ),
    }
    (output_dir / "label_propagation_evaluation.json").write_text(
        json.dumps(evaluation, indent=2) + "\n",
        encoding="utf-8",
    )
    return evaluation


def run_configured_label_propagation(
    output_dir: Path | None = None,
    *,
    config: AppConfig | None = None,
    max_iterations: int | None = None,
) -> dict[str, Any]:
    """Build the configured undirected graph and run LPA."""
    config = config or load_config()
    from src.spark_session import create_spark_session

    spark = create_spark_session(config)
    try:
        graph = build_undirected_graph(spark, config)
        return run_label_propagation(
            graph,
            output_dir or config.paths.results,
            config=config,
            max_iterations=max_iterations,
        )
    finally:
        spark.stop()
