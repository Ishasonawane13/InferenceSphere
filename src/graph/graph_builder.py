"""Build GraphFrames from preprocessed edge Parquet datasets."""

from __future__ import annotations

from pathlib import Path

from graphframes import GraphFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from src.config import AppConfig, load_config


def _read_graph(
    spark: SparkSession,
    edge_path: Path,
) -> GraphFrame:
    edges = spark.read.parquet(str(edge_path)).select("src", "dst")
    vertices = (
        edges.select(col("src").alias("id"))
        .union(edges.select(col("dst").alias("id")))
        .distinct()
    )
    return GraphFrame(vertices, edges)


def build_directed_graph(
    spark: SparkSession,
    config: AppConfig | None = None,
) -> GraphFrame:
    """Build a directed GraphFrame from cleaned directed edges."""
    config = config or load_config()
    edge_path = config.paths.data_processed / "cleaned_directed_edges"
    return _read_graph(spark, edge_path)


def build_undirected_graph(
    spark: SparkSession,
    config: AppConfig | None = None,
) -> GraphFrame:
    """Build a GraphFrame from symmetrized, deduplicated undirected edges."""
    config = config or load_config()
    edge_path = config.paths.data_processed / "cleaned_undirected_edges"
    return _read_graph(spark, edge_path)
