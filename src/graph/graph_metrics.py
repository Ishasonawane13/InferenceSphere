"""Spark aggregations and persisted metrics for graph datasets."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from graphframes import GraphFrame
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    col,
    coalesce,
    count,
    lit,
    max as spark_max,
    min as spark_min,
)

from src.utils.helpers import ensure_directories


def _degree_table(graph: GraphFrame) -> DataFrame:
    vertices = graph.vertices.select("id")
    in_degree = graph.inDegrees.select("id", "inDegree")
    out_degree = graph.outDegrees.select("id", "outDegree")
    return (
        vertices.join(in_degree, "id", "left")
        .join(out_degree, "id", "left")
        .fillna(0, ["inDegree", "outDegree"])
        .withColumnRenamed("inDegree", "in_degree")
        .withColumnRenamed("outDegree", "out_degree")
        .withColumn(
            "total_degree",
            coalesce(col("in_degree"), lit(0))
            + coalesce(col("out_degree"), lit(0)),
        )
        .select("id", "in_degree", "out_degree", "total_degree")
    )


def calculate_graph_metrics(
    graph: GraphFrame,
    output_dir: Path,
    *,
    directed: bool = True,
) -> dict[str, Any]:
    """Calculate graph metrics and write degrees Parquet plus summary JSON.

    Undirected inputs use the project's symmetric edge representation. Their
    physical edge count is divided by two for logical edge count and density.
    Degree aggregations stay distributed by vertex key, avoiding collection of
    high-degree adjacency lists on the driver.
    """
    output_dir = Path(output_dir)
    ensure_directories(output_dir)
    degrees = _degree_table(graph)
    degree_path = output_dir / "degrees"
    degrees.write.mode("overwrite").parquet(str(degree_path))

    physical_edges = graph.edges.count()
    node_count = graph.vertices.select("id").count()
    edge_count = physical_edges if directed else physical_edges // 2
    possible_edges = node_count * (node_count - 1)
    if not directed:
        possible_edges //= 2
    summary_row = degrees.agg(
        avg("in_degree").alias("average_in_degree"),
        avg("out_degree").alias("average_out_degree"),
        avg("total_degree").alias("average_total_degree"),
        spark_min("total_degree").alias("minimum_degree"),
        spark_max("total_degree").alias("maximum_degree"),
    ).first()
    distribution = [
        {"total_degree": int(row["total_degree"]), "node_count": int(row["node_count"])}
        for row in degrees.groupBy("total_degree")
        .agg(count("*").alias("node_count"))
        .orderBy("total_degree")
        .collect()
    ]
    summary: dict[str, Any] = {
        "node_count": node_count,
        "edge_count": edge_count,
        "physical_edge_count": physical_edges,
        "average_in_degree": float(summary_row["average_in_degree"] or 0),
        "average_out_degree": float(summary_row["average_out_degree"] or 0),
        "average_total_degree": float(summary_row["average_total_degree"] or 0),
        "minimum_degree": int(summary_row["minimum_degree"] or 0),
        "maximum_degree": int(summary_row["maximum_degree"] or 0),
        "density": float(edge_count / possible_edges) if possible_edges else 0.0,
        "degree_distribution": distribution,
    }
    (output_dir / "graph_summary.json").write_text(
        json.dumps(summary, indent=2) + "\n",
        encoding="utf-8",
    )
    return summary
