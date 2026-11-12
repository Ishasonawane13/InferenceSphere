"""CURE clustering over a deterministic sample of distributed graph features."""

from __future__ import annotations

import heapq
import json
import math
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from graphframes import GraphFrame
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    col,
    count,
    isnan,
    lit,
    log1p,
    stddev_pop,
    sum as spark_sum,
    when,
    xxhash64,
)
from pyspark.sql.types import IntegerType, StructField, StructType

from src.config import AppConfig, load_config
from src.graph.graph_builder import build_directed_graph
from src.utils.helpers import ensure_directories


FEATURE_COLUMNS = (
    "in_degree",
    "out_degree",
    "total_degree",
    "pagerank",
    "hub",
    "authority",
    "triangle_count",
    "clustering_coefficient",
)

Point = tuple[float, ...]


@dataclass
class _Cluster:
    cluster_id: int
    members: list[tuple[int, Point]]
    centroid: Point
    representatives: list[Point]


def _squared_distance(left: Sequence[float], right: Sequence[float]) -> float:
    return sum((left[index] - right[index]) ** 2 for index in range(len(left)))


def _select_representatives(
    points: Sequence[Sequence[float]],
    *,
    count: int,
    alpha: float,
) -> list[Point]:
    """Choose farthest-first points, then shrink each toward the cluster centroid."""
    if not points:
        raise ValueError("A CURE cluster must contain at least one point")
    if count <= 0:
        raise ValueError("representative count must be positive")
    if not math.isfinite(alpha) or not 0.0 <= alpha <= 1.0:
        raise ValueError("alpha must be between 0 and 1")
    dimensions = len(points[0])
    if dimensions == 0 or any(len(point) != dimensions for point in points):
        raise ValueError("CURE points must have the same non-zero number of features")

    centroid = tuple(
        sum(point[dimension] for point in points) / len(points)
        for dimension in range(dimensions)
    )
    selected = [
        max(
            range(len(points)),
            key=lambda index: (
                _squared_distance(points[index], centroid),
                -index,
            ),
        )
    ]
    while len(selected) < min(count, len(points)):
        remaining = (
            index for index in range(len(points)) if index not in selected
        )
        selected.append(
            max(
                remaining,
                key=lambda index: (
                    min(
                        _squared_distance(points[index], points[chosen])
                        for chosen in selected
                    ),
                    -index,
                ),
            )
        )
    return [
        tuple(
            centroid[dimension]
            + alpha * (points[index][dimension] - centroid[dimension])
            for dimension in range(dimensions)
        )
        for index in selected
    ]


def _make_cluster(
    cluster_id: int,
    members: list[tuple[int, Point]],
    representative_count: int,
    alpha: float,
) -> _Cluster:
    dimensions = len(members[0][1])
    centroid = tuple(
        sum(point[dimension] for _, point in members) / len(members)
        for dimension in range(dimensions)
    )
    representatives = _select_representatives(
        [point for _, point in members], count=representative_count, alpha=alpha
    )
    return _Cluster(cluster_id, members, centroid, representatives)


def _representative_distance(left: _Cluster, right: _Cluster) -> float:
    return min(
        _squared_distance(left_point, right_point)
        for left_point in left.representatives
        for right_point in right.representatives
    )


def cure_cluster(
    features: Sequence[Sequence[float]],
    *,
    k: int,
    c: int,
    alpha: float,
) -> list[int]:
    """Cluster points using CURE's representative-point agglomerative merges."""
    point_count = len(features)
    if k < 2 or k >= point_count:
        raise ValueError("k must be at least 2 and smaller than the sample size")
    if c <= 0:
        raise ValueError("c must be positive")
    if not math.isfinite(alpha) or not 0.0 <= alpha <= 1.0:
        raise ValueError("alpha must be between 0 and 1")
    dimensions = len(features[0])
    if dimensions == 0 or any(len(point) != dimensions for point in features):
        raise ValueError("CURE points must have the same non-zero number of features")
    points: list[Point] = []
    for point in features:
        converted = tuple(float(value) for value in point)
        if not all(math.isfinite(value) for value in converted):
            raise ValueError("CURE features must be finite")
        points.append(converted)

    clusters = {
        index: _make_cluster(index, [(index, point)], c, alpha)
        for index, point in enumerate(points)
    }
    queue: list[tuple[float, int, int]] = []
    for left_id in range(point_count):
        for right_id in range(left_id + 1, point_count):
            heapq.heappush(
                queue,
                (
                    _representative_distance(
                        clusters[left_id],
                        clusters[right_id],
                    ),
                    left_id,
                    right_id,
                ),
            )

    next_id = point_count
    while len(clusters) > k:
        while queue:
            _, left_id, right_id = heapq.heappop(queue)
            if left_id in clusters and right_id in clusters:
                break
        else:
            raise RuntimeError("CURE exhausted merge candidates before reaching k")

        members = clusters.pop(left_id).members + clusters.pop(right_id).members
        merged = _make_cluster(next_id, members, c, alpha)
        clusters[next_id] = merged
        for other_id, other in clusters.items():
            if other_id != next_id:
                heapq.heappush(
                    queue,
                    (
                        _representative_distance(merged, other),
                        min(next_id, other_id),
                        max(next_id, other_id),
                    ),
                )
        next_id += 1

    ordered_clusters = sorted(
        clusters.values(),
        key=lambda cluster: min(index for index, _ in cluster.members),
    )
    labels = [0] * point_count
    for label, cluster in enumerate(ordered_clusters):
        for index, _ in cluster.members:
            labels[index] = label
    return labels


def _silhouette_score(features: Sequence[Point], labels: Sequence[int]) -> float:
    clusters: dict[int, list[int]] = {}
    for index, label in enumerate(labels):
        clusters.setdefault(label, []).append(index)
    if len(clusters) < 2:
        raise ValueError("Silhouette score requires at least two clusters")

    values: list[float] = []
    for index, label in enumerate(labels):
        own = clusters[label]
        if len(own) == 1:
            values.append(0.0)
            continue
        within = sum(
            math.sqrt(_squared_distance(features[index], features[other]))
            for other in own
            if other != index
        ) / (len(own) - 1)
        between = min(
            sum(
                math.sqrt(_squared_distance(features[index], features[other]))
                for other in members
            )
            / len(members)
            for other_label, members in clusters.items()
            if other_label != label
        )
        denominator = max(within, between)
        values.append((between - within) / denominator if denominator else 0.0)
    return sum(values) / len(values)


def _feature_table(
    graph: GraphFrame,
    pagerank: DataFrame,
    hits: DataFrame,
    triangles: DataFrame,
) -> DataFrame:
    required = {
        "pagerank": {"user_id", "pagerank"},
        "hits": {"user_id", "hub_score", "authority_score"},
        "triangles": {"user_id", "triangle_count", "clustering_coefficient"},
    }
    for name, frame in (
        ("pagerank", pagerank),
        ("hits", hits),
        ("triangles", triangles),
    ):
        missing = required[name] - set(frame.columns)
        if missing:
            raise ValueError(f"{name} table is missing columns: {sorted(missing)}")

    in_degree = graph.inDegrees.select(
        col("id").alias("user_id"),
        col("inDegree").cast("double").alias("in_degree"),
    )
    out_degree = graph.outDegrees.select(
        col("id").alias("user_id"),
        col("outDegree").cast("double").alias("out_degree"),
    )
    features = (
        graph.vertices.select(col("id").alias("user_id"))
        .join(in_degree, "user_id", "left")
        .join(out_degree, "user_id", "left")
        .join(pagerank.select("user_id", "pagerank"), "user_id", "left")
        .join(
            hits.select(
                "user_id",
                col("hub_score").alias("hub"),
                col("authority_score").alias("authority"),
            ),
            "user_id",
            "left",
        )
        .join(
            triangles.select(
                "user_id",
                "triangle_count",
                "clustering_coefficient",
            ),
            "user_id",
            "left",
        )
        .withColumn(
            "total_degree",
            col("in_degree").cast("double") + col("out_degree").cast("double"),
        )
    )

    for name in FEATURE_COLUMNS:
        features = features.withColumn(
            name,
            when(
                col(name).cast("double").isNull()
                | isnan(col(name).cast("double")),
                lit(0.0),
            )
            .otherwise(col(name).cast("double"))
            .cast("double"),
        )
    negative_counts = features.agg(
        *[
            spark_sum(when(col(name) < 0, 1).otherwise(0)).alias(name)
            for name in FEATURE_COLUMNS
        ]
    ).first()
    invalid = [name for name in FEATURE_COLUMNS if (negative_counts[name] or 0) > 0]
    if invalid:
        raise ValueError(f"Graph features must be non-negative before log1p: {invalid}")

    transformed = features.select(
        "user_id",
        *[log1p(col(name)).alias(name) for name in FEATURE_COLUMNS],
    )
    statistics = transformed.agg(
        *[
            expression
            for name in FEATURE_COLUMNS
            for expression in (
                avg(col(name)).alias(f"{name}_mean"),
                stddev_pop(col(name)).alias(f"{name}_std"),
            )
        ]
    ).first()
    for name in FEATURE_COLUMNS:
        mean = float(statistics[f"{name}_mean"] or 0.0)
        deviation = float(statistics[f"{name}_std"] or 0.0)
        transformed = transformed.withColumn(
            name,
            (col(name) - lit(mean)) / lit(deviation)
            if deviation > 0.0
            else lit(0.0),
        )
    return transformed


def run_cure(
    graph: GraphFrame,
    pagerank: DataFrame,
    hits: DataFrame,
    triangles: DataFrame,
    output_dir: Path,
    *,
    config: AppConfig | None = None,
    k: int | None = None,
    c: int | None = None,
    alpha: float | None = None,
    sample_size: int | None = None,
) -> dict[str, Any]:
    """Build distributed features and run CURE on a deterministic sample only."""
    config = config or load_config()
    k = config.algorithm.cure_k if k is None else k
    c = config.algorithm.cure_representatives if c is None else c
    alpha = config.algorithm.cure_alpha if alpha is None else alpha
    sample_size = config.algorithm.cure_sample_size if sample_size is None else sample_size
    seed = config.algorithm.random_seed
    if k < 2 or c <= 0 or sample_size <= k:
        raise ValueError("Require k >= 2, c > 0, and sample_size > k")
    if not math.isfinite(alpha) or not 0.0 <= alpha <= 1.0:
        raise ValueError("alpha must be between 0 and 1")

    output_dir = Path(output_dir)
    ensure_directories(output_dir)
    started = time.perf_counter()
    features = _feature_table(graph, pagerank, hits, triangles).cache()
    try:
        feature_table_users = features.count()
        if feature_table_users <= k:
            raise ValueError(
                f"CURE needs more than k={k} feature rows; found {feature_table_users}"
            )
        features.write.mode("overwrite").parquet(str(output_dir / "feature_table"))

        sample_rows = (
            features.orderBy(
                xxhash64(col("user_id"), lit(seed)),
                col("user_id"),
            )
            .limit(sample_size)
            .collect()
        )
        if len(sample_rows) <= k:
            raise ValueError(
                f"CURE needs more than k={k} sampled users; got {len(sample_rows)}"
            )
        vectors = [
            tuple(float(row[name]) for name in FEATURE_COLUMNS)
            for row in sample_rows
        ]
        labels = cure_cluster(vectors, k=k, c=c, alpha=alpha)
        silhouette = _silhouette_score(vectors, labels)
        clustered_rows = [
            {**row.asDict(), "cluster_id": int(label)}
            for row, label in zip(sample_rows, labels)
        ]
        clustered_schema = StructType(
            [
                *features.schema.fields,
                StructField("cluster_id", IntegerType(), nullable=False),
            ]
        )
        clustered = features.sparkSession.createDataFrame(
            clustered_rows,
            schema=clustered_schema,
        ).select("user_id", "cluster_id", *FEATURE_COLUMNS)
        clustered.write.mode("overwrite").parquet(str(output_dir / "clusters"))

        summary_expressions = [
            avg(col(name)).alias(f"mean_{name}") for name in FEATURE_COLUMNS
        ]
        cluster_summary = clustered.groupBy("cluster_id").agg(
            count("*").alias("user_count"),
            *summary_expressions,
        )
        cluster_summary.write.mode("overwrite").parquet(
            str(output_dir / "cluster_summary")
        )
        cluster_sizes = [
            {
                "cluster_id": int(row["cluster_id"]),
                "user_count": int(row["user_count"]),
            }
            for row in cluster_summary.select("cluster_id", "user_count")
            .orderBy("cluster_id")
            .collect()
        ]
        summary: dict[str, Any] = {
            "total_users": feature_table_users,
            "feature_table_users": feature_table_users,
            "users_clustered": len(sample_rows),
            "sampling_strategy": (
                "Deterministic seeded hash ordering of user_id; only the first "
                "sample_size rows are collected and clustered on the driver."
            ),
            "parameters": {
                "k": k,
                "c": c,
                "alpha": alpha,
                "sample_size": sample_size,
                "seed": seed,
            },
            "missing_values": (
                "Null and NaN feature metrics, including missing joined metric rows, "
                "are filled with 0 before log1p and standard scaling."
            ),
            "scaling": (
                "Distributed log1p followed by population-standard-deviation "
                "standardization; constant features are set to 0."
            ),
            "silhouette_score": silhouette,
            "cluster_sizes": cluster_sizes,
            "execution_seconds": time.perf_counter() - started,
            "limitations": [
                "Only the deterministic sample is clustered; other users are not assigned.",
                "CURE clustering and silhouette evaluation run on the driver.",
                "Hierarchical pairwise merging can be expensive in time and memory; "
                "increase sample_size only when driver resources permit.",
            ],
            "outputs": {
                "feature_table": str(output_dir / "feature_table"),
                "clusters": str(output_dir / "clusters"),
                "cluster_summary": str(output_dir / "cluster_summary"),
            },
        }
        (output_dir / "cure_evaluation.json").write_text(
            json.dumps(summary, indent=2) + "\n",
            encoding="utf-8",
        )
        return summary
    finally:
        features.unpersist()


def run_configured_cure(
    output_dir: Path | None = None,
    *,
    config: AppConfig | None = None,
    k: int | None = None,
    c: int | None = None,
    alpha: float | None = None,
    sample_size: int | None = None,
) -> dict[str, Any]:
    """Load outputs from Parts 6, 7, and 9 and run configured CURE."""
    config = config or load_config()
    from src.spark_session import create_spark_session

    metric_paths = {
        "PageRank": config.paths.results / "pagerank",
        "HITS": config.paths.results / "hits",
        "triangle count": config.paths.results / "triangles",
    }
    for name, path in metric_paths.items():
        if not path.is_dir():
            raise FileNotFoundError(
                f"{name} output not found at {path}; run its Part 6, 7, or 9 "
                "pipeline before CURE."
            )

    spark = create_spark_session(config)
    try:
        graph = build_directed_graph(spark, config)
        return run_cure(
            graph,
            spark.read.parquet(str(metric_paths["PageRank"])),
            spark.read.parquet(str(metric_paths["HITS"])),
            spark.read.parquet(str(metric_paths["triangle count"])),
            output_dir or config.paths.results / "cure",
            config=config,
            k=k,
            c=c,
            alpha=alpha,
            sample_size=sample_size,
        )
    finally:
        spark.stop()
