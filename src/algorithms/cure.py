"""Sampled CURE clustering over distributed graph-derived features.

CURE is intentionally applied only to a deterministic bounded sample. Missing
algorithm outputs are filled with zero before log1p and standard scaling; this
keeps feature assembly restartable while making the limitation explicit.
"""

from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import numpy as np
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    array,
    col,
    lit,
    log1p,
    rand,
    avg,
    stddev_pop,
    udf,
    when,
)
from pyspark.sql.window import Window

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


@dataclass
class _Cluster:
    members: list[int]
    points: np.ndarray
    representatives: np.ndarray


def _representatives(
    points: np.ndarray,
    count: int,
    alpha: float,
) -> np.ndarray:
    centroid = points.mean(axis=0)
    if len(points) <= count:
        selected = points.copy()
    else:
        selected_indices = [int(np.argmax(np.linalg.norm(points - centroid, axis=1)))]
        while len(selected_indices) < count:
            distances = np.min(
                np.linalg.norm(
                    points[:, None, :] - points[selected_indices][None, :, :],
                    axis=2,
                ),
                axis=1,
            )
            distances[selected_indices] = -1
            selected_indices.append(int(np.argmax(distances)))
        selected = points[selected_indices]
    return centroid + alpha * (selected - centroid)


def _cure(
    user_ids: list[int],
    features: np.ndarray,
    cluster_count: int,
    representative_count: int,
    shrink_factor: float,
) -> tuple[np.ndarray, list[np.ndarray]]:
    """Run deterministic CURE and return labels plus final representatives."""
    clusters = [
        _Cluster(
            members=[index],
            points=features[index : index + 1],
            representatives=features[index : index + 1].copy(),
        )
        for index in range(len(user_ids))
    ]
    while len(clusters) > cluster_count:
        best_pair: tuple[int, int] | None = None
        best_distance = float("inf")
        for left in range(len(clusters)):
            for right in range(left + 1, len(clusters)):
                distance = float(
                    np.min(
                        np.linalg.norm(
                            clusters[left].representatives[:, None, :]
                            - clusters[right].representatives[None, :, :],
                            axis=2,
                        )
                    )
                )
                if distance < best_distance:
                    best_distance = distance
                    best_pair = (left, right)
        if best_pair is None:
            break
        left, right = best_pair
        merged_points = np.vstack((clusters[left].points, clusters[right].points))
        merged = _Cluster(
            members=clusters[left].members + clusters[right].members,
            points=merged_points,
            representatives=_representatives(
                merged_points,
                representative_count,
                shrink_factor,
            ),
        )
        clusters = [
            cluster
            for index, cluster in enumerate(clusters)
            if index not in best_pair
        ]
        clusters.append(merged)

    labels = np.zeros(len(user_ids), dtype=np.int64)
    representatives: list[np.ndarray] = []
    for label, cluster in enumerate(clusters):
        labels[cluster.members] = label
        representatives.append(cluster.representatives)
    return labels, representatives


def _silhouette(features: np.ndarray, labels: np.ndarray) -> float | None:
    if len(np.unique(labels)) < 2 or len(features) < 2:
        return None
    distances = np.linalg.norm(features[:, None, :] - features[None, :, :], axis=2)
    values: list[float] = []
    for index in range(len(features)):
        same = labels == labels[index]
        same[index] = False
        if not same.any():
            continue
        within = float(distances[index, same].mean())
        between = min(
            float(distances[index, labels == other].mean())
            for other in np.unique(labels)
            if other != labels[index]
        )
        values.append((between - within) / max(within, between, 1e-12))
    return float(np.mean(values)) if values else None


def _read_optional(
    spark: SparkSession,
    path: Path,
    columns: list[str],
) -> DataFrame | None:
    if not path.is_dir():
        return None
    return spark.read.parquet(str(path)).select(*columns)


def build_feature_table(spark: SparkSession, config: AppConfig) -> DataFrame:
    """Build and standardize graph features with zero-filled missing sources."""
    graph = build_directed_graph(spark, config)
    degrees = graph.inDegrees.select(
        col("id").alias("user_id"),
        col("inDegree").cast("double").alias("in_degree"),
    ).join(
        graph.outDegrees.select(
            col("id").alias("user_id"),
            col("outDegree").cast("double").alias("out_degree"),
        ),
        "user_id",
        "full",
    ).fillna(0.0, ["in_degree", "out_degree"]).withColumn(
        "total_degree",
        col("in_degree") + col("out_degree"),
    )
    features = degrees
    optional_sources = (
        (
            config.paths.results / "pagerank",
            ["user_id", "pagerank"],
        ),
        (
            config.paths.results / "hits",
            ["user_id", "hub_score", "authority_score"],
        ),
        (
            config.paths.results / "triangles",
            ["user_id", "triangle_count", "clustering_coefficient"],
        ),
    )
    for path, columns in optional_sources:
        source = _read_optional(spark, path, columns)
        if source is not None:
            features = features.join(source, "user_id", "left")
    for column in FEATURE_COLUMNS:
        if column not in features.columns:
            features = features.withColumn(column, lit(0.0))
        else:
            features = features.withColumn(
                column,
                col(column).cast("double"),
            ).fillna(0.0, [column])
    transformed = features
    for column in FEATURE_COLUMNS:
        transformed = transformed.withColumn(
            f"{column}_log",
            log1p(when(col(column) >= 0, col(column)).otherwise(lit(0.0))),
        )
    log_columns = [f"{column}_log" for column in FEATURE_COLUMNS]
    means = transformed.agg(
        *[avg(column).alias(f"{column}_mean") for column in log_columns]
    ).first().asDict()
    deviations = transformed.agg(
        *[stddev_pop(column).alias(f"{column}_std") for column in log_columns]
    ).first().asDict()
    scaled = transformed
    for column in log_columns:
        mean = float(means[f"{column}_mean"] or 0.0)
        deviation = float(deviations[f"{column}_std"] or 0.0)
        scaled = scaled.withColumn(
            column,
            (col(column) - lit(mean)) / lit(deviation if deviation > 0 else 1.0),
        )
    return scaled.select(
        "user_id",
        array(*[col(column) for column in log_columns]).alias("features"),
        *FEATURE_COLUMNS,
    )


def run_cure(
    spark: SparkSession,
    output_dir: Path,
    *,
    config: AppConfig | None = None,
    cluster_count: int = 2,
    representative_count: int = 5,
    shrink_factor: float = 0.2,
    sample_size: int = 1000,
    assign_remaining: bool = False,
    silhouette_limit: int = 1000,
) -> dict[str, Any]:
    """Run sampled CURE and persist clustered users and evaluation metadata."""
    config = config or load_config()
    if cluster_count <= 0 or representative_count <= 0 or sample_size <= 0:
        raise ValueError("cluster_count, representative_count, and sample_size must be positive")
    if not 0 < shrink_factor <= 1:
        raise ValueError("shrink_factor must be greater than 0 and at most 1")
    output_dir = Path(output_dir)
    ensure_directories(output_dir)
    started = time.perf_counter()
    feature_table = build_feature_table(spark, config).cache()
    total_users = feature_table.count()
    sample = feature_table.orderBy(
        rand(seed=config.algorithm.random_seed)
    ).limit(min(sample_size, total_users))
    sample_rows = sample.select("user_id", "features").collect()
    if len(sample_rows) < cluster_count:
        raise ValueError("sample_size must be at least cluster_count")
    user_ids = [int(row["user_id"]) for row in sample_rows]
    matrix = np.asarray([row["features"] for row in sample_rows], dtype=np.float64)
    labels, representatives = _cure(
        user_ids,
        matrix,
        cluster_count,
        representative_count,
        shrink_factor,
    )
    label_rows = [
        (user_id, int(label))
        for user_id, label in zip(user_ids, labels)
    ]
    labels_df = spark.createDataFrame(label_rows, ["user_id", "cluster_id"])
    clustered = sample.join(labels_df, "user_id").select(
        "user_id",
        "cluster_id",
        "features",
    )
    users_clustered = len(sample_rows)
    if assign_remaining and len(representatives) > 0:
        representative_rows = [
            (cluster_id, point.tolist())
            for cluster_id, points in enumerate(representatives)
            for point in points
        ]
        broadcast_representatives = spark.sparkContext.broadcast(representative_rows)

        def nearest_cluster(values: list[float]) -> int:
            vector = np.asarray(values, dtype=np.float64)
            return min(
                broadcast_representatives.value,
                key=lambda item: float(np.linalg.norm(vector - np.asarray(item[1]))),
            )[0]

        nearest_cluster_udf = udf(nearest_cluster, "int")
        remaining = feature_table.join(
            sample.select("user_id"),
            "user_id",
            "left_anti",
        ).select("user_id", "features").withColumn(
            "cluster_id",
            nearest_cluster_udf(col("features")),
        )
        clustered = clustered.unionByName(
            remaining.select("user_id", "cluster_id", "features")
        )
        users_clustered = total_users
        broadcast_representatives.unpersist()
    clustered.write.mode("overwrite").parquet(str(output_dir / "cure_clusters"))
    cluster_summary = clustered.groupBy("cluster_id").count().withColumnRenamed(
        "count", "cluster_size"
    )
    cluster_summary.write.mode("overwrite").parquet(
        str(output_dir / "cure_cluster_summary")
    )
    silhouette_matrix = matrix
    silhouette_labels = labels
    if len(matrix) > silhouette_limit:
        indices = np.linspace(0, len(matrix) - 1, silhouette_limit, dtype=int)
        silhouette_matrix = matrix[indices]
        silhouette_labels = labels[indices]
    summary_rows = cluster_summary.collect()
    evaluation = {
        "total_users": total_users,
        "feature_table_users": total_users,
        "users_clustered": len(sample_rows),
        "users_assigned": users_clustered,
        "sampling_strategy": "seeded_random_order_limit",
        "parameters": {
            "cluster_count": cluster_count,
            "representative_count": representative_count,
            "shrink_factor": shrink_factor,
            "sample_size": sample_size,
            "assign_remaining": assign_remaining,
        },
        "cluster_sizes": [
            {
                "cluster_id": int(row["cluster_id"]),
                "cluster_size": int(row["cluster_size"]),
            }
            for row in summary_rows
        ],
        "silhouette_score": _silhouette(silhouette_matrix, silhouette_labels),
        "silhouette_sample_size": len(silhouette_matrix),
        "execution_seconds": time.perf_counter() - started,
        "limitations": [
            "CURE clustering is performed on the deterministic driver sample only.",
            "Missing PageRank, HITS, or triangle outputs are filled with zero.",
            "Remaining users are not assigned unless assign_remaining is implemented and enabled.",
        ],
    }
    (output_dir / "cure_evaluation.json").write_text(
        json.dumps(evaluation, indent=2) + "\n",
        encoding="utf-8",
    )
    feature_table.unpersist()
    return evaluation


def run_configured_cure(
    output_dir: Path | None = None,
    *,
    config: AppConfig | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    """Create a Spark session and run sampled CURE."""
    config = config or load_config()
    from src.spark_session import create_spark_session

    spark = create_spark_session(config)
    try:
        return run_cure(
            spark,
            output_dir or config.paths.results,
            config=config,
            **kwargs,
        )
    finally:
        spark.stop()
