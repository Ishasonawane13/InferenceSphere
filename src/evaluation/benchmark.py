"""Deterministic graph-scale benchmarking with explicit pending statuses."""

from __future__ import annotations

import csv
import json
import time
from pathlib import Path
from typing import Any, Iterable

from graphframes import GraphFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand

from src.utils.helpers import ensure_directories


ALGORITHMS = (
    "pagerank",
    "hits",
    "label_propagation",
    "triangle_count",
    "son",
    "cure",
)


def benchmark_scales(
    spark: SparkSession,
    graph: GraphFrame,
    output_dir: Path,
    *,
    scales: Iterable[float] = (0.01, 0.05, 0.10, 0.25),
    seed: int = 42,
    include_full: bool = False,
) -> dict[str, Any]:
    """Record deterministic scale sizes and pending algorithm benchmark rows.

    Algorithm execution is deliberately not inferred from artifact presence.
    Callers can later replace pending rows with measured runs, preserving the
    schema and avoiding fabricated timings.
    """
    normalized = sorted(set(float(scale) for scale in scales))
    if include_full:
        normalized.append(1.0)
    if any(scale <= 0 or scale > 1 for scale in normalized):
        raise ValueError("scales must be greater than 0 and at most 1")
    output_dir = Path(output_dir)
    ensure_directories(output_dir)
    vertex_count = graph.vertices.count()
    rows: list[dict[str, Any]] = []
    for scale in normalized:
        started = time.perf_counter()
        sampled_vertices = (
            graph.vertices
            if scale == 1.0
            else graph.vertices.orderBy(rand(seed=seed)).limit(
                max(1, int(vertex_count * scale))
            )
        )
        sampled = graph.edges.join(
            sampled_vertices.select(col("id").alias("src")),
            "src",
        ).join(
            sampled_vertices.select(col("id").alias("dst")),
            "dst",
        )
        vertices = sampled_vertices.count()
        edges = sampled.count()
        sampling_seconds = time.perf_counter() - started
        for algorithm in ALGORITHMS:
            rows.append(
                {
                    "scale_percent": scale * 100.0,
                    "algorithm": algorithm,
                    "status": "pending",
                    "vertices": vertices,
                    "edges": edges,
                    "sampling_seconds": sampling_seconds,
                    "algorithm_seconds": None,
                    "result_count": None,
                    "memory_bytes": None,
                    "parameters": None,
                    "reason": "algorithm execution not run by benchmark scaffold",
                }
            )
    report = {
        "status": "completed",
        "seed": seed,
        "scales": normalized,
        "rows": rows,
        "pending_count": len(rows),
    }
    (output_dir / "scalability_benchmark.json").write_text(
        json.dumps(report, indent=2) + "\n",
        encoding="utf-8",
    )
    with (output_dir / "scalability_benchmark.csv").open(
        "w",
        newline="",
        encoding="utf-8",
    ) as target:
        fieldnames = list(rows[0]) if rows else [
            "scale_percent",
            "algorithm",
            "status",
        ]
        writer = csv.DictWriter(target, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return report
