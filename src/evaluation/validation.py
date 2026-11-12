"""Schema and artifact validation with pending status for missing outputs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from src.utils.helpers import ensure_directories


EXPECTED_OUTPUTS = {
    "pagerank": ("pagerank", ("user_id", "pagerank", "rank")),
    "hits": (
        "hits",
        ("user_id", "hub_score", "authority_score", "hub_rank", "authority_rank"),
    ),
    "label_propagation": (
        "communities",
        ("user_id", "community_id", "community_size"),
    ),
    "triangle_count": (
        "triangles",
        ("user_id", "triangle_count", "clustering_coefficient"),
    ),
    "cure": ("cure_clusters", ("user_id", "cluster_id", "features")),
    "son": (
        "frequent_itemsets",
        ("itemset", "support_count", "support_percent"),
    ),
}


def validate_project_outputs(
    spark: SparkSession,
    results_dir: Path,
    output_dir: Path,
) -> dict[str, Any]:
    """Validate available Parquet schemas; report absent artifacts as pending."""
    results_dir = Path(results_dir)
    output_dir = Path(output_dir)
    ensure_directories(output_dir)
    checks: list[dict[str, Any]] = []
    for name, (relative_path, required_columns) in EXPECTED_OUTPUTS.items():
        path = results_dir / relative_path
        if not path.is_dir():
            checks.append(
                {
                    "name": name,
                    "status": "pending",
                    "path": str(path),
                    "reason": "expected output is missing",
                }
            )
            continue
        actual = set(spark.read.parquet(str(path)).columns)
        missing = sorted(set(required_columns) - actual)
        checks.append(
            {
                "name": name,
                "status": "completed" if not missing else "failed",
                "path": str(path),
                "missing_columns": missing,
            }
        )
    report = {
        "status": "completed",
        "checks": checks,
        "pending_count": sum(check["status"] == "pending" for check in checks),
        "failed_count": sum(check["status"] == "failed" for check in checks),
    }
    (output_dir / "validation.json").write_text(
        json.dumps(report, indent=2) + "\n",
        encoding="utf-8",
    )
    return report
