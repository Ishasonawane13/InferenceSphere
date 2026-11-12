"""Aggregate already-produced algorithm evaluations without inventing results."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from src.utils.helpers import ensure_directories


_EVALUATIONS = {
    "pagerank": "pagerank_evaluation.json",
    "hits": "hits_evaluation.json",
    "label_propagation": "label_propagation_evaluation.json",
    "triangle_count": "triangle_count_stats.json",
    "son": "son_evaluation.json",
    "cure": "cure_evaluation.json",
}


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    with path.open(encoding="utf-8") as source:
        value = json.load(source)
    return value if isinstance(value, dict) else None


def _comparison_rows(results_dir: Path) -> list[dict[str, Any]]:
    loaded = {
        name: _read_json(results_dir / filename)
        for name, filename in _EVALUATIONS.items()
    }
    son_runs = [
        _read_json(path / "son_evaluation.json")
        for path in sorted(results_dir.glob("son_*"))
        if path.is_dir()
    ]
    son_runs = [run for run in son_runs if run is not None]
    rows: list[dict[str, Any]] = []
    comparisons = (
        ("pagerank_vs_degree", loaded["pagerank"], "degree_top_100_overlap"),
        (
            "pagerank_vs_hits",
            loaded["hits"],
            "pagerank_top_100_overlap_authorities",
        ),
        ("lpa_statistics", loaded["label_propagation"], None),
        ("triangle_statistics", loaded["triangle_count"], None),
        ("son_thresholds", son_runs or loaded["son"], "min_support"),
        ("cure_parameters", loaded["cure"], "parameters"),
    )
    for name, value, metric in comparisons:
        row: dict[str, Any] = {"comparison": name}
        if value is None:
            row["status"] = "pending"
            row["reason"] = "required evaluation artifact is missing"
        else:
            row["status"] = "completed"
            if isinstance(value, list):
                row["metric"] = [
                    run.get(metric) for run in value if metric is not None
                ]
            elif metric is not None:
                row["metric"] = value.get(metric)
            row["source"] = str(results_dir)
        rows.append(row)
    return rows


def build_comparison_report(
    results_dir: Path,
    output_dir: Path,
) -> dict[str, Any]:
    """Write comparison CSV/JSON using only existing evaluation artifacts."""
    results_dir = Path(results_dir)
    output_dir = Path(output_dir)
    ensure_directories(output_dir)
    rows = _comparison_rows(results_dir)
    report = {
        "status": "completed",
        "results_dir": str(results_dir),
        "comparisons": rows,
        "pending_count": sum(row["status"] == "pending" for row in rows),
    }
    (output_dir / "comparisons.json").write_text(
        json.dumps(report, indent=2) + "\n",
        encoding="utf-8",
    )
    with (output_dir / "comparisons.csv").open(
        "w",
        newline="",
        encoding="utf-8",
    ) as target:
        fieldnames = ["comparison", "status", "metric", "source", "reason"]
        writer = csv.DictWriter(target, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return report
