"""Comparison reports assembled only from algorithm outputs that exist."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Sequence

from src.config import AppConfig, load_config
from src.utils.helpers import ensure_directories


DEFAULT_SON_SUPPORTS = (0.005, 0.01, 0.05)


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    with path.open(encoding="utf-8") as source:
        value = json.load(source)
    if not isinstance(value, dict):
        raise ValueError(f"Expected a JSON object in {path}")
    return value


def default_cure_parameter_sets(config: AppConfig) -> list[dict[str, Any]]:
    algorithm = config.algorithm
    return [
        {
            "k": algorithm.cure_k,
            "c": algorithm.cure_representatives,
            "alpha": algorithm.cure_alpha,
            "sample_size": algorithm.cure_sample_size,
        },
        {
            "k": algorithm.cure_k,
            "c": algorithm.cure_representatives * 2,
            "alpha": algorithm.cure_alpha,
            "sample_size": algorithm.cure_sample_size,
        },
        {
            "k": algorithm.cure_k,
            "c": algorithm.cure_representatives,
            "alpha": min(1.0, algorithm.cure_alpha + 0.25),
            "sample_size": algorithm.cure_sample_size,
        },
    ]


def _record(
    comparison: str,
    variant: str,
    path: Path,
    metrics: dict[str, Any] | None,
    reason: str,
) -> dict[str, Any]:
    return {
        "comparison": comparison,
        "variant": variant,
        "status": "complete" if metrics is not None else "pending",
        "metrics": metrics,
        "source": str(path),
        "pending_reason": None if metrics is not None else reason,
    }


def build_comparison_records(
    results_dir: Path,
    *,
    config: AppConfig | None = None,
    son_supports: Sequence[float] = DEFAULT_SON_SUPPORTS,
    cure_parameter_sets: Sequence[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Make records for each requested comparison, marking absent results pending."""
    config = config or load_config()
    root = Path(results_dir)
    records: list[dict[str, Any]] = []
    pagerank_path = root / "pagerank_evaluation.json"
    pagerank = _read_json(pagerank_path)
    records.append(
        _record(
            "pagerank_vs_degree",
            "top_100_overlap",
            pagerank_path,
            (
                {"degree_top_100_overlap": pagerank["degree_top_100_overlap"]}
                if pagerank is not None
                and "degree_top_100_overlap" in pagerank
                else None
            ),
            "PageRank/degree evaluation has not been produced.",
        )
    )

    hits_path = root / "hits_evaluation.json"
    hits = _read_json(hits_path)
    hit_fields = (
        "pagerank_top_100_overlap_hubs",
        "pagerank_top_100_overlap_authorities",
        "pagerank_spearman_hubs",
        "pagerank_spearman_authorities",
    )
    hit_metrics = (
        {name: hits[name] for name in hit_fields if name in hits}
        if hits
        else None
    )
    records.append(
        _record(
            "pagerank_vs_hits",
            "top_100_overlap_and_spearman",
            hits_path,
            hit_metrics or None,
            "PageRank/HITS evaluation has not been produced.",
        )
    )

    for comparison, filename in (
        ("label_propagation", "label_propagation_evaluation.json"),
        ("triangle_and_clustering", "triangle_count_stats.json"),
    ):
        path = root / filename
        records.append(
            _record(
                comparison,
                "summary",
                path,
                _read_json(path),
                f"{comparison} evaluation has not been produced.",
            )
        )

    for support in son_supports:
        if not 0 < support <= 1:
            raise ValueError("SON supports must be greater than 0 and at most 1")
        path = root / f"son_{support:g}" / "son_evaluation.json"
        records.append(
            _record(
                "son_min_support",
                f"min_support={support:g}",
                path,
                _read_json(path),
                f"SON at min_support={support:g} has not been run.",
            )
        )

    parameter_sets = list(
        cure_parameter_sets
        if cure_parameter_sets is not None
        else default_cure_parameter_sets(config)
    )
    for index, parameters in enumerate(parameter_sets):
        if {"k", "c", "alpha", "sample_size"} - parameters.keys():
            raise ValueError("CURE parameter sets require k, c, alpha, sample_size")
        path = (
            root / "cure" / "cure_evaluation.json"
            if index == 0
            else root
            / "cure"
            / (
                f"k_{parameters['k']}_c_{parameters['c']}"
                f"_alpha_{float(parameters['alpha']):g}"
                f"_sample_{parameters['sample_size']}"
            )
            / "cure_evaluation.json"
        )
        variant = ",".join(
            f"{key}={parameters[key]}" for key in ("k", "c", "alpha", "sample_size")
        )
        records.append(
            _record(
                "cure_parameters",
                variant,
                path,
                _read_json(path),
                "CURE with this parameter set has not been run.",
            )
        )
    return records


def write_comparison_reports(
    records: Sequence[dict[str, Any]],
    output_dir: Path,
) -> tuple[Path, Path]:
    """Save comparison JSON and a CSV with nested metrics encoded as JSON."""
    output_dir = Path(output_dir)
    ensure_directories(output_dir)
    json_path = output_dir / "algorithm_comparisons.json"
    csv_path = output_dir / "algorithm_comparisons.csv"
    json_path.write_text(
        json.dumps(list(records), indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    fields = ("comparison", "variant", "status", "metrics_json", "source", "pending_reason")
    with csv_path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        for item in records:
            writer.writerow(
                {
                    "comparison": item["comparison"],
                    "variant": item["variant"],
                    "status": item["status"],
                    "metrics_json": (
                        json.dumps(item["metrics"], allow_nan=False)
                        if item["metrics"] is not None
                        else ""
                    ),
                    "source": item["source"],
                    "pending_reason": item["pending_reason"] or "",
                }
            )
    return json_path, csv_path


def run_comparisons(
    config: AppConfig | None = None,
    *,
    results_dir: Path | None = None,
    output_dir: Path | None = None,
    son_supports: Sequence[float] = DEFAULT_SON_SUPPORTS,
    cure_parameter_sets: Sequence[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    config = config or load_config()
    records = build_comparison_records(
        results_dir or config.paths.results, config=config,
        son_supports=son_supports, cure_parameter_sets=cure_parameter_sets,
    )
    write_comparison_reports(records, output_dir or config.paths.tables)
    return records