"""Run InfluenceSphere algorithms in dependency order with restartable status."""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.config import AppConfig, load_config
from src.utils.helpers import ensure_directories


LOGGER = logging.getLogger("influencesphere.pipeline")
ALGORITHMS = (
    "pagerank",
    "hits",
    "label_propagation",
    "triangle_count",
    "son",
    "cure",
)
DEPENDENCIES = {
    "pagerank": (),
    "hits": ("pagerank",),
    "label_propagation": (),
    "triangle_count": (),
    "son": (),
    "cure": ("pagerank", "hits", "triangle_count"),
}


def _timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def _output_exists(path: Path) -> bool:
    return path.is_dir() and any(path.iterdir())


def _output_path(
    output_dir: Path,
    algorithm: str,
    min_support: float,
) -> Path:
    if algorithm == "son":
        return output_dir / f"son_{min_support:g}" / "frequent_itemsets"
    return output_dir / {
        "pagerank": "pagerank",
        "hits": "hits",
        "label_propagation": "communities",
        "triangle_count": "triangles",
        "cure": "cure_clusters",
    }[algorithm]


def _write_status(path: Path, statuses: dict[str, dict[str, Any]]) -> None:
    path.write_text(
        json.dumps(
            {"updated_at": _timestamp(), "algorithms": statuses},
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def _parse_spark_conf(values: list[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for value in values:
        if "=" not in value:
            raise ValueError(f"--spark-conf must use key=value: {value}")
        key, setting = value.split("=", 1)
        if not key:
            raise ValueError("--spark-conf key cannot be empty")
        result[key] = setting
    return result


def run_pipeline(
    config: AppConfig,
    *,
    algorithms: list[str],
    output_dir: Path | None = None,
    force: bool = False,
    min_support: float = 0.01,
    num_clusters: int = 2,
    iterations: int | None = None,
    sample_percent: float | None = None,
    spark_conf: dict[str, str] | None = None,
    benchmark: bool = False,
) -> dict[str, dict[str, Any]]:
    """Run selected algorithms and persist dashboard-friendly status JSON."""
    unknown = sorted(set(algorithms) - set(ALGORITHMS))
    if unknown:
        raise ValueError(f"Unknown algorithms: {', '.join(unknown)}")
    if not 0 < min_support <= 1:
        raise ValueError("min_support must be greater than 0 and at most 1")
    if sample_percent is not None and not 0 < sample_percent <= 100:
        raise ValueError("sample_percent must be greater than 0 and at most 100")
    selected = set(algorithms)
    changed = True
    while changed:
        changed = False
        for algorithm in tuple(selected):
            for dependency in DEPENDENCIES[algorithm]:
                if dependency not in selected:
                    selected.add(dependency)
                    changed = True
    algorithms = [algorithm for algorithm in ALGORITHMS if algorithm in selected]
    output_dir = Path(output_dir or config.paths.results)
    ensure_directories(output_dir)
    status_path = output_dir / "pipeline_status.json"
    statuses = {
        algorithm: {
            "status": "pending",
            "runtime_seconds": None,
            "timestamp": None,
        }
        for algorithm in algorithms
    }
    _write_status(status_path, statuses)
    spark = None
    directed = None
    undirected = None
    try:
        for algorithm in algorithms:
            output_path = _output_path(output_dir, algorithm, min_support)
            if _output_exists(output_path) and not force:
                statuses[algorithm].update(
                    {
                        "status": "done",
                        "skipped": True,
                        "reason": "output exists and --force was not supplied",
                        "timestamp": _timestamp(),
                    }
                )
                _write_status(status_path, statuses)
                continue
            blocked = [
                dependency
                for dependency in DEPENDENCIES[algorithm]
                if statuses.get(dependency, {}).get("status") != "done"
            ]
            if blocked:
                statuses[algorithm].update(
                    {
                        "status": "failed",
                        "error": f"dependencies not completed: {blocked}",
                        "timestamp": _timestamp(),
                    }
                )
                _write_status(status_path, statuses)
                continue
            statuses[algorithm].update(
                {"status": "running", "timestamp": _timestamp()}
            )
            _write_status(status_path, statuses)
            started = time.perf_counter()
            try:
                if spark is None:
                    spark = __import__(
                        "src.spark_session",
                        fromlist=["create_spark_session"],
                    ).create_spark_session(config, spark_conf=spark_conf)
                if algorithm in {"pagerank", "hits"}:
                    if directed is None:
                        from src.graph.graph_builder import build_directed_graph

                        directed = build_directed_graph(spark, config)
                    if algorithm == "pagerank":
                        from src.algorithms.pagerank import run_pagerank

                        run_pagerank(
                            directed,
                            output_dir,
                            config=config,
                            max_iterations=iterations,
                        )
                    else:
                        from src.algorithms.hits import run_hits

                        run_hits(
                            directed,
                            output_dir,
                            config=config,
                            max_iterations=iterations,
                        )
                elif algorithm in {"label_propagation", "triangle_count"}:
                    if undirected is None:
                        from src.graph.graph_builder import build_undirected_graph

                        undirected = build_undirected_graph(spark, config)
                    if algorithm == "label_propagation":
                        from src.algorithms.label_propagation import (
                            run_label_propagation,
                        )

                        run_label_propagation(
                            undirected,
                            output_dir,
                            config=config,
                            max_iterations=iterations,
                        )
                    else:
                        from src.algorithms.triangle_count import run_triangle_count

                        run_triangle_count(undirected, output_dir, config=config)
                elif algorithm == "son":
                    from src.algorithms.son import run_son

                    run_son(
                        spark,
                        output_dir / f"son_{min_support:g}",
                        config=config,
                        min_support=min_support,
                    )
                elif algorithm == "cure":
                    from src.algorithms.cure import run_cure

                    run_cure(
                        spark,
                        output_dir,
                        config=config,
                        cluster_count=num_clusters,
                    )
                statuses[algorithm].update(
                    {
                        "status": "done",
                        "timestamp": _timestamp(),
                        "runtime_seconds": time.perf_counter() - started,
                    }
                )
            except Exception as error:
                LOGGER.exception("Algorithm failed: %s", algorithm)
                statuses[algorithm].update(
                    {
                        "status": "failed",
                        "timestamp": _timestamp(),
                        "runtime_seconds": time.perf_counter() - started,
                        "error": str(error),
                    }
                )
            _write_status(status_path, statuses)
        if benchmark:
            if spark is None:
                spark = __import__(
                    "src.spark_session",
                    fromlist=["create_spark_session"],
                ).create_spark_session(config, spark_conf=spark_conf)
            if directed is None:
                from src.graph.graph_builder import build_directed_graph

                directed = build_directed_graph(spark, config)
            from src.evaluation.benchmark import benchmark_scales

            scales = (
                (sample_percent / 100.0,)
                if sample_percent is not None
                else (0.01, 0.05, 0.10, 0.25)
            )
            benchmark_scales(
                spark,
                directed,
                output_dir / "benchmark",
                scales=scales,
                seed=config.algorithm.random_seed,
            )
    finally:
        if spark is not None:
            spark.stop()
    return statuses


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--all", action="store_true")
    selection.add_argument("--algorithm", choices=ALGORITHMS, action="append")
    parser.add_argument("--sample-percent", type=float)
    parser.add_argument("--min-support", type=float, default=0.01)
    parser.add_argument("--num-clusters", type=int, default=2)
    parser.add_argument("--iterations", type=int)
    parser.add_argument("--spark-conf", action="append", default=[])
    parser.add_argument("--output-dir")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--benchmark", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        config = load_config()
        run_pipeline(
            config,
            algorithms=list(ALGORITHMS) if args.all else args.algorithm,
            output_dir=Path(args.output_dir) if args.output_dir else None,
            force=args.force,
            min_support=args.min_support,
            num_clusters=args.num_clusters,
            iterations=args.iterations,
            sample_percent=args.sample_percent,
            spark_conf=_parse_spark_conf(args.spark_conf),
            benchmark=args.benchmark,
        )
    except (OSError, RuntimeError, ValueError) as error:
        LOGGER.error("%s", error)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
