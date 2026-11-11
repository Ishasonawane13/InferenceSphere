"""Load and validate project configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv


DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config.yaml"


@dataclass(frozen=True)
class PathsConfig:
    project_root: Path
    data_raw: Path
    data_processed: Path
    samples: Path
    results: Path
    spark_local_dir: Path
    checkpoint_dir: Path
    reports: Path
    charts: Path
    tables: Path


@dataclass(frozen=True)
class SparkConfig:
    master: str
    app_name: str
    driver_memory: str
    shuffle_partitions: int
    graphframes_package: str
    ui_enabled: bool


@dataclass(frozen=True)
class AlgorithmConfig:
    random_seed: int
    pagerank_reset_probability: float
    pagerank_max_iterations: int


@dataclass(frozen=True)
class PreflightConfig:
    minimum_memory_gb: float
    minimum_free_disk_gb: float
    minimum_cpu_cores: int


@dataclass(frozen=True)
class AppConfig:
    paths: PathsConfig
    spark: SparkConfig
    algorithm: AlgorithmConfig
    preflight: PreflightConfig


def _path(value: str, project_root: Path) -> Path:
    candidate = Path(value).expanduser()
    return candidate if candidate.is_absolute() else project_root / candidate


def _required(mapping: dict[str, Any], key: str) -> Any:
    if key not in mapping:
        raise ValueError(f"Missing required configuration key: {key}")
    return mapping[key]


def load_config(config_path: Path | str = DEFAULT_CONFIG_PATH) -> AppConfig:
    """Load YAML configuration and resolve all project paths."""
    load_dotenv()
    path = Path(config_path).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    with path.open(encoding="utf-8") as config_file:
        raw = yaml.safe_load(config_file) or {}

    configured_root = os.getenv(
        "INFLUENCESPHERE_PROJECT_ROOT",
        str(raw.get("paths", {}).get("project_root", path.parent)),
    )
    project_root = _path(configured_root, path.parent).resolve()
    paths = raw.get("paths", {})
    spark = raw.get("spark", {})
    algorithm = raw.get("algorithm", {})
    preflight = raw.get("preflight", {})

    return AppConfig(
        paths=PathsConfig(
            project_root=project_root,
            data_raw=_path(_required(paths, "data_raw"), project_root),
            data_processed=_path(_required(paths, "data_processed"), project_root),
            samples=_path(_required(paths, "samples"), project_root),
            results=_path(_required(paths, "results"), project_root),
            spark_local_dir=_path(_required(paths, "spark_local_dir"), project_root),
            checkpoint_dir=_path(_required(paths, "checkpoint_dir"), project_root),
            reports=_path(_required(paths, "reports"), project_root),
            charts=_path(_required(paths, "charts"), project_root),
            tables=_path(_required(paths, "tables"), project_root),
        ),
        spark=SparkConfig(
            master=os.getenv("SPARK_MASTER", _required(spark, "master")),
            app_name=os.getenv("SPARK_APP_NAME", _required(spark, "app_name")),
            driver_memory=os.getenv(
                "SPARK_DRIVER_MEMORY", _required(spark, "driver_memory")
            ),
            shuffle_partitions=int(
                os.getenv(
                    "SPARK_SHUFFLE_PARTITIONS",
                    str(_required(spark, "shuffle_partitions")),
                )
            ),
            graphframes_package=os.getenv(
                "GRAPHFRAMES_PACKAGE",
                _required(spark, "graphframes_package"),
            ),
            ui_enabled=bool(_required(spark, "ui_enabled")),
        ),
        algorithm=AlgorithmConfig(
            random_seed=int(_required(algorithm, "random_seed")),
            pagerank_reset_probability=float(
                _required(algorithm, "pagerank_reset_probability")
            ),
            pagerank_max_iterations=int(
                _required(algorithm, "pagerank_max_iterations")
            ),
        ),
        preflight=PreflightConfig(
            minimum_memory_gb=float(_required(preflight, "minimum_memory_gb")),
            minimum_free_disk_gb=float(_required(preflight, "minimum_free_disk_gb")),
            minimum_cpu_cores=int(_required(preflight, "minimum_cpu_cores")),
        ),
    )
