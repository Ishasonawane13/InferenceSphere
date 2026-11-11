"""Clean, symmetrize, sample, and report graph edge data with PySpark."""

from __future__ import annotations

import csv
import json
import logging
import shutil
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import (
    col,
    count,
    lit,
    regexp_extract,
    size,
    split,
    sum as spark_sum,
    trim,
)

from src.config import AppConfig
from src.data_loader import read_raw_edges
from src.utils.helpers import ensure_directories


LOGGER = logging.getLogger("influencesphere.preprocessing")


def _output_exists(path: Path) -> bool:
    return path.is_file() or (path.is_dir() and any(path.iterdir()))


def _remove_output(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)


def _write_parquet(data: DataFrame, path: Path, force: bool) -> None:
    if _output_exists(path) and not force:
        LOGGER.info("Output exists; skipping: %s", path)
        return
    if force:
        _remove_output(path)
    data.write.mode("overwrite").parquet(str(path))


def _parse_edges(raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    tokens = split(trim(col("line")), r"\s+")
    parsed = raw.select(
        "line",
        tokens.alias("tokens"),
    ).withColumn("token_count", size(col("tokens")))
    parsed = parsed.withColumn(
        "src_text",
        regexp_extract(col("line"), r"^\s*(\S+)", 1),
    ).withColumn(
        "dst_text",
        regexp_extract(col("line"), r"^\s*\S+\s+(\S+)", 1),
    )
    missing = (
        (col("line") == "")
        | (col("token_count") < 2)
        | (col("src_text") == "")
        | (col("dst_text") == "")
    )
    numeric = (
        col("src_text").rlike(r"^[0-9]+$")
        & col("dst_text").rlike(r"^[0-9]+$")
    )
    valid_condition = ~missing & numeric
    valid = parsed.filter(valid_condition).select(
        col("src_text").cast("long").alias("src"),
        col("dst_text").cast("long").alias("dst"),
    )
    invalid = parsed.filter(~valid_condition).select(
        lit(1).alias("invalid"),
        missing.alias("missing"),
    )
    return valid, invalid


def _count_invalid(invalid: DataFrame) -> tuple[int, int]:
    counts = invalid.select(
        count(lit(1)).alias("invalid_rows"),
        spark_sum(col("missing").cast("long")).alias("missing_rows"),
    ).first()
    return int(counts["invalid_rows"] or 0), int(counts["missing_rows"] or 0)


def _stats(
    raw_rows: int,
    invalid_rows: int,
    missing_rows: int,
    self_loops: int,
    duplicate_directed: int,
    cleaned: DataFrame,
    undirected: DataFrame,
) -> dict[str, int]:
    return {
        "raw_rows": raw_rows,
        "invalid_rows": invalid_rows,
        "missing_rows": missing_rows,
        "self_loops": self_loops,
        "duplicate_directed_edges": duplicate_directed,
        "final_directed_edges": cleaned.count(),
        "final_undirected_edges": undirected.count(),
        "final_nodes": cleaned.select(
            col("src").alias("id")
        ).union(cleaned.select(col("dst").alias("id"))).distinct().count(),
    }


def _write_reports(stats: dict[str, int], report_dir: Path, force: bool) -> None:
    ensure_directories(report_dir)
    json_path = report_dir / "preprocessing_stats.json"
    csv_path = report_dir / "preprocessing_stats.csv"
    if not force and (_output_exists(json_path) or _output_exists(csv_path)):
        LOGGER.info("Statistics reports exist; skipping report generation.")
        return
    json_path.write_text(json.dumps(stats, indent=2) + "\n", encoding="utf-8")
    with csv_path.open("w", newline="", encoding="utf-8") as report:
        writer = csv.DictWriter(report, fieldnames=list(stats))
        writer.writeheader()
        writer.writerow(stats)


def preprocess(
    spark: SparkSession,
    input_path: Path,
    config: AppConfig,
    *,
    sample_percent: float | None = None,
    force: bool = False,
) -> dict[str, int]:
    """Run the complete preprocessing pipeline and return dataset statistics."""
    if sample_percent is not None and not 0 < sample_percent <= 100:
        raise ValueError("sample_percent must be greater than 0 and at most 100")

    directed_path = config.paths.data_processed / "cleaned_directed_edges"
    undirected_path = config.paths.data_processed / "cleaned_undirected_edges"
    report_path = config.paths.reports / "preprocessing_stats.json"
    sample_path = (
        config.paths.samples / f"directed_edges_{sample_percent:g}pct"
        if sample_percent is not None
        else None
    )
    outputs_ready = (
        not force
        and _output_exists(directed_path)
        and _output_exists(undirected_path)
        and _output_exists(report_path)
        and (sample_path is None or _output_exists(sample_path))
    )
    if outputs_ready:
        LOGGER.info("Preprocessing outputs already exist; skipping Spark work.")
        return json.loads(report_path.read_text(encoding="utf-8"))
    if not input_path.is_file():
        raise FileNotFoundError(f"Raw edge file not found: {input_path}")

    raw = read_raw_edges(spark, input_path)
    raw_rows = raw.count()
    valid, invalid = _parse_edges(raw)
    invalid_rows, missing_rows = _count_invalid(invalid)
    valid = valid.persist(StorageLevel.DISK_ONLY)
    self_loops = valid.filter(col("src") == col("dst")).count()
    distinct_valid = valid.dropDuplicates(["src", "dst"])
    duplicate_directed = valid.count() - distinct_valid.count()
    cleaned = distinct_valid.filter(col("src") != col("dst")).select("src", "dst")
    undirected = (
        cleaned.select(col("src").alias("src"), col("dst").alias("dst"))
        .union(cleaned.select(col("dst").alias("src"), col("src").alias("dst")))
        .dropDuplicates(["src", "dst"])
    )

    ensure_directories(config.paths.data_processed, config.paths.samples)
    _write_parquet(cleaned, directed_path, force)
    _write_parquet(undirected, undirected_path, force)

    if sample_percent is not None:
        fraction = sample_percent / 100
        sampled = cleaned if sample_percent == 100 else cleaned.sample(
            withReplacement=False,
            fraction=fraction,
            seed=config.algorithm.random_seed,
        )
        sample_name = f"directed_edges_{sample_percent:g}pct"
        _write_parquet(sampled, config.paths.samples / sample_name, force)

    stats = _stats(
        raw_rows,
        invalid_rows,
        missing_rows,
        self_loops,
        duplicate_directed,
        cleaned,
        undirected,
    )
    _write_reports(stats, config.paths.reports, force)
    valid.unpersist()
    LOGGER.info("Preprocessing complete: %s", stats)
    return stats
