"""PySpark readers for raw edge-list data."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, trim


def read_raw_lines(spark: SparkSession, input_path: Path) -> DataFrame:
    """Read raw text lines and remove comment/header lines."""
    return (
        spark.read.text(str(input_path))
        .filter(col("value").isNotNull())
        .filter(~trim(col("value")).startswith("#"))
        .select(trim(col("value")).alias("line"))
    )


def read_raw_edges(spark: SparkSession, input_path: Path) -> DataFrame:
    """Read raw lines and expose whitespace-separated fields for validation."""
    return read_raw_lines(spark, input_path)
