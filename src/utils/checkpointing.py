"""Spark checkpoint configuration helpers."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

from src.utils.helpers import ensure_directories


def configure_checkpointing(spark: SparkSession, checkpoint_dir: Path) -> None:
    """Configure a durable checkpoint directory for the active Spark context."""
    ensure_directories(checkpoint_dir)
    spark.sparkContext.setCheckpointDir(str(checkpoint_dir))
