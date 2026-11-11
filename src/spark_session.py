"""Create a consistently configured local Spark session."""

from __future__ import annotations

from pyspark.sql import SparkSession

from src.config import AppConfig, load_config
from src.utils.checkpointing import configure_checkpointing
from src.utils.helpers import ensure_directories
from src.utils.preflight import check_preflight


def create_spark_session(config: AppConfig | None = None) -> SparkSession:
    """Build a Spark session from application configuration."""
    config = config or load_config()
    check_preflight(config.preflight, config.paths.project_root)
    ensure_directories(
        config.paths.spark_local_dir,
        config.paths.checkpoint_dir,
    )
    builder = (
        SparkSession.builder.master(config.spark.master)
        .appName(config.spark.app_name)
        .config("spark.driver.memory", config.spark.driver_memory)
        .config("spark.sql.shuffle.partitions", config.spark.shuffle_partitions)
        .config("spark.jars.packages", config.spark.graphframes_package)
        .config("spark.ui.enabled", str(config.spark.ui_enabled).lower())
        .config("spark.local.dir", str(config.paths.spark_local_dir))
        .config("spark.hadoop.io.native.lib.available", "false")
    )
    spark = builder.getOrCreate()
    configure_checkpointing(spark, config.paths.checkpoint_dir)
    return spark
