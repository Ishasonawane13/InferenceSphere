"""Quickly verify that the configured Spark session can be created."""

from __future__ import annotations

import sys

from src.config import load_config
from src.spark_session import create_spark_session


def main() -> int:
    config = load_config()
    spark = create_spark_session(config)
    try:
        assert spark.version.startswith("3.5."), spark.version
        print(f"Spark session test passed: {spark.version}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
