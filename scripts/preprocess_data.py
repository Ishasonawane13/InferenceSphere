"""Run the InfluenceSphere raw-edge preprocessing pipeline."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from src.config import load_config
from src.preprocessing import preprocess
from src.spark_session import create_spark_session


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path)
    parser.add_argument("--sample-percent", type=float)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--master")
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = _parser().parse_args(argv)
    config = load_config()
    input_path = args.input or config.paths.data_raw / "soc-LiveJournal1.txt"
    if args.master:
        config = config.__class__(
            paths=config.paths,
            spark=config.spark.__class__(
                master=args.master,
                app_name=config.spark.app_name,
                driver_memory=config.spark.driver_memory,
                shuffle_partitions=config.spark.shuffle_partitions,
                graphframes_package=config.spark.graphframes_package,
                ui_enabled=config.spark.ui_enabled,
            ),
            algorithm=config.algorithm,
            preflight=config.preflight,
        )
    spark = None
    try:
        spark = create_spark_session(config)
        preprocess(
            spark,
            input_path,
            config,
            sample_percent=args.sample_percent,
            force=args.force,
        )
        return 0
    except (OSError, RuntimeError, ValueError) as error:
        logging.getLogger("influencesphere.preprocess_data").error("%s", error)
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
