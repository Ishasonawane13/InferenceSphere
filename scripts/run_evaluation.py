"""Generate evaluation comparisons, validations, and optional scale benchmarks."""

from __future__ import annotations

import argparse
import logging
import sys

from src.config import load_config
from src.evaluation.metrics import build_comparison_report
from src.evaluation.validation import validate_project_outputs


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results-dir")
    parser.add_argument("--output-dir")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    config = load_config()
    results_dir = args.results_dir or str(config.paths.results)
    output_dir = args.output_dir or str(config.paths.tables)
    build_comparison_report(results_dir, output_dir)
    try:
        from src.spark_session import create_spark_session

        spark = create_spark_session(config)
        try:
            validate_project_outputs(spark, results_dir, output_dir)
        finally:
            spark.stop()
    except (OSError, RuntimeError, ValueError) as error:
        logging.getLogger("influencesphere.evaluation").error("%s", error)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
