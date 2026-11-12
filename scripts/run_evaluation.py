"""Write Part 12 comparison reports and optional scalability benchmarks."""

from __future__ import annotations

import argparse
import logging
import sys

from src.config import load_config
from src.evaluation.benchmark import run_configured_benchmark
from src.evaluation.metrics import run_comparisons


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--comparisons-only", action="store_true")
    parser.add_argument("--benchmark-only", action="store_true")
    args = parser.parse_args(argv)
    if args.comparisons_only and args.benchmark_only:
        parser.error("Choose at most one of --comparisons-only and --benchmark-only")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    try:
        config = load_config()
        if not args.benchmark_only:
            run_comparisons(config)
        if not args.comparisons_only:
            run_configured_benchmark(config)
            run_comparisons(config)
    except (OSError, RuntimeError, ValueError) as error:
        logging.getLogger("influencesphere.evaluation").error("%s", error)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
