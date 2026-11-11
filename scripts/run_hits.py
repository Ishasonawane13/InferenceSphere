"""Run configured distributed HITS."""

from __future__ import annotations

import argparse
import logging
import sys

from src.algorithms.hits import run_configured_hits


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir")
    parser.add_argument("--max-iterations", type=int)
    parser.add_argument("--tolerance", type=float, default=1e-6)
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        config = None
        if args.max_iterations is not None:
            from src.config import load_config

            loaded = load_config()
            config = loaded.__class__(
                paths=loaded.paths,
                spark=loaded.spark,
                algorithm=loaded.algorithm.__class__(
                    random_seed=loaded.algorithm.random_seed,
                    pagerank_reset_probability=loaded.algorithm.pagerank_reset_probability,
                    pagerank_max_iterations=args.max_iterations,
                ),
                preflight=loaded.preflight,
            )
        run_configured_hits(
            output_dir=args.output_dir,
            config=config,
            max_iterations=args.max_iterations,
            tolerance=args.tolerance,
        )
    except (OSError, RuntimeError, ValueError) as error:
        logging.getLogger("influencesphere.hits").error("%s", error)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
