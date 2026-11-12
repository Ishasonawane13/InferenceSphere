"""Run CURE clustering on the configured graph-feature sample."""

from __future__ import annotations

import argparse
import logging
import sys

from src.algorithms.cure import run_configured_cure


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir")
    parser.add_argument("--k", type=int)
    parser.add_argument("--c", type=int)
    parser.add_argument("--alpha", type=float)
    parser.add_argument("--sample-size", type=int)
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        run_configured_cure(
            output_dir=args.output_dir,
            k=args.k,
            c=args.c,
            alpha=args.alpha,
            sample_size=args.sample_size,
        )
    except (OSError, RuntimeError, ValueError) as error:
        logging.getLogger("influencesphere.cure").error("%s", error)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
