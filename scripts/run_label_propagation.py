"""Run configured GraphFrames label propagation."""

from __future__ import annotations

import argparse
import logging
import sys

from src.algorithms.label_propagation import run_configured_label_propagation


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir")
    parser.add_argument("--max-iterations", type=int)
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        run_configured_label_propagation(
            output_dir=args.output_dir,
            max_iterations=args.max_iterations,
        )
    except (OSError, RuntimeError, ValueError) as error:
        logging.getLogger("influencesphere.label_propagation").error("%s", error)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
