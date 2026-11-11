"""Run configured exact GraphFrames triangle counting."""

from __future__ import annotations

import argparse
import logging
import sys

from src.algorithms.triangle_count import run_configured_triangle_count


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir")
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        run_configured_triangle_count(output_dir=args.output_dir)
    except (OSError, RuntimeError, ValueError) as error:
        logging.getLogger("influencesphere.triangle_count").error("%s", error)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
