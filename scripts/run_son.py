"""Run SON frequent-itemset mining."""

from __future__ import annotations

import argparse
import logging
import sys

from src.algorithms.son import run_configured_son


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir")
    parser.add_argument("--min-support", type=float, action="append")
    parser.add_argument("--max-items-per-basket", type=int, default=1000)
    parser.add_argument("--max-itemset-size", type=int, default=3)
    parser.add_argument("--no-prefilter-items", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        run_configured_son(
            output_dir=args.output_dir,
            thresholds=args.min_support or [0.01],
            max_items_per_basket=args.max_items_per_basket,
            max_itemset_size=args.max_itemset_size,
            prefilter_items=not args.no_prefilter_items,
        )
    except (OSError, RuntimeError, ValueError) as error:
        logging.getLogger("influencesphere.son").error("%s", error)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
