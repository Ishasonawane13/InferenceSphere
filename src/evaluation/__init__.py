"""Evaluation reports and scalability benchmarks."""

from src.evaluation.metrics import (
    build_comparison_records,
    run_comparisons,
    write_comparison_reports,
)

__all__ = ["build_comparison_records", "run_comparisons", "write_comparison_reports"]
