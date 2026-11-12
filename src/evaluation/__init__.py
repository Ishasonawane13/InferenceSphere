"""Evaluation, validation, and benchmarking utilities."""

__all__ = ["benchmark_scales", "build_comparison_report", "validate_project_outputs"]


def __getattr__(name: str):
    if name == "benchmark_scales":
        from src.evaluation.benchmark import benchmark_scales

        return benchmark_scales
    if name == "build_comparison_report":
        from src.evaluation.metrics import build_comparison_report

        return build_comparison_report
    if name == "validate_project_outputs":
        from src.evaluation.validation import validate_project_outputs

        return validate_project_outputs
    raise AttributeError(name)
