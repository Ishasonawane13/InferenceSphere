"""Performance analytics dashboard page."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from dashboard.components.loaders import load_csv, load_json, show_unavailable
from dashboard.visualizations import bar, scatter


EVALUATIONS = {
    "pagerank": "pagerank_evaluation.json",
    "hits": "hits_evaluation.json",
    "label_propagation": "label_propagation_evaluation.json",
    "triangle_count": "triangle_count_stats.json",
    "son": "son_evaluation.json",
    "cure": "cure_evaluation.json",
}


def _runtime_rows(results_dir: Path) -> list[dict[str, Any]]:
    rows = []
    for algorithm, filename in EVALUATIONS.items():
        evaluation = load_json(results_dir / filename)
        rows.append(
            {
                "algorithm": algorithm,
                "status": "done" if evaluation else "pending",
                "runtime_seconds": (
                    evaluation.get("execution_seconds") if evaluation else None
                ),
                "result_count": (
                    evaluation.get("node_count")
                    or evaluation.get("frequent_itemset_count")
                    or evaluation.get("users_clustered")
                    if evaluation
                    else None
                ),
            }
        )
    return rows


def render(results_dir: Path, tables_dir: Path) -> None:
    st.title("Performance Analytics")
    st.caption("Measured runtimes and benchmark records; unrun experiments remain pending.")
    runtime_frame = pd.DataFrame(_runtime_rows(results_dir))
    st.dataframe(runtime_frame, use_container_width=True, hide_index=True)
    measured = runtime_frame.dropna(subset=["runtime_seconds"])
    if not measured.empty:
        st.plotly_chart(
            bar(
                measured,
                "algorithm",
                "runtime_seconds",
                title="Algorithm runtimes",
            ),
            use_container_width=True,
        )
    else:
        show_unavailable("Algorithm runtime measurements")
    st.download_button(
        "Download runtime table CSV",
        runtime_frame.to_csv(index=False).encode("utf-8"),
        "algorithm_runtimes.csv",
        "text/csv",
    )

    benchmark_dir = results_dir / "benchmark"
    benchmark = load_json(benchmark_dir / "scalability_benchmark.json")
    benchmark_csv = load_csv(benchmark_dir / "scalability_benchmark.csv")
    if benchmark_csv is None and benchmark is not None:
        benchmark_csv = pd.DataFrame(benchmark.get("rows", []))
    if benchmark_csv is None:
        show_unavailable("Scalability benchmark")
    else:
        st.subheader("Scalability benchmark")
        st.dataframe(benchmark_csv, use_container_width=True, hide_index=True)
        _measured_benchmark = benchmark_csv[
            benchmark_csv["status"].eq("completed")
        ] if "status" in benchmark_csv else pd.DataFrame()
        if not _measured_benchmark.empty and "algorithm_seconds" in _measured_benchmark:
            st.plotly_chart(
                scatter(
                    _measured_benchmark,
                    "vertices",
                    "algorithm_seconds",
                    color="algorithm",
                    title="Measured scalability",
                ),
                use_container_width=True,
            )
        st.download_button(
            "Download benchmark CSV",
            benchmark_csv.to_csv(index=False).encode("utf-8"),
            "scalability_benchmark.csv",
            "text/csv",
        )

    comparisons = load_json(tables_dir / "comparisons.json")
    if comparisons:
        st.subheader("Experimental configuration and comparisons")
        st.dataframe(
            pd.DataFrame(comparisons.get("comparisons", [])),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("Comparison report: pending - run the evaluation pipeline.")
