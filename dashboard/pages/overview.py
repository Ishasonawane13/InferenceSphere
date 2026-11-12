"""Overview page for pipeline outputs."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from dashboard.components.loaders import (
    load_json,
    load_parquet,
    show_unavailable,
)
from dashboard.visualizations import bar, histogram


def _metric(label: str, value: Any, help_text: str | None = None) -> None:
    st.metric(label, "Not available" if value is None else value, help=help_text)


def _status_table(status: dict[str, Any] | None) -> None:
    if not status:
        show_unavailable("Pipeline status")
        return
    rows = []
    for algorithm, details in status.get("algorithms", {}).items():
        rows.append(
            {
                "Algorithm": algorithm.replace("_", " ").title(),
                "Status": details.get("status", "unknown"),
                "Runtime (s)": details.get("runtime_seconds"),
                "Timestamp": details.get("timestamp"),
            }
        )
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def render(results_dir: Path) -> None:
    st.title("Overview")
    st.caption("A factual view of the latest InfluenceSphere pipeline artifacts.")
    summary = load_json(results_dir / "graph_summary.json")
    triangle_stats = load_json(results_dir / "triangle_count_stats.json")
    lpa_stats = load_json(results_dir / "label_propagation_evaluation.json")
    pipeline_status = load_json(results_dir / "pipeline_status.json")

    metrics = [
        ("Nodes", summary.get("node_count") if summary else None, None),
        ("Directed edges", summary.get("edge_count") if summary else None, None),
        (
            "Communities",
            lpa_stats.get("community_count") if lpa_stats else None,
            None,
        ),
        (
            "Triangles",
            triangle_stats.get("total_triangles") if triangle_stats else None,
            "Exact when triangle_count_stats.json reports mode=exact.",
        ),
    ]
    columns = st.columns(4)
    for column, (label, value, help_text) in zip(columns, metrics):
        with column:
            _metric(label, value, help_text)
    if triangle_stats:
        mode = triangle_stats.get("mode", "unknown")
        st.caption(f"Triangle count mode: **{mode}** (from the pipeline artifact).")

    st.subheader("Graph metrics")
    if summary:
        columns = st.columns(4)
        for column, key, label in zip(
            columns,
            (
                "average_in_degree",
                "average_out_degree",
                "average_total_degree",
                "density",
            ),
            ("Average in-degree", "Average out-degree", "Average total degree", "Density"),
        ):
            with column:
                _metric(label, summary.get(key))
    else:
        show_unavailable("Graph metrics")

    degree_distribution = summary.get("degree_distribution") if summary else None
    if degree_distribution:
        st.plotly_chart(
            histogram(
                pd.DataFrame(degree_distribution).rename(
                    columns={"total_degree": "degree"}
                ),
                "degree",
                title="Degree distribution",
                y="node_count",
            ),
            use_container_width=True,
        )
    else:
        show_unavailable("Degree distribution")

    st.subheader("Community size distribution")
    community_sizes = (
        lpa_stats.get("size_distribution") if lpa_stats else None
    )
    if community_sizes:
        community_frame = pd.DataFrame(community_sizes)
        st.plotly_chart(
            bar(
                community_frame,
                x="community_size",
                y="community_count",
                title="Communities by size",
            ),
            use_container_width=True,
        )
    else:
        show_unavailable("Community size distribution")

    st.subheader("Algorithm processing status")
    _status_table(pipeline_status)

    degrees = load_parquet(
        results_dir / "degrees",
        columns=["in_degree", "out_degree", "total_degree"],
    )
    if degrees is not None and not degrees.empty:
        with st.expander("Degree sample"):
            st.dataframe(degrees.head(100), use_container_width=True, hide_index=True)

