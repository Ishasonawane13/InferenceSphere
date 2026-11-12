"""Network structure dashboard page."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from dashboard.components.loaders import load_json, load_parquet, show_unavailable
from dashboard.visualizations import box, histogram, scatter


def render(results_dir: Path) -> None:
    st.title("Network Structure")
    st.caption("Structural distributions from persisted graph metrics and exact triangle outputs.")
    summary = load_json(results_dir / "graph_summary.json")
    triangles = load_parquet(
        results_dir / "triangles",
        columns=["user_id", "triangle_count", "clustering_coefficient"],
    )
    degrees = load_parquet(
        results_dir / "degrees",
        columns=["id", "in_degree", "out_degree", "total_degree"],
    )
    if triangles is None:
        show_unavailable("Triangle and clustering metrics")
    else:
        st.plotly_chart(
            histogram(
                triangles,
                "clustering_coefficient",
                title="Clustering coefficient distribution",
            ),
            use_container_width=True,
        )
        _download = st.download_button
        _download(
            "Download triangle metrics CSV",
            triangles.to_csv(index=False).encode("utf-8"),
            "triangle_metrics.csv",
            "text/csv",
        )
        st.subheader("Highly connected users by triangle count")
        st.dataframe(
            triangles.sort_values("triangle_count", ascending=False).head(100),
            use_container_width=True,
            hide_index=True,
        )
    if degrees is None:
        show_unavailable("Degree metrics")
    else:
        degrees = degrees.rename(columns={"id": "user_id"})
        st.plotly_chart(
            histogram(degrees, "total_degree", title="Degree distribution"),
            use_container_width=True,
        )
        st.plotly_chart(
            scatter(
                degrees,
                "in_degree",
                "out_degree",
                title="In-degree vs out-degree",
                hover_name="user_id",
            ),
            use_container_width=True,
        )
        st.dataframe(
            degrees.sort_values("total_degree", ascending=False).head(100),
            use_container_width=True,
            hide_index=True,
        )
    if summary:
        st.subheader("Connectivity statistics")
        st.json(
            {
                key: summary[key]
                for key in (
                    "node_count",
                    "edge_count",
                    "density",
                    "minimum_degree",
                    "maximum_degree",
                )
                if key in summary
            }
        )
    else:
        show_unavailable("Connectivity statistics")
