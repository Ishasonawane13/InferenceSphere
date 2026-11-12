"""Community discovery dashboard page."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboard.components.loaders import load_json, load_parquet, show_unavailable
from dashboard.visualizations import bar


def _network_figure(nodes: pd.DataFrame, edges: pd.DataFrame) -> go.Figure:
    nodes = nodes.head(1000).copy()
    ids = set(nodes["user_id"].astype(str))
    edges = edges[
        edges["src"].astype(str).isin(ids) & edges["dst"].astype(str).isin(ids)
    ].head(2000)
    positions = {
        user_id: (
            float(np.cos(index * 2 * np.pi / max(len(nodes), 1))),
            float(np.sin(index * 2 * np.pi / max(len(nodes), 1))),
        )
        for index, user_id in enumerate(nodes["user_id"].astype(str))
    }
    figure = go.Figure()
    edge_x: list[float | None] = []
    edge_y: list[float | None] = []
    for row in edges.itertuples(index=False):
        source = positions.get(str(row.src))
        target = positions.get(str(row.dst))
        if source and target:
            edge_x.extend([source[0], target[0], None])
            edge_y.extend([source[1], target[1], None])
    figure.add_trace(
        go.Scatter(
            x=edge_x,
            y=edge_y,
            mode="lines",
            line={"width": 0.5, "color": "#64748b"},
            hoverinfo="skip",
            name="sampled edges",
        )
    )
    for community, group in nodes.groupby("community_id"):
        figure.add_trace(
            go.Scatter(
                x=[positions[str(value)][0] for value in group["user_id"]],
                y=[positions[str(value)][1] for value in group["user_id"]],
                mode="markers",
                marker={"size": 7},
                name=f"Community {community}",
                text=group["user_id"].astype(str),
                hovertemplate="User %{text}<extra></extra>",
            )
        )
    figure.update_layout(
        template="plotly_dark",
        title="Saved sampled community subgraph (bounded to 1,000 nodes / 2,000 edges)",
        showlegend=True,
        xaxis={"visible": False},
        yaxis={"visible": False},
        margin={"l": 0, "r": 0, "t": 48, "b": 0},
    )
    return figure


def render(results_dir: Path) -> None:
    st.title("Community Discovery")
    evaluation = load_json(results_dir / "label_propagation_evaluation.json")
    if not evaluation:
        show_unavailable("Community results")
        return
    sizes = evaluation.get("size_distribution", [])
    if sizes:
        frame = pd.DataFrame(sizes)
        st.plotly_chart(
            bar(frame, "community_size", "community_count", title="Community size distribution"),
            use_container_width=True,
        )
        largest = frame.sort_values("community_size", ascending=False).head(10)
        st.subheader("Largest communities")
        st.dataframe(largest, use_container_width=True, hide_index=True)
        _download = st.download_button
        _download(
            "Download community sizes CSV",
            frame.to_csv(index=False).encode("utf-8"),
            "community_sizes.csv",
            "text/csv",
        )
    else:
        show_unavailable("Community size distribution")

    nodes = load_parquet(results_dir / "community_sample" / "nodes")
    edges = load_parquet(results_dir / "community_sample" / "edges")
    if nodes is None or edges is None:
        show_unavailable("Saved sampled community graph")
        return
    community_ids = sorted(nodes["community_id"].dropna().unique().tolist())
    selected = st.multiselect("Filter communities", community_ids)
    filtered = nodes[nodes["community_id"].isin(selected)] if selected else nodes
    visible_ids = set(filtered["user_id"].astype(str))
    visible_edges = edges[
        edges["src"].astype(str).isin(visible_ids)
        & edges["dst"].astype(str).isin(visible_ids)
    ]
    st.plotly_chart(_network_figure(filtered, visible_edges), use_container_width=True)
