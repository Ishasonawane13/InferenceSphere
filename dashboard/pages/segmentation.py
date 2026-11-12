"""CURE user segmentation dashboard page."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st

from dashboard.components.loaders import load_json, load_parquet, show_unavailable
from dashboard.visualizations import bar, scatter


FEATURE_NAMES = (
    "in_degree",
    "out_degree",
    "total_degree",
    "pagerank",
    "hub_score",
    "authority_score",
    "triangle_count",
    "clustering_coefficient",
)


def _pca(frame: pd.DataFrame) -> pd.DataFrame:
    matrix = np.asarray(frame["features"].tolist(), dtype=float)
    centered = matrix - matrix.mean(axis=0)
    components = np.linalg.svd(centered, full_matrices=False)[2][:2]
    projected = centered @ components.T
    return pd.DataFrame({"pca_1": projected[:, 0], "pca_2": projected[:, 1]})


def render(results_dir: Path) -> None:
    st.title("User Segmentation")
    evaluation = load_json(results_dir / "cure_evaluation.json")
    clusters = load_parquet(results_dir / "cure_clusters")
    if not evaluation or clusters is None:
        show_unavailable("CURE segmentation")
        return
    clustered_count = int(evaluation.get("users_clustered", 0))
    assigned_count = int(evaluation.get("users_assigned", 0))
    st.info(
        f"CURE actually clustered {clustered_count:,} users out of "
        f"{evaluation.get('total_users', 'unknown'):,}. "
        f"Users assigned to clusters: {assigned_count:,}."
    )
    sizes = pd.DataFrame(evaluation.get("cluster_sizes", []))
    if not sizes.empty:
        st.plotly_chart(
            bar(sizes, "cluster_id", "cluster_size", title="CURE cluster sizes"),
            use_container_width=True,
        )
        st.download_button(
            "Download cluster sizes CSV",
            sizes.to_csv(index=False).encode("utf-8"),
            "cure_cluster_sizes.csv",
            "text/csv",
        )

    if assigned_count != clustered_count:
        st.warning(
            "PCA is withheld because this artifact includes assigned remaining users "
            "and does not persist a sample-membership flag."
        )
        return
    sample = clusters.head(clustered_count).copy()
    if sample.empty or "features" not in sample:
        show_unavailable("CURE feature vectors")
        return
    projected = _pca(sample)
    projected["cluster_id"] = sample["cluster_id"].to_numpy()
    projected["user_id"] = sample["user_id"].to_numpy()
    st.plotly_chart(
        scatter(
            projected,
            "pca_1",
            "pca_2",
            color="cluster_id",
            hover_name="user_id",
            title="PCA of the clustered CURE sample",
        ),
        use_container_width=True,
    )
    expanded = pd.DataFrame(sample["features"].tolist(), columns=FEATURE_NAMES)
    expanded["cluster_id"] = sample["cluster_id"].to_numpy()
    profiles = expanded.groupby("cluster_id")[list(FEATURE_NAMES)].mean().reset_index()
    st.subheader("Cluster feature profiles")
    st.dataframe(profiles, use_container_width=True, hide_index=True)
    st.download_button(
        "Download cluster profiles CSV",
        profiles.to_csv(index=False).encode("utf-8"),
        "cure_cluster_profiles.csv",
        "text/csv",
    )
