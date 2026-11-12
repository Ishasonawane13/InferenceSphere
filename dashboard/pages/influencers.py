"""Influencer analytics dashboard page."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from dashboard.components.loaders import load_csv, load_parquet, show_unavailable
from dashboard.visualizations import bar, scatter


def _download(frame: pd.DataFrame, name: str, label: str) -> None:
    st.download_button(
        label,
        frame.to_csv(index=False).encode("utf-8"),
        file_name=name,
        mime="text/csv",
    )


def render(results_dir: Path) -> None:
    st.title("Influencer Analytics")
    st.caption("Rankings and comparisons from persisted PageRank, HITS, and degree results.")
    pagerank = load_csv(results_dir / "top_1000.csv")
    hits = load_parquet(
        results_dir / "hits",
        columns=["user_id", "hub_score", "authority_score", "hub_rank", "authority_rank"],
    )
    degrees = load_parquet(
        results_dir / "degrees",
        columns=["id", "in_degree", "out_degree", "total_degree"],
    )
    if degrees is not None:
        degrees = degrees.rename(columns={"id": "user_id"})

    if pagerank is None:
        show_unavailable("PageRank rankings")
        return
    pagerank = pagerank.sort_values("rank").head(1000)
    if hits is not None:
        hits = hits.sort_values("hub_rank").head(1000)
    else:
        show_unavailable("HITS rankings")

    left, middle, right = st.columns(3)
    with left:
        st.subheader("Top PageRank")
        st.dataframe(pagerank.head(100), use_container_width=True, hide_index=True)
        _download(pagerank.head(100), "pagerank_top_1000.csv", "Download PageRank CSV")
    if hits is not None:
        with middle:
            st.subheader("Top hubs")
            st.dataframe(hits.sort_values("hub_rank").head(100), use_container_width=True, hide_index=True)
            _download(hits.head(100), "hits_top_hubs.csv", "Download hubs CSV")
        with right:
            st.subheader("Top authorities")
            authorities = hits.sort_values("authority_rank").head(100)
            st.dataframe(authorities, use_container_width=True, hide_index=True)
            _download(authorities, "hits_top_authorities.csv", "Download authorities CSV")

    if degrees is not None:
        comparison = pagerank.merge(degrees, on="user_id", how="inner")
        if not comparison.empty:
            st.plotly_chart(
                scatter(
                    comparison,
                    "total_degree",
                    "pagerank",
                    color="in_degree",
                    hover_name="user_id",
                    title="Degree vs PageRank (top PageRank users)",
                    labels={"total_degree": "Total degree", "pagerank": "PageRank"},
                ),
                use_container_width=True,
            )
    else:
        show_unavailable("Degree metrics")

    if hits is not None:
        ranking = pagerank[["user_id", "rank"]].merge(
            hits[["user_id", "hub_rank", "authority_rank"]],
            on="user_id",
            how="outer",
        )
        st.plotly_chart(
            bar(
                ranking.head(100),
                x="user_id",
                y=["rank", "hub_rank", "authority_rank"],
                title="Ranking comparison",
                barmode="group",
            ),
            use_container_width=True,
        )

    query = st.text_input("Search user ID", help="Searches the loaded ranking artifacts.")
    if query:
        matches = pagerank[
            pagerank["user_id"].astype(str).str.contains(query, case=False, na=False)
        ]
        if hits is not None:
            matches = matches.merge(hits, on="user_id", how="left")
        if matches.empty:
            st.info("No matching user was found in the loaded rankings.")
        else:
            st.dataframe(matches, use_container_width=True, hide_index=True)
