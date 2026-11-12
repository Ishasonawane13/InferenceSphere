"""Frequent pattern mining dashboard page."""

from __future__ import annotations

import json
from itertools import combinations
from pathlib import Path

import pandas as pd
import streamlit as st

from dashboard.components.loaders import load_csv, load_json, show_unavailable
from dashboard.visualizations import bar, heatmap


def _download(frame: pd.DataFrame, name: str) -> None:
    st.download_button(
        "Download CSV",
        frame.to_csv(index=False).encode("utf-8"),
        file_name=name,
        mime="text/csv",
    )


def render(results_dir: Path) -> None:
    st.title("Frequent Pattern Mining")
    son_runs = sorted(path for path in results_dir.glob("son_*") if path.is_dir())
    if not son_runs:
        show_unavailable("SON frequent itemsets")
        return
    labels = [path.name.removeprefix("son_") for path in son_runs]
    selected = st.selectbox("SON minimum-support run", labels)
    run_dir = results_dir / f"son_{selected}"
    itemsets = load_csv(run_dir / "frequent_itemsets.csv")
    evaluation = load_json(run_dir / "son_evaluation.json")
    if itemsets is None:
        show_unavailable("SON itemsets")
        return
    itemsets["support_percent"] = pd.to_numeric(
        itemsets["support_percent"], errors="coerce"
    )
    threshold = st.slider(
        "Minimum support percentage filter",
        min_value=0.0,
        max_value=100.0,
        value=0.0,
        step=0.1,
        help="Filters persisted itemsets; it does not rerun SON.",
    )
    filtered = itemsets[itemsets["support_percent"] >= threshold].copy()
    st.dataframe(
        filtered.sort_values("support_count", ascending=False),
        use_container_width=True,
        hide_index=True,
    )
    _download(filtered, "frequent_itemsets_filtered.csv")
    if evaluation:
        st.caption(
            f"Configured minimum support: {evaluation.get('min_support', 'unknown')}; "
            f"transactions: {evaluation.get('transaction_count', 'unknown')}."
        )
    if filtered.empty:
        st.info("No persisted itemsets meet the selected support filter.")
        return
    ranking = filtered.sort_values("support_count", ascending=False).head(25)
    st.plotly_chart(
        bar(
            ranking,
            x="itemset",
            y="support_count",
            title="Most frequent itemsets",
            hover_data=["support_percent"],
        ),
        use_container_width=True,
    )
    pairs: dict[tuple[str, str], int] = {}
    for itemset in filtered["itemset"]:
        try:
            values = [str(value) for value in json.loads(itemset)]
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        for pair in combinations(sorted(values), 2):
            pairs[pair] = pairs.get(pair, 0) + 1
    if pairs:
        pair_frame = pd.DataFrame(
            [
                {"item_a": pair[0], "item_b": pair[1], "count": count}
                for pair, count in pairs.items()
            ]
        )
        matrix = pair_frame.pivot_table(
            index="item_a", columns="item_b", values="count", fill_value=0
        )
        st.plotly_chart(
            heatmap(matrix, title="Frequent pair co-occurrence"),
            use_container_width=True,
        )
