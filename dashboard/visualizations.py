"""Reusable Plotly visualizations for dashboard pages."""

from __future__ import annotations

from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


THEME = "plotly_dark"


def _style(figure: go.Figure, title: str | None = None) -> go.Figure:
    figure.update_layout(
        template=THEME,
        title=title,
        margin={"l": 16, "r": 16, "t": 48 if title else 16, "b": 16},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return figure


def bar(data: pd.DataFrame, x: str, y: str, *, title: str | None = None, **kwargs: Any) -> go.Figure:
    return _style(px.bar(data, x=x, y=y, **kwargs), title)


def histogram(data: pd.DataFrame, x: str, *, title: str | None = None, **kwargs: Any) -> go.Figure:
    return _style(px.histogram(data, x=x, **kwargs), title)


def scatter(
    data: pd.DataFrame,
    x: str,
    y: str,
    *,
    title: str | None = None,
    **kwargs: Any,
) -> go.Figure:
    return _style(px.scatter(data, x=x, y=y, **kwargs), title)


def box(data: pd.DataFrame, y: str, *, title: str | None = None, **kwargs: Any) -> go.Figure:
    return _style(px.box(data, y=y, **kwargs), title)


def heatmap(
    data: pd.DataFrame,
    *,
    title: str | None = None,
    **kwargs: Any,
) -> go.Figure:
    return _style(px.imshow(data, text_auto=True, **kwargs), title)
