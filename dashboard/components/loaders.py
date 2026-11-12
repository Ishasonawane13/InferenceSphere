"""Cached loaders for pipeline result artifacts."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Sequence

import pandas as pd
import streamlit as st


LOGGER = logging.getLogger("influencesphere.dashboard")
UNAVAILABLE_MESSAGE = "Results not available - run the pipeline."


@st.cache_data(show_spinner=False)
def load_json(path: str | Path) -> dict[str, Any] | None:
    """Load a JSON artifact, returning None when it is unavailable."""
    artifact = Path(path)
    if not artifact.is_file():
        return None
    try:
        import json

        value = json.loads(artifact.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        LOGGER.warning("Could not load JSON artifact %s: %s", artifact, error)
        return None
    return value if isinstance(value, dict) else None


@st.cache_data(show_spinner=False)
def load_parquet(
    path: str | Path,
    columns: Sequence[str] | None = None,
) -> pd.DataFrame | None:
    """Load a Parquet artifact, returning None when it is unavailable."""
    artifact = Path(path)
    if not artifact.exists():
        return None
    try:
        return pd.read_parquet(artifact, columns=list(columns) if columns else None)
    except (ImportError, OSError, ValueError) as error:
        LOGGER.warning("Could not load Parquet artifact %s: %s", artifact, error)
        return None


@st.cache_data(show_spinner=False)
def load_csv(
    path: str | Path,
    usecols: Sequence[str] | None = None,
) -> pd.DataFrame | None:
    """Load a CSV artifact, returning None when it is unavailable."""
    artifact = Path(path)
    if not artifact.is_file():
        return None
    try:
        return pd.read_csv(artifact, usecols=list(usecols) if usecols else None)
    except (OSError, ValueError) as error:
        LOGGER.warning("Could not load CSV artifact %s: %s", artifact, error)
        return None


def show_unavailable(label: str) -> None:
    """Render a consistent missing-artifact message."""
    st.info(f"{label}: {UNAVAILABLE_MESSAGE}")
