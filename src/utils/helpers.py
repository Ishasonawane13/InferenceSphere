"""Small filesystem and validation helpers."""

from __future__ import annotations

from pathlib import Path


def ensure_directories(*directories: Path) -> None:
    """Create directories required by the application."""
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
