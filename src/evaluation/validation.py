"""Schema and fixture validation for evaluation reports."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Iterable


COMPARISON_FIELDS = (
    "comparison",
    "variant",
    "status",
    "metrics",
    "source",
    "pending_reason",
)
COMPARISON_CSV_FIELDS = (
    "comparison",
    "variant",
    "status",
    "metrics_json",
    "source",
    "pending_reason",
)
BENCHMARK_FIELDS = (
    "sample_percent", "vertices", "edges", "algorithm", "status",
    "execution_seconds", "parameters", "result_count", "memory_bytes", "error",
)


def validate_records(records: Iterable[dict[str, Any]], fields: tuple[str, ...]) -> bool:
    for record in records:
        if not set(fields).issubset(record):
            return False
        if record.get("status") not in {"complete", "pending", "failed", "skipped"}:
            return False
        if record["status"] == "pending" and not (
            record.get("pending_reason") or record.get("error")
        ):
            return False
    return True


def read_synthetic_edges(path: Path) -> list[tuple[int, int]]:
    edges = []
    with Path(path).open(encoding="utf-8") as source:
        for line in source:
            fields = line.split()
            if len(fields) < 2 or fields[0].startswith("#"):
                continue
            try:
                src, dst = int(fields[0]), int(fields[1])
            except ValueError:
                continue
            if src >= 0 and dst >= 0 and src != dst:
                edges.append((src, dst))
    return edges


def validate_csv_schema(path: Path, fields: tuple[str, ...]) -> bool:
    with Path(path).open(newline="", encoding="utf-8") as source:
        return tuple(next(csv.reader(source), ())) == fields
