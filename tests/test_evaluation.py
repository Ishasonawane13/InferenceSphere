import csv
import json
import tempfile
import unittest
from pathlib import Path

from src.evaluation.metrics import build_comparison_report


class EvaluationTests(unittest.TestCase):
    def test_missing_artifacts_are_pending_and_schema_is_stable(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            results = root / "results"
            tables = root / "tables"
            results.mkdir()
            (results / "pagerank_evaluation.json").write_text(
                json.dumps({"degree_top_100_overlap": 0.5}),
                encoding="utf-8",
            )
            report = build_comparison_report(results, tables)
            self.assertEqual(report["pending_count"], 5)
            with (tables / "comparisons.csv").open(newline="", encoding="utf-8") as source:
                rows = list(csv.DictReader(source))
            self.assertEqual(len(rows), 6)
            self.assertEqual(rows[0]["status"], "completed")


if __name__ == "__main__":
    unittest.main()
