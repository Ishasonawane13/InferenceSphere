import json
import tempfile
import unittest
from pathlib import Path

from src.config import load_config
from src.evaluation.benchmark import (
    ALGORITHMS,
    pending_benchmark_records,
    _write_records,
)
from src.evaluation.metrics import build_comparison_records, write_comparison_reports
from src.evaluation.validation import (
    BENCHMARK_FIELDS,
    COMPARISON_CSV_FIELDS,
    COMPARISON_FIELDS,
    read_synthetic_edges,
    validate_csv_schema,
    validate_records,
)


class EvaluationSchemaTests(unittest.TestCase):
    def test_synthetic_fixture_and_pending_comparison_schema(self) -> None:
        fixture = Path(__file__).parent / "fixtures" / "synthetic_edges.txt"
        edges = read_synthetic_edges(fixture)
        self.assertTrue(edges)
        self.assertIn((1, 2), edges)
        self.assertNotIn((2, 2), edges)

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            records = build_comparison_records(
                root,
                config=load_config(),
                son_supports=(0.01, 0.05),
                cure_parameter_sets=(
                    {"k": 2, "c": 2, "alpha": 0.5, "sample_size": 10},
                    {"k": 2, "c": 4, "alpha": 0.5, "sample_size": 10},
                ),
            )
            json_path, csv_path = write_comparison_reports(records, root / "tables")
            self.assertTrue(validate_records(records, COMPARISON_FIELDS))
            self.assertTrue(all(record["status"] == "pending" for record in records))
            self.assertTrue(validate_csv_schema(csv_path, COMPARISON_CSV_FIELDS))
            self.assertEqual(len(json.loads(json_path.read_text(encoding="utf-8"))), len(records))

    def test_pending_benchmark_schema_and_full_preflight_gate(self) -> None:
        pending = pending_benchmark_records((1, 5, 10, 25, 100), full_allowed=False)
        self.assertEqual(len(pending), 5 * len(ALGORITHMS))
        self.assertTrue(validate_records(pending, BENCHMARK_FIELDS))
        self.assertTrue(all(row["status"] == "skipped" for row in pending if row["sample_percent"] == 100))
        self.assertTrue(all(row["status"] == "pending" for row in pending if row["sample_percent"] < 100))

        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            _write_records(pending, output)
            self.assertTrue(validate_csv_schema(
                output / "scalability_benchmark.csv", BENCHMARK_FIELDS
            ))
            stored = json.loads((output / "scalability_benchmark.json").read_text(encoding="utf-8"))
            self.assertEqual(len(stored), len(pending))


if __name__ == "__main__":
    unittest.main()
