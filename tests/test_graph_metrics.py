import json
import os
import tempfile
import unittest
from pathlib import Path

from pyspark.sql import SparkSession

from src.config import load_config
from src.graph.graph_builder import build_directed_graph
from src.graph.graph_metrics import calculate_graph_metrics
from src.preprocessing import preprocess


@unittest.skipIf(
    os.name == "nt",
    "Spark GraphFrames and Parquet integration requires WSL2 or Docker on Windows",
)
class GraphMetricsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("InfluenceSphere-Graph-Metrics-Test")
            .config("spark.ui.enabled", "false")
            .config("spark.hadoop.io.native.lib.available", "false")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_synthetic_graph_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw_path = root / "synthetic_edges.txt"
            raw_path.write_text(
                (Path(__file__).parent / "fixtures/synthetic_edges.txt").read_text(),
                encoding="utf-8",
            )
            config = load_config()
            test_config = config.__class__(
                paths=config.paths.__class__(
                    project_root=root,
                    data_raw=root / "raw",
                    data_processed=root / "processed",
                    samples=root / "samples",
                    results=root / "results",
                    spark_local_dir=root / "spark",
                    checkpoint_dir=root / "checkpoints",
                    reports=root / "reports",
                    charts=root / "charts",
                    tables=root / "tables",
                ),
                spark=config.spark,
                algorithm=config.algorithm,
                preflight=config.preflight,
            )
            preprocess(self.spark, raw_path, test_config, force=True)
            graph = build_directed_graph(self.spark, test_config)
            summary = calculate_graph_metrics(
                graph,
                test_config.paths.results,
            )

            self.assertEqual(summary["node_count"], 4)
            self.assertEqual(summary["edge_count"], 4)
            self.assertAlmostEqual(summary["density"], 1 / 3)
            self.assertEqual(summary["average_in_degree"], 1.0)
            self.assertEqual(summary["average_out_degree"], 1.0)
            self.assertEqual(summary["average_total_degree"], 2.0)
            self.assertEqual(
                summary["degree_distribution"],
                [{"total_degree": 2, "node_count": 4}],
            )
            self.assertTrue((root / "results/degrees").is_dir())
            persisted = json.loads(
                (root / "results/graph_summary.json").read_text(encoding="utf-8")
            )
            self.assertEqual(persisted, summary)


if __name__ == "__main__":
    unittest.main()
