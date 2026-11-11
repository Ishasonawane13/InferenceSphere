import os
import tempfile
import unittest
from pathlib import Path

from pyspark.sql import SparkSession

from src.config import load_config
from src.preprocessing import preprocess


@unittest.skipIf(
    os.name == "nt",
    "Spark Parquet commit requires WSL2 or Docker on Windows",
)
class PreprocessingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("InfluenceSphere-Preprocessing-Test")
            .config("spark.ui.enabled", "false")
            .config("spark.hadoop.io.native.lib.available", "false")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_counts_clean_edges_and_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw_path = root / "synthetic_edges.txt"
            raw_path.write_text(
                (Path(__file__).parent / "fixtures/synthetic_edges.txt").read_text(),
                encoding="utf-8",
            )
            config = load_config()
            paths = config.paths
            test_config = config.__class__(
                paths=paths.__class__(
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

            stats = preprocess(
                self.spark,
                raw_path,
                test_config,
                sample_percent=100,
                force=True,
            )

            self.assertEqual(
                stats,
                {
                    "raw_rows": 11,
                    "invalid_rows": 4,
                    "missing_rows": 1,
                    "self_loops": 1,
                    "duplicate_directed_edges": 2,
                    "final_directed_edges": 4,
                    "final_undirected_edges": 4,
                    "final_nodes": 4,
                },
            )
            self.assertTrue(
                (root / "processed/cleaned_directed_edges").is_dir()
            )
            self.assertTrue((root / "reports/preprocessing_stats.json").is_file())
            self.assertTrue((root / "reports/preprocessing_stats.csv").is_file())
            self.assertTrue((root / "samples/directed_edges_100pct").is_dir())

            rerun = preprocess(
                self.spark,
                raw_path,
                test_config,
                sample_percent=100,
                force=False,
            )
            self.assertEqual(rerun, stats)


if __name__ == "__main__":
    unittest.main()
