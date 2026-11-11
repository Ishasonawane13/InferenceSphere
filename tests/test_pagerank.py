import os
import tempfile
import unittest
from pathlib import Path

from graphframes import GraphFrame
from pyspark.sql import SparkSession

from src.algorithms.pagerank import run_pagerank
from src.config import load_config


@unittest.skipIf(
    os.name == "nt",
    "Spark GraphFrames integration requires WSL2 or Docker on Windows",
)
class PageRankTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("InfluenceSphere-PageRank-Test")
            .config("spark.ui.enabled", "false")
            .config("spark.hadoop.io.native.lib.available", "false")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_sink_star_scores_sum_and_ordering(self) -> None:
        vertices = self.spark.createDataFrame([(0,), (1,), (2,), (3,)], ["id"])
        edges = self.spark.createDataFrame(
            [(0, 1), (0, 2), (0, 3)],
            ["src", "dst"],
        )
        graph = GraphFrame(vertices, edges)
        config = load_config()
        with tempfile.TemporaryDirectory() as temporary:
            summary = run_pagerank(
                graph,
                Path(temporary),
                config=config,
                max_iterations=10,
                stability_iterations=[5, 10],
            )
            result = self.spark.read.parquet(str(Path(temporary) / "pagerank"))
            scores = {
                row["user_id"]: row["pagerank"]
                for row in result.collect()
            }
            self.assertAlmostEqual(sum(scores.values()), 1.0, places=5)
            self.assertGreater(scores[1], scores[0])
            self.assertEqual(summary["node_count"], 4)
            self.assertTrue((Path(temporary) / "pagerank_evaluation.json").is_file())


if __name__ == "__main__":
    unittest.main()
