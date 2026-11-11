import os
import tempfile
import unittest
from pathlib import Path

from graphframes import GraphFrame
from pyspark.sql import SparkSession

from src.algorithms.hits import run_hits
from src.config import load_config


@unittest.skipIf(
    os.name == "nt",
    "Spark GraphFrames integration requires WSL2 or Docker on Windows",
)
class HITSTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("InfluenceSphere-HITS-Test")
            .config("spark.ui.enabled", "false")
            .config("spark.hadoop.io.native.lib.available", "false")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_star_hub_authority_ordering_and_normalization(self) -> None:
        vertices = self.spark.createDataFrame([(0,), (1,), (2,), (3,)], ["id"])
        edges = self.spark.createDataFrame(
            [(0, 1), (0, 2), (0, 3)],
            ["src", "dst"],
        )
        graph = GraphFrame(vertices, edges)
        with tempfile.TemporaryDirectory() as temporary:
            result = run_hits(
                graph,
                Path(temporary),
                config=load_config(),
                max_iterations=20,
            )
            scores = self.spark.read.parquet(str(Path(temporary) / "hits"))
            norms = scores.select(
                (scores.hub_score * scores.hub_score).alias("hub_sq"),
                (scores.authority_score * scores.authority_score).alias(
                    "authority_sq"
                ),
            ).groupBy().sum().first()
            self.assertAlmostEqual(norms["sum(hub_sq)"], 1.0, places=5)
            self.assertAlmostEqual(norms["sum(authority_sq)"], 1.0, places=5)
            self.assertLess(
                scores.filter("user_id = 0").select("hub_rank").first()[0],
                scores.filter("user_id = 1").select("hub_rank").first()[0],
            )
            self.assertLess(
                scores.filter("user_id = 1").select("authority_rank").first()[0],
                scores.filter("user_id = 0").select("authority_rank").first()[0],
            )
            self.assertGreaterEqual(result["iterations"], 1)


if __name__ == "__main__":
    unittest.main()
