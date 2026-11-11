import os
import tempfile
import unittest
from pathlib import Path

from graphframes import GraphFrame
from pyspark.sql import SparkSession

from src.algorithms.label_propagation import run_label_propagation
from src.config import load_config


@unittest.skipIf(
    os.name == "nt",
    "Spark GraphFrames integration requires WSL2 or Docker on Windows",
)
class LabelPropagationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("InfluenceSphere-LPA-Test")
            .config("spark.ui.enabled", "false")
            .config("spark.hadoop.io.native.lib.available", "false")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_two_cliques_have_two_communities(self) -> None:
        vertices = self.spark.createDataFrame([(1,), (2,), (3,), (4,), (5,), (6,)], ["id"])
        edges = self.spark.createDataFrame(
            [
                (1, 2), (2, 1), (1, 3), (3, 1), (2, 3), (3, 2),
                (4, 5), (5, 4), (4, 6), (6, 4), (5, 6), (6, 5),
                (3, 4), (4, 3),
            ],
            ["src", "dst"],
        )
        graph = GraphFrame(vertices, edges)
        with tempfile.TemporaryDirectory() as temporary:
            result = run_label_propagation(
                graph,
                Path(temporary),
                config=load_config(),
                max_iterations=20,
            )
            communities = self.spark.read.parquet(
                str(Path(temporary) / "communities")
            )
            self.assertGreaterEqual(result["community_count"], 1)
            self.assertEqual(
                communities.select("user_id").distinct().count(),
                6,
            )
            self.assertGreater(result["modularity"], 0.0)


if __name__ == "__main__":
    unittest.main()
