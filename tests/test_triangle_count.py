import os
import tempfile
import unittest
from pathlib import Path

from graphframes import GraphFrame
from pyspark.sql import SparkSession

from src.algorithms.triangle_count import (
    run_triangle_count,
    validate_against_bruteforce,
)


@unittest.skipIf(
    os.name == "nt",
    "Spark GraphFrames integration requires WSL2 or Docker on Windows",
)
class TriangleCountTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("InfluenceSphere-Triangle-Test")
            .config("spark.ui.enabled", "false")
            .config("spark.hadoop.io.native.lib.available", "false")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def _run(self, edge_values: list[tuple[int, int]]) -> dict:
        nodes = sorted({node for edge in edge_values for node in edge})
        graph = GraphFrame(
            self.spark.createDataFrame([(node,) for node in nodes], ["id"]),
            self.spark.createDataFrame(edge_values, ["src", "dst"]),
        )
        with tempfile.TemporaryDirectory() as temporary:
            stats = run_triangle_count(graph, Path(temporary))
            metrics = self.spark.read.parquet(str(Path(temporary) / "triangles"))
            self.assertTrue(validate_against_bruteforce(graph, metrics))
            return {
                "stats": stats,
                "metrics": {
                    row["user_id"]: row.asDict()
                    for row in metrics.collect()
                },
            }

    def test_k4_has_four_triangles(self) -> None:
        edges = [
            (left, right)
            for left in range(1, 5)
            for right in range(1, 5)
            if left != right
        ]
        result = self._run(edges)
        self.assertEqual(result["stats"]["total_triangles"], 4.0)
        self.assertAlmostEqual(result["stats"]["average_clustering_coefficient"], 1.0)

    def test_triangle_with_tail(self) -> None:
        result = self._run(
            [(1, 2), (2, 1), (1, 3), (3, 1), (2, 3), (3, 2), (3, 4), (4, 3)]
        )
        self.assertEqual(result["stats"]["total_triangles"], 1.0)
        self.assertEqual(result["metrics"][4]["triangle_count"], 0)

    def test_tree_has_no_triangles(self) -> None:
        result = self._run([(1, 2), (2, 1), (2, 3), (3, 2), (3, 4), (4, 3)])
        self.assertEqual(result["stats"]["total_triangles"], 0.0)


if __name__ == "__main__":
    unittest.main()
