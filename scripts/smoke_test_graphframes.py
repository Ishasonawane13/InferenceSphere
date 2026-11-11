"""Verify that local Spark can load GraphFrames and run PageRank."""

import logging
import os
import sys

from graphframes import GraphFrame
from pyspark.sql import SparkSession


GRAPHFRAMES_PACKAGE = os.getenv(
    "GRAPHFRAMES_PACKAGE",
    "io.graphframes:graphframes-spark3_2.12:0.12.3",
)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logger = logging.getLogger("influencesphere.smoke_test")

    spark = (
        SparkSession.builder.master(os.getenv("SPARK_MASTER", "local[2]"))
        .appName("InfluenceSphere-GraphFrames-Smoke-Test")
        .config("spark.jars.packages", GRAPHFRAMES_PACKAGE)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    try:
        vertices = spark.createDataFrame(
            [("1",), ("2",), ("3",), ("4",), ("5",)],
            ["id"],
        )
        edges = spark.createDataFrame(
            [
                ("1", "2"),
                ("1", "3"),
                ("2", "3"),
                ("3", "1"),
                ("4", "3"),
                ("5", "4"),
            ],
            ["src", "dst"],
        )

        graph = GraphFrame(vertices, edges)
        ranked = graph.pageRank(resetProbability=0.15, maxIter=10)
        rows = ranked.vertices.select("id", "pagerank").orderBy("id").collect()

        if len(rows) != 5:
            raise RuntimeError(f"Expected PageRank for 5 vertices, got {len(rows)}")

        print(f"Spark version: {spark.version}")
        print(f"GraphFrames package: {GRAPHFRAMES_PACKAGE}")
        print("PageRank results (toy graph):")
        for row in rows:
            print(f"  id={row['id']}, pagerank={row['pagerank']:.6f}")
        print("SMOKE TEST PASSED")
        logger.info("GraphFrames PageRank smoke test passed")
        return 0
    except Exception:
        logger.exception("GraphFrames PageRank smoke test failed")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        sys.exit(1)
