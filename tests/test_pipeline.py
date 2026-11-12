import os
import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

import pandas as pd

from scripts.run_pipeline import run_pipeline


class PipelineTests(unittest.TestCase):
    def test_existing_outputs_are_skipped_and_status_written(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            output = root / "results"
            (output / "pagerank").mkdir(parents=True)
            (output / "pagerank" / "_SUCCESS").write_text("", encoding="utf-8")
            from src.config import load_config

            statuses = run_pipeline(
                load_config(),
                algorithms=["pagerank"],
                output_dir=output,
            )
            self.assertEqual(statuses["pagerank"]["status"], "done")
            self.assertTrue(statuses["pagerank"]["skipped"])
            status_json = json.loads(
                (output / "pipeline_status.json").read_text(encoding="utf-8")
            )
            self.assertEqual(
                status_json["algorithms"]["pagerank"]["status"],
                "done",
            )

    @unittest.skipIf(
        os.name == "nt",
        "Spark GraphFrames integration requires WSL2 or Docker on Windows",
    )
    def test_small_dataset_runs_all_algorithms(self) -> None:
        from src.config import load_config

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            processed = root / "processed"
            results = root / "results"
            processed.mkdir()
            directed = pd.DataFrame(
                {
                    "src": [1, 1, 2, 2, 3, 3, 4, 4],
                    "dst": [2, 3, 1, 4, 1, 4, 2, 3],
                }
            )
            undirected = pd.concat(
                [directed, directed.rename(columns={"src": "dst", "dst": "src"})],
                ignore_index=True,
            ).drop_duplicates()
            directed.to_parquet(processed / "cleaned_directed_edges")
            undirected.to_parquet(processed / "cleaned_undirected_edges")
            config = load_config()
            config = replace(
                config,
                paths=replace(
                    config.paths,
                    project_root=root,
                    data_processed=processed,
                    results=results,
                    spark_local_dir=root / "spark-tmp",
                    checkpoint_dir=root / "checkpoints",
                ),
            )
            statuses = run_pipeline(
                config,
                algorithms=["pagerank", "hits", "label_propagation", "triangle_count", "son", "cure"],
                output_dir=results,
                num_clusters=2,
                iterations=2,
                spark_conf={"spark.master": "local[1]"},
            )
            self.assertTrue(all(item["status"] == "done" for item in statuses.values()))
            self.assertTrue((results / "pipeline_status.json").is_file())


if __name__ == "__main__":
    unittest.main()
