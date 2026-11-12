import json
import tempfile
import unittest
from pathlib import Path

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


if __name__ == "__main__":
    unittest.main()
