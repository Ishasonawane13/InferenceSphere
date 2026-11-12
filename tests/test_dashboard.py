import tempfile
import unittest
from pathlib import Path

import pandas as pd

from dashboard.components.loaders import load_csv, load_json, load_parquet
from dashboard.visualizations import bar, histogram


class DashboardArtifactTests(unittest.TestCase):
    def test_loaders_read_real_artifact_shapes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            json_path = root / "summary.json"
            csv_path = root / "ranking.csv"
            parquet_path = root / "degrees"
            json_path.write_text('{"node_count": 3}', encoding="utf-8")
            frame = pd.DataFrame(
                {"degree": [1, 2, 2], "node_count": [1, 1, 1]}
            )
            frame.to_csv(csv_path, index=False)
            frame.to_parquet(parquet_path)

            self.assertEqual(load_json(json_path)["node_count"], 3)
            self.assertEqual(len(load_csv(csv_path)), 3)
            self.assertEqual(list(load_parquet(parquet_path).columns), list(frame.columns))

    def test_visualizations_return_figures(self) -> None:
        frame = pd.DataFrame({"degree": [1, 2], "node_count": [3, 4]})
        self.assertEqual(len(bar(frame, "degree", "node_count").data), 1)
        self.assertEqual(len(histogram(frame, "degree").data), 1)


if __name__ == "__main__":
    unittest.main()
