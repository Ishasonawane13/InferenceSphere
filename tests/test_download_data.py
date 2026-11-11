import gzip
import hashlib
import tempfile
import unittest
from pathlib import Path

from scripts.download_data import download_dataset


class DownloadDataTests(unittest.TestCase):
    def test_dry_run_does_not_create_files(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "mock.txt.gz"
            with gzip.open(source, "wb") as compressed:
                compressed.write(b"mock")
            output_dir = Path(temporary) / "raw"
            archive, extracted = download_dataset(
                source.as_uri(),
                output_dir,
                dry_run=True,
            )
            self.assertFalse(archive.exists())
            self.assertFalse(extracted.exists())

    def test_downloads_and_extracts_local_gzip(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "mock.txt.gz"
            content = b"1 2\n2 3\n"
            with gzip.open(source, "wb") as compressed:
                compressed.write(content)

            output_dir = root / "raw"
            archive, extracted = download_dataset(
                source.as_uri(),
                output_dir,
                expected_size=source.stat().st_size,
                expected_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
            )

            self.assertEqual(archive.read_bytes(), source.read_bytes())
            self.assertEqual(extracted.read_bytes(), content)
            self.assertTrue(archive.exists())


if __name__ == "__main__":
    unittest.main()