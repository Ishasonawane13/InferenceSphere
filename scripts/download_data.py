"""Download and extract the SNAP LiveJournal social graph dataset."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import logging
import os
import shutil
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse


DEFAULT_URL = "https://snap.stanford.edu/data/soc-LiveJournal1.txt.gz"
DEFAULT_OUTPUT_DIR = Path("data/raw")
DEFAULT_FILENAME = "soc-LiveJournal1.txt.gz"
CHUNK_SIZE = 1024 * 1024
LOGGER = logging.getLogger("influencesphere.download_data")


@dataclass(frozen=True)
class RemoteMetadata:
    size: int | None
    supports_resume: bool


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def _metadata(url: str) -> RemoteMetadata:
    if url.startswith("file://"):
        path_text = urllib.request.url2pathname(urlparse(url).path)
        if os.name == "nt" and path_text.startswith("/") and path_text[2:3] == ":":
            path_text = path_text[1:]
        path = Path(path_text)
        return RemoteMetadata(path.stat().st_size, False)

    request = urllib.request.Request(url, method="HEAD")
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            length = response.headers.get("Content-Length")
            return RemoteMetadata(
                int(length) if length is not None else None,
                response.headers.get("Accept-Ranges", "").lower() == "bytes",
            )
    except (urllib.error.HTTPError, urllib.error.URLError, ValueError) as error:
        LOGGER.warning("Could not read remote metadata for %s: %s", url, error)
        return RemoteMetadata(None, False)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(CHUNK_SIZE), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _verify(path: Path, expected_size: int | None, expected_sha256: str | None) -> None:
    if expected_size is not None and path.stat().st_size != expected_size:
        raise ValueError(
            f"Size mismatch for {path}: expected {expected_size} bytes, "
            f"got {path.stat().st_size}"
        )
    if expected_sha256 is not None:
        actual = _sha256(path)
        if actual.lower() != expected_sha256.lower():
            raise ValueError(
                f"SHA-256 mismatch for {path}: expected {expected_sha256}, got {actual}"
            )


def _download(
    url: str,
    destination: Path,
    metadata: RemoteMetadata,
    expected_size: int | None,
) -> None:
    partial = destination.with_name(destination.name + ".part")
    start = partial.stat().st_size if partial.exists() else 0
    headers = {"Range": f"bytes={start}-"} if start and metadata.supports_resume else {}

    if start and not metadata.supports_resume:
        LOGGER.info("Remote source does not support resume; restarting partial download.")
        partial.unlink()
        start = 0

    request = urllib.request.Request(url, headers=headers)
    try:
        response = urllib.request.urlopen(request, timeout=60)
    except (urllib.error.HTTPError, urllib.error.URLError) as error:
        raise RuntimeError(f"Unable to download {url}: {error}") from error

    status = getattr(response, "status", None)
    append = start > 0 and status == 206
    if start > 0 and not append:
        LOGGER.info("Server ignored resume request; restarting download.")
        start = 0

    mode = "ab" if append else "wb"
    downloaded = start
    progress_total = metadata.size
    LOGGER.info("Downloading %s", url)
    try:
        with response, partial.open(mode) as output:
            while True:
                chunk = response.read(CHUNK_SIZE)
                if not chunk:
                    break
                output.write(chunk)
                downloaded += len(chunk)
                if progress_total:
                    LOGGER.info(
                        "Download progress: %.1f%% (%d/%d bytes)",
                        downloaded * 100 / progress_total,
                        downloaded,
                        progress_total,
                    )
                else:
                    LOGGER.info("Download progress: %d bytes", downloaded)
    except OSError as error:
        raise RuntimeError(f"Unable to write download {partial}: {error}") from error

    if expected_size is not None and downloaded != expected_size:
        raise RuntimeError(
            f"Incomplete download: expected {expected_size} bytes, received {downloaded}"
        )
    partial.replace(destination)


def _extract_gzip(source: Path, destination: Path) -> None:
    partial = destination.with_name(destination.name + ".part")
    LOGGER.info("Extracting %s to %s", source, destination)
    try:
        with gzip.open(source, "rb") as compressed, partial.open("wb") as output:
            shutil.copyfileobj(compressed, output, length=CHUNK_SIZE)
        partial.replace(destination)
    except (OSError, EOFError) as error:
        partial.unlink(missing_ok=True)
        raise RuntimeError(f"Unable to extract {source}: {error}") from error


def download_dataset(
    url: str,
    output_dir: Path,
    *,
    expected_size: int | None = None,
    expected_sha256: str | None = None,
    dry_run: bool = False,
    force: bool = False,
) -> tuple[Path, Path]:
    """Download a gzip dataset and atomically extract its text file."""
    output_dir.mkdir(parents=True, exist_ok=True)
    archive = output_dir / DEFAULT_FILENAME
    extracted = output_dir / DEFAULT_FILENAME.removesuffix(".gz")
    metadata = _metadata(url)
    size = expected_size if expected_size is not None else metadata.size

    if dry_run:
        LOGGER.info(
            "Dry run: would download %s to %s (%s bytes reported).",
            url,
            archive,
            size if size is not None else "unknown",
        )
        return archive, extracted

    if archive.exists() and not force:
        LOGGER.info("Archive already exists; skipping download: %s", archive)
    else:
        _download(url, archive, metadata, expected_size)
    _verify(archive, expected_size, expected_sha256)

    if extracted.exists() and not force:
        LOGGER.info("Extracted file already exists; skipping extraction: %s", extracted)
    else:
        _extract_gzip(archive, extracted)
    return archive, extracted


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--expected-size", type=int)
    parser.add_argument("--sha256", dest="expected_sha256")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    _configure_logging(args.verbose)
    try:
        download_dataset(
            args.url,
            args.output_dir,
            expected_size=args.expected_size,
            expected_sha256=args.expected_sha256,
            dry_run=args.dry_run,
            force=args.force,
        )
    except (OSError, RuntimeError, ValueError) as error:
        LOGGER.error("%s", error)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())