"""
Download the database published by the scrape workflow (GitHub Release asset).
"""

from __future__ import annotations

import gzip
import shutil
import tempfile
from pathlib import Path

import requests

from src.settings import data_url

CHUNK = 1 << 20


def download_database(target: Path, url: str | None = None, timeout: int = 120) -> Path:
    """Download and gunzip the published DB, replacing ``target`` atomically."""
    url = url or data_url()
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=target.parent) as tmp:
        compressed = Path(tmp) / "db.gz"
        with requests.get(url, stream=True, timeout=timeout) as response:
            response.raise_for_status()
            with compressed.open("wb") as fh:
                for chunk in response.iter_content(CHUNK):
                    fh.write(chunk)
        plain = Path(tmp) / "db.sqlite"
        if url.endswith(".gz"):
            with gzip.open(compressed, "rb") as src, plain.open("wb") as dst:
                shutil.copyfileobj(src, dst, CHUNK)
        else:
            compressed.replace(plain)
        for suffix in ("-wal", "-shm"):
            Path(str(target) + suffix).unlink(missing_ok=True)
        plain.replace(target)
    return target
