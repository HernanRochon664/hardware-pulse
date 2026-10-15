"""Start the Streamlit app against the synthetic fixture database."""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest
import requests

from tests.e2e.fixture_db import build_fixture_db

ROOT = Path(__file__).resolve().parents[2]
PORT = 8592
START_TIMEOUT = 60.0


@pytest.fixture(scope="session")
def app_url(tmp_path_factory) -> str:
    db = build_fixture_db(tmp_path_factory.mktemp("e2e") / "hardware_pulse.db")
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "streamlit",
            "run",
            str(ROOT / "streamlit_app.py"),
            "--server.port",
            str(PORT),
            "--server.headless",
            "true",
            "--browser.gatherUsageStats",
            "false",
        ],
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env={**os.environ, "HP_DB_PATH": str(db)},
    )
    url = f"http://localhost:{PORT}"
    deadline = time.monotonic() + START_TIMEOUT
    while time.monotonic() < deadline:
        try:
            if requests.get(f"{url}/_stcore/health", timeout=2).status_code == 200:
                break
        except requests.ConnectionError:
            pass
        time.sleep(0.5)
    else:
        proc.terminate()
        pytest.fail("Streamlit did not start in time")
    yield url
    proc.terminate()
    proc.wait(10)
