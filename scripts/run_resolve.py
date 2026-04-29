"""
Entrypoint for the hardware-pulse entity resolution pipeline.

Responsibilities:
- Load configuration and canonical catalog
- Open SQLite database connection
- Execute the resolution pipeline
- Log aggregated results

Usage:
    uv run scripts/run_resolve.py
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

# Add the project root to sys.path to enable imports from src
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import load_config
from src.entities.catalog import load_catalog
from src.pipelines.resolve import resolve
from src.storage.schema import init_db

logger = logging.getLogger(__name__)

DB_PATH = Path("data/hardware_pulse.db")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    logger.info("Loading configuration...")
    config = load_config()

    logger.info("Loading canonical catalog...")
    catalog = load_catalog()

    logger.info("Initializing database at %s...", DB_PATH)
    conn = init_db(DB_PATH)

    result = resolve(conn=conn, catalog=catalog)
    logger.info("Resolution complete: %s", result)


if __name__ == "__main__":
    main()