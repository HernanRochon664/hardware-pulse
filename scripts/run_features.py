"""
Entry point for the feature engineering pipeline.

Usage:
    uv run scripts/run_features.py
    uv run scripts/run_features.py --since 2026-04-01

This script is intended to be run after resolve:
    run_ingest.py → run_resolve.py → run_features.py
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

# Make src importable when running from project root
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.pipelines.features import build_features
from src.storage.schema import init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

DB_PATH = Path("data/hardware_pulse.db")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the feature engineering pipeline.")
    parser.add_argument(
        "--since",
        type=str,
        default=None,
        help="Only process snapshots at or after this date (YYYY-MM-DD). "
             "If omitted, processes all available data.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    since: datetime | None = None
    if args.since:
        try:
            since = datetime.fromisoformat(args.since).replace(tzinfo=timezone.utc)
        except ValueError:
            logger.error("Invalid --since format. Expected YYYY-MM-DD, got: %s", args.since)
            sys.exit(1)

    logger.info("Initializing database at %s", DB_PATH)
    conn = init_db(DB_PATH)

    logger.info("Starting feature pipeline (since=%s)", since)
    result = build_features(conn, since=since)

    logger.info(
        "Feature pipeline finished | weeks=%d | skus=%d | rows=%d | fx_fetched=%d | errors=%d",
        result.weeks_processed,
        result.skus_processed,
        result.rows_written,
        result.fx_rates_fetched,
        len(result.errors),
    )

    if result.errors:
        logger.warning("Errors during run:")
        for err in result.errors:
            logger.warning("  - %s", err)
        sys.exit(1)


if __name__ == "__main__":
    main()