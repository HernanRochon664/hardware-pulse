"""
Master pipeline entrypoint that runs all three stages sequentially:
    ingest → resolve → features

If any stage fails, the pipeline stops immediately with a non-zero exit code
so Windows Task Scheduler (or any caller) can detect the failure.

Usage:
    uv run scripts/run_pipeline.py
    uv run scripts/run_pipeline.py --since 2026-04-01

This replaces the three separate Task Scheduler entries at 17:00 / 17:15 / 17:30
with a single scheduled task.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import load_config
from src.entities.catalog import load_catalog
from src.pipelines.features import build_features
from src.pipelines.ingest import ingest
from src.pipelines.resolve import resolve
from src.scrapers.banifox import BanifoxScraper
from src.scrapers.pccompu import PCCompuScraper
from src.scrapers.thot import ThotScraper
from src.storage.schema import init_db

logger = logging.getLogger(__name__)

DB_PATH = Path("data/hardware_pulse.db")
LOG_PATH = Path("logs/scheduler.log")


def setup_logging() -> None:
    log_dir = LOG_PATH.parent
    log_dir.mkdir(parents=True, exist_ok=True)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    root_logger.addHandler(console)

    file_handler = logging.FileHandler(LOG_PATH)
    file_handler.setFormatter(fmt)
    root_logger.addHandler(file_handler)


def build_scrapers(config):
    scrapers = []

    if config.thot.enabled:
        for job in config.thot.jobs:
            scrapers.append(
                ThotScraper(
                    urls=job.urls,
                    delay=config.resolve_request_delay(
                        config.thot.defaults,
                        job.request_delay,
                    ),
                    max_pages_per_url=config.resolve_max_pages(
                        config.thot.defaults,
                        job.max_pages_per_url,
                    ),
                )
            )

    if config.banifox.enabled:
        for job in config.banifox.jobs:
            scrapers.append(
                BanifoxScraper(
                    urls=job.urls,
                    delay=config.resolve_request_delay(
                        config.banifox.defaults,
                        job.request_delay,
                    ),
                    max_pages_per_url=config.resolve_max_pages(
                        config.banifox.defaults,
                        job.max_pages_per_url,
                    ),
                )
            )

    if config.pccompu.enabled:
        for job in config.pccompu.jobs:
            scrapers.append(
                PCCompuScraper(
                    urls=job.urls,
                    delay=config.resolve_request_delay(
                        config.pccompu.defaults,
                        job.request_delay,
                    ),
                    max_pages_per_url=config.resolve_max_pages(
                        config.pccompu.defaults,
                        job.max_pages_per_url,
                    ),
                )
            )

    return scrapers


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the full hardware-pulse pipeline (ingest → resolve → features)."
    )
    parser.add_argument(
        "--since",
        type=str,
        default=None,
        help="Only process snapshots at or after this date (YYYY-MM-DD). "
             "Passed through to the feature-engineering stage.",
    )
    return parser.parse_args()


def main() -> None:
    setup_logging()

    args = parse_args()

    since: datetime | None = None
    if args.since:
        try:
            since = datetime.fromisoformat(args.since).replace(tzinfo=UTC)
        except ValueError:
            logger.error("Invalid --since format. Expected YYYY-MM-DD, got: %s", args.since)
            sys.exit(1)

    run_ts = datetime.now(UTC)
    logger.info("=== Pipeline start at %s ===", run_ts.isoformat())

    # -----------------------------------------------------------------------
    # Stage 1 — Ingestion
    # -----------------------------------------------------------------------
    logger.info("--- Stage 1/3: Ingestion ---")
    config = load_config()
    logger.info("Initializing database at %s...", DB_PATH)
    conn = init_db(DB_PATH)
    scrapers = build_scrapers(config)

    if not scrapers:
        logger.warning("No scrapers enabled. Skipping ingestion.")
    else:
        ingest_result = ingest(conn=conn, scrapers=scrapers)
        logger.info("Ingestion complete: %s", ingest_result)
        if ingest_result.errors:
            logger.error("Ingestion failed with %d error(s)", ingest_result.errors)
            sys.exit(1)

    conn.close()

    # -----------------------------------------------------------------------
    # Stage 2 — Entity Resolution
    # -----------------------------------------------------------------------
    logger.info("--- Stage 2/3: Entity Resolution ---")
    conn = init_db(DB_PATH)
    catalog = load_catalog()
    resolve_result = resolve(conn=conn, catalog=catalog)
    logger.info("Resolution complete: %s", resolve_result)
    if resolve_result.errors:
        logger.error("Resolution failed with %d error(s)", resolve_result.errors)
        sys.exit(1)

    conn.close()

    # -----------------------------------------------------------------------
    # Stage 3 — Feature Engineering
    # -----------------------------------------------------------------------
    logger.info("--- Stage 3/3: Feature Engineering ---")
    conn = init_db(DB_PATH)
    feature_result = build_features(conn, since=since)

    logger.info(
        "Feature pipeline finished | weeks=%d | skus=%d | rows=%d | fx_fetched=%d | errors=%d",
        feature_result.weeks_processed,
        feature_result.skus_processed,
        feature_result.rows_written,
        feature_result.fx_rates_fetched,
        len(feature_result.errors),
    )

    if feature_result.errors:
        logger.warning("Errors during feature pipeline:")
        for err in feature_result.errors:
            logger.warning("  - %s", err)
        sys.exit(1)

    conn.close()

    logger.info("=== Pipeline complete at %s ===", datetime.now(UTC).isoformat())


if __name__ == "__main__":
    main()
