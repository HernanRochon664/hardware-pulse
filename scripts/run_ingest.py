"""
Entrypoint for the hardware-pulse ingestion pipeline.

Responsibilities:
- Load configuration from configs/scrapers.yaml
- Initialize the SQLite database
- Instantiate enabled scrapers from config
- Execute the ingestion pipeline and log results

Usage:
    uv run scripts/run_ingest.py
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from src.config import load_config
from src.pipelines.ingest import ingest
from src.scrapers.banifox import BanifoxScraper
from src.scrapers.mercadolibre import MercadoLibreScraper
from src.scrapers.thot import ThotScraper
from src.storage.schema import init_db

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DB_PATH = Path("data/hardware_pulse.db")


# ---------------------------------------------------------------------------
# Scraper factory
# ---------------------------------------------------------------------------


def build_scrapers(config):
    """
    Instantiate all enabled scrapers from configuration.

    Applies precedence rules (job > defaults > global) via config.resolve_*.
    Skips disabled scrapers silently.
    """
    scrapers = []

    if config.mercadolibre.enabled:
        for job in config.mercadolibre.jobs:
            scrapers.append(
                MercadoLibreScraper(
                    query=job.query,
                    category_id=job.category_id,
                    max_results=config.resolve_max_results(
                        config.mercadolibre.defaults,
                        job.max_results,
                    ),
                )
            )

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

    return scrapers


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

    logger.info("Initializing database at %s...", DB_PATH)
    conn = init_db(DB_PATH)

    logger.info("Building scrapers...")
    scrapers = build_scrapers(config)

    if not scrapers:
        logger.warning("No scrapers enabled. Exiting.")
        sys.exit(0)

    logger.info("Starting ingestion pipeline with %d scraper(s)...", len(scrapers))
    result = ingest(conn=conn, scrapers=scrapers)

    logger.info("Pipeline complete: %s", result)


if __name__ == "__main__":
    main()