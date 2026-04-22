"""
Entrypoint for the hardware-pulse ingestion pipeline.

Responsibilities:
- Load configuration from configs/scrapers.yaml
- Initialize the SQLite database
- Manage Playwright browser lifecycle
- Instantiate enabled scrapers from config
- Execute the ingestion pipeline and log results

Usage:
    uv run scripts/run_ingest.py
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

# Add the project root to sys.path to enable imports from src
sys.path.insert(0, str(Path(__file__).parent.parent))

from playwright.sync_api import sync_playwright

from src.config import load_config
from src.pipelines.ingest import ingest
from src.scrapers.banifox import BanifoxScraper
from src.scrapers.mercadolibre import MercadoLibreScraper
from src.scrapers.thot import ThotScraper
from src.storage.schema import init_db

logger = logging.getLogger(__name__)

DB_PATH = Path("data/hardware_pulse.db")

# ---------------------------------------------------------------------------
# Scraper factory
# ---------------------------------------------------------------------------


def build_scrapers(config, page):
    """
    Instantiate all enabled scrapers from configuration.

    Playwright Page is passed in from main() where the browser
    lifecycle is managed. Scrapers don't own the browser.
    """
    scrapers = []

    if config.mercadolibre.enabled:
        for job in config.mercadolibre.jobs:
            scrapers.append(
                MercadoLibreScraper(
                    queries=job.queries,
                    page=page,
                    delay=config.resolve_request_delay(
                        config.mercadolibre.defaults,
                        job.request_delay,
                    ),
                    max_offsets_per_query=config.resolve_max_pages(
                        config.mercadolibre.defaults,
                        job.max_offsets_per_query,
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

    # Playwright context manager guarantees browser.close() even on error.
    # A leaked browser process would silently consume memory across runs.
    with sync_playwright() as p:
        browser = p.chromium.launch(
                    headless=True,
                    args=["--disable-blink-features=AutomationControlled"]
                )
        page = browser.new_page()
        page.set_extra_http_headers({
            "Accept-Language": "es-UY,es;q=0.9"
        })

        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        logger.info("Building scrapers...")
        scrapers = build_scrapers(config, page)

        if not scrapers:
            logger.warning("No scrapers enabled. Exiting.")
            browser.close()
            sys.exit(0)

        logger.info(
            "Starting ingestion pipeline with %d scraper(s)...", len(scrapers)
        )
        result = ingest(conn=conn, scrapers=scrapers)

        browser.close()

    logger.info("Pipeline complete: %s", result)


if __name__ == "__main__":
    main()