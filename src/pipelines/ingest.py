"""
Ingestion pipeline for hardware-pulse.

Responsibilities:
- Orchestrate scraper execution across all sources
- Persist RawListing objects via the repository layer
- Aggregate results into a single IngestResult

Does NOT:
- Implement scraping logic
- Normalize or enrich data
- Manage database connections (caller responsibility)
"""

import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Protocol

from src.domain.models import RawListing
from src.storage.repository import upsert_raw_listing

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


class Scraper(Protocol):
    """
    Structural interface for all scrapers.

    Using Protocol instead of ABC means scrapers don't need to inherit
    from anything, they just need a .fetch() method. Pyright verifies
    this structurally at type-check time, not at runtime.
    """

    @property
    def name(self) -> str:
        """Human-readable scraper name for logging."""
        ...

    def fetch(self) -> Iterable[RawListing]:
        """Fetch listings from the source."""
        ...


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


@dataclass
class IngestResult:
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    errors: int = 0

    @property
    def total_processed(self) -> int:
        return self.inserted + self.updated + self.unchanged + self.errors

    def __str__(self) -> str:
        return (
            f"IngestResult("
            f"inserted={self.inserted}, "
            f"updated={self.updated}, "
            f"unchanged={self.unchanged}, "
            f"errors={self.errors})"
        )


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


def ingest(
    *,
    conn: sqlite3.Connection,
    scrapers: Iterable[Scraper],
    run_at: datetime | None = None,
) -> IngestResult:
    """
    Run the full ingestion pipeline across all scrapers.

    For each scraper:
    1. Fetch listings
    2. Upsert each listing into raw_listings
    3. Accumulate results

    A scraper failure is logged and counted as errors. It does not
    abort the pipeline. This ensures a broken Thot scraper doesn't
    prevent MercadoLibre data from being persisted.

    Args:
        conn: Open sqlite3.Connection. Caller manages lifecycle.
        scrapers: Iterable of Scraper-compatible objects.
        run_at: Timestamp for this pipeline run. Defaults to UTC now.
                Inject a fixed value in tests for deterministic results.

    Returns:
        IngestResult aggregating outcomes across all scrapers.
    """
    run_at = run_at or datetime.now(timezone.utc)
    result = IngestResult()

    logger.info("Starting ingestion run at %s", run_at.isoformat())

    for scraper in scrapers:
        scraper_name = scraper.name
        logger.info("Running scraper: %s", scraper_name)

        try:
            listings = list(scraper.fetch())
            logger.info("  %s → fetched %d listings", scraper_name, len(listings))
        except Exception as exc:
            # Fetch failure: entire scraper is down. Count all as errors
            # but continue with remaining scrapers.
            logger.error("  %s → fetch failed: %s", scraper_name, exc)
            result.errors += 1
            continue

        for listing in listings:
            try:
                upsert_result = upsert_raw_listing(listing, conn)

                if upsert_result.inserted:
                    result.inserted += 1
                elif upsert_result.updated:
                    result.updated += 1
                else:
                    result.unchanged += 1

            except Exception as exc:
                # Per-listing failure: log and continue. One bad row
                # should not abort the remaining listings.
                logger.warning(
                    "  %s → upsert failed for %r: %s",
                    scraper_name,
                    listing.title,
                    exc,
                )
                result.errors += 1

    logger.info("Ingestion complete: %s", result)
    return result