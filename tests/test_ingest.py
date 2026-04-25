"""
Tests for src/pipelines/ingest.py

Covers pipeline orchestration: scraper execution, result aggregation,
error handling, and upsert delegation.
Uses MagicMock for scrapers and in-memory SQLite for storage.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock


from src.domain.models import Condition, Currency, RawListing, Source
from src.pipelines.ingest import IngestResult, ingest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_scraper(name: str, listings: list[RawListing], raises: bool = False) -> MagicMock:
    """
    Build a mock Scraper that returns the given listings or raises.

    We mock the Scraper Protocol, fetch() returns listings,
    name returns the scraper identifier for logging.
    """
    scraper = MagicMock()
    scraper.name = name
    if raises:
        scraper.fetch.side_effect = RuntimeError(f"{name} fetch failed")
    else:
        scraper.fetch.return_value = listings
    return scraper


def make_listing(url: str, price: float = 500.0) -> RawListing:
    return RawListing(
        source=Source.THOT,
        url=url,
        timestamp=datetime.now(timezone.utc),
        title=f"Product at {url}",
        price=price,
        currency=Currency.USD,
        seller="thot",
        condition=Condition.NEW,
    )


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestIngestHappyPath:
    def test_single_scraper_inserts_listings(self, db_conn):
        listings = [
            make_listing("https://thot.uy/a"),
            make_listing("https://thot.uy/b"),
            make_listing("https://thot.uy/c"),
        ]
        scraper = make_scraper("thot", listings)
        result = ingest(conn=db_conn, scrapers=[scraper])

        assert result.inserted == 3
        assert result.updated == 0
        assert result.unchanged == 0
        assert result.errors == 0

    def test_multiple_scrapers_aggregate_results(self, db_conn):
        scraper1 = make_scraper("thot", [
            make_listing("https://thot.uy/a"),
            make_listing("https://thot.uy/b"),
        ])
        scraper2 = make_scraper("banifox", [
            make_listing("https://banifox.com/x"),
        ])
        result = ingest(conn=db_conn, scrapers=[scraper1, scraper2])

        assert result.inserted == 3
        assert result.errors == 0

    def test_empty_scraper_returns_zero_results(self, db_conn):
        scraper = make_scraper("thot", [])
        result = ingest(conn=db_conn, scrapers=[scraper])

        assert result.inserted == 0
        assert result.total_processed == 0

    def test_custom_run_at_is_accepted(self, db_conn):
        """run_at should be accepted without error."""
        scraper = make_scraper("thot", [make_listing("https://thot.uy/a")])
        fixed_time = datetime(2026, 4, 25, 12, 0, 0, tzinfo=timezone.utc)

        result = ingest(conn=db_conn, scrapers=[scraper], run_at=fixed_time)
        assert result.inserted == 1


# ---------------------------------------------------------------------------
# Duplicate handling
# ---------------------------------------------------------------------------


class TestIngestDuplicates:
    def test_same_listing_twice_counts_unchanged(self, db_conn):
        listing = make_listing("https://thot.uy/a")
        scraper1 = make_scraper("thot", [listing])
        scraper2 = make_scraper("thot", [listing])

        result = ingest(conn=db_conn, scrapers=[scraper1, scraper2])

        assert result.inserted == 1
        assert result.unchanged == 1

    def test_price_change_counts_as_updated(self, db_conn):
        listing_v1 = make_listing("https://thot.uy/a", price=500.0)
        listing_v2 = make_listing("https://thot.uy/a", price=450.0)

        scraper1 = make_scraper("thot", [listing_v1])
        scraper2 = make_scraper("thot", [listing_v2])

        result = ingest(conn=db_conn, scrapers=[scraper1, scraper2])

        assert result.inserted == 1
        assert result.updated == 1


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestIngestErrorHandling:
    def test_failing_scraper_counts_as_error(self, db_conn):
        scraper = make_scraper("thot", [], raises=True)
        result = ingest(conn=db_conn, scrapers=[scraper])

        assert result.errors == 1
        assert result.inserted == 0

    def test_failing_scraper_does_not_abort_pipeline(self, db_conn):
        """A broken scraper should not prevent other scrapers from running."""
        broken = make_scraper("thot", [], raises=True)
        working = make_scraper("banifox", [make_listing("https://banifox.com/x")])

        result = ingest(conn=db_conn, scrapers=[broken, working])

        assert result.errors == 1
        assert result.inserted == 1

    def test_no_scrapers_returns_empty_result(self, db_conn):
        result = ingest(conn=db_conn, scrapers=[])

        assert result.total_processed == 0
        assert result.errors == 0


# ---------------------------------------------------------------------------
# IngestResult
# ---------------------------------------------------------------------------


class TestIngestResult:
    def test_total_processed_sums_all_fields(self):
        result = IngestResult(inserted=3, updated=1, unchanged=2, errors=1)
        assert result.total_processed == 7

    def test_str_representation_includes_all_fields(self):
        result = IngestResult(inserted=1, updated=0, unchanged=2, errors=0)
        s = str(result)
        assert "inserted=1" in s
        assert "unchanged=2" in s