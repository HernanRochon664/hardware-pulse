"""
Tests for src/scrapers/base.py

Covers the Template Method orchestration: pagination, deduplication,
error handling, and delay behavior.
Uses a concrete subclass and mocked HTTP responses.
"""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from bs4 import BeautifulSoup, Tag

from src.domain.models import Condition, Currency, RawListing, Source
from src.scrapers.base import BaseHTMLScraper


# ---------------------------------------------------------------------------
# Concrete subclass for testing
# ---------------------------------------------------------------------------


class StubScraper(BaseHTMLScraper):
    """
    Minimal concrete implementation of BaseHTMLScraper for tests.

    Using a real subclass (not MagicMock) ensures we test the actual
    Template Method logic in BaseHTMLScraper.fetch(), not a mock of it.
    """

    def __init__(self, product_tags: list[str], **kwargs):
        super().__init__(**kwargs)
        # Each string is raw HTML for one product card
        self._product_tags = product_tags
        self._parse_calls = 0

    @property
    def name(self) -> str:
        return "stub"

    @property
    def source(self) -> Source:
        return Source.THOT

    def _build_page_url(self, base_url: str, page: int) -> str:
        return f"{base_url}?page={page}"

    def _get_product_containers(self, soup: BeautifulSoup) -> list[Tag]:
        return soup.select("div.product")

    def _parse_listing(self, product: Tag, fetched_at: datetime) -> RawListing | None:
        self._parse_calls += 1
        url = product.get("data-url")
        if not url:
            return None
        return RawListing(
            source=self.source,
            url=str(url),
            timestamp=fetched_at,
            title=product.get_text(strip=True) or "Product",
            price=100.0,
            currency=Currency.USD,
            seller="stub",
            condition=Condition.NEW,
        )


def make_html_page(*product_urls: str) -> str:
    """Build a minimal HTML page with product divs."""
    products = "".join(
        f'<div class="product" data-url="{url}">Product {i}</div>'
        for i, url in enumerate(product_urls)
    )
    return f"<html><body>{products}</body></html>"


def make_mock_response(html: str, status_code: int = 200) -> MagicMock:
    response = MagicMock()
    response.status_code = status_code
    response.text = html
    response.raise_for_status = MagicMock()
    return response


# ---------------------------------------------------------------------------
# Basic fetch behavior
# ---------------------------------------------------------------------------


class TestFetchBasic:
    @patch("src.scrapers.base.requests.get")
    def test_single_page_single_product(self, mock_get):
        mock_get.return_value = make_mock_response(
            make_html_page("https://thot.uy/a")
        )
        scraper = StubScraper(
            product_tags=[],
            urls=["https://thot.uy/gpus/"],
            delay=0,
            max_pages_per_url=1,
        )
        listings = scraper.fetch()
        assert len(listings) == 1
        assert str(listings[0].url) == "https://thot.uy/a"

    @patch("src.scrapers.base.requests.get")
    def test_empty_page_returns_no_listings(self, mock_get):
        mock_get.return_value = make_mock_response("<html><body></body></html>")
        scraper = StubScraper(
            product_tags=[],
            urls=["https://thot.uy/gpus/"],
            delay=0,
            max_pages_per_url=5,
        )
        listings = scraper.fetch()
        assert listings == []

    @patch("src.scrapers.base.requests.get")
    def test_parse_none_results_are_skipped(self, mock_get):
        """Products without data-url → _parse_listing returns None → skipped."""
        mock_get.return_value = make_mock_response(
            '<html><body><div class="product">No URL here</div></body></html>'
        )
        scraper = StubScraper(
            product_tags=[],
            urls=["https://thot.uy/gpus/"],
            delay=0,
            max_pages_per_url=1,
        )
        listings = scraper.fetch()
        assert listings == []


# ---------------------------------------------------------------------------
# Pagination behavior
# ---------------------------------------------------------------------------


class TestPagination:
    @patch("src.scrapers.base.time.sleep")
    @patch("src.scrapers.base.requests.get")
    def test_stops_on_empty_page(self, mock_get, mock_sleep):
        """Scraper should stop when a page returns no products."""
        mock_get.side_effect = [
            make_mock_response(make_html_page("https://thot.uy/a")),
            make_mock_response("<html><body></body></html>"),  # empty → stop
        ]
        scraper = StubScraper(
            product_tags=[],
            urls=["https://thot.uy/gpus/"],
            delay=0,
            max_pages_per_url=10,
        )
        listings = scraper.fetch()
        assert len(listings) == 1
        assert mock_get.call_count == 2

    @patch("src.scrapers.base.time.sleep")
    @patch("src.scrapers.base.requests.get")
    def test_respects_max_pages(self, mock_get, mock_sleep):
        """Scraper stops at max_pages_per_url even if more pages exist."""
        mock_get.return_value = make_mock_response(
            make_html_page("https://thot.uy/a", "https://thot.uy/b")
        )
        scraper = StubScraper(
            product_tags=[],
            urls=["https://thot.uy/gpus/"],
            delay=0,
            max_pages_per_url=2,
        )
        scraper.fetch()
        assert mock_get.call_count == 2

    @patch("src.scrapers.base.time.sleep")
    @patch("src.scrapers.base.requests.get")
    def test_404_stops_pagination(self, mock_get, mock_sleep):
        """404 on a page should stop pagination without raising."""
        mock_get.side_effect = [
            make_mock_response(make_html_page("https://thot.uy/a")),
            make_mock_response("", status_code=404),
        ]
        scraper = StubScraper(
            product_tags=[],
            urls=["https://thot.uy/gpus/"],
            delay=0,
            max_pages_per_url=10,
        )
        listings = scraper.fetch()
        assert len(listings) == 1  # first page only

    @patch("src.scrapers.base.time.sleep")
    @patch("src.scrapers.base.requests.get")
    def test_delay_called_between_pages(self, mock_get, mock_sleep):
        """time.sleep should be called once per page fetched."""
        mock_get.side_effect = [
            make_mock_response(make_html_page("https://thot.uy/a")),
            make_mock_response("<html><body></body></html>"),
        ]
        scraper = StubScraper(
            product_tags=[],
            urls=["https://thot.uy/gpus/"],
            delay=1.5,
            max_pages_per_url=10,
        )
        scraper.fetch()
        mock_sleep.assert_called_once_with(1.5)


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------


class TestDeduplication:
    @patch("src.scrapers.base.requests.get")
    def test_duplicate_urls_deduplicated_within_run(self, mock_get):
        """Same URL appearing on two pages → only one listing returned."""
        same_url_page = make_html_page("https://thot.uy/a")
        mock_get.side_effect = [
            make_mock_response(same_url_page),
            make_mock_response(same_url_page),
            make_mock_response("<html><body></body></html>"),
        ]
        scraper = StubScraper(
            product_tags=[],
            urls=["https://thot.uy/gpus/"],
            delay=0,
            max_pages_per_url=10,
        )
        listings = scraper.fetch()
        # Second page has same URL → new_items=0 → stops, deduplication works
        assert len(listings) == 1

    @patch("src.scrapers.base.requests.get")
    def test_different_urls_both_returned(self, mock_get):
        mock_get.side_effect = [
            make_mock_response(make_html_page("https://thot.uy/a", "https://thot.uy/b")),
            make_mock_response("<html><body></body></html>"),
        ]
        scraper = StubScraper(
            product_tags=[],
            urls=["https://thot.uy/gpus/"],
            delay=0,
            max_pages_per_url=10,
        )
        listings = scraper.fetch()
        assert len(listings) == 2


# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------


class TestInitialization:
    def test_empty_urls_raises(self):
        with pytest.raises(ValueError):
            StubScraper(product_tags=[], urls=[], delay=0)

    def test_name_property(self):
        scraper = StubScraper(product_tags=[], urls=["https://thot.uy/"], delay=0)
        assert scraper.name == "stub"

    def test_default_start_page_is_one(self):
        scraper = StubScraper(product_tags=[], urls=["https://thot.uy/"], delay=0)
        assert scraper._start_page == 1