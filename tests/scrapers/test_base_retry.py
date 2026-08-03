"""
Tests for retry/backoff behavior in BaseHTMLScraper.

Covers:
- Transient Timeout → retry succeeds
- Persistent Timeout → propagates so ingest.py counts it as an error
- 5xx response → retry, eventual success
- 4xx response → no retry, propagates immediately

Uses the StubScraper pattern from tests/test_base_scraper.py for the
fetch loop, and mocks src.scrapers.base.requests.Session.get to control
HTTP outcomes without real network calls.
"""

from unittest.mock import MagicMock, patch

import pytest
import requests

from tests.test_base_scraper import StubScraper, make_html_page, make_mock_response

# ---------------------------------------------------------------------------
# Retry on transient Timeout
# ---------------------------------------------------------------------------


class TestRetryOnTimeout:
    @patch("src.scrapers.base.time.sleep")
    @patch("src.scrapers.base.requests.Session.get")
    def test_timeout_then_success_returns_listings(self, mock_get, mock_sleep):
        """Two timeouts followed by success → 1 page fetched, listing returned."""
        good_response = make_mock_response(make_html_page("https://thot.uy/a"))
        mock_get.side_effect = [
            requests.exceptions.Timeout("read timed out"),
            requests.exceptions.Timeout("read timed out"),
            good_response,
        ]
        scraper = StubScraper(
            product_tags=[],
            urls=["https://thot.uy/gpus/"],
            delay=0,
            max_pages_per_url=1,
            max_retries=3,
            retry_backoff=0,  # keep test fast
        )
        listings = scraper.fetch()
        assert len(listings) == 1
        assert str(listings[0].url) == "https://thot.uy/a"
        assert mock_get.call_count == 3

    @patch("src.scrapers.base.time.sleep")
    @patch("src.scrapers.base.requests.Session.get")
    def test_persistent_timeout_propagates(self, mock_get, mock_sleep):
        """All attempts time out → exception propagates to caller (ingest.py counts it)."""
        mock_get.side_effect = requests.exceptions.Timeout("read timed out")
        scraper = StubScraper(
            product_tags=[],
            urls=["https://thot.uy/gpus/"],
            delay=0,
            max_pages_per_url=1,
            max_retries=3,
            retry_backoff=0,
        )
        with pytest.raises(requests.exceptions.Timeout):
            scraper.fetch()
        assert mock_get.call_count == 3


# ---------------------------------------------------------------------------
# Retry on 5xx
# ---------------------------------------------------------------------------


class TestRetryOn5xx:
    @patch("src.scrapers.base.time.sleep")
    @patch("src.scrapers.base.requests.Session.get")
    def test_500_then_success(self, mock_get, mock_sleep):
        """5xx is retryable; subsequent success returns listings."""
        good = make_mock_response(make_html_page("https://thot.uy/a"))
        bad = make_mock_response("", status_code=500)
        mock_get.side_effect = [bad, bad, good]
        scraper = StubScraper(
            product_tags=[],
            urls=["https://thot.uy/gpus/"],
            delay=0,
            max_pages_per_url=1,
            max_retries=3,
            retry_backoff=0,
        )
        listings = scraper.fetch()
        assert len(listings) == 1
        assert mock_get.call_count == 3

    @patch("src.scrapers.base.time.sleep")
    @patch("src.scrapers.base.requests.Session.get")
    def test_persistent_500_raises(self, mock_get, mock_sleep):
        """Persistent 5xx → raise_for_status → HTTPError → propagates."""
        # raise_for_status is a MagicMock by default (does nothing); force HTTPError
        err_response = MagicMock()
        err_response.status_code = 503
        err_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "503 Server Error"
        )
        mock_get.side_effect = [err_response, err_response, err_response]
        scraper = StubScraper(
            product_tags=[],
            urls=["https://thot.uy/gpus/"],
            delay=0,
            max_pages_per_url=1,
            max_retries=3,
            retry_backoff=0,
        )
        with pytest.raises(requests.exceptions.HTTPError):
            scraper.fetch()
        assert mock_get.call_count == 3


# ---------------------------------------------------------------------------
# No retry on 4xx (preserves 404-stops-pagination contract)
# ---------------------------------------------------------------------------


class TestNoRetryOn4xx:
    @patch("src.scrapers.base.time.sleep")
    @patch("src.scrapers.base.requests.Session.get")
    def test_404_does_not_retry(self, mock_get, mock_sleep):
        """404 returns immediately; no retries, pagination stops on the 404."""
        mock_get.side_effect = [
            make_mock_response(make_html_page("https://thot.uy/a")),
            make_mock_response("", status_code=404),
            make_mock_response(make_html_page("https://thot.uy/c")),  # should not be called
        ]
        scraper = StubScraper(
            product_tags=[],
            urls=["https://thot.uy/gpus/"],
            delay=0,
            max_pages_per_url=5,
            max_retries=3,
            retry_backoff=0,
        )
        listings = scraper.fetch()
        assert len(listings) == 1
        assert mock_get.call_count == 2  # first page + 404, no retry on 404


# ---------------------------------------------------------------------------
# Retry count config
# ---------------------------------------------------------------------------


class TestRetryConfig:
    def test_max_retries_must_be_at_least_one(self):
        with pytest.raises(ValueError):
            StubScraper(
                product_tags=[],
                urls=["https://thot.uy/gpus/"],
                delay=0,
                max_retries=0,
            )

    @patch("src.scrapers.base.time.sleep")
    @patch("src.scrapers.base.requests.Session.get")
    def test_single_attempt_no_retry(self, mock_get, mock_sleep):
        """max_retries=1 means one attempt; failure propagates immediately."""
        mock_get.side_effect = requests.exceptions.Timeout("read timed out")
        scraper = StubScraper(
            product_tags=[],
            urls=["https://thot.uy/gpus/"],
            delay=0,
            max_pages_per_url=1,
            max_retries=1,
            retry_backoff=0,
        )
        with pytest.raises(requests.exceptions.Timeout):
            scraper.fetch()
        assert mock_get.call_count == 1
