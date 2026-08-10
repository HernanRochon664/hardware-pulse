"""
Tests for retry/backoff behavior in BaseHTMLScraper.

Covers:
- Transient Timeout → retry succeeds
- Persistent Timeout → propagates so ingest.py counts it as an error
- 5xx response → retry, eventual success
- 4xx response → no retry, propagates immediately
- Retry budget is attached to the HTTPAdapter with the configured size

Retries are delegated to urllib3's ``Retry`` policy (see base.py).
These tests use a real ``requests.Session`` and patch
``urllib3.connectionpool.HTTPConnectionPool._make_request``, the layer
inside ``urlopen`` where a single logical request makes its actual
attempts. urllib3's own retry recursion then drives the attempt count,
so the tests exercise the real production code path without the network.
"""

from unittest.mock import MagicMock, patch

import pytest
import requests
from urllib3.exceptions import ReadTimeoutError

from tests.test_base_scraper import StubScraper, make_html_page


def make_pool_response(html: str = "", status: int = 200) -> MagicMock:
    """Build a urllib3-style HTTPResponse mock for _make_request."""
    response = MagicMock(status=status, reason="OK", version=11)
    response.headers.get.return_value = None
    response.read.return_value = html.encode()
    response.data = html.encode()
    response.length_remaining = len(html)
    response.stream = lambda *args, **kwargs: iter([html.encode()])
    return response


def pool_response(html: str, status: int = 200) -> MagicMock:
    """A successful pool response carrying a parseable HTML page."""
    return make_pool_response(html, status)


# ---------------------------------------------------------------------------
# Retry on transient Timeout
# ---------------------------------------------------------------------------


class TestRetryOnTimeout:
    @patch("urllib3.connectionpool.HTTPConnectionPool._make_request")
    def test_timeout_then_success_returns_listings(self, mock_make_request):
        """Two timeouts followed by success → 1 page fetched, listing returned."""
        mock_make_request.side_effect = [
            ReadTimeoutError(MagicMock(), MagicMock(), "read timed out"),
            ReadTimeoutError(MagicMock(), MagicMock(), "read timed out"),
            pool_response(make_html_page("https://thot.uy/a")),
        ]
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
        assert str(listings[0].url) == "https://thot.uy/a"
        assert mock_make_request.call_count == 3

    @patch("urllib3.connectionpool.HTTPConnectionPool._make_request")
    def test_persistent_timeout_propagates(self, mock_make_request):
        """All attempts time out → ConnectionError propagates to caller."""
        mock_make_request.side_effect = ReadTimeoutError(MagicMock(), MagicMock(), "read timed out")
        scraper = StubScraper(
            product_tags=[],
            urls=["https://thot.uy/gpus/"],
            delay=0,
            max_pages_per_url=1,
            max_retries=3,
            retry_backoff=0,
        )
        with pytest.raises(requests.exceptions.ConnectionError):
            scraper.fetch()
        assert mock_make_request.call_count == 3


# ---------------------------------------------------------------------------
# Retry on 5xx
# ---------------------------------------------------------------------------


class TestRetryOn5xx:
    @patch("urllib3.connectionpool.HTTPConnectionPool._make_request")
    def test_500_then_success(self, mock_make_request):
        """5xx is retryable; subsequent success returns listings."""
        mock_make_request.side_effect = [
            make_pool_response("", status=500),
            make_pool_response("", status=500),
            pool_response(make_html_page("https://thot.uy/a")),
        ]
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
        assert mock_make_request.call_count == 3

    @patch("urllib3.connectionpool.HTTPConnectionPool._make_request")
    def test_persistent_500_raises(self, mock_make_request):
        """Persistent 5xx → raise_for_status raises HTTPError to the caller."""
        mock_make_request.side_effect = [
            make_pool_response("", status=503),
            make_pool_response("", status=503),
            make_pool_response("", status=503),
        ]
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
        assert mock_make_request.call_count == 3


# ---------------------------------------------------------------------------
# No retry on 4xx (preserves 404-stops-pagination contract)
# ---------------------------------------------------------------------------


class TestNoRetryOn4xx:
    @patch("urllib3.connectionpool.HTTPConnectionPool._make_request")
    def test_404_does_not_retry(self, mock_make_request):
        """404 returns immediately; no retries, pagination stops on the 404."""
        mock_make_request.side_effect = [
            pool_response(make_html_page("https://thot.uy/a")),
            make_pool_response("", status=404),
            pool_response(make_html_page("https://thot.uy/c")),  # should not be called
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
        assert mock_make_request.call_count == 2  # first page + 404, no retry on 404


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

    @patch("urllib3.connectionpool.HTTPConnectionPool._make_request")
    def test_single_attempt_no_retry(self, mock_make_request):
        """max_retries=1 means one attempt; failure propagates immediately."""
        mock_make_request.side_effect = ReadTimeoutError(MagicMock(), MagicMock(), "read timed out")
        scraper = StubScraper(
            product_tags=[],
            urls=["https://thot.uy/gpus/"],
            delay=0,
            max_pages_per_url=1,
            max_retries=1,
            retry_backoff=0,
        )
        with pytest.raises(requests.exceptions.ConnectionError):
            scraper.fetch()
        assert mock_make_request.call_count == 1


# ---------------------------------------------------------------------------
# Retry configuration is attached to the adapter
# ---------------------------------------------------------------------------


class TestRetryAttachedToAdapter:
    @patch("src.scrapers.base.requests.Session")
    def test_adapter_has_configured_retry_budget(self, mock_session_cls):
        """HTTPAdapter.max_retries reflects max_retries total attempts."""
        session = mock_session_cls.return_value
        scraper = StubScraper(
            product_tags=[],
            urls=["https://thot.uy/gpus/"],
            delay=0,
            max_pages_per_url=1,
            max_retries=3,
            retry_backoff=0.5,
        )
        scraper._get_session()
        https_adapter = session.mount.call_args_list[1].args[1]
        mounted = https_adapter.max_retries
        assert mounted.total == 2  # max_retries total attempts = 1 + 2 retries
        assert mounted.backoff_factor == 0.5
        assert 500 in mounted.status_forcelist
        assert 503 in mounted.status_forcelist
