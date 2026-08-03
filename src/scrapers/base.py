"""
Base HTML scraper for hardware-pulse.

Implements the Template Method pattern:
- BaseHTMLScraper defines the scraping algorithm (fetch loop)
- Subclasses implement site-specific steps (_build_page_url,
  _get_product_containers, _parse_listing)

This eliminates duplicated orchestration logic across Thot, Banifox,
and PCCompu scrapers.
"""

import logging
import time
from abc import ABC, abstractmethod
from datetime import UTC, datetime

import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.domain.models import RawListing, Source

logger = logging.getLogger(__name__)

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

RETRYABLE_STATUS_CODES = (500, 502, 503, 504)


class BaseHTMLScraper(ABC):
    """
    Template base class for HTML-based scrapers.

    Defines the scraping contract and orchestrates the fetch loop.
    Subclasses must implement the four abstract methods to handle
    site-specific URL patterns, selectors, and price parsing.
    """

    _session: requests.Session | None = None

    def __init__(
        self,
        *,
        urls: list[str],
        delay: float = 1.5,
        max_pages_per_url: int = 20,
        timeout: int = 10,
        max_retries: int = 3,
        retry_backoff: float = 1.0,
    ) -> None:
        if not urls:
            raise ValueError(f"{self.__class__.__name__}: urls must not be empty")
        if max_retries < 1:
            raise ValueError(f"{self.__class__.__name__}: max_retries must be >= 1")
        self._urls = urls
        self._delay = delay
        self._max_pages_per_url = max_pages_per_url
        self._timeout = timeout
        self._max_retries = max_retries
        self._retry_backoff = retry_backoff
        self._session = None

    # ---------------------------------------------------------------------------
    # Abstract interface, subclasses must implement these
    # ---------------------------------------------------------------------------

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable scraper name for logging (e.g. 'banifox')."""
        ...

    @property
    @abstractmethod
    def source(self) -> Source:
        """Domain enum identifying the data source."""
        ...

    @abstractmethod
    def _build_page_url(self, base_url: str, page: int) -> str:
        """
        Build a paginated URL from a base URL and page index.

        The subclass decides the pagination dialect:
        - Thot:    base_url/page/N/   (1-indexed)
        - Banifox: base_url/pag/N/    (1-indexed)
        - PCCompu: base_url?pagina=N  (0-indexed)
        """
        ...

    @abstractmethod
    def _get_product_containers(self, soup: BeautifulSoup) -> list[Tag]:
        """
        Extract the list of product card Tags from a parsed page.

        Returns an empty list if no products are found, signals
        end of pagination to the fetch loop.
        """
        ...

    @abstractmethod
    def _parse_listing(self, product: Tag, fetched_at: datetime) -> RawListing | None:
        """
        Transform a product Tag into a RawListing domain object.

        Returns None if the product is invalid or unparseable.
        The fetch loop skips None results silently.
        """
        ...

    # ---------------------------------------------------------------------------
    # HTTP helpers
    # ---------------------------------------------------------------------------

    def _http_get(self, url: str) -> requests.Response:
        """
        GET with retry/backoff on transient failures.

        Retries up to ``max_retries`` total attempts on:
        - ``requests.exceptions.Timeout``
        - ``requests.exceptions.ConnectionError``
        - 5xx responses (500, 502, 503, 504)

        4xx responses are NOT retried; they propagate immediately so
        the caller can decide (e.g. 404 stops pagination).

        Uses a shared ``requests.Session`` with a stable User-Agent
        and an HTTPAdapter configured for connection pooling.
        """
        session = self._get_session()
        last_exc: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                response = session.get(url, timeout=self._timeout)
                if response.status_code in RETRYABLE_STATUS_CODES:
                    if attempt < self._max_retries:
                        logger.debug(
                            "  %s → %s on %s (attempt %d/%d), retrying",
                            self.name,
                            response.status_code,
                            url,
                            attempt,
                            self._max_retries,
                        )
                        time.sleep(self._retry_backoff * (2 ** (attempt - 1)))
                        continue
                    response.raise_for_status()
                return response
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
                last_exc = exc
                if attempt < self._max_retries:
                    logger.debug(
                        "  %s → %s on %s (attempt %d/%d), retrying",
                        self.name,
                        type(exc).__name__,
                        url,
                        attempt,
                        self._max_retries,
                    )
                    time.sleep(self._retry_backoff * (2 ** (attempt - 1)))
                    continue
                raise
        # Should not be reachable; loop either returns or raises.
        if last_exc is not None:
            raise last_exc
        raise RuntimeError(f"_http_get exited without returning for {url}")

    def _get_session(self) -> requests.Session:
        """Return a requests.Session configured with retries and a UA."""
        if self._session is None:
            self._session = requests.Session()
            self._session.headers.update({"User-Agent": DEFAULT_USER_AGENT})
            retry_cfg = Retry(
                total=self._max_retries - 1,
                backoff_factor=self._retry_backoff,
                status_forcelist=RETRYABLE_STATUS_CODES,
                allowed_methods=("GET",),
                raise_on_status=False,
            )
            adapter = HTTPAdapter(max_retries=retry_cfg, pool_connections=4, pool_maxsize=4)
            self._session.mount("http://", adapter)
            self._session.mount("https://", adapter)
        return self._session

    # ---------------------------------------------------------------------------
    # Template method — shared orchestration logic
    # ---------------------------------------------------------------------------

    def fetch(self) -> list[RawListing]:
        """
        Scrape all configured URLs and return a deduplicated list of listings.

        Algorithm:
        1. For each base URL:
           a. Iterate pages until no products found or max_pages reached
           b. For each page: GET → parse → extract containers → parse listings
           c. Deduplicate by URL within this run
           d. Stop early if a page yields no new items
        2. Log final count and return

        fetched_at is set once per fetch() call so all listings from
        the same run share a consistent timestamp.
        """
        fetched_at = datetime.now(UTC)  # ①
        listings: list[RawListing] = []
        seen_urls: set[str] = set()

        for base_url in self._urls:
            page = self._start_page

            while page <= self._start_page + self._max_pages_per_url - 1:
                url = self._build_page_url(base_url, page)

                response = self._http_get(url)

                # 404 signals end of pagination, not a fatal error
                if response.status_code == 404:
                    break
                response.raise_for_status()

                soup = BeautifulSoup(response.text, "html.parser")
                products = self._get_product_containers(soup)

                if not products:
                    break

                new_items = 0
                for product in products:
                    parsed = self._parse_listing(product, fetched_at)
                    if not parsed:
                        continue
                    if parsed.url in seen_urls:
                        continue
                    seen_urls.add(parsed.url)
                    listings.append(parsed)
                    new_items += 1

                # Stop if page returned no new items, avoids infinite loops
                # on sites that repeat content on out-of-range pages
                if new_items == 0:
                    break

                page += 1
                time.sleep(self._delay)

        logger.info(
            "Fetched %d listings from %s (%d URLs scraped)",
            len(listings),
            self.name,
            len(self._urls),
        )
        return listings

    @property
    def _start_page(self) -> int:
        """
        Starting page index for pagination.

        Override in subclasses that use 0-indexed pagination (e.g. PCCompu).
        Default is 1 (Thot, Banifox).
        """
        return 1
