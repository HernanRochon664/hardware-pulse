"""
Thot Computación scraper (WooCommerce HTML).

- Input: category URLs (no search)
- Pagination: /page/N/
- Output: RawListing list
"""

import logging
import time
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup, Tag
from urllib.parse import urljoin

from src.domain.models import Condition, Currency, RawListing, Source

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

REQUEST_DELAY_DEFAULT = 1.5
BASE_URL = "https://thotcomputacion.com.uy"

# ---------------------------------------------------------------------------
# Price parsing
# ---------------------------------------------------------------------------


def _parse_price_and_currency(text: str) -> tuple[float | None, Currency | None]:
    """
    Parse price string into (amount, currency).

    Handles:
    - 'US$ 44.99'   → (44.99, Currency.USD)
    - '$ 25.990'    → (25990.0, Currency.UYU)
    - '$ 25.990,00' → (25990.0, Currency.UYU)

    Returns (None, None) if parsing fails.
    """
    if not text:
        return None, None

    raw = text.strip()

    try:
        if "US$" in raw:
            value = raw.replace("US$", "").strip()
            return float(value), Currency.USD

        if "$" in raw:
            value = raw.replace("$", "").strip()
            # UYU uses '.' as thousands separator and ',' as decimal
            value = value.replace(".", "").replace(",", ".")
            return float(value), Currency.UYU

    except (ValueError, AttributeError):
        return None, None

    return None, None


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def _parse_listing(product: Tag, fetched_at: datetime) -> RawListing | None:
    """
    Parse a single WooCommerce product card into a RawListing.
    Returns None if any critical field is missing or unparseable.
    """
    try:
        price_tag = product.select_one(".price")
        link_tag = product.select_one("a.product-loop-title")
        title_tag = link_tag.select_one("h3") if link_tag else None

        if not title_tag or not price_tag or not link_tag:
            return None

        title = title_tag.text.strip()

        href = link_tag.get("href")
        if not isinstance(href, str) or not href:
            return None

        url = urljoin(BASE_URL, href)

        # WooCommerce nests multiple spans inside .price (e.g. sale + original)
        # get_text() flattens them; we take the first price found
        price_text = price_tag.get_text(separator=" ").strip()
        price, currency = _parse_price_and_currency(price_text)

        if price is None or currency is None:
            logger.warning("Could not parse price from: %r (title=%r)", price_text, title)
            return None

        return RawListing(
            source=Source.THOT,
            url=url,
            timestamp=fetched_at,
            title=title,
            price=price,
            currency=currency,
            seller="thot",
            item_id=None,
            condition=Condition.NEW,
            available_quantity=None,
            base_price=None,
        )

    except Exception as exc:
        logger.warning("Failed to parse product: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------


def _build_page_url(base_url: str, page: int) -> str:
    """
    Build WooCommerce paginated URL.

    Page 1: https://thotcomputacion.com.uy/categoria-producto/tarjetas-de-video/
    Page 2: https://thotcomputacion.com.uy/categoria-producto/tarjetas-de-video/page/2/
    """
    if page == 1:
        return base_url.rstrip("/") + "/"
    return base_url.rstrip("/") + f"/page/{page}/"


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------


def fetch_thot_listings(
    urls: list[str],
    delay: float = REQUEST_DELAY_DEFAULT,
    max_pages_per_url: int = 20,
) -> list[RawListing]:
    """
    Scrape product listings from Thot Computación category pages.

    Args:
        urls:              List of WooCommerce category URLs to scrape.
        delay:             Seconds to wait between page requests.
        max_pages_per_url: Safety cap to avoid infinite pagination loops.

    Returns:
        List of RawListing objects. May be empty if scraping fails.

    Raises:
        requests.HTTPError: On non-2xx responses.
    """
    fetched_at = datetime.now(timezone.utc)
    listings: list[RawListing] = []

    for base_url in urls:
        page = 1

        while page <= max_pages_per_url:
            url = _build_page_url(base_url, page)
            response = requests.get(url, timeout=10)
            if response.status_code == 404:
                break
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            products = soup.select("li.product")

            if not products:
                break

            for product in products:
                parsed = _parse_listing(product, fetched_at)
                if parsed:
                    listings.append(parsed)

            page += 1
            time.sleep(delay)

    logger.info(
        "Fetched %d listings from Thot (%d URLs scraped)",
        len(listings),
        len(urls),
    )

    return listings

# ---------------------------------------------------------------------------
# Scraper adapter (Protocol-compliant)
# ---------------------------------------------------------------------------


class ThotScraper:
    """
    Thin adapter to make the functional Thot scraper compatible
    with the Scraper Protocol.

    Holds configuration state and exposes parameterless .fetch().
    """

    def __init__(
        self,
        *,
        urls: list[str],
        delay: float = REQUEST_DELAY_DEFAULT,
        max_pages_per_url: int = 20,
    ):
        if not urls:
            raise ValueError("urls must not be empty")

        self._urls = urls
        self._delay = delay
        self._max_pages_per_url = max_pages_per_url

    @property
    def name(self) -> str:
        return "thot"

    def fetch(self) -> list[RawListing]:
        return fetch_thot_listings(
            urls=self._urls,
            delay=self._delay,
            max_pages_per_url=self._max_pages_per_url,
        )