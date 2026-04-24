"""
Banifox scraper (custom HTML).

- Input: category URLs (no search)
- Pagination: /pag/N/
- Output: RawListing list
"""

import logging
import re
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
BASE_URL = "https://www.banifox.com"

PRICE_REGEX = re.compile(r"USD\s?([\d\.,]+)", re.IGNORECASE)

# ---------------------------------------------------------------------------
# Price parsing
# ---------------------------------------------------------------------------


def _parse_price(text: str) -> tuple[float | None, Currency | None]:
    """
    Extract USD price from arbitrary text using regex.

    Examples:
    - "USD 499.99" → (499.99, USD)
    - "USD499,99"  → (499.99, USD)
    """
    if not text:
        return None, None

    match = PRICE_REGEX.search(text)
    if not match:
        return None, None

    raw = match.group(1)

    try:
        # Normalize "1.234,56"
        normalized = raw.replace(".", "").replace(",", ".")
        return float(normalized), Currency.USD
    except ValueError:
        return None, None


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def _parse_listing(product: Tag, fetched_at: datetime) -> RawListing | None:
    """
    Parse a single Banifox product card into RawListing.

    Strategy:
    - Title: from <a> tag's title attribute
    - URL: from <a> tag's href attribute
    - Price: regex over full product text (robust to layout changes)
    """
    try:
        # Title
        link_tag = product.select_one("a[title]")
        title_raw = link_tag.get("title") if link_tag else None

        if isinstance(title_raw, list):
            title: str | None = title_raw[0] if title_raw else None
        elif isinstance(title_raw, str):
            title = title_raw
        else:
            title = None

        # URL
        href_raw = link_tag.get("href") if link_tag else None

        if isinstance(href_raw, list):
            href: str | None = href_raw[0] if href_raw else None
        elif isinstance(href_raw, str):
            href = href_raw
        else:
            href = None

        if not title or not href:
            return None

        url = urljoin(BASE_URL, href)

        # Price
        price_container = product.select_one("div.precio")
        if not price_container:
            return None

        texts = [t.strip() for t in price_container.find_all(string=True, recursive=False)]
        price_text = " ".join(t for t in texts if t)
        price, currency = _parse_price(price_text)

        if price is None or currency is None:
            logger.warning("Could not parse price (title=%r)", title)
            return None

        return RawListing(
            source=Source.BANIFOX,
            url=url,
            timestamp=fetched_at,
            title=title,
            price=price,
            currency=currency,
            seller="banifox",
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
    Build Banifox paginated URL.

    Page 1: base_url
    Page 2: base_url/pag/2/
    """
    base = base_url.rstrip("/")

    if page == 1:
        return base + "/"

    return f"{base}/pag/{page}/"


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------


def fetch_banifox_listings(
    urls: list[str],
    delay: float = REQUEST_DELAY_DEFAULT,
    max_pages_per_url: int = 20,
) -> list[RawListing]:
    """
    Scrape product listings from Banifox category pages.
    """
    fetched_at = datetime.now(timezone.utc)
    listings: list[RawListing] = []

    for base_url in urls:
        page = 1
        seen_urls: set[str] = set()

        while page <= max_pages_per_url:
            url = _build_page_url(base_url, page)

            response = requests.get(url, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")

            # Flexible selector: target product containers
            products = soup.select(".card-producto, .cont-producto")

            if not products:
                break

            new_items = 0

            for product in products:
                parsed = _parse_listing(product, fetched_at)

                if not parsed:
                    continue

                # Deduplication safeguard (important for Banifox)
                if parsed.url in seen_urls:
                    continue

                seen_urls.add(parsed.url)
                listings.append(parsed)
                new_items += 1

            if new_items == 0:
                # Likely pagination exhaustion or repetition
                break

            page += 1
            time.sleep(delay)

    logger.info(
        "Fetched %d listings from Banifox (%d URLs scraped)",
        len(listings),
        len(urls),
    )

    return listings

# ---------------------------------------------------------------------------
# Scraper adapter (Protocol-compliant)
# ---------------------------------------------------------------------------


class BanifoxScraper:
    """
    Thin adapter to make the functional Banifox scraper compatible
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
        return "banifox"

    def fetch(self) -> list[RawListing]:
        return fetch_banifox_listings(
            urls=self._urls,
            delay=self._delay,
            max_pages_per_url=self._max_pages_per_url,
        )