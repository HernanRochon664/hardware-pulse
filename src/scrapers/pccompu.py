"""
PCCompu Uruguay scraper for hardware-pulse.

Responsibilities:
- Scrape PCCompu product listing pages (server-rendered HTML)
- Handle pagination via ?pagina=N query parameter
- Return a list of RawListing objects

Does NOT:
- Persist data
- Deduplicate globally
- Resolve canonical products
- Convert currencies
"""

import logging
import time
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse, urlencode, parse_qs, urlunparse

import requests
from bs4 import BeautifulSoup, Tag

from src.domain.models import Condition, Currency, RawListing, Source

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL = "https://www.pccompu.com.uy"
REQUEST_DELAY_DEFAULT = 1.5


# ---------------------------------------------------------------------------
# URL construction
# ---------------------------------------------------------------------------


def _build_page_url(base_url: str, page: int) -> str:
    """
    Build paginated URL by setting ?pagina=N.

    Page 0: base_url?...&pagina=0
    Page 1: base_url?...&pagina=1

    PCCompu uses 0-indexed pagination, first page is pagina=0.
    We pass page index directly, no offset calculation needed.
    """
    parsed = urlparse(base_url)
    params = parse_qs(parsed.query)
    params["pagina"] = [str(page)]
    new_query = urlencode({k: v[0] for k, v in params.items()})
    return urlunparse(parsed._replace(query=new_query))


# ---------------------------------------------------------------------------
# Price parsing
# ---------------------------------------------------------------------------


def _parse_price(product: Tag) -> tuple[float | None, Currency | None]:
    """
    Extract price from PCCompu product card.

    Structure:
    div.opcionespreciocont
      div.precios
        div.precio_cont
          span.ele
            span.pmoneda  → "USD"
            span.pprecio  → "499" (integer, no separators)
    """
    try:
        moneda_tag = product.select_one("span.pmoneda")
        precio_tag = product.select_one("span.pprecio")

        if not moneda_tag or not precio_tag:
            return None, None

        raw_currency = moneda_tag.get_text(strip=True)
        raw_price = precio_tag.get_text(strip=True)

        # Currency is explicit string "USD", no symbol parsing needed
        if raw_currency == "USD":
            currency = Currency.USD
        elif raw_currency in ("$", "UYU"):
            currency = Currency.UYU
        else:
            logger.warning("Unknown currency: %r", raw_currency)
            return None, None

        # Price is a clean integer, no thousands separator to strip
        price = float(raw_price)
        return price, currency

    except (ValueError, AttributeError):
        return None, None


# ---------------------------------------------------------------------------
# Listing parsing
# ---------------------------------------------------------------------------


def _parse_listing(product: Tag, fetched_at: datetime) -> RawListing | None:
    """
    Parse a single PCCompu product card into a RawListing.

    Structure:
    div.prod_cont
      div.cont
        div.accont
          h2
            a[href]        → product URL
            span[itemprop="name"]  → product title
      div.opcionespreciocont  → price container
    """
    try:
        # Title and URL
        link_tag = product.select_one("div.accont h2 a")
        title_tag = product.select_one("span[itemprop='name']")

        if not link_tag or not title_tag:
            return None

        title = title_tag.get_text(strip=True)
        href = link_tag.get("href")

        if not title or not isinstance(href, str):
            return None

        url = urljoin(BASE_URL, href)

        price, currency = _parse_price(product)
        if price is None or currency is None:
            logger.warning("Could not parse price (title=%r)", title)
            return None

        return RawListing(
            source=Source.PCCOMPU,
            url=url,
            timestamp=fetched_at,
            title=title,
            price=price,
            currency=currency,
            seller="pccompu",
            item_id=None,
            condition=Condition.NEW,
            available_quantity=None,
            base_price=None,
        )

    except Exception as exc:
        logger.warning("Failed to parse product: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------


def fetch_pccompu_listings(
    urls: list[str],
    delay: float = REQUEST_DELAY_DEFAULT,
    max_pages_per_url: int = 20,
) -> list[RawListing]:
    """
    Scrape product listings from PCCompu category pages.

    Args:
        urls:             List of category URLs with query params (e.g. path=...).
        delay:            Seconds between page requests.
        max_pages_per_url: Safety cap on pagination depth.

    Returns:
        List of RawListing objects. May be empty if scraping fails.

    Raises:
        requests.HTTPError: On non-2xx responses.
    """
    fetched_at = datetime.now(timezone.utc)
    listings: list[RawListing] = []
    seen_urls: set[str] = set()

    for base_url in urls:
        page = 0  # PCCompu is 0-indexed

        while page < max_pages_per_url:
            url = _build_page_url(base_url, page)
            response = requests.get(url, timeout=10)

            if response.status_code == 404:
                break
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")

            # Products are children of #resultado_productos
            container = soup.select_one("#resultado_productos")
            if not container:
                break

            products = container.select("div.prod_cont")
            if not products:
                break

            new_items = 0
            for product in products:
                parsed = _parse_listing(product, fetched_at)
                if not parsed:
                    continue
                if parsed.url in seen_urls:
                    continue
                seen_urls.add(parsed.url)
                listings.append(parsed)
                new_items += 1

            if new_items == 0:
                break

            page += 1
            time.sleep(delay)

    logger.info(
        "Fetched %d listings from PCCompu (%d URLs scraped)",
        len(listings),
        len(urls),
    )
    return listings


# ---------------------------------------------------------------------------
# Scraper adapter (Protocol-compliant)
# ---------------------------------------------------------------------------


class PCCompuScraper:
    """
    Thin adapter making the PCCompu scraper compatible
    with the Scraper Protocol used by the ingestion pipeline.
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
        return "pccompu"

    def fetch(self) -> list[RawListing]:
        return fetch_pccompu_listings(
            urls=self._urls,
            delay=self._delay,
            max_pages_per_url=self._max_pages_per_url,
        )