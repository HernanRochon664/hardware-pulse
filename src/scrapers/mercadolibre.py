"""
MercadoLibre Uruguay scraper (Playwright) for hardware-pulse.

Uses a real browser to render JavaScript-heavy ML listing pages.
The caller is responsible for managing the Playwright browser lifecycle.

Responsibilities:
- Navigate ML listing pages using an injected Playwright Page
- Wait for product cards to render before extracting HTML
- Handle offset-based pagination (_Desde_N_ URL pattern)
- Return a list of RawListing objects

Does NOT:
- Initialize or close the Playwright browser (caller responsibility)
- Persist data
- Deduplicate globally
- Resolve canonical products
- Convert currencies
"""

import logging
import re
import time
from datetime import datetime, timezone

from bs4 import BeautifulSoup, Tag
from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError

from src.domain.models import Condition, Currency, RawListing, Source

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_SEARCH_URL = "https://listado.mercadolibre.com.uy"
REQUEST_DELAY_DEFAULT = 2.0
OFFSET_STEP = 48

# We wait up to 10s for the first product card to appear.
# If ML doesn't render products within that window, the page is likely
# a captcha, redirect, or empty result we stop pagination.
PRODUCT_SELECTOR = ".poly-card"
SELECTOR_TIMEOUT_MS = 20_000


# ---------------------------------------------------------------------------
# URL construction
# ---------------------------------------------------------------------------


def _normalize_query(query: str) -> str:
    """Convert search query to URL slug: "rtx 4070" → "rtx-4070"."""
    return re.sub(r"\s+", "-", query.strip().lower())


def _build_urls(query: str, max_offsets: int, step: int = OFFSET_STEP) -> list[str]:
    """
    Build paginated URLs for a query.

    Pattern (empirically verified):
    - Page 1: /rtx-4070
    - Page 2: /rtx-4070_Desde_49
    - Page N: /rtx-4070_Desde_{1 + (N-1) * step}
    """
    slug = _normalize_query(query)
    base = f"{BASE_SEARCH_URL}/{slug}"

    urls = [base]
    for i in range(1, max_offsets):
        offset = 1 + i * step
        urls.append(f"{base}_Desde_{offset}")

    return urls


# ---------------------------------------------------------------------------
# Price parsing
# ---------------------------------------------------------------------------


def _parse_price(product: Tag) -> tuple[float | None, Currency | None]:
    """
    Extract price from a rendered ML product card.

    ML structure:
    div.poly-price__current
      span.andes-money-amount
        span.andes-money-amount__currency-symbol  → "U$S" or "$"
        span.andes-money-amount__fraction         → "1.299" (dot = thousands)

    Dot is thousands separator in both UYU and USD, strip before float().
    """
    try:
        price_container = product.select_one(
            ".poly-price__current .andes-money-amount"
        )
        if not price_container:
            return None, None

        currency_tag = price_container.select_one(
            ".andes-money-amount__currency-symbol"
        )
        fraction_tag = price_container.select_one(".andes-money-amount__fraction")

        if not fraction_tag:
            return None, None

        currency_symbol = currency_tag.get_text(strip=True) if currency_tag else ""
        currency = (
            Currency.USD if "U$S" in currency_symbol or "US$" in currency_symbol
            else Currency.UYU
        )

        raw = fraction_tag.get_text(strip=True).replace(".", "")
        return float(raw), currency

    except (ValueError, AttributeError):
        return None, None


# ---------------------------------------------------------------------------
# Listing parsing
# ---------------------------------------------------------------------------


def _parse_listing(product: Tag, fetched_at: datetime) -> RawListing | None:
    """Parse a single ML product card into a RawListing."""
    try:
        link_tag = product.select_one("a.poly-component__title")
        if not link_tag:
            return None

        title = link_tag.get_text(strip=True)
        href = link_tag.get("href")

        if not title or not isinstance(href, str):
            return None

        # Strip tracking params
        url = href.split("?")[0] if "?" in href else href

        price, currency = _parse_price(product)
        if price is None or currency is None:
            logger.warning("Could not parse price (title=%r)", title)
            return None

        return RawListing(
            source=Source.MERCADOLIBRE,
            url=url,
            timestamp=fetched_at,
            title=title,
            price=price,
            currency=currency,
            seller="mercadolibre",
            item_id=None,
            condition=Condition.NEW,
            available_quantity=None,
            base_price=None,
        )

    except Exception as exc:
        logger.warning("Failed to parse ML listing: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------


def fetch_mercadolibre_listings(
    queries: list[str],
    page: Page,
    delay: float = REQUEST_DELAY_DEFAULT,
    max_offsets_per_query: int = 20,
    offset_step: int = OFFSET_STEP,
) -> list[RawListing]:
    """
    Scrape MercadoLibre Uruguay listing pages using Playwright.

    Args:
        queries:               Search terms (e.g. ["rtx 4070", "rx 7800 xt"]).
        page:                  Playwright Page instance. Caller manages lifecycle.
        delay:                 Seconds between page navigations.
        max_offsets_per_query: Safety cap on pagination depth per query.
        offset_step:           Items per page (default 48, empirically verified).

    Returns:
        List of RawListing objects across all queries.
    """
    fetched_at = datetime.now(timezone.utc)
    listings: list[RawListing] = []
    seen_urls: set[str] = set()

    for query in queries:
        logger.info("Scraping ML query: %r", query)
        urls = _build_urls(query, max_offsets_per_query, offset_step)

        for url in urls:
            try:
                page.goto(url, timeout=30_000)

                # Wait for product cards, if timeout, page has no results
                # (captcha, empty query, end of pagination)
                page.wait_for_selector(PRODUCT_SELECTOR, timeout=SELECTOR_TIMEOUT_MS)

            except PlaywrightTimeoutError:
                logger.debug("No products rendered at %s — stopping pagination", url)
                break

            soup = BeautifulSoup(page.content(), "html.parser")
            products = soup.select(PRODUCT_SELECTOR)

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

            time.sleep(delay)

    logger.info(
        "Fetched %d listings from MercadoLibre (queries=%r)",
        len(listings),
        queries,
    )
    return listings


# ---------------------------------------------------------------------------
# Scraper adapter (Protocol-compliant)
# ---------------------------------------------------------------------------


class MercadoLibreScraper:
    """
    Thin adapter making the Playwright ML scraper compatible
    with the Scraper Protocol used by the ingestion pipeline.

    The Playwright Page is injected at construction time.
    The caller (run_ingest.py) owns the browser lifecycle and must
    close it after all scrapers have run.
    """

    def __init__(
        self,
        *,
        queries: list[str],
        page: Page,
        delay: float = REQUEST_DELAY_DEFAULT,
        max_offsets_per_query: int = 20,
        offset_step: int = OFFSET_STEP,
    ):
        if not queries:
            raise ValueError("queries must not be empty")
        self._queries = queries
        self._page = page
        self._delay = delay
        self._max_offsets_per_query = max_offsets_per_query
        self._offset_step = offset_step

    @property
    def name(self) -> str:
        return "mercadolibre"

    def fetch(self) -> list[RawListing]:
        return fetch_mercadolibre_listings(
            queries=self._queries,
            page=self._page,
            delay=self._delay,
            max_offsets_per_query=self._max_offsets_per_query,
            offset_step=self._offset_step,
        )