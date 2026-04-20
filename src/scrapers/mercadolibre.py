"""
MercadoLibre scraper for hardware-pulse.

Responsibilities:
- Query the MLU search API by query string or category_id
- Handle pagination transparently
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

import requests
from src.auth.ml_auth import get_valid_access_token
from src.domain.models import Condition, Currency, RawListing, Source

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL = "https://api.mercadolibre.com"
SITE_ID = "MLU"
PAGE_SIZE = 50          # ML's max results per request
REQUEST_DELAY = 1.0     # seconds between requests.Be a polite scraper


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _build_params(
    query: str | None,
    category_id: str | None,
    offset: int,
) -> dict:
    """Build query parameters for the search endpoint."""
    # offset tells ML where to start in the result set.
    # This is how we paginate: 0, 50, 100, ...
    params: dict = {
        "limit": PAGE_SIZE,
        "offset": offset,
        "condition": "new", # only new hardware by default
    }
    if query:
        params["q"] = query
    if category_id:
        params["category"] = category_id
    return params


def _parse_listing(item: dict, fetched_at: datetime) -> RawListing | None:
    """
    Parse a single item dict from the ML API into a RawListing.

    Returns None if the item is missing critical fields, rather than
    raising. This lets the caller skip bad records without crashing.

    We use .get() defensively throughout because the ML API is not
    fully typed: fields that appear in the docs may be absent in practice.
    """
    try:
        # currency_id from ML is already "UYU" or "USD". Matches our enum
        raw_currency = item.get("currency_id", "")
        try:
            currency = Currency(raw_currency)
        except ValueError:
            logger.warning("Unknown currency '%s' for item %s", raw_currency, item.get("id"))
            return None

        raw_condition = item.get("condition")
        condition = None
        if raw_condition:
            try:
                condition = Condition(raw_condition)
            except ValueError:
                # unknown condition value, store as None rather than crash
                pass

        return RawListing(
            source=Source.MERCADOLIBRE,
            url=item["permalink"], # always present in ML search results
            timestamp=fetched_at,
            title=item["title"],
            price=float(item["price"]),
            currency=currency,
            seller=item.get("seller", {}).get("nickname", "unknown"),
            item_id=item.get("id"),
            condition=condition,
            available_quantity=item.get("available_quantity"),
            base_price=float(item["base_price"]) if item.get("base_price") else None,
        )

    except (KeyError, ValueError, TypeError) as exc:
        # Log and skip rather than propagate. One bad listing should not
        # abort an entire scrape run of 200 items.
        logger.warning("Failed to parse item %s: %s", item.get("id"), exc)
        return None


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------


def fetch_mercadolibre_listings(
    query: str | None = None,
    category_id: str | None = None,
    max_results: int = 200,
) -> list[RawListing]:
    """
    Fetch listings from MercadoLibre Uruguay.

    Args:
        query: Free-text search query (e.g. "rtx 4070").
        category_id: ML category identifier (e.g. "MLU1700"). Takes
                    precedence over query for catalog coverage.
        max_results: Maximum total listings to return. The API caps
                    at ~1000 results per query regardless of this value.

    Returns:
        List of RawListing objects. May be shorter than max_results
        if the API returns fewer results or parsing failures occur.

    Raises:
        ValueError: If neither query nor category_id is provided.
        requests.HTTPError: If the API returns a non-2xx response.
    """
    if query is None and category_id is None:
        raise ValueError("Provide at least one of: query, category_id")

    search_url = f"{BASE_URL}/sites/{SITE_ID}/search"
    fetched_at = datetime.now(timezone.utc) # single timestamp for entire run

    listings: list[RawListing] = []
    offset = 0

    token = get_valid_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
    }

    while len(listings) < max_results:
        params = _build_params(query, category_id, offset)

        # raise_for_status() converts 4xx/5xx into exceptions immediately.
        # Better to fail loudly than to silently process an error response.
        response = requests.get(
            search_url,
            params=params,
            headers=headers,
            timeout=10,
        )
        response.raise_for_status()

        data = response.json()
        results = data.get("results", [])

        if not results:
            # API returned an empty page, we have exhausted the result set
            break

        for item in results:
            if len(listings) >= max_results:
                break
            parsed = _parse_listing(item, fetched_at)
            if parsed is not None:
                listings.append(parsed)

        # We paginate by incrementing the offset by PAGE_SIZE.
        # If the API returns fewer than PAGE_SIZE, it means we've reached
        # the end. The break above handles this.
        offset += PAGE_SIZE

        # be polite: don't hammer the API
        time.sleep(REQUEST_DELAY)

    logger.info("Fetched %d listings from MercadoLibre (query=%r, category=%r)",
                len(listings), query, category_id)
    return listings

# ---------------------------------------------------------------------------
# Scraper adapter (Protocol-compliant)
# ---------------------------------------------------------------------------


class MercadoLibreScraper:
    """
    Thin adapter to make the functional MercadoLibre scraper compatible
    with the Scraper Protocol used by the ingestion pipeline.

    This class holds configuration state (query/category/max_results)
    and exposes a parameterless .fetch() method as required.
    """

    def __init__(
        self,
        *,
        query: str | None = None,
        category_id: str | None = None,
        max_results: int = 200,
    ):
        if query is None and category_id is None:
            raise ValueError("Provide at least one of: query, category_id")

        self._query = query
        self._category_id = category_id
        self._max_results = max_results

    @property
    def name(self) -> str:
        return "mercadolibre"

    def fetch(self) -> list[RawListing]:
        return fetch_mercadolibre_listings(
            query=self._query,
            category_id=self._category_id,
            max_results=self._max_results,
        )