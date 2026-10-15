"""
WooCommerce Store API adapter (Thot, Laaca, NetPC, Obsidian, PCParts, Tecnocity).

Uses the public JSON endpoint ``/wp-json/wc/store/v1/products``, which every
WooCommerce >= 6 store exposes for its block-based cart. Compared with HTML it
gives exact prices (integer minor units), sale vs regular price, stock state
and stable product ids, and it is immune to theme changes.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterator
from datetime import datetime
from typing import Any

from src.domain.models import Category, Currency, ScrapedListing
from src.scrapers.base import ScrapeError, StoreAdapter

logger = logging.getLogger(__name__)

API_PATH = "/wp-json/wc/store/v1/products"
PER_PAGE = 100
_STOCK_QTY = re.compile(r"(\d+)\s+disponible", re.IGNORECASE)


class WooCommerceAdapter(StoreAdapter):
    _category_ids: dict[str, int] | None = None

    def category_ids(self) -> dict[str, int]:
        """Map category slug -> id. Older WooCommerce versions only filter by id."""
        if self._category_ids is None:
            ids: dict[str, int] = {}
            url = self.store.base_url + API_PATH + "/categories"
            for page in range(1, 21):
                response = self.client.get(url, params={"per_page": PER_PAGE, "page": page})
                if response.status_code != 200:
                    raise ScrapeError(f"{url} returned HTTP {response.status_code}")
                batch = response.json()
                if not isinstance(batch, list) or not batch:
                    break
                ids.update({c["slug"]: int(c["id"]) for c in batch})
                if len(batch) < PER_PAGE:
                    break
            self._category_ids = ids
        return self._category_ids

    def fetch_source(
        self, category: Category, source: str, scraped_at: datetime
    ) -> Iterator[ScrapedListing]:
        # A unique sort key keeps pagination stable while prices change.
        params: dict[str, Any] = {"per_page": PER_PAGE, "orderby": "id", "order": "asc"}
        if source.startswith("search:"):
            params["search"] = source.removeprefix("search:").strip()
        else:
            category_id = self.category_ids().get(source)
            if category_id is None:
                raise ScrapeError(f"{self.store.id}: unknown category slug {source!r}")
            params["category"] = category_id

        url = self.store.base_url + API_PATH
        page, total_pages = 1, 1
        while page <= min(total_pages, self.store.page_limit):
            response = self.client.get(url, params={**params, "page": page})
            if response.status_code != 200:
                raise ScrapeError(f"{url} page {page} returned HTTP {response.status_code}")
            try:
                products = response.json()
            except ValueError as exc:
                raise ScrapeError(f"{url} did not return JSON") from exc
            if not isinstance(products, list):
                # Unknown category slugs come back as an error object.
                raise ScrapeError(
                    f"{url} category {source!r}: unexpected payload {products!r:.200}"
                )
            total_pages = int(response.headers.get("X-WP-TotalPages", total_pages) or 1)
            for product in products:
                listing = self.parse_product(product, category, scraped_at)
                if listing is not None:
                    yield listing
            if not products:
                break
            page += 1

    def parse_product(
        self, product: dict[str, Any], category: Category, scraped_at: datetime
    ) -> ScrapedListing | None:
        prices = product.get("prices") or {}
        minor = int(prices.get("currency_minor_unit") or 0)
        price = _to_amount(prices.get("price"), minor)
        if price is None:
            return None  # "consultar precio" products have no price
        regular = _to_amount(prices.get("regular_price"), minor)
        currency_code = (prices.get("currency_code") or "USD").upper()
        if currency_code not in Currency.__members__:
            logger.warning("%s: unsupported currency %s", self.store.id, currency_code)
            return None

        availability = (product.get("stock_availability") or {}).get("text") or ""
        qty_match = _STOCK_QTY.search(availability)
        images = product.get("images") or []
        brands = product.get("brands") or []
        return ScrapedListing(
            store_id=self.store.id,
            category=category,
            url=product["permalink"],
            title=product["name"],
            price=price,
            regular_price=regular,
            currency=Currency(currency_code),
            in_stock=bool(product.get("is_in_stock")),
            stock_qty=int(qty_match.group(1)) if qty_match else None,
            store_sku=str(product["id"]),
            brand=brands[0].get("name") if brands else None,
            image_url=(images[0].get("thumbnail") or images[0].get("src")) if images else None,
            scraped_at=scraped_at,
        )


def _to_amount(value: Any, minor_unit: int) -> float | None:
    if value in (None, ""):
        return None
    amount = int(value) / (10**minor_unit)
    return amount if amount > 0 else None
