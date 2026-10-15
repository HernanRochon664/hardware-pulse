"""
Store adapter contract.

An adapter turns one category source of one store (a category slug, a search
term or a category URL path, as configured in ``configs/stores.yaml``) into
``ScrapedListing`` objects. Adapters raise ``ScrapeError`` when a source is
broken (e.g. the category URL now returns 404) instead of silently returning
nothing: an empty result must never be mistaken for "the store sells nothing".
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Iterator
from datetime import datetime
from urllib.parse import urljoin

from src.config import StoreConfig
from src.domain.models import Category, ScrapedListing
from src.scrapers.http import HttpClient

logger = logging.getLogger(__name__)


class ScrapeError(RuntimeError):
    """A category source could not be scraped."""


class StoreAdapter(ABC):
    def __init__(self, store: StoreConfig, client: HttpClient | None = None) -> None:
        self.store = store
        self.client = client or HttpClient(delay=store.delay, timeout=store.timeout_s)

    def absolute(self, href: str) -> str:
        return urljoin(self.store.base_url + "/", href)

    def fetch_category(self, category: Category, scraped_at: datetime) -> Iterator[ScrapedListing]:
        """All listings for a category, deduplicated by URL across its sources."""
        seen: set[str] = set()
        for source in self.store.categories.get(category, []):
            for listing in self.fetch_source(category, source, scraped_at):
                if listing.url in seen:
                    continue
                seen.add(listing.url)
                yield listing

    @abstractmethod
    def fetch_source(
        self, category: Category, source: str, scraped_at: datetime
    ) -> Iterator[ScrapedListing]: ...


def build_adapter(store: StoreConfig, client: HttpClient | None = None) -> StoreAdapter:
    from src.domain.models import Platform
    from src.scrapers.nopcommerce import NopCommerceAdapter
    from src.scrapers.sme import SmeAdapter
    from src.scrapers.woocommerce import WooCommerceAdapter

    adapters: dict[Platform, type[StoreAdapter]] = {
        Platform.WOOCOMMERCE: WooCommerceAdapter,
        Platform.SME: SmeAdapter,
        Platform.NOPCOMMERCE: NopCommerceAdapter,
    }
    return adapters[store.platform](store, client)
