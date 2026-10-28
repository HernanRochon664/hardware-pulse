"""
Scrape stage: run every enabled store adapter and persist what it publishes.

For each (store, category):
1. A ``scrape_runs`` row is opened.
2. Every listing found extends or opens its price period (repository rule 1).
3. The run is classified (ok / empty / anomaly / error). Only an "ok" run
   deactivates listings that disappeared from the store (rule 2): a failed or
   partial scrape never makes products vanish. An anomalous drop is accepted
   as "ok" when most of a sample of the missing product pages answer 404/410,
   i.e. the store really removed them.

Categories are scraped in the order of ``Category``. A URL that a store lists
in two categories is kept in the first one and skipped in the others, so a
listing never flips between categories (the first four categories keep their
URLs when new ones are added).
"""

from __future__ import annotations

import logging
import sqlite3
import time
from collections.abc import Callable
from dataclasses import dataclass

from src.config import StoreConfig, StoresConfig
from src.domain.models import Category
from src.pipelines.fx import ensure_fx_rate, to_usd
from src.scrapers.base import StoreAdapter, build_adapter
from src.settings import local_date, utcnow
from src.storage import repository
from src.storage.repository import canonical_url

logger = logging.getLogger(__name__)

AdapterFactory = Callable[[StoreConfig], StoreAdapter]

PROBE_SAMPLE = 10  # missing product pages checked when a run looks anomalous
PROBE_GONE_RATIO = 0.8


@dataclass
class ScrapeResult:
    store_id: str
    category: Category
    status: str
    n_found: int = 0
    n_new: int = 0
    n_changed: int = 0
    n_delisted: int = 0
    seconds: float = 0.0
    error: str | None = None

    @property
    def failed(self) -> bool:
        return self.status != "ok"


def scrape_stores(
    conn: sqlite3.Connection,
    config: StoresConfig,
    *,
    run_id: str,
    stores: list[str] | None = None,
    categories: list[Category] | None = None,
    adapter_factory: AdapterFactory = build_adapter,
) -> list[ScrapeResult]:
    selected = config.enabled(stores)
    repository.sync_stores(
        conn, [(s.id, s.name, s.base_url, s.platform.value) for s in config.stores.values()]
    )
    now = utcnow()
    usd_uyu = ensure_fx_rate(conn, local_date(now), now)

    results: list[ScrapeResult] = []
    order = list(Category)
    for store in selected:
        adapter = adapter_factory(store)
        claimed: dict[str, Category] = {}  # canonical URL -> category that kept it
        for category in sorted(store.categories, key=order.index):
            if categories and category not in categories:
                continue
            results.append(_scrape_one(conn, adapter, store, category, run_id, usd_uyu, claimed))
    return results


def _scrape_one(
    conn: sqlite3.Connection,
    adapter: StoreAdapter,
    store: StoreConfig,
    category: Category,
    run_id: str,
    usd_uyu: float | None,
    claimed: dict[str, Category] | None = None,
) -> ScrapeResult:
    started = utcnow()
    clock = time.monotonic()
    run = repository.start_scrape_run(conn, run_id, store.id, category, started)
    result = ScrapeResult(store.id, category, status="running")
    seen: set[int] = set()
    claimed = {} if claimed is None else claimed
    skipped = 0
    try:
        for listing in adapter.fetch_category(category, started):
            url = canonical_url(listing.url)
            if claimed.setdefault(url, category) is not category:
                skipped += 1
                continue
            price_usd = to_usd(listing.price, listing.currency, usd_uyu)
            if price_usd is None:
                logger.warning("%s: no FX rate to convert %s", store.id, listing.url)
                continue
            with conn:
                obs = repository.record_observation(conn, listing, price_usd)
            seen.add(obs.listing_id)
            result.n_found += 1
            result.n_new += obs.is_new_listing
            result.n_changed += obs.price_changed
    except Exception as exc:  # one broken store must not stop the others
        result.error = f"{type(exc).__name__}: {exc}"[:500]
        logger.error("%s/%s failed: %s", store.id, category.value, result.error)

    if skipped:
        logger.info(
            "%s/%s: %d URLs already listed in another category", store.id, category, skipped
        )
    if result.error:
        result.status = "error"
    else:
        result.status = repository.classify_run(conn, store.id, category, result.n_found)
    if result.status == "anomaly" and _drop_confirmed(conn, adapter, store, category, seen):
        result.status = "ok"
    if result.status == "ok":
        with conn:
            result.n_delisted = repository.deactivate_unseen(conn, store.id, category, seen)

    result.seconds = round(time.monotonic() - clock, 1)
    repository.finish_scrape_run(
        conn,
        run,
        status=result.status,
        finished_at=utcnow(),
        n_found=result.n_found,
        n_new=result.n_new,
        n_changed=result.n_changed,
        n_delisted=result.n_delisted,
        error=result.error,
    )
    logger.info(
        "%-10s %-11s %-7s found=%d new=%d changed=%d delisted=%d (%.1fs)",
        store.id,
        category.value,
        result.status,
        result.n_found,
        result.n_new,
        result.n_changed,
        result.n_delisted,
        result.seconds,
    )
    return result


def _drop_confirmed(
    conn: sqlite3.Connection,
    adapter: StoreAdapter,
    store: StoreConfig,
    category: Category,
    seen: set[int],
) -> bool:
    """True when most of the missing product pages are gone from the store."""
    urls = repository.unseen_active_urls(conn, store.id, category, seen, PROBE_SAMPLE)
    gone = sum(adapter.listing_gone(url) is True for url in urls)
    logger.info(
        "%s/%s: anomalous drop, %d of %d missing pages are gone",
        store.id,
        category.value,
        gone,
        len(urls),
    )
    return bool(urls) and gone >= PROBE_GONE_RATIO * len(urls)
