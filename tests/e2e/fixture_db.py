"""Synthetic database for the dashboard E2E tests (no network, no real data)."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock

import src.pipelines.scrape as scrape_module
from src.config import StoreConfig, StoresConfig
from src.domain.models import Category, Currency, Platform, ScrapedListing
from src.pipelines.aggregate import aggregate
from src.pipelines.resolve import resolve_listings
from src.pipelines.scrape import scrape_stores
from src.settings import utcnow
from src.storage import repository
from src.storage.db import connect

EAGLE = "GPU Gigabyte GeForce RTX5060 Eagle OC 8Gb"
EAGLE_B = "Tarjeta de Video Gigabyte GeForce RTX 5060 8GB GDDR7 Eagle OC"
SHADOW = "GPU MSI GeForce RTX 5070 Shadow 2X OC 12GB"
FURY = "Memoria RAM Kingston Fury Beast 16GB DDR5 5600MHz"
RYZEN = "AMD Ryzen 7 7800X3D"
B550 = "Motherboard MSI B550M-A PRO DDR4 AM4"
B650 = "Motherboard ASUS TUF Gaming B650M-E WiFi DDR5 AM5"
OLED = "Monitor Gigabyte MO27Q28G QHD 27″ 280Hz OLED"

# store -> category -> [(slug, title, price_then, price_now)]
CATALOG: dict[str, dict[Category, list[tuple[str, str, float, float]]]] = {
    "alfa": {
        Category.GPU: [("eagle", EAGLE, 600.0, 560.0), ("shadow", SHADOW, 1050.0, 1050.0)],
        Category.RAM: [("fury", FURY, 75.0, 72.0)],
        Category.CPU: [("r7", RYZEN, 420.0, 410.0)],
        Category.MOTHERBOARD: [("b550", B550, 95.0, 90.0), ("b650", B650, 270.0, 266.0)],
        Category.MONITOR: [("oled", OLED, 800.0, 790.0)],
    },
    "beta": {
        Category.GPU: [("eagle", EAGLE_B, 590.0, 575.0)],
        Category.RAM: [],
    },
}


class FakeAdapter:
    phase = 0

    def __init__(self, store: StoreConfig):
        self.store = store

    def fetch_category(self, category: Category, scraped_at: datetime) -> Iterator[ScrapedListing]:
        for slug, title, then, now in CATALOG[self.store.id].get(category, []):
            yield ScrapedListing(
                store_id=self.store.id,
                category=category,
                url=f"{self.store.base_url}/p/{slug}",
                title=title,
                price=now if FakeAdapter.phase else then,
                currency=Currency.USD,
                in_stock=True,
                scraped_at=scraped_at,
            )


def _config() -> StoresConfig:
    return StoresConfig(
        stores={
            sid: StoreConfig(
                id=sid,
                name=sid.title(),
                base_url=f"https://{sid}.example",
                platform=Platform.WOOCOMMERCE,
                categories={c: [c.value] for c in CATALOG["alfa"]},
            )
            for sid in CATALOG
        }
    )


def build_fixture_db(path: Path) -> Path:
    """Two scrape runs ten days apart, ending now, so offers are fresh."""
    path.unlink(missing_ok=True)
    conn = connect(path)
    now = utcnow()
    with mock.patch.object(scrape_module, "ensure_fx_rate", lambda conn, day, now: 40.0):
        for phase, at in enumerate((now - timedelta(days=10), now)):
            FakeAdapter.phase = phase
            with mock.patch.object(scrape_module, "utcnow", lambda at=at: at):
                scrape_stores(conn, _config(), run_id=f"e2e{phase}", adapter_factory=FakeAdapter)
    with conn:
        repository.save_fx_rate(conn, now.date().isoformat(), 40.0, "fixture", now)
    resolve_listings(conn)
    aggregate(conn)
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()
    return path
