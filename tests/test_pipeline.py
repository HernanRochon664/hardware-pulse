"""Integration tests: scrape -> resolve -> aggregate on an in-memory DB."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime

import pytest

import src.pipelines.scrape as scrape_module
from src.config import StoreConfig, StoresConfig
from src.domain.models import Category, Currency, Platform, ScrapedListing
from src.pipelines.aggregate import aggregate
from src.pipelines.resolve import resolve_listings
from src.pipelines.scrape import scrape_stores
from src.storage.db import connect

STORE_ITEMS: dict[str, list[tuple[str, str, float, bool | None]]] = {}
GONE_URLS: set[str] = set()  # product pages that answer 404


class FakeAdapter:
    def __init__(self, store: StoreConfig):
        self.store = store

    def fetch_category(self, category: Category, scraped_at: datetime) -> Iterator[ScrapedListing]:
        items = STORE_ITEMS.get(self.store.id)
        if items is None:
            raise RuntimeError("store down")
        for slug, title, price, in_stock in items:
            yield ScrapedListing(
                store_id=self.store.id,
                category=category,
                url=f"{self.store.base_url}/p/{slug}",
                title=title,
                price=price,
                currency=Currency.USD,
                in_stock=in_stock,
                scraped_at=scraped_at,
            )

    def listing_gone(self, url: str) -> bool | None:
        return url in GONE_URLS


def _config() -> StoresConfig:
    stores = {
        sid: StoreConfig(
            id=sid,
            name=sid.title(),
            base_url=f"https://{sid}.test",
            platform=Platform.WOOCOMMERCE,
            categories={Category.GPU: ["x"]},
        )
        for sid in ("a", "b", "c")
    }
    return StoresConfig(stores=stores)


@pytest.fixture
def conn(monkeypatch):
    monkeypatch.setattr(scrape_module, "ensure_fx_rate", lambda conn, day, now: 40.0)
    STORE_ITEMS.clear()
    GONE_URLS.clear()
    db = connect(":memory:")
    yield db
    db.close()


def _run(conn, run_id: str) -> list:
    results = scrape_stores(conn, _config(), run_id=run_id, adapter_factory=FakeAdapter)
    resolve_listings(conn)
    aggregate(conn)
    return results


EAGLE = "GPU Gigabyte GeForce RTX5060 Eagle OC 8Gb"
EAGLE_B = "Tarjeta de Video Gigabyte GeForce RTX 5060 8GB GDDR7 Eagle OC"
EAGLE_C = "Tarjeta De Video GIGABYTE RTX 5060 8Gb Gddr7 Eagle OC Hdmi Dp"


def _stats(conn, name_like: str):
    return conn.execute(
        "SELECT s.* FROM product_stats s JOIN products p ON p.id = s.product_id "
        "WHERE p.name LIKE ?",
        (f"%{name_like}%",),
    ).fetchone()


def test_same_product_from_three_stores_gets_best_in_stock_price(conn):
    STORE_ITEMS["a"] = [("eagle", EAGLE, 570.0, True)]
    STORE_ITEMS["b"] = [("eagle", EAGLE_B, 549.0, False)]  # cheaper but out of stock
    STORE_ITEMS["c"] = [("eagle", EAGLE_C, 579.0, True)]
    _run(conn, "r1")
    stats = _stats(conn, "RTX 5060 8GB Eagle OC")
    assert stats["best_price_usd"] == 570.0
    assert stats["best_store_id"] == "a"
    assert stats["n_offers"] == 2 and stats["n_stores"] == 2


def test_failed_store_keeps_its_offers_and_empty_run_does_not_delist(conn):
    STORE_ITEMS["a"] = [("eagle", EAGLE, 570.0, True)]
    STORE_ITEMS["b"] = [("eagle", EAGLE_B, 560.0, True)]
    _run(conn, "r1")
    del STORE_ITEMS["b"]  # store b is down on the second run
    STORE_ITEMS["a"] = []  # store a returns nothing (layout change?)
    results = {r.store_id: r.status for r in _run(conn, "r2")}
    assert results == {"a": "empty", "b": "error", "c": "error"}
    active = conn.execute("SELECT COUNT(*) FROM listings WHERE is_active = 1").fetchone()[0]
    assert active == 2  # nothing was delisted by a broken run
    assert _stats(conn, "Eagle OC")["best_price_usd"] == 560.0


def test_listing_removed_by_store_stops_counting(conn):
    STORE_ITEMS["a"] = [
        ("eagle", EAGLE, 570.0, True),
        ("other", "GPU MSI GeForce RTX 5070 Shadow 2X OC 12GB", 1050.0, True),
    ]
    STORE_ITEMS["b"] = [("eagle", EAGLE_B, 540.0, True)]
    _run(conn, "r1")
    STORE_ITEMS["b"] = [("x", "GPU MSI GeForce RTX 5070 Shadow 2X OC 12GB", 1040.0, True)]
    _run(conn, "r2")  # b no longer sells the Eagle
    assert _stats(conn, "Eagle OC")["best_price_usd"] == 570.0
    assert _stats(conn, "RTX 5070 12GB Shadow")["n_stores"] == 2


def _coolers(n: int) -> list[tuple[str, str, float, bool | None]]:
    return [(f"c{i}", f"Cooler DeepCool AK{i}", 30.0 + i, True) for i in range(n)]


def _active(conn, store_id: str) -> int:
    return conn.execute(
        "SELECT COUNT(*) FROM listings WHERE store_id = ? AND is_active = 1", (store_id,)
    ).fetchone()[0]


def test_drop_confirmed_by_gone_pages_is_ok_and_delists(conn):
    STORE_ITEMS["a"] = _coolers(35)
    _run(conn, "r1")
    STORE_ITEMS["a"] = _coolers(13)
    GONE_URLS.update(f"https://a.test/p/c{i}" for i in range(13, 35))
    results = {r.store_id: r for r in _run(conn, "r2")}
    assert results["a"].status == "ok"
    assert results["a"].n_delisted == 22
    assert _active(conn, "a") == 13


def test_drop_with_live_pages_stays_anomalous_and_keeps_listings(conn):
    STORE_ITEMS["a"] = _coolers(35)
    _run(conn, "r1")
    STORE_ITEMS["a"] = _coolers(13)  # partial scrape: the pages still exist
    results = {r.store_id: r.status for r in _run(conn, "r2")}
    assert results["a"] == "anomaly"
    assert _active(conn, "a") == 35


def test_parse_error_price_is_flagged_and_excluded(conn):
    STORE_ITEMS["a"] = [("eagle", EAGLE, 570.0, True)]
    STORE_ITEMS["b"] = [("eagle", EAGLE_B, 560.0, True)]
    STORE_ITEMS["c"] = [("eagle", EAGLE_C, 1.049, True)]  # the v1 PCCompu bug
    _run(conn, "r1")
    issues = conn.execute("SELECT kind FROM quality_issues").fetchall()
    assert [i[0] for i in issues] == ["below_floor"]
    assert _stats(conn, "Eagle OC")["best_price_usd"] == 560.0


def test_rerun_is_idempotent(conn):
    STORE_ITEMS["a"] = [("eagle", EAGLE, 570.0, True)]
    _run(conn, "r1")
    _run(conn, "r2")
    assert conn.execute("SELECT COUNT(*) FROM price_periods").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM products").fetchone()[0] == 1


def test_product_daily_and_change_from_history(conn, monkeypatch):
    import src.pipelines.scrape as sp

    clock = iter(datetime(2026, 10, d, 15, 0, tzinfo=UTC) for d in (1, 1, 1, 1, 8, 8, 8, 8))
    monkeypatch.setattr(sp, "utcnow", lambda: next(clock))
    STORE_ITEMS["a"] = [("eagle", EAGLE, 600.0, True)]
    scrape_stores(
        conn,
        StoresConfig(stores={"a": _config().stores["a"]}),
        run_id="r1",
        adapter_factory=FakeAdapter,
    )
    STORE_ITEMS["a"] = [("eagle", EAGLE, 540.0, True)]
    scrape_stores(
        conn,
        StoresConfig(stores={"a": _config().stores["a"]}),
        run_id="r2",
        adapter_factory=FakeAdapter,
    )
    resolve_listings(conn)
    aggregate(conn, now=datetime(2026, 10, 8, 16, 0, tzinfo=UTC))
    days = conn.execute("SELECT date, min_usd FROM product_daily ORDER BY date").fetchall()
    assert [tuple(d) for d in days] == [("2026-10-01", 600.0), ("2026-10-08", 540.0)]
    stats = _stats(conn, "Eagle OC")
    assert stats["change_7d"] == pytest.approx(-0.10)
    assert stats["last_change_at"] == "2026-10-08"


# ---------------------------------------------------------------------------
# Catalog expansion: URLs listed in two categories, and products whose stores
# disagree on how much the title says.
# ---------------------------------------------------------------------------


class CategoryAdapter:
    """Each category returns its own titles; the same URL may appear in two."""

    items: dict[Category, list[tuple[str, str, float]]] = {}

    def __init__(self, store: StoreConfig):
        self.store = store

    def fetch_category(self, category: Category, scraped_at: datetime) -> Iterator[ScrapedListing]:
        for slug, title, price in self.items.get(category, []):
            yield ScrapedListing(
                store_id=self.store.id,
                category=category,
                url=f"{self.store.base_url}/p/{slug}",
                title=title,
                price=price,
                currency=Currency.USD,
                in_stock=True,
                scraped_at=scraped_at,
            )


def _multi_config(*categories: Category) -> StoresConfig:
    store = StoreConfig(
        id="a",
        name="A",
        base_url="https://a.test",
        platform=Platform.WOOCOMMERCE,
        # YAML order on purpose different from the Category order.
        categories={c: ["x"] for c in reversed(categories)},
    )
    return StoresConfig(stores={"a": store})


def test_url_in_two_categories_stays_in_the_first_one(conn):
    CategoryAdapter.items = {
        Category.CPU: [
            ("r5", "AMD Ryzen 5 7600 AM5", 200.0),
            ("kit", "Cooler AMD Wraith Stealth", 15.0),
        ],
        Category.COOLER: [("kit", "Cooler AMD Wraith Stealth", 15.0)],
    }
    results = scrape_stores(
        conn,
        _multi_config(Category.CPU, Category.COOLER),
        run_id="r1",
        adapter_factory=CategoryAdapter,
    )
    assert [r.category for r in results] == [Category.CPU, Category.COOLER]
    assert {r.category: r.n_found for r in results} == {Category.CPU: 2, Category.COOLER: 0}
    rows = conn.execute("SELECT url, category FROM listings ORDER BY url").fetchall()
    assert [(r["url"].rsplit("/", 1)[1], r["category"]) for r in rows] == [
        ("kit", "cpu"),
        ("r5", "cpu"),
    ]


def test_listing_that_changes_category_is_resolved_again(conn):
    CategoryAdapter.items = {Category.CPU: [("w", "Cooler AMD Wraith Stealth", 15.0)]}
    scrape_stores(conn, _multi_config(Category.CPU), run_id="r1", adapter_factory=CategoryAdapter)
    resolve_listings(conn)
    first = conn.execute("SELECT match_status FROM listings").fetchone()
    assert first["match_status"] == "rejected"  # a cooler is not a CPU

    CategoryAdapter.items = {Category.COOLER: [("w", "Cooler AMD Wraith Stealth", 15.0)]}
    scrape_stores(
        conn, _multi_config(Category.COOLER), run_id="r2", adapter_factory=CategoryAdapter
    )
    assert resolve_listings(conn).processed == 1
    row = conn.execute("SELECT category, match_status FROM listings").fetchone()
    assert (row["category"], row["match_status"]) == ("cooler", "matched")


@pytest.mark.parametrize("panel_first", [True, False])
def test_product_keeps_the_most_informative_family(conn, panel_first):
    listings = [
        ("a", "Monitor Gigabyte MO27Q28G QHD 27″ 280Hz OLED", 800.0),
        ("b", "Monitor Gigabyte MO27Q28G QHD 27″ 280Hz", 790.0),
    ]
    CategoryAdapter.items = {Category.MONITOR: listings if panel_first else listings[::-1]}
    scrape_stores(
        conn, _multi_config(Category.MONITOR), run_id="r1", adapter_factory=CategoryAdapter
    )
    resolve_listings(conn)
    products = conn.execute("SELECT family_id FROM products").fetchall()
    assert [p["family_id"] for p in products] == ["monitor:27-qhd-200-280hz-oled"]
