"""Offline tests for the store adapters, using trimmed real responses."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from src.config import StoreConfig
from src.domain.models import Category, Platform
from src.scrapers.base import ScrapeError, build_adapter
from tests.conftest import FIXTURES_DIR

STORES = FIXTURES_DIR / "stores"
NOW = datetime(2026, 10, 14, 12, 0, tzinfo=UTC)


class FakeResponse:
    def __init__(self, status: int = 200, body: bytes = b"", headers: dict | None = None):
        self.status_code = status
        self.content = body
        self.headers = headers or {}

    def json(self):
        return json.loads(self.content)


class FakeClient:
    """Serves canned responses keyed by (url, page) and records the calls."""

    def __init__(self, routes):
        self.routes = routes
        self.calls: list[tuple[str, dict]] = []

    def get(self, url, params=None, **_):
        self.calls.append((url, dict(params or {})))
        return self.routes(url, dict(params or {}))


def store(platform: Platform, categories: dict, base: str = "https://shop.test") -> StoreConfig:
    return StoreConfig(
        id="teststore", name="Test", base_url=base, platform=platform, categories=categories
    )


def test_woocommerce_maps_prices_stock_and_resolves_category_ids():
    products = (STORES / "woocommerce_products.json").read_bytes()
    categories = (STORES / "woocommerce_categories.json").read_bytes()

    def routes(url, params):
        if url.endswith("/categories"):
            return FakeResponse(body=categories)
        assert params["category"] == 30  # slug "gpu" resolved to its id
        if params["page"] == 1:
            return FakeResponse(body=products, headers={"X-WP-TotalPages": "1"})
        return FakeResponse(body=b"[]")

    client = FakeClient(routes)
    adapter = build_adapter(store(Platform.WOOCOMMERCE, {"gpu": ["gpu"]}), client)  # type: ignore[arg-type]
    listings = list(adapter.fetch_category(Category.GPU, NOW))

    assert len(listings) == 7
    on_sale = listings[0]
    assert on_sale.price == 345.0 and on_sale.regular_price == 350.0
    assert on_sale.in_stock is False
    assert all(item.store_sku for item in listings)
    assert any(item.in_stock for item in listings)


def test_woocommerce_unknown_slug_is_an_error_not_an_empty_result():
    def routes(url, params):
        return FakeResponse(body=b"[]")

    adapter = build_adapter(store(Platform.WOOCOMMERCE, {"gpu": ["missing"]}), FakeClient(routes))  # type: ignore[arg-type]
    with pytest.raises(ScrapeError):
        list(adapter.fetch_category(Category.GPU, NOW))


def test_sme_parses_thousands_hidden_old_price_and_stock():
    page0 = (STORES / "sme_category.html").read_bytes()

    def routes(url, params):
        return FakeResponse(body=page0 if url.endswith("pagina=0") else b"<html></html>")

    adapter = build_adapter(
        store(Platform.SME, {"gpu": ["/catalogo/componentes/tarjetas-de-video/"]}),
        FakeClient(routes),  # type: ignore[arg-type]
    )
    listings = {item.store_sku: item for item in adapter.fetch_category(Category.GPU, NOW)}

    assert listings["8296"].price == 1049.0  # "1.049", previously parsed as 1.049
    assert listings["8296"].regular_price is None  # "Antes" block is hidden
    assert listings["8409"].regular_price == 509.0  # visible "Antes"
    assert listings["3838"].in_stock is False  # .agotado badge
    assert listings["8409"].brand == "ASROCK"
    assert listings["8409"].url.startswith("https://shop.test/catalogo/")


def test_sme_missing_grid_on_first_page_raises():
    adapter = build_adapter(
        store(Platform.SME, {"gpu": ["/x/"]}),
        FakeClient(lambda url, params: FakeResponse(body=b"<html><body>nada</body></html>")),  # type: ignore[arg-type]
    )
    with pytest.raises(ScrapeError):
        list(adapter.fetch_category(Category.GPU, NOW))


def test_nopcommerce_paginates_until_empty_and_404_category_fails():
    page1 = (STORES / "nopcommerce_category.html").read_bytes()
    empty = b'<html><body><div class="product-grid"></div></body></html>'

    def routes(url, params):
        return FakeResponse(body=page1 if url.endswith("pagenumber=1") else empty)

    adapter = build_adapter(
        store(Platform.NOPCOMMERCE, {"gpu": ["/tarjetas-de-video"]}), FakeClient(routes)
    )  # type: ignore[arg-type]
    listings = list(adapter.fetch_category(Category.GPU, NOW))
    assert len(listings) == 6
    assert listings[-1].price == 1160.0  # "U$S 1.160"
    assert all(item.in_stock is None for item in listings)

    broken = build_adapter(
        store(Platform.NOPCOMMERCE, {"gpu": ["/tarjetas-de-video"]}),
        FakeClient(lambda url, params: FakeResponse(status=302)),  # type: ignore[arg-type]
    )
    with pytest.raises(ScrapeError):
        list(broken.fetch_category(Category.GPU, NOW))


@pytest.mark.parametrize(
    ("status", "expected"), [(404, True), (410, True), (200, False), (503, None)]
)
def test_listing_gone_reads_the_product_page_status(status, expected):
    client = FakeClient(lambda url, params: FakeResponse(status=status))
    adapter = build_adapter(store(Platform.WOOCOMMERCE, {"gpu": ["gpu"]}), client)  # type: ignore[arg-type]
    assert adapter.listing_gone("https://shop.test/product/x/") is expected
