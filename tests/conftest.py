"""Shared pytest fixtures."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from src.domain.models import Category, Currency, ScrapedListing
from src.storage import repository
from src.storage.db import connect

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def db_conn():
    """Fresh, fully migrated in-memory database with two stores."""
    conn = connect(":memory:")
    repository.sync_stores(
        conn,
        [
            ("thot", "Thot", "https://thotcomputacion.com.uy", "woocommerce"),
            ("banifox", "Banifox", "https://www.banifox.com", "nopcommerce"),
        ],
    )
    yield conn
    conn.close()


def make_listing(
    *,
    url: str = "https://thotcomputacion.com.uy/producto/rtx-5070/",
    title: str = "GPU MSI GeForce RTX 5070 Shadow 2X OC 12GB",
    price: float = 1050.0,
    regular_price: float | None = None,
    in_stock: bool | None = True,
    store_id: str = "thot",
    category: Category = Category.GPU,
    at: datetime | None = None,
) -> ScrapedListing:
    return ScrapedListing(
        store_id=store_id,
        category=category,
        url=url,
        title=title,
        price=price,
        regular_price=regular_price,
        currency=Currency.USD,
        in_stock=in_stock,
        scraped_at=at or datetime(2026, 10, 14, 12, 0, tzinfo=UTC),
    )
