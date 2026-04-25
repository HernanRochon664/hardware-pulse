"""
Shared pytest fixtures for hardware-pulse tests.
"""

from datetime import datetime
from pathlib import Path

import pytest

from src.domain.models import Condition, Currency, RawListing, Source
from src.storage.schema import init_db


# ---------------------------------------------------------------------------
# Database fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def db_conn():
    """
    In-memory SQLite connection for tests.

    Uses :memory: so each test gets a clean database with no
    state leaking between runs. Connection is closed after the test.
    """
    conn = init_db(Path(":memory:"))
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Domain fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_listing() -> RawListing:
    """A valid RawListing for use across multiple test modules."""
    return RawListing(
        source=Source.THOT,
        url="https://thotcomputacion.com.uy/producto/rtx-4070",
        timestamp=datetime.fromisoformat("2026-04-25T12:00:00+00:00"),
        title="GPU ASUS TUF RTX 4070 OC 12GB",
        price=750.0,
        currency=Currency.USD,
        seller="thot",
        condition=Condition.NEW,
    )


@pytest.fixture
def sample_listing_with_item_id() -> RawListing:
    """A valid RawListing with item_id for deduplication tests."""
    return RawListing(
        source=Source.THOT,
        url="https://thotcomputacion.com.uy/producto/MLU123456",
        timestamp=datetime.fromisoformat("2026-04-25T12:00:00+00:00"),
        title="RTX 4070 ASUS TUF",
        price=780.0,
        currency=Currency.USD,
        seller="thot",
        item_id="MLU123456",
        condition=Condition.NEW,
    )