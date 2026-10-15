"""Price changes and historic lows compare the same stores over time."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

import src.pipelines.scrape as scrape_module
from src.pipelines.aggregate import aggregate
from src.pipelines.resolve import resolve_listings
from src.pipelines.scrape import scrape_stores
from src.storage.db import connect
from tests.test_pipeline import EAGLE, EAGLE_B, STORE_ITEMS, FakeAdapter, _config, _stats


@pytest.fixture
def conn(monkeypatch):
    monkeypatch.setattr(scrape_module, "ensure_fx_rate", lambda conn, day, now: 40.0)
    STORE_ITEMS.clear()
    db = connect(":memory:")
    yield db
    db.close()


def _scrape_on(conn, monkeypatch, day: int, items: dict[str, list]) -> None:
    at = datetime(2026, 10, day, 15, 0, tzinfo=UTC)
    monkeypatch.setattr(scrape_module, "utcnow", lambda: at)
    STORE_ITEMS.clear()
    STORE_ITEMS.update(items)
    scrape_stores(conn, _config(), run_id=f"d{day}", adapter_factory=FakeAdapter)


def _finish(conn, day: int) -> None:
    resolve_listings(conn)
    aggregate(conn, now=datetime(2026, 10, day, 16, 0, tzinfo=UTC))


def test_cheaper_store_joining_is_not_a_price_drop(conn, monkeypatch):
    _scrape_on(conn, monkeypatch, 1, {"a": [("eagle", EAGLE, 600.0, True)], "b": [], "c": []})
    _scrape_on(
        conn,
        monkeypatch,
        8,
        {"a": [("eagle", EAGLE, 600.0, True)], "b": [("eagle", EAGLE_B, 540.0, True)], "c": []},
    )
    _finish(conn, 8)
    stats = _stats(conn, "Eagle OC")
    assert stats["best_price_usd"] == 540.0  # the new store does set today's best price
    assert stats["change_7d"] == 0.0  # but store a did not change its price


def test_store_leaving_is_not_a_price_rise(conn, monkeypatch):
    both = {"a": [("eagle", EAGLE, 600.0, True)], "b": [("eagle", EAGLE_B, 540.0, True)], "c": []}
    _scrape_on(conn, monkeypatch, 1, both)
    _scrape_on(conn, monkeypatch, 8, {**both, "b": [("other", "GPU MSI RTX 5070 12GB", 1.0, True)]})
    _finish(conn, 8)
    stats = _stats(conn, "Eagle OC")
    assert stats["best_price_usd"] == 600.0
    assert stats["change_7d"] == 0.0


def test_same_store_drop_is_still_reported(conn, monkeypatch):
    _scrape_on(conn, monkeypatch, 1, {"a": [("eagle", EAGLE, 600.0, True)], "b": [], "c": []})
    _scrape_on(conn, monkeypatch, 8, {"a": [("eagle", EAGLE, 540.0, True)], "b": [], "c": []})
    _finish(conn, 8)
    assert _stats(conn, "Eagle OC")["change_7d"] == pytest.approx(-0.10)


def _history_then(conn, monkeypatch, b_from_start: bool) -> None:
    a_only = {"a": [("eagle", EAGLE, 620.0, True)], "b": [], "c": []}
    b_early = [("eagle", EAGLE_B, 590.0, True)] if b_from_start else []
    _scrape_on(conn, monkeypatch, 1, a_only)
    _scrape_on(conn, monkeypatch, 2, {**a_only, "a": [("eagle", EAGLE, 600.0, True)], "b": b_early})
    _scrape_on(
        conn,
        monkeypatch,
        20,
        {"a": [("eagle", EAGLE, 600.0, True)], "b": [("eagle", EAGLE_B, 580.0, True)], "c": []},
    )
    _finish(conn, 20)


def test_new_store_does_not_create_a_historic_low(conn, monkeypatch):
    _history_then(conn, monkeypatch, b_from_start=False)
    assert _stats(conn, "Eagle OC")["is_historic_low"] == 0


def test_tracked_store_reaching_its_lowest_is_a_historic_low(conn, monkeypatch):
    _history_then(conn, monkeypatch, b_from_start=True)
    assert _stats(conn, "Eagle OC")["is_historic_low"] == 1
