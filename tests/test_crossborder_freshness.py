"""Stale-data check behind the monthly freshness workflow."""

from __future__ import annotations

from datetime import date, timedelta

from src.crossborder.config import load_config
from src.crossborder.freshness import checklist, stale_items


def test_nothing_is_stale_right_after_a_check():
    cfg = load_config()
    newest = max(c.verified_at for c in cfg.couriers.values())
    assert stale_items(cfg, newest) == []


def test_everything_is_listed_once_it_gets_old():
    cfg = load_config()
    later = date(2026, 10, 16) + timedelta(days=61)
    items = stale_items(cfg, later)
    whats = [item.what for item in items]
    assert "Tarifa de Gripper" in whats
    assert any(w.startswith("Reglas") for w in whats)
    assert "Registro de vendedores de EE. UU. (Aduanas)" in whats
    text = checklist(items, later)
    assert text.count("- [ ]") == len(items)
    assert "configs/couriers.yaml" in text
