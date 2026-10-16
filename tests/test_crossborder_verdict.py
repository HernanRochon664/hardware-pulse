"""Verdict, alerts and risks of the import calculator."""

from __future__ import annotations

from dataclasses import replace
from datetime import date

import pytest

from src.crossborder.config import CrossBorderConfig, load_config
from src.crossborder.engine import ForeignItem, FranchisePool, Settings, compare
from src.crossborder.verdict import (
    Threshold,
    VerdictCode,
    alerts,
    judge,
    risks,
)
from tests.test_crossborder_engine import GRIP

TODAY = date(2026, 10, 16)


@pytest.fixture(scope="module")
def cfg() -> CrossBorderConfig:
    real = load_config()
    return CrossBorderConfig(real.rules, {"grip": GRIP}, real.stores, real.profiles)


@pytest.mark.parametrize(
    ("local", "imported", "code"),
    [
        (500.0, 400.0, VerdictCode.IMPORT),  # saves 100, threshold 50 (10%)
        (500.0, 470.0, VerdictCode.SMALL_GAP),  # saves 30 < 50
        (100.0, 80.0, VerdictCode.SMALL_GAP),  # saves 20 < USD 25 minimum
        (500.0, 520.0, VerdictCode.BUY_LOCAL),
        (None, 300.0, VerdictCode.ONLY_ABROAD),
    ],
)
def test_judge(local, imported, code):
    verdict = judge(local, imported, Threshold(min_usd=25, min_pct=0.10))
    assert verdict.code is code


def test_judge_without_an_import_quote():
    verdict = judge(500.0, None, Threshold(), ["el envío vale U$S 900: pasa los U$S 800 por envío"])
    assert verdict.code is VerdictCode.NOT_BY_COURIER
    assert verdict.detail.startswith("El envío vale U$S 900")  # 'U$S' not lowercased
    assert judge(500.0, None, Threshold()).code is VerdictCode.NO_PRICES


def _alerts(cfg, item, *, arrival=date(2026, 11, 20), pool=None, **changes):
    settings = replace(Settings(arrival=arrival, courier_id="grip"), **changes)
    pool = pool or FranchisePool()
    comparison = compare([item], settings, cfg, pool)
    assert comparison.best is not None
    return [
        a.text for a in alerts(comparison.best, comparison, settings, cfg, today=TODAY, pool=pool)
    ]


def test_alert_when_sales_tax_pushes_over_200(cfg):
    texts = _alerts(cfg, ForeignItem("RAM", 190, unit_weight_kg=0.18))
    first = texts[0]
    assert "te pasás por U$S 3,30" in first
    assert "hasta U$S 186,92" in first


def test_alert_for_a_third_party_seller_shows_the_saving(cfg):
    texts = _alerts(
        cfg, ForeignItem("SSD", 100, store_id="amazon_marketplace", unit_weight_kg=0.08)
    )
    assert "registro de Aduanas" in texts[0]
    assert "ahorrarías U$S 23,54" in texts[0]  # the IVA of a 107.00 invoice


def test_alert_before_the_registry_deadline(cfg):
    texts = _alerts(
        cfg,
        ForeignItem("SSD", 100, store_id="other", unit_weight_kg=0.08),
        arrival=date(2026, 10, 30),
    )
    assert any("02/11/2026" in t for t in texts)


def test_alerts_for_estimates_franchise_use_and_new_year(cfg):
    item = ForeignItem("GPU", 300, unit_weight_kg=1.4, weight_estimated=True)
    texts = _alerts(cfg, item, arrival=date(2027, 1, 4))
    assert any("estimados" in t for t in texts)
    assert any("quedarían 2 envíos" in t for t in texts)
    assert any("Llegaría en 2027" in t for t in texts)


def test_risks_are_listed_apart():
    titles = [risk.title for risk in risks(None)]
    assert titles == ["Plazo", "Garantía", "Devoluciones", "Pérdida o daño", "Requisitos"]
