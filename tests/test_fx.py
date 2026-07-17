"""Tests for src/pipelines/fx.py FX rate helpers."""

import pytest

from src.pipelines.fx import normalize_to_usd


def test_uyu_to_usd_conversion():
    price_usd = normalize_to_usd(40000.0, "UYU", 40.0)
    assert price_usd == pytest.approx(1000.0)


def test_usd_passthrough():
    price_usd = normalize_to_usd(100.0, "USD", None)
    assert price_usd == 100.0


def test_uyu_missing_rate_returns_none():
    price_usd = normalize_to_usd(40000.0, "UYU", None)
    assert price_usd is None


def test_unsupported_currency_raises():
    with pytest.raises(ValueError, match="Unsupported currency"):
        normalize_to_usd(100.0, "EUR", 1.0)
