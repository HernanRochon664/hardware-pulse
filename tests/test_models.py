"""
Tests for src/domain/models.py

Covers Pydantic validation rules for RawListing and PriceSnapshot.
No mocks needed, pure unit tests against domain logic.
"""

import pytest
from pydantic import ValidationError

from src.domain.models import Condition, Currency, RawListing, Source


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_listing(**overrides) -> RawListing:
    """Build a valid RawListing with optional field overrides."""
    defaults = {
        "source": Source.THOT,
        "url": "https://thotcomputacion.com.uy/producto/rtx-4070",
        "timestamp": "2026-04-25T12:00:00+00:00",
        "title": "GPU ASUS TUF RTX 4070 OC 12GB",
        "price": 750.0,
        "currency": Currency.USD,
        "seller": "thot",
    }
    return RawListing(**{**defaults, **overrides})


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestRawListingValidInstantiation:
    def test_valid_listing_creates_successfully(self):
        listing = make_listing()
        assert listing.title == "GPU ASUS TUF RTX 4070 OC 12GB"
        assert listing.price == 750.0
        assert listing.currency == Currency.USD

    def test_optional_fields_default_to_none(self):
        listing = make_listing()
        assert listing.item_id is None
        assert listing.condition is None
        assert listing.available_quantity is None
        assert listing.base_price is None

    def test_optional_fields_accept_values(self):
        listing = make_listing(
            item_id="MLU123456",
            condition=Condition.NEW,
            available_quantity=5,
            base_price=800.0,
        )
        assert listing.item_id == "MLU123456"
        assert listing.condition == Condition.NEW
        assert listing.available_quantity == 5
        assert listing.base_price == 800.0

    def test_title_is_stripped(self):
        listing = make_listing(title="  RTX 4070  ")
        assert listing.title == "RTX 4070"

    def test_uyu_currency_accepted(self):
        listing = make_listing(currency=Currency.UYU, price=30000.0)
        assert listing.currency == Currency.UYU


# ---------------------------------------------------------------------------
# Title validation
# ---------------------------------------------------------------------------


class TestTitleValidation:
    def test_empty_title_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            make_listing(title="")
        assert "title" in str(exc_info.value)

    def test_whitespace_only_title_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            make_listing(title="   ")
        assert "title" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Price validation
# ---------------------------------------------------------------------------


class TestPriceValidation:
    def test_zero_price_raises(self):
        with pytest.raises(ValidationError):
            make_listing(price=0.0)

    def test_negative_price_raises(self):
        with pytest.raises(ValidationError):
            make_listing(price=-100.0)

    def test_negative_base_price_raises(self):
        with pytest.raises(ValidationError):
            make_listing(base_price=-50.0)

    def test_none_base_price_accepted(self):
        listing = make_listing(base_price=None)
        assert listing.base_price is None

    def test_valid_base_price_accepted(self):
        listing = make_listing(base_price=800.0)
        assert listing.base_price == 800.0


# ---------------------------------------------------------------------------
# Enum validation
# ---------------------------------------------------------------------------


class TestEnumValidation:
    def test_invalid_currency_raises(self):
        with pytest.raises(ValidationError):
            make_listing(currency="INVALID")

    def test_invalid_source_raises(self):
        with pytest.raises(ValidationError):
            make_listing(source="invalid_source")

    def test_invalid_condition_raises(self):
        with pytest.raises(ValidationError):
            make_listing(condition="broken")

    def test_all_sources_accepted(self):
        for source in Source:
            listing = make_listing(source=source)
            assert listing.source == source

    def test_all_currencies_accepted(self):
        for currency in Currency:
            listing = make_listing(currency=currency)
            assert listing.currency == currency