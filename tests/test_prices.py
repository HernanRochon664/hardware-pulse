import pytest

from src.domain.models import Currency
from src.scrapers.prices import detect_currency, parse_price


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("1.049", 1049.0),  # the old PCCompu bug read this as 1.049
        ("U$S 2.639", 2639.0),
        ("USD 1.049,50", 1049.5),
        ("490", 490.0),
        ("U$S 5.465", 5465.0),
        ("U$S 0", None),
        ("Consultar", None),
        ("", None),
    ],
)
def test_parse_price_uruguayan_format(text, expected):
    assert parse_price(text) == expected


def test_parse_price_english_format():
    assert parse_price("US$1,000.00", thousands=",", decimal=".") == 1000.0
    assert parse_price("US$ 44.99", thousands=",", decimal=".") == 44.99


def test_parse_price_rejects_ambiguous_grouping():
    with pytest.raises(ValueError):
        parse_price("1.04", thousands=".", decimal=",")


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("U$S 725", Currency.USD),
        ("USD", Currency.USD),
        ("US$", Currency.USD),
        ("$ 1.200", Currency.UYU),
    ],
)
def test_detect_currency(text, expected):
    assert detect_currency(text) == expected
