"""Bookmarklet intake: parsing US page texts and matching them to the catalog."""

from __future__ import annotations

import pytest

from src.crossborder.config import load_config
from src.crossborder.intake import (
    CatalogProduct,
    match_catalog,
    parse_dims_cm,
    parse_intake,
    parse_money,
    parse_weight_kg,
    sold_by_amazon,
)
from src.domain.models import Category
from src.entities.resolver import resolve_title


@pytest.mark.parametrize(
    ("text", "value", "currency"),
    [
        ("$1,099.99", 1099.99, "USD"),
        ("US$199.99", 199.99, "USD"),
        ("$1,099", 1099.0, "USD"),
        ("$45.50", 45.5, "USD"),
        ("1.099,99 USD", 1099.99, "USD"),
        ("UYU 12.345", 12345.0, "UYU"),
        ("€ 89,90", 89.9, "EUR"),
        ("Consultar", None, None),
        (None, None, None),
    ],
)
def test_parse_money(text, value, currency):
    assert parse_money(text) == (value, currency)


@pytest.mark.parametrize(
    ("text", "kg"),
    [
        ("2.43 Pounds", 1.102),
        ("1.1 Kilograms", 1.1),
        ("500 grams", 0.5),
        ("39 ounces", 1.106),
        ("0.125 kg", 0.125),
        ("2,43 libras", 1.102),
        ("13.5 x 9.84 x 3.66 inches; 2.43 Pounds", 1.102),
        ("sin dato", None),
        (None, None),
    ],
)
def test_parse_weight(text, kg):
    assert parse_weight_kg(text) == kg


@pytest.mark.parametrize(
    ("text", "dims"),
    [
        ("13.5 x 9.84 x 3.66 inches", (34.3, 25.0, 9.3)),
        ("34,3 x 25 x 9,3 cm", (34.3, 25.0, 9.3)),
        ("350 x 250 x 90 mm", (35.0, 25.0, 9.0)),
        ("12 x 10 x 3", (30.5, 25.4, 7.6)),  # Amazon US without unit: inches
        ("12 x 10", None),
    ],
)
def test_parse_dims(text, dims):
    assert parse_dims_cm(text) == dims


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("Amazon.com", True),
        ("Sold by Amazon.com Services LLC", True),
        ("Vendido por: Amazon.com", True),
        ("Amazon.com, Inc.", True),
        ("TechDeals US", False),
        ("AmazonBasics Store", False),
        ("", None),
        (None, None),
    ],
)
def test_sold_by_amazon(text, expected):
    assert sold_by_amazon(text) is expected


def test_parse_intake_from_an_amazon_third_party_offer():
    intake = parse_intake(
        {
            "t": "ASUS TUF Gaming GeForce RTX 5070 Ti 16GB GDDR7 OC Edition",
            "p": "$749.99",
            "u": "https://www.amazon.com/dp/B0DS6S98ZG",
            "sold": "Shop XYZ",
            "w": "4.85 pounds",
            "d": "14.37 x 9.06 x 3.54 inches",
            "mpn": "TUF-RTX5070TI-O16G-GAMING",
        },
        load_config(),
    )
    assert intake is not None
    assert intake.store_id == "amazon_marketplace"
    assert intake.price_usd == 749.99
    assert intake.weight_kg == 2.2
    assert intake.dims_cm == (36.5, 23.0, 9.0)
    assert intake.mpn == "TUF-RTX5070TI-O16G-GAMING"
    assert intake.warnings == []


def test_parse_intake_warnings():
    cfg = load_config()
    unknown_seller = parse_intake(
        {"t": "Samsung 990 PRO 2TB", "p": "$169.99", "u": "https://www.amazon.com/dp/X"}, cfg
    )
    assert unknown_seller is not None and unknown_seller.store_id == "amazon"
    assert "quién vende" in unknown_seller.warnings[0]
    pesos = parse_intake({"t": "Samsung 990 PRO 2TB", "p": "UYU 6.990"}, cfg)
    assert pesos is not None and pesos.price_usd is None and pesos.store_id == "other"
    assert parse_intake({}, cfg) is None


PRODUCTS = [
    CatalogProduct(
        1,
        "ASUS GeForce RTX 5070 Ti 16GB TUF OC",
        "gpu",
        "gpu:rtx-5070-ti-16gb",
        "gpu|asus|rtx 5070 ti|16|tuf|oc",
        None,
    ),
    CatalogProduct(
        2,
        "MSI GeForce RTX 5070 Ti 16GB Ventus 3X OC",
        "gpu",
        "gpu:rtx-5070-ti-16gb",
        "gpu|msi|rtx 5070 ti|16|ventus 3x|oc",
        None,
    ),
    CatalogProduct(
        3,
        "Kingston Fury Beast DDR5 32GB 5600 MT/s",
        "ram",
        "ram:ddr5-32gb-5600-dimm",
        "ram|kingston|fury beast|ddr5|32|2x16GB|5600|dimm||",
        "KF556C40BBK232",
    ),
    CatalogProduct(
        4, "AMD Ryzen 7 7800X3D", "cpu", "cpu:amd-ryzen-7-7800x3d", "cpu:amd-ryzen-7-7800x3d", None
    ),
]


def test_match_by_mpn():
    match = match_catalog("Some unparseable title", "KF556C40BBK2-32", PRODUCTS)
    assert match is not None and match.kind == "mpn" and match.product_ids == (3,)


def test_match_exact_product_from_an_english_title():
    title = "ASUS TUF Gaming GeForce RTX 5070 Ti 16GB GDDR7 OC Edition Gaming Graphics Card"
    match = match_catalog(title, None, PRODUCTS)
    assert match is not None and match.kind == "exact" and match.product_ids == (1,)
    cpu = match_catalog("AMD Ryzen 7 7800X3D 8-Core, 16-Thread Desktop Processor", None, PRODUCTS)
    assert cpu is not None and cpu.kind == "exact" and cpu.product_ids == (4,)


def test_other_brand_of_the_same_chip_is_only_an_equivalent():
    title = "GIGABYTE GeForce RTX 5070 Ti Gaming OC 16G Graphics Card"
    match = match_catalog(title, None, PRODUCTS)
    assert match is not None
    assert match.kind == "family"
    assert not match.is_exact
    assert set(match.product_ids) == {1, 2}


def test_unknown_product_is_not_matched():
    assert match_catalog("Logitech G502 Hero Gaming Mouse", None, PRODUCTS) is None


def _catalog_product(pid: int, category: str, store_title: str) -> CatalogProduct:
    parsed = resolve_title(Category(category), store_title).parsed
    assert parsed is not None and parsed.fingerprint, store_title
    return CatalogProduct(pid, parsed.name, category, parsed.family_id, parsed.fingerprint, None)


@pytest.mark.parametrize(
    ("category", "store_title", "us_title"),
    [
        (
            "motherboard",
            "Motherboard ASUS TUF Gaming B650M-E WiFi DDR5 AM5",
            "ASUS TUF Gaming B650M-E WiFi AM5 Micro-ATX Motherboard, DDR5, PCIe 4.0",
        ),
        (
            "monitor",
            "Monitor ASUS TUF Gaming VG34VQL3A 34″ UWQHD 180Hz VA 1ms curvo",
            'ASUS TUF Gaming 34" Ultrawide WQHD (3440x1440) Curved Monitor (VG34VQL3A) 180Hz',
        ),
        (
            "cooler",
            "Water Cooling DeepCool LE360 V2 360mm ARGB",
            "DeepCool LE360 V2 360mm AIO CPU Liquid Cooler, ARGB, Intel/AMD",
        ),
        (
            "psu",
            "Fuente Corsair RM850e 850W 80 Plus Gold Modular",
            "CORSAIR RM850e Fully Modular Low-Noise ATX Power Supply - 80 PLUS Gold - 850 Watt",
        ),
        (
            "case",
            "Gabinete Cougar Fv270 Rgb Black",
            "COUGAR FV270 RGB Black Mid-Tower Case with Tempered Glass",
        ),
    ],
)
def test_us_titles_of_the_new_categories_match_the_local_product(category, store_title, us_title):
    products = [*PRODUCTS, _catalog_product(99, category, store_title)]
    match = match_catalog(us_title, None, products)
    assert match is not None and match.kind == "exact" and match.product_ids == (99,), match
