"""
Domain types shared across scraping, resolution, storage and the dashboard.
"""

from __future__ import annotations

import html
import re
from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, Field, field_validator, model_validator


class Category(StrEnum):
    GPU = "gpu"
    CPU = "cpu"
    RAM = "ram"
    SSD = "ssd"

    @property
    def label(self) -> str:
        return CATEGORY_LABELS[self]


CATEGORY_LABELS = {
    Category.GPU: "Tarjetas de video",
    Category.CPU: "Procesadores",
    Category.RAM: "Memorias RAM",
    Category.SSD: "Discos SSD",
}


class Condition(StrEnum):
    NEW = "new"
    OPEN_BOX = "open_box"
    USED = "used"
    REFURBISHED = "refurbished"


class Currency(StrEnum):
    USD = "USD"
    UYU = "UYU"


class Platform(StrEnum):
    WOOCOMMERCE = "woocommerce"
    SME = "sme"
    NOPCOMMERCE = "nopcommerce"


_WS = re.compile(r"\s+")


def clean_text(value: str) -> str:
    """Unescape HTML entities and collapse whitespace."""
    return _WS.sub(" ", html.unescape(value)).strip()


class ScrapedListing(BaseModel):
    """One product card/offer as published by a store, with light normalization only."""

    store_id: str
    category: Category
    url: str
    title: str
    price: float = Field(gt=0)
    regular_price: float | None = Field(default=None, gt=0)
    currency: Currency
    in_stock: bool | None = None
    stock_qty: int | None = Field(default=None, ge=0)
    store_sku: str | None = None
    brand: str | None = None
    image_url: str | None = None
    scraped_at: datetime

    @field_validator("title")
    @classmethod
    def _clean_title(cls, value: str) -> str:
        value = clean_text(value)
        if not value:
            raise ValueError("title must not be empty")
        return value

    @field_validator("brand", "store_sku")
    @classmethod
    def _clean_optional(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = clean_text(value)
        return value or None

    @model_validator(mode="after")
    def _drop_meaningless_regular_price(self) -> ScrapedListing:
        # A "regular" price only matters when it is above the selling price.
        if self.regular_price is not None and self.regular_price <= self.price:
            self.regular_price = None
        return self
