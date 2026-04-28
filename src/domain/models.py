from datetime import datetime
from enum import Enum

from pydantic import BaseModel, field_validator


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class Currency(str, Enum):
    UYU = "UYU"
    USD = "USD"


class Condition(str, Enum):
    NEW = "new"
    USED = "used"


class Source(str, Enum):
    THOT = "thot"
    BANIFOX = "banifox"
    PCCOMPU = "pccompu"


# ---------------------------------------------------------------------------
# Raw listing
# Represents a listing exactly as collected from the source.
# No enrichment, no inference. Only light type coercion.
# ---------------------------------------------------------------------------


class RawListing(BaseModel):
    # --- Identity ---
    source: Source
    url: str
    timestamp: datetime

    # --- Core fields (always present) ---
    title: str
    price: float
    currency: Currency
    seller: str

    # --- Optional fields ---
    item_id: str | None = None
    condition: Condition | None = None
    available_quantity: int | None = None
    base_price: float | None = None

    @field_validator("title")
    @classmethod
    def title_must_not_be_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("title must not be empty")
        return v.strip()

    @field_validator("price", "base_price")
    @classmethod
    def price_must_be_positive(cls, v: float | None) -> float | None:
        if v is not None and v <= 0:
            raise ValueError("price must be positive")
        return v


# ---------------------------------------------------------------------------
# Resolved listing
# Represents a listing that has been matched to a canonical product.
# Used by the entity resolution pipeline before price snapshot creation.
# ---------------------------------------------------------------------------


class ResolvedListing(BaseModel):
    # --- Traceability ---
    source: Source
    url: str
    timestamp: datetime

    # --- Core fields ---
    title: str
    price: float
    currency: Currency
    seller: str

    # --- Optional raw traceability ---
    item_id: str | None = None
    condition: Condition | None = None
    available_quantity: int | None = None
    base_price: float | None = None

    # --- Resolution result ---
    canonical_product_id: str | None = None
    confidence_score: float = 0.0
    matched_by: str | None = None

    # --- Enrichment ---
    brand: str | None = None
    variant: str | None = None


# ---------------------------------------------------------------------------
# Price snapshot
# Represents a normalized, enriched record ready for storage and analysis.
# Produced by the pipeline after entity resolution and currency conversion.
# ---------------------------------------------------------------------------


class PriceSnapshot(BaseModel):
    # --- Traceability ---
    source: Source
    listing_id: str
    timestamp: datetime

    # --- Resolved identity ---
    canonical_product_id: str  # e.g. "RTX 4070"
    seller: str

    # --- Normalized price ---
    price: float
    currency: Currency
    price_usd: float  # always present after normalization

    # --- Optional context ---
    availability: int | None = None
    condition: Condition | None = None
