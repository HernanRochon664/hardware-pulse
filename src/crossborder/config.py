"""
Typed loaders for the import calculator configuration.

- ``configs/import_rules.yaml``      Uruguayan rules (franchise, treaty exemption, 60%)
- ``configs/couriers.yaml``          courier tariffs
- ``configs/us_stores.yaml``         US stores, DNA registry status and all-in routes
- ``configs/shipping_profiles.yaml`` estimated weight and box size per product family

``load_config()`` reads the four files once per process; tests pass their own paths.
"""

from __future__ import annotations

import re
import urllib.parse
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, ValidationError, model_validator

from src.settings import (
    COURIERS_PATH,
    IMPORT_RULES_PATH,
    SHIPPING_PROFILES_PATH,
    US_STORES_PATH,
)

# ---------------------------------------------------------------------------
# Rules
# ---------------------------------------------------------------------------


class Source(BaseModel):
    label: str
    url: str


class FranchiseRules(BaseModel):
    annual_usd_cap: float = Field(gt=0)
    max_shipments: int = Field(gt=0)
    max_invoice_usd: float = Field(gt=0)
    max_weight_kg: float = Field(gt=0)
    iva_rate: float = Field(ge=0, lt=1)
    iva_min_usd: float = Field(ge=0)


class TreatyRules(BaseModel):
    countries: list[str]
    max_invoice_usd: float = Field(gt=0)
    registry_required_from: date


class SingleTaxRules(BaseModel):
    rate: float = Field(gt=0, lt=1)
    min_usd: float = Field(ge=0)
    max_invoice_usd: float = Field(gt=0)
    max_weight_kg: float = Field(gt=0)


class RuleSet(BaseModel):
    id: str
    name: str
    valid_from: date
    verified_at: date
    franchise: FranchiseRules
    treaty: TreatyRules
    single_tax: SingleTaxRules
    sources: dict[str, Source] = {}

    def source_url(self, key: str) -> str | None:
        source = self.sources.get(key)
        return source.url if source else None


class ImportRules(BaseModel):
    rule_sets: list[RuleSet] = Field(min_length=1)

    @model_validator(mode="after")
    def _sort(self) -> ImportRules:
        self.rule_sets.sort(key=lambda r: r.valid_from)
        return self

    def for_date(self, day: date) -> RuleSet:
        """Rule set in force for a shipment released by customs on ``day``."""
        applicable = [r for r in self.rule_sets if r.valid_from <= day]
        if not applicable:
            raise ValueError(f"No import rules in force on {day.isoformat()}")
        return applicable[-1]


# ---------------------------------------------------------------------------
# Couriers
# ---------------------------------------------------------------------------


class WeightBand(BaseModel):
    up_to_kg: float = Field(gt=0)
    flat_usd: float | None = Field(default=None, gt=0)
    per_kg_usd: float | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def _one_price(self) -> WeightBand:
        if (self.flat_usd is None) == (self.per_kg_usd is None):
            raise ValueError("a weight band needs exactly one of flat_usd / per_kg_usd")
        return self


class Volumetric(BaseModel):
    divisor_cm3_per_kg: float = Field(gt=0)
    divisor_published: bool = True
    # Volume is free up to this multiple of the real weight...
    free_ratio: float = Field(default=1.0, ge=1)
    # ...and above it each extra kg of volume costs this. None: the chargeable
    # weight is simply max(real, volumetric).
    surcharge_per_kg_usd: float | None = Field(default=None, gt=0)


class Insurance(BaseModel):
    rate: float = Field(gt=0, lt=1)
    min_usd: float = Field(ge=0)


class Delivery(BaseModel):
    montevideo_usd: float | None = Field(default=None, ge=0)
    interior_usd: float | None = Field(default=None, ge=0)
    interior_per_extra_kg_usd: float = Field(default=0.0, ge=0)
    iva: bool = False


class Courier(BaseModel):
    id: str
    name: str
    site: str
    source_url: str
    source_kind: Literal["official", "third_party"]
    verified_at: date
    warehouse: str
    sales_tax_rate: float = Field(ge=0, lt=0.2)
    tax_free_certificate: bool = False
    transit_days: tuple[int, int]
    weight_step_kg: float = Field(gt=0)
    weight_step_published: bool = True
    bands: list[WeightBand] = Field(min_length=1)
    handling_usd: float = Field(ge=0)
    handling_iva: bool = False
    tfspu_rate: float = Field(ge=0, lt=1)
    tfspu_on_handling: bool = True
    volumetric: Volumetric | None = None
    insurance: Insurance | None = None
    delivery: Delivery | None = None
    notes: list[str] = []

    @model_validator(mode="after")
    def _check_bands(self) -> Courier:
        limits = [band.up_to_kg for band in self.bands]
        if limits != sorted(set(limits)):
            raise ValueError(f"{self.id}: weight bands must be sorted by up_to_kg")
        kinds = [band.flat_usd is not None for band in self.bands]
        if kinds != sorted(kinds, reverse=True):
            raise ValueError(f"{self.id}: flat bands must come before per-kg bands")
        if self.transit_days[0] > self.transit_days[1]:
            raise ValueError(f"{self.id}: transit_days must be [min, max]")
        return self

    @property
    def max_kg(self) -> float:
        return self.bands[-1].up_to_kg


# ---------------------------------------------------------------------------
# US stores and all-in routes
# ---------------------------------------------------------------------------


class RegistryStatus(StrEnum):
    LISTED = "listed"
    NOT_LISTED = "not_listed"
    UNCERTAIN = "uncertain"


class UsStore(BaseModel):
    id: str
    name: str
    registry_status: RegistryStatus
    registry_entry: str | None = None
    sales_tax_exemption_program: bool = False
    search_url: str | None = None
    hosts: list[str] = []
    note: str | None = None

    def search_link(self, query: str) -> str | None:
        if not self.search_url or not query.strip():
            return None
        return self.search_url.format(query=urllib.parse.quote_plus(query.strip()))


class AllInRoute(BaseModel):
    id: str
    name: str
    site: str
    note: str


class Registry(BaseModel):
    source_url: str
    checked_at: date


class UsStoresConfig(BaseModel):
    registry: Registry
    stores: dict[str, UsStore]
    all_in: dict[str, AllInRoute] = {}

    @model_validator(mode="before")
    @classmethod
    def _inject_ids(cls, data: Any) -> Any:
        return _inject_ids(data, "stores", "all_in")

    def store_for_host(self, host: str) -> UsStore | None:
        host = host.lower().removeprefix("www.")
        for store in self.stores.values():
            if any(host == h or host.endswith("." + h) for h in store.hosts):
                return store
        return None


# ---------------------------------------------------------------------------
# Shipping profiles
# ---------------------------------------------------------------------------


class PackageProfile(BaseModel):
    id: str
    label: str
    weight_kg: float = Field(gt=0)
    dims_cm: tuple[float, float, float]


class ProfileRule(BaseModel):
    pattern: str
    profile: str


class ShippingProfiles(BaseModel):
    profiles: dict[str, PackageProfile]
    rules: list[ProfileRule]
    category_defaults: dict[str, str] = {}

    @model_validator(mode="before")
    @classmethod
    def _inject_ids(cls, data: Any) -> Any:
        return _inject_ids(data, "profiles")

    @model_validator(mode="after")
    def _check_references(self) -> ShippingProfiles:
        if "generic" not in self.profiles:
            raise ValueError("shipping profiles need a 'generic' profile")
        referenced = [r.profile for r in self.rules] + list(self.category_defaults.values())
        unknown = sorted(set(referenced) - set(self.profiles))
        if unknown:
            raise ValueError(f"unknown shipping profiles: {', '.join(unknown)}")
        for rule in self.rules:
            re.compile(rule.pattern)
        return self

    def for_product(self, family_id: str | None, category: str | None) -> PackageProfile:
        """Profile of the first rule matching the family, else the category default."""
        if family_id:
            for rule in self.rules:
                if re.search(rule.pattern, family_id):
                    return self.profiles[rule.profile]
        if category and category in self.category_defaults:
            return self.profiles[self.category_defaults[category]]
        return self.profiles["generic"]


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CrossBorderConfig:
    rules: ImportRules
    couriers: dict[str, Courier]
    stores: UsStoresConfig
    profiles: ShippingProfiles


def _inject_ids(data: Any, *sections: str) -> Any:
    """Copy each mapping key into its value as ``id`` (like src/config.py)."""
    if isinstance(data, dict):
        for section in sections:
            entries = data.get(section)
            if isinstance(entries, dict):
                data[section] = {
                    key: {**value, "id": key} if isinstance(value, dict) else value
                    for key, value in entries.items()
                }
    return data


def _read_yaml(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"Expected a mapping at the root of {path}")
    return raw


def _validate(model: type[BaseModel], path: Path) -> Any:
    try:
        return model.model_validate(_read_yaml(path))
    except ValidationError as exc:
        raise ValueError(f"Invalid configuration in {path}:\n{exc}") from exc


def load_rules(path: Path | None = None) -> ImportRules:
    return _validate(ImportRules, path or IMPORT_RULES_PATH)


def load_couriers(path: Path | None = None) -> dict[str, Courier]:
    path = path or COURIERS_PATH
    raw = _inject_ids(_read_yaml(path), "couriers")
    try:
        return {key: Courier.model_validate(value) for key, value in raw["couriers"].items()}
    except (ValidationError, KeyError, TypeError) as exc:
        raise ValueError(f"Invalid courier configuration in {path}:\n{exc}") from exc


def load_us_stores(path: Path | None = None) -> UsStoresConfig:
    return _validate(UsStoresConfig, path or US_STORES_PATH)


def load_profiles(path: Path | None = None) -> ShippingProfiles:
    return _validate(ShippingProfiles, path or SHIPPING_PROFILES_PATH)


@lru_cache(maxsize=1)
def load_config() -> CrossBorderConfig:
    return CrossBorderConfig(
        rules=load_rules(),
        couriers=load_couriers(),
        stores=load_us_stores(),
        profiles=load_profiles(),
    )
