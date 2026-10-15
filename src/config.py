"""
Typed loader for ``configs/stores.yaml``.

Store-level values override ``defaults``; callers use the resolved
properties (``delay``, ``timeout_s``, ``page_limit``) and never re-implement
the precedence rules.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, ValidationError, model_validator

from src.domain.models import Category, Platform
from src.settings import STORES_CONFIG_PATH


class StoreDefaults(BaseModel):
    request_delay: float = Field(default=1.5, ge=0)
    timeout: int = Field(default=30, gt=0)
    max_pages: int = Field(default=60, gt=0)


class StoreConfig(BaseModel):
    id: str
    name: str
    base_url: str
    platform: Platform
    enabled: bool = True
    tax_included: bool = True
    request_delay: float | None = Field(default=None, ge=0)
    timeout: int | None = Field(default=None, gt=0)
    max_pages: int | None = Field(default=None, gt=0)
    categories: dict[Category, list[str]] = Field(min_length=1)

    # Filled in by StoresConfig so the resolved values travel with the store.
    defaults: StoreDefaults = StoreDefaults()

    @model_validator(mode="after")
    def _strip_base_url(self) -> StoreConfig:
        self.base_url = self.base_url.rstrip("/")
        for category, sources in self.categories.items():
            if not sources:
                raise ValueError(f"{self.id}: category {category} has no sources")
        return self

    @property
    def delay(self) -> float:
        return self.request_delay if self.request_delay is not None else self.defaults.request_delay

    @property
    def timeout_s(self) -> int:
        return self.timeout or self.defaults.timeout

    @property
    def page_limit(self) -> int:
        return self.max_pages or self.defaults.max_pages


class StoresConfig(BaseModel):
    defaults: StoreDefaults = StoreDefaults()
    stores: dict[str, StoreConfig]

    @model_validator(mode="before")
    @classmethod
    def _inject_ids(cls, data: Any) -> Any:
        if isinstance(data, dict) and isinstance(data.get("stores"), dict):
            data["stores"] = {
                key: {**value, "id": key} if isinstance(value, dict) else value
                for key, value in data["stores"].items()
            }
        return data

    @model_validator(mode="after")
    def _propagate_defaults(self) -> StoresConfig:
        for store in self.stores.values():
            store.defaults = self.defaults
        return self

    def enabled(self, only: list[str] | None = None) -> list[StoreConfig]:
        stores = [s for s in self.stores.values() if s.enabled]
        if only:
            unknown = set(only) - set(self.stores)
            if unknown:
                raise ValueError(f"Unknown store(s): {', '.join(sorted(unknown))}")
            stores = [s for s in stores if s.id in only]
        return stores


def load_stores_config(path: Path | None = None) -> StoresConfig:
    path = path or STORES_CONFIG_PATH
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"Expected a mapping at the root of {path}")
    try:
        return StoresConfig.model_validate(raw)
    except ValidationError as exc:
        raise ValueError(f"Invalid store configuration in {path}:\n{exc}") from exc
