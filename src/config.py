"""
Configuration models for hardware-pulse.

Responsibilities:
- Parse and validate configs/scrapers.yaml
- Expose typed config objects to the pipeline
- Apply precedence rules: job > scraper.defaults > global

Does NOT:
- Instantiate scrapers (see scripts/run_ingest.py)
- Contain scraping logic
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, model_validator


# ---------------------------------------------------------------------------
# Global settings
# ---------------------------------------------------------------------------


class GlobalConfig(BaseModel):
    request_delay: float = Field(default=1.0, gt=0)
    timeout: int = Field(default=10, gt=0)


# ---------------------------------------------------------------------------
# Per-scraper defaults
# ---------------------------------------------------------------------------


class ScraperDefaults(BaseModel):
    request_delay: float | None = Field(default=None, gt=0)
    max_results: int | None = Field(default=None, gt=0)
    max_pages_per_url: int | None = Field(default=None, gt=0)


# ---------------------------------------------------------------------------
# Job definitions
# ---------------------------------------------------------------------------


class MercadoLibreJob(BaseModel):
    name: str
    queries: list[str] = Field(min_length=1)
    max_offsets_per_query: int | None = None
    request_delay: float | None = None

class HTMLScraperJob(BaseModel):
    """Shared job schema for Thot and Banifox (both URL-based)."""
    name: str
    urls: list[str] = Field(min_length=1)
    request_delay: float | None = Field(default=None, gt=0)
    max_pages_per_url: int | None = Field(default=None, gt=0)


# ---------------------------------------------------------------------------
# Per-scraper config blocks
# ---------------------------------------------------------------------------


class MercadoLibreConfig(BaseModel):
    enabled: bool = True
    defaults: ScraperDefaults = ScraperDefaults()
    jobs: list[MercadoLibreJob] = []


class ThotConfig(BaseModel):
    enabled: bool = True
    defaults: ScraperDefaults = ScraperDefaults()
    jobs: list[HTMLScraperJob] = []


class BanifoxConfig(BaseModel):
    enabled: bool = True
    defaults: ScraperDefaults = ScraperDefaults()
    jobs: list[HTMLScraperJob] = []


# ---------------------------------------------------------------------------
# Root config
# ---------------------------------------------------------------------------


class ScrapersConfig(BaseModel):
    """
    Root configuration object parsed from configs/scrapers.yaml.

    Precedence rules (documented here and in the YAML):
    - job-level values override scraper defaults
    - scraper defaults override global settings
    - global settings are the final fallback
    """

    global_: GlobalConfig = Field(default=GlobalConfig(), alias="global")
    mercadolibre: MercadoLibreConfig = MercadoLibreConfig()
    thot: ThotConfig = ThotConfig()
    banifox: BanifoxConfig = BanifoxConfig()

    model_config = {"populate_by_name": True}

    # Convenience resolution helpers, callers don't need to implement
    # the precedence logic themselves.

    def resolve_request_delay(
        self,
        scraper_defaults: ScraperDefaults,
        job_override: float | None = None,
    ) -> float:
        """Resolve request_delay following job > defaults > global precedence."""
        return (
            job_override
            or scraper_defaults.request_delay
            or self.global_.request_delay
        )

    def resolve_max_results(
        self,
        scraper_defaults: ScraperDefaults,
        job_override: int | None = None,
    ) -> int:
        """Resolve max_results following job > defaults > global precedence."""
        return (
            job_override
            or scraper_defaults.max_results
            or 200  # fallback default
        )

    def resolve_max_pages(
        self,
        scraper_defaults: ScraperDefaults,
        job_override: int | None = None,
    ) -> int:
        """Resolve max_pages_per_url following job > defaults > global precedence."""
        return (
            job_override
            or scraper_defaults.max_pages_per_url
            or 20  # fallback default
        )


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------


def load_config(path: Path | None = None) -> ScrapersConfig:
    """
    Load and validate scrapers.yaml.

    Args:
        path: Path to the YAML file. Defaults to configs/scrapers.yaml
            relative to the project root.

    Returns:
        Validated ScrapersConfig object.

    Raises:
        FileNotFoundError: If the config file does not exist.
        ValidationError:   If the YAML structure is invalid.

    We resolve the default path relative to this file's location,
    not relative to cwd. This makes the loader work regardless of
    where the script is executed from.
    """
    if path is None:
        path = Path(__file__).parent.parent / "configs" / "scrapers.yaml"

    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    raw: dict[str, Any] = yaml.safe_load(path.read_text(encoding="utf-8"))
    return ScrapersConfig.model_validate(raw)