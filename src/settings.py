"""
Project-wide settings: filesystem locations, timezone and timestamp helpers.

Every path is resolved from the project root (never from the current working
directory), so the pipeline behaves the same from cron, GitHub Actions,
Streamlit Cloud or a terminal. Paths that change between environments can be
overridden with environment variables:

- ``HP_DB_PATH``   SQLite database file
- ``HP_DATA_URL``  URL of the published database (GitHub Release asset)
"""

from __future__ import annotations

import os
from datetime import UTC, date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = PROJECT_ROOT / "configs"
DATA_DIR = PROJECT_ROOT / "data"

STORES_CONFIG_PATH = CONFIG_DIR / "stores.yaml"
TAXONOMY_PATH = CONFIG_DIR / "taxonomy.yaml"

# Import calculator ("¿comprar acá o importar?"), see src/crossborder/.
IMPORT_RULES_PATH = CONFIG_DIR / "import_rules.yaml"
COURIERS_PATH = CONFIG_DIR / "couriers.yaml"
US_STORES_PATH = CONFIG_DIR / "us_stores.yaml"
SHIPPING_PROFILES_PATH = CONFIG_DIR / "shipping_profiles.yaml"

# Uruguay has no daylight saving time since 2015 (UTC-3 all year).
LOCAL_TZ = ZoneInfo("America/Montevideo")

GITHUB_REPO = "HernanRochon664/hardware-pulse"
DATA_RELEASE_TAG = "data-latest"
DB_ASSET_NAME = "hardware_pulse.db.gz"
STATUS_ASSET_NAME = "status.json"


def db_path() -> Path:
    """Location of the SQLite database (``HP_DB_PATH`` overrides the default)."""
    override = os.environ.get("HP_DB_PATH")
    return Path(override) if override else DATA_DIR / "hardware_pulse.db"


def data_url() -> str:
    """Public URL of the gzipped database published by the scrape workflow."""
    default = (
        f"https://github.com/{GITHUB_REPO}/releases/download/{DATA_RELEASE_TAG}/{DB_ASSET_NAME}"
    )
    return os.environ.get("HP_DATA_URL", default)


# ---------------------------------------------------------------------------
# Time helpers. All timestamps are stored as UTC ISO-8601 strings ending in "Z"
# (e.g. "2026-10-14T20:03:58Z") so that plain string comparison in SQL is a
# correct chronological comparison.
# ---------------------------------------------------------------------------


def utcnow() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def to_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        raise ValueError("to_iso requires a timezone-aware datetime")
    return dt.astimezone(UTC).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def from_iso(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def local_date(dt: datetime) -> date:
    """Calendar date in Uruguay for a timezone-aware datetime."""
    return dt.astimezone(LOCAL_TZ).date()
