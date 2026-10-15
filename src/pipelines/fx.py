"""
USD/UYU exchange rates.

Rates come from the free fawazahmed0 currency API (two mirrors) and are stored
per local date in ``fx_rates``, so every UYU price can be converted again with
the exact rate used at the time. All 10 stores currently publish USD; the rate
is still needed to show prices in pesos.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import date, datetime

import requests

from src.domain.models import Currency
from src.storage import repository

logger = logging.getLogger(__name__)

FX_URLS = [
    "https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@{tag}/v1/currencies/usd.json",
    "https://{tag}.currency-api.pages.dev/v1/currencies/usd.json",
]
REQUEST_TIMEOUT = 15
SANE_RANGE = (20.0, 80.0)  # UYU per USD; anything else is a broken response


def fetch_usd_uyu(day: date, session: requests.Session | None = None) -> tuple[float, str] | None:
    """Rate for ``day`` (falls back to the latest published rate). Returns (rate, source)."""
    http = session or requests.Session()
    for tag in (day.isoformat(), "latest"):
        for template in FX_URLS:
            url = template.format(tag=tag)
            try:
                response = http.get(url, timeout=REQUEST_TIMEOUT)
                if response.status_code != 200:
                    continue
                rate = float(response.json()["usd"]["uyu"])
            except (requests.RequestException, ValueError, KeyError, TypeError) as exc:
                logger.debug("FX source failed %s: %s", url, exc)
                continue
            if SANE_RANGE[0] <= rate <= SANE_RANGE[1]:
                return rate, url
            logger.warning("Ignoring implausible USD/UYU rate %.3f from %s", rate, url)
    return None


def ensure_fx_rate(conn: sqlite3.Connection, day: date, now: datetime) -> float | None:
    """Rate stored for ``day``; fetched and saved when missing. None if unavailable."""
    stored = conn.execute("SELECT usd_uyu FROM fx_rates WHERE date = ?", (day.isoformat(),))
    row = stored.fetchone()
    if row:
        return float(row[0])
    fetched = fetch_usd_uyu(day)
    if fetched is None:
        logger.warning("No USD/UYU rate available for %s, using the latest stored one", day)
        return repository.get_fx_rate(conn, day.isoformat())
    rate, source = fetched
    repository.save_fx_rate(conn, day.isoformat(), rate, source, now)
    return rate


def to_usd(price: float, currency: Currency, usd_uyu: float | None) -> float | None:
    if currency == Currency.USD:
        return price
    if usd_uyu is None:
        return None
    return price / usd_uyu
