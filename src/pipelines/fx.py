"""
FX rate helpers for hardware-pulse.

Responsibilities:
- Fetch USD/UYU exchange rates from the currency API
- Normalize prices to USD given currency and FX rate

Does NOT:
- Access the database
- Contain pipeline orchestration logic
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import requests

logger = logging.getLogger(__name__)

FX_API_URL = "https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@{date}/v1/currencies/usd.json"

REQUEST_TIMEOUT = 10


def fetch_fx_rates(week_starts: list[str]) -> dict[str, float | None]:
    """
    Fetch USD/UYU FX rates for all requested dates.

    Args:
        week_starts: List of ISO dates (YYYY-MM-DD).

    Returns:
        Dict mapping date -> USD/UYU rate.
    """
    results: dict[str, float | None] = {}
    unique_dates = sorted(set(week_starts))

    for date_str in unique_dates:
        url = FX_API_URL.format(date=date_str)

        try:
            response = requests.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()

            data = response.json()

            usd_rates = data.get("usd", {})
            rate = usd_rates.get("uyu")

            if rate is None:
                logger.warning("No USD/UYU FX rate found for %s", date_str)
                results[date_str] = None
                continue

            results[date_str] = float(rate)

            logger.debug(
                "Fetched USD/UYU FX rate | date=%s | rate=%.4f",
                date_str,
                results[date_str],
            )

        except Exception as exc:
            logger.info("Failed to fetch FX rate for %s: %s", date_str, exc)
            results[date_str] = None

    return results


def normalize_to_usd(
    price: float,
    currency: str,
    fx_rate: float | None,
) -> float | None:
    """
    Convert price to USD given currency and optional FX rate.

    Args:
        price: Price in the local currency.
        currency: Currency code (USD or UYU).
        fx_rate: USD/UYU exchange rate (required for UYU).

    Returns:
        Price in USD, or None if conversion is not possible.
    """
    if currency == "USD":
        return price
    if currency == "UYU":
        if fx_rate is None:
            return None
        return price / fx_rate
    raise ValueError(f"Unsupported currency: {currency}")


def compute_week_start(dt: datetime) -> str:
    """Compute the Monday of the week containing dt, as YYYY-MM-DD."""
    if dt.tzinfo is None:
        raise ValueError("compute_week_start requires timezone-aware datetime")
    week_start = dt - timedelta(days=dt.weekday())
    return week_start.replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d")
