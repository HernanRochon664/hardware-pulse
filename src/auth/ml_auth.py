"""
MercadoLibre OAuth2 authentication for hardware-pulse.

Responsibilities:
- Obtain access tokens via client credentials flow
- Cache tokens to disk to avoid redundant OAuth requests
- Refresh proactively before expiration

Does NOT:
- Handle scraping logic
- Manage HTTP sessions
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TOKEN_URL = "https://api.mercadolibre.com/oauth/token"
TOKEN_CACHE_PATH = Path("data/ml_token.json")

# Refresh the token 5 minutes before actual expiration.
# This prevents edge cases where the token expires mid-run.
EXPIRY_BUFFER_SECONDS = 300


# ---------------------------------------------------------------------------
# Cache I/O
# ---------------------------------------------------------------------------


def _load_cached_token() -> dict | None:
    """
    Load token from disk cache.
    Returns None if cache doesn't exist or is malformed.
    """
    if not TOKEN_CACHE_PATH.exists():
        return None
    try:
        return json.loads(TOKEN_CACHE_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        logger.warning("Token cache unreadable; will fetch a new token.")
        return None


def _save_token(token_data: dict) -> None:
    """Persist token data to disk cache."""
    TOKEN_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    TOKEN_CACHE_PATH.write_text(
        json.dumps(token_data, indent=2),
        encoding="utf-8",
    )


def _is_token_valid(token_data: dict) -> bool:
    """
    Check if the cached token is still valid.

    We compare against UTC now minus the buffer, so a token expiring
    in less than EXPIRY_BUFFER_SECONDS is treated as already expired.
    """
    try:
        expires_at = datetime.fromisoformat(token_data["expires_at"])
        threshold = datetime.now(timezone.utc) + timedelta(seconds=EXPIRY_BUFFER_SECONDS)
        return expires_at > threshold
    except (KeyError, ValueError):
        return False


# ---------------------------------------------------------------------------
# Token fetch
# ---------------------------------------------------------------------------


def _fetch_new_token() -> dict:
    """
    Request a new access token from MercadoLibre via client credentials.

    Reads credentials from environment variables:
    - ML_APP_ID
    - ML_CLIENT_SECRET

    Returns:
        Dict with access_token and expires_at (ISO 8601 UTC string).

    Raises:
        EnvironmentError: If credentials are missing.
        requests.HTTPError: If the OAuth request fails.
    """
    app_id = os.getenv("ML_APP_ID")
    client_secret = os.getenv("ML_CLIENT_SECRET")

    if not app_id or not client_secret:
        raise EnvironmentError(
            "Missing ML_APP_ID or ML_CLIENT_SECRET in environment. "
            "Check your .env file."
        )

    logger.info("Requesting new MercadoLibre access token...")

    response = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": app_id,
            "client_secret": client_secret,
        },
        timeout=10,
    )
    response.raise_for_status()
    payload = response.json()

    # ML returns expires_in as seconds from now. We convert to an
    # absolute UTC timestamp for reliable cache validation across runs.
    expires_in: int = payload["expires_in"]
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

    token_data = {
        "access_token": payload["access_token"],
        "expires_at": expires_at.isoformat(),
    }

    _save_token(token_data)
    logger.info("Token obtained. Expires at %s", expires_at.isoformat())
    return token_data


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------


def get_valid_access_token() -> str:
    """
    Return a valid MercadoLibre access token.

    Strategy:
    1. Load token from disk cache
    2. If valid (not expiring within EXPIRY_BUFFER_SECONDS) → return it
    3. Otherwise → fetch a new token, cache it, return it

    Returns:
        A valid JWT access token string.

    Raises:
        EnvironmentError: If credentials are not configured.
        requests.HTTPError: If the OAuth endpoint returns an error.
    """
    cached = _load_cached_token()

    if cached and _is_token_valid(cached):
        logger.debug("Using cached ML token (expires at %s)", cached["expires_at"])
        return cached["access_token"]

    token_data = _fetch_new_token()
    return token_data["access_token"]