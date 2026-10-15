"""
Polite HTTP client shared by all store adapters.

- Retries transient failures (connection errors, timeouts, 429 and 5xx) with
  exponential backoff via urllib3, honouring ``Retry-After``.
- Waits ``delay`` seconds between consecutive requests to the same store.
- Sends ordinary browser headers; several stores reject bare clients.
"""

from __future__ import annotations

import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-UY,es;q=0.9,en;q=0.6",
}

RETRYABLE_STATUS = (429, 500, 502, 503, 504)


class HttpClient:
    def __init__(
        self,
        *,
        delay: float = 1.5,
        timeout: int = 30,
        max_retries: int = 3,
        backoff: float = 1.5,
        session: requests.Session | None = None,
    ) -> None:
        self.delay = delay
        self.timeout = timeout
        self._last_request = 0.0
        self.session = session or self._build_session(max_retries, backoff)

    @staticmethod
    def _build_session(max_retries: int, backoff: float) -> requests.Session:
        session = requests.Session()
        session.headers.update(BROWSER_HEADERS)
        retry = Retry(
            total=max_retries,
            backoff_factor=backoff,
            status_forcelist=RETRYABLE_STATUS,
            allowed_methods=("GET",),
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=2, pool_maxsize=2)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def get(self, url: str, **kwargs) -> requests.Response:
        wait = self.delay - (time.monotonic() - self._last_request)
        if wait > 0:
            time.sleep(wait)
        try:
            return self.session.get(url, timeout=self.timeout, **kwargs)
        finally:
            self._last_request = time.monotonic()
