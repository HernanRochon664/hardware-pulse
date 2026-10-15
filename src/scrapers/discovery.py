"""
Helpers to find the category sources of a store when adding it to stores.yaml.
"""

from __future__ import annotations

import re
from collections.abc import Iterator

from bs4 import BeautifulSoup

from src.config import StoreConfig
from src.domain.models import Platform
from src.scrapers.http import HttpClient
from src.scrapers.woocommerce import API_PATH, PER_PAGE

KEYWORDS = re.compile(
    r"video|grafic|gpu|procesador|cpu|memori|ram|ssd|solido|nvme|disco|almacen", re.I
)


def discover_categories(store: StoreConfig) -> Iterator[str]:
    client = HttpClient(delay=0.5, timeout=store.timeout_s)
    if store.platform == Platform.WOOCOMMERCE:
        url = store.base_url + API_PATH + "/categories"
        for page in range(1, 11):
            batch = client.get(url, params={"per_page": PER_PAGE, "page": page}).json()
            if not isinstance(batch, list) or not batch:
                break
            for cat in batch:
                if cat.get("count") and KEYWORDS.search(cat["slug"]):
                    yield f"{cat['slug']:45} productos={cat['count']}"
            if len(batch) < PER_PAGE:
                break
        return
    soup = BeautifulSoup(client.get(store.base_url + "/").content, "html.parser")
    seen: set[str] = set()
    for link in soup.select("a[href]"):
        href = str(link["href"]).replace(store.base_url, "")
        if not href.startswith("/") or href in seen or not KEYWORDS.search(href):
            continue
        if store.platform == Platform.SME and not href.startswith("/catalogo/"):
            continue
        if re.search(r"-\d+/?$", href):  # product pages end with an id
            continue
        seen.add(href)
        yield f"{href:60} {link.get_text(' ', strip=True)[:40]}"
