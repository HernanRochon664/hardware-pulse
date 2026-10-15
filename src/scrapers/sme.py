"""
Adapter for the e-commerce platform shared by PCCompu, HardPC and Infinit.

Category pages render a ``#resultado_productos`` grid of ``article.prod_item``
cards paginated with ``?pagina=N`` (0-based). Prices use dot thousands
("USD 1.049") with hidden decimals; a crossed-out "Antes" price is only real
when its container is visible; out-of-stock cards carry an ``.agotado`` badge.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterator
from datetime import datetime
from urllib.parse import urlencode, urlsplit, urlunsplit

from bs4 import BeautifulSoup, Tag

from src.domain.models import Category, ScrapedListing
from src.scrapers.base import ScrapeError, StoreAdapter
from src.scrapers.prices import detect_currency, parse_price

logger = logging.getLogger(__name__)

_PRODUCT_ID = re.compile(r"nav_prod_masinfo\(this,\s*(\d+)\)")


def _visible(tag: Tag | None) -> bool:
    if tag is None:
        return False
    style = str(tag.get("style") or "").replace(" ", "").lower()
    return "display:none" not in style


def _amount(container: Tag | None, value_selector: str) -> float | None:
    if container is None:
        return None
    value = container.select_one(value_selector)
    if value is None:
        return None
    text = value.get_text(strip=True)
    decimals = value.find_next_sibling("span", class_="pdeci")
    if decimals is not None and _visible(decimals):
        text += decimals.get_text(strip=True)
    return parse_price(text, thousands=".", decimal=",")


class SmeAdapter(StoreAdapter):
    def page_url(self, path: str, page: int) -> str:
        parts = urlsplit(self.absolute(path))
        query = parts.query + ("&" if parts.query else "") + urlencode({"pagina": page})
        return urlunsplit((parts.scheme, parts.netloc, parts.path, query, ""))

    def fetch_source(
        self, category: Category, source: str, scraped_at: datetime
    ) -> Iterator[ScrapedListing]:
        seen: set[str] = set()
        for page in range(self.store.page_limit):
            url = self.page_url(source, page)
            response = self.client.get(url)
            if response.status_code != 200:
                if page == 0:
                    raise ScrapeError(f"{url} returned HTTP {response.status_code}")
                break
            soup = BeautifulSoup(response.content, "html.parser")
            grid = soup.select_one("#resultado_productos")
            if grid is None:
                if page == 0:
                    raise ScrapeError(f"{url}: product grid not found (layout changed?)")
                break
            new = 0
            for card in grid.select("article.prod_item"):
                listing = self.parse_card(card, category, scraped_at)
                if listing is None or listing.url in seen:
                    continue
                seen.add(listing.url)
                new += 1
                yield listing
            if new == 0:
                break

    def parse_card(
        self, card: Tag, category: Category, scraped_at: datetime
    ) -> ScrapedListing | None:
        link = card.select_one("h2 a")
        if link is None or not link.get("href"):
            return None
        title = link.get_text(" ", strip=True)
        current = card.select_one(".precio_cont")
        price = _amount(current, "span.pprecio")
        if not title or price is None:
            return None

        before = card.select_one(".precio_antes")
        regular = _amount(before, "span.antes") if _visible(before) else None
        currency_tag = current.select_one("span.pmoneda") if current else None
        if card.select_one(".agotado") is not None:
            in_stock: bool | None = False
        elif card.select_one(".enstock") is not None:
            in_stock = True
        else:
            in_stock = None
        brand_tag = card.select_one(".cont .u")
        image = card.select_one(".foto img")
        sku_match = _PRODUCT_ID.search(str(link.get("onclick") or ""))
        return ScrapedListing(
            store_id=self.store.id,
            category=category,
            url=self.absolute(str(link["href"])),
            title=title,
            price=price,
            regular_price=regular,
            currency=detect_currency(currency_tag.get_text() if currency_tag else None),
            in_stock=in_stock,
            store_sku=sku_match.group(1) if sku_match else None,
            brand=brand_tag.get_text(strip=True) if brand_tag else None,
            image_url=str(image["src"]) if image and image.get("src") else None,
            scraped_at=scraped_at,
        )
