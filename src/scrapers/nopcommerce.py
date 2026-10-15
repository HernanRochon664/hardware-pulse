"""
nopCommerce adapter (Banifox).

Category pages list ``.product-item[data-productid]`` cards, 10 per page,
paginated with ``?pagenumber=N`` (1-based). Prices look like "U$S 1.160";
"U$S 0" means "consultar". Category pages do not expose stock, so
``in_stock`` stays unknown (None) and the offer still counts as available.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import datetime
from urllib.parse import urlencode

from bs4 import BeautifulSoup, Tag

from src.domain.models import Category, ScrapedListing
from src.scrapers.base import ScrapeError, StoreAdapter
from src.scrapers.prices import detect_currency, parse_price

logger = logging.getLogger(__name__)


class NopCommerceAdapter(StoreAdapter):
    def fetch_source(
        self, category: Category, source: str, scraped_at: datetime
    ) -> Iterator[ScrapedListing]:
        seen: set[str] = set()
        base = self.absolute(source)
        for page in range(1, self.store.page_limit + 1):
            url = f"{base}?{urlencode({'pagenumber': page})}"
            response = self.client.get(url, allow_redirects=False)
            if response.status_code != 200:
                # Banifox redirects unknown categories to /page-not-found.
                if page == 1:
                    raise ScrapeError(f"{url} returned HTTP {response.status_code}")
                break
            soup = BeautifulSoup(response.content, "html.parser")
            cards = soup.select(".product-grid .product-item[data-productid]")
            if not cards and page == 1 and soup.select_one(".product-grid") is None:
                raise ScrapeError(f"{url}: product grid not found (layout changed?)")
            new = 0
            for card in cards:
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
        link = card.select_one("h2.product-title a")
        price_tag = card.select_one(".prices .actual-price")
        if link is None or not link.get("href") or price_tag is None:
            return None
        price_text = price_tag.get_text(strip=True)
        price = parse_price(price_text, thousands=".", decimal=",")
        if price is None:
            return None
        old = card.select_one(".prices .old-price")
        sku = card.select_one(".sku")
        image = card.select_one("img.picture-img")
        image_url = None
        if image is not None:
            image_url = str(image.get("data-lazyloadsrc") or image.get("src") or "") or None
            if image_url and image_url.startswith("data:"):
                image_url = None
        return ScrapedListing(
            store_id=self.store.id,
            category=category,
            url=self.absolute(str(link["href"])),
            title=link.get_text(" ", strip=True),
            price=price,
            regular_price=parse_price(old.get_text(strip=True)) if old else None,
            currency=detect_currency(price_text),
            in_stock=None,
            store_sku=sku.get_text(strip=True) if sku else str(card.get("data-productid")),
            image_url=image_url,
            scraped_at=scraped_at,
        )
