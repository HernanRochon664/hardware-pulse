import logging
from datetime import datetime
from bs4 import BeautifulSoup, Tag
from urllib.parse import urljoin

from src.domain.models import Condition, Currency, RawListing, Source
from .base import BaseHTMLScraper

logger = logging.getLogger(__name__)

class ThotScraper(BaseHTMLScraper):
    """Scraper for Thot website that extracts hardware product listings."""

    @property
    def name(self) -> str:
        """Return the name of the scraper."""
        return "thot"

    @property
    def source(self) -> Source:
        """Return the source identifier for Thot."""
        return Source.THOT

    def _build_page_url(self, base_url: str, page: int) -> str:
        """Build the URL for a specific page by appending the page path.

        Args:
            base_url: The base URL of the page.
            page: The page number to build the URL for.

        Returns:
            The modified URL with the page appended.
        """
        base = base_url.rstrip("/")
        return f"{base}/" if page == 1 else f"{base}/page/{page}/"

    def _get_product_containers(self, soup: BeautifulSoup) -> list[Tag]:
        """Extract the product container elements from the parsed HTML.

        Args:
            soup: The BeautifulSoup object of the page.

        Returns:
            A list of Tag elements containing product information.
        """
        return soup.select("li.product")

    def _parse_listing(self, product: Tag, fetched_at: datetime) -> RawListing | None:
        """Parse a single product container into a RawListing object.

        Args:
            product: The BeautifulSoup Tag containing product data.
            fetched_at: The timestamp when the data was fetched.

        Returns:
            A RawListing object if parsing succeeds, None otherwise.
        """
        title = "Unknown"
        try:
            price_tag = product.select_one(".price")
            link_tag = product.select_one("a.product-loop-title")
            if not price_tag or not link_tag: return None

            title = link_tag.get_text(strip=True)
            href = link_tag.get("href")
            
            raw_price = price_tag.get_text(separator=" ").strip()
            if "US$" in raw_price:
                val = raw_price.replace("US$", "").strip().replace(",", "")
                price, currency = float(val), Currency.USD
            else:
                val = raw_price.replace("$", "").strip().replace(".", "").replace(",", ".")
                price, currency = float(val), Currency.UYU

            return RawListing(
                source=self.source,
                url=urljoin("https://thotcomputacion.com.uy", str(href)),
                timestamp=fetched_at,
                title=title,
                price=price,
                currency=currency,
                seller="thot",
                condition=Condition.NEW,
            )
        except Exception as exc:
            logger.warning("Thot: Error parseando producto '%s': %s", title, exc)
            return None