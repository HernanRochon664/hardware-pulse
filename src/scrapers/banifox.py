import logging
import re
from datetime import datetime
from bs4 import BeautifulSoup, Tag
from urllib.parse import urljoin

from src.domain.models import Condition, Currency, RawListing, Source
from .base import BaseHTMLScraper

logger = logging.getLogger(__name__)
PRICE_REGEX = re.compile(r"USD\s?([\d\.,]+)", re.IGNORECASE)

class BanifoxScraper(BaseHTMLScraper):
    """Scraper for Banifox website that extracts hardware product listings."""

    @property
    def name(self) -> str:
        """Return the name of the scraper."""
        return "banifox"

    @property
    def source(self) -> Source:
        """Return the source identifier for Banifox."""
        return Source.BANIFOX

    def _build_page_url(self, base_url: str, page: int) -> str:
        """Build the URL for a specific page by appending the page path.

        Args:
            base_url: The base URL of the page.
            page: The page number to build the URL for.

        Returns:
            The modified URL with the page appended.
        """
        base = base_url.rstrip("/")
        return f"{base}/" if page == 1 else f"{base}/pag/{page}/"

    def _get_product_containers(self, soup: BeautifulSoup) -> list[Tag]:
        """Extract the product container elements from the parsed HTML.

        Args:
            soup: The BeautifulSoup object of the page.

        Returns:
            A list of Tag elements containing product information.
        """
        return soup.select(".card-producto, .cont-producto")

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
            link_tag = product.select_one("a[title]")
            if not link_tag: return None

            title_raw = link_tag.get("title")
            href = link_tag.get("href")
            if not title_raw or not href: return None
            title = str(title_raw)

            price_container = product.select_one("div.precio")
            if not price_container: return None

            texts = [t.strip() for t in price_container.find_all(string=True, recursive=False)]
            price_text = " ".join(t for t in texts if t)

            match = PRICE_REGEX.search(price_text)
            if not match:
                logger.debug("Banifox: No se encontró precio para '%s' en texto: '%s'", title, price_text)
                return None

            price = float(match.group(1).replace(".", "").replace(",", "."))

            return RawListing(
                source=self.source,
                url=urljoin("https://www.banifox.com", str(href)),
                timestamp=fetched_at,
                title=title,
                price=price,
                currency=Currency.USD,
                seller="banifox",
                condition=Condition.NEW,
            )
        except Exception as exc:
            logger.warning("Banifox: Error parseando producto '%s': %s", title, exc)
            return None