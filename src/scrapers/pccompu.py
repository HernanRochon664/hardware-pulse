import logging
from datetime import datetime
from urllib.parse import urlparse, urlencode, parse_qs, urlunparse, urljoin
from bs4 import BeautifulSoup, Tag

from src.domain.models import Condition, Currency, RawListing, Source
from .base import BaseHTMLScraper

logger = logging.getLogger(__name__)

class PCCompuScraper(BaseHTMLScraper):
    """Scraper for PCCompu website that extracts hardware product listings."""

    @property
    def name(self) -> str:
        """Return the name of the scraper."""
        return "pccompu"

    @property
    def source(self) -> Source:
        """Return the source identifier for PCCompu."""
        return Source.PCCOMPU

    @property
    def _start_page(self) -> int:
        """Return the starting page number for pagination."""
        return 0

    def _build_page_url(self, base_url: str, page: int) -> str:
        """Build the URL for a specific page by modifying the query parameters.

        Args:
            base_url: The base URL of the page.
            page: The page number to build the URL for.

        Returns:
            The modified URL with the page parameter.
        """
        parsed = urlparse(base_url)
        params = parse_qs(parsed.query)
        params["pagina"] = [str(page)]
        new_query = urlencode({k: v[0] for k, v in params.items()})
        return urlunparse(parsed._replace(query=new_query))

    def _get_product_containers(self, soup: BeautifulSoup) -> list[Tag]:
        """Extract the product container elements from the parsed HTML.

        Args:
            soup: The BeautifulSoup object of the page.

        Returns:
            A list of Tag elements containing product information.
        """
        container = soup.select_one("#resultado_productos")
        return container.select("div.prod_cont") if container else []

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
            link_tag = product.select_one("div.accont h2 a")
            title_tag = product.select_one("span[itemprop='name']")
            moneda_tag = product.select_one("span.pmoneda")
            precio_tag = product.select_one("span.pprecio")

            if link_tag is None or title_tag is None or moneda_tag is None or precio_tag is None:
                return None

            title = title_tag.get_text(strip=True)
            currency = Currency.USD if moneda_tag.get_text(strip=True) == "USD" else Currency.UYU

            return RawListing(
                source=self.source,
                url=urljoin("https://www.pccompu.com.uy", str(link_tag.get("href"))),
                timestamp=fetched_at,
                title=title,
                price=float(precio_tag.get_text(strip=True)),
                currency=currency,
                seller="pccompu",
                condition=Condition.NEW,
            )
        except Exception as exc:
            logger.warning("PCCompu: Error parseando producto '%s': %s", title, exc)
            return None