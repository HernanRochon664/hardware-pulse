"""Drive the dashboard with Playwright against the synthetic fixture database."""

from __future__ import annotations

import re

import pytest
from playwright.sync_api import Page, expect

pytestmark = pytest.mark.e2e

TIMEOUT = 20_000


def _open(page: Page, url: str) -> None:
    page.goto(url)
    expect(page.locator('[data-testid="stMain"]')).to_be_visible(timeout=TIMEOUT)
    expect(page.locator('[data-testid="stStatusWidget"]')).to_have_count(0, timeout=TIMEOUT)


def _no_exceptions(page: Page) -> None:
    expect(page.locator('[data-testid="stException"]')).to_have_count(0)


def test_home_shows_kpis_and_recent_drops(page: Page, app_url: str):
    _open(page, app_url)
    expect(page.get_by_role("heading", name="Hardware Pulse")).to_be_visible(timeout=TIMEOUT)
    expect(page.get_by_text("Bajaron en los últimos 7 días")).to_be_visible()
    expect(page.get_by_role("link", name=re.compile("RTX 5060 8GB Eagle OC"))).to_be_visible()
    _no_exceptions(page)


def test_catalog_category_comes_from_query_params(page: Page, app_url: str):
    _open(page, f"{app_url}/catalogo?cat=ram")
    expect(page.get_by_text("1 producto", exact=False)).to_be_visible(timeout=TIMEOUT)
    expect(page.get_by_role("link", name=re.compile("Kingston Fury Beast"))).to_be_visible()
    expect(page.get_by_role("link", name=re.compile("RTX 5070"))).to_have_count(0)
    _no_exceptions(page)


def test_catalog_search_narrows_results(page: Page, app_url: str):
    _open(page, f"{app_url}/catalogo")
    search = page.get_by_label("Buscar")
    search.fill("5070")
    search.press("Enter")
    expect(page.get_by_role("link", name=re.compile("RTX 5070 12GB Shadow"))).to_be_visible(
        timeout=TIMEOUT
    )
    expect(page.get_by_role("link", name=re.compile("Eagle OC"))).to_have_count(0)


def test_product_page_shows_best_offer_history_and_store_link(page: Page, app_url: str):
    _open(page, f"{app_url}/catalogo?cat=gpu")
    page.get_by_role("link", name=re.compile("RTX 5060 8GB Eagle OC")).first.click()
    expect(page.get_by_text("PRECIO MÁS BAJO", exact=False)).to_be_visible(timeout=TIMEOUT)
    expect(page.locator(".hp-price")).to_contain_text("U$S 560")
    store_link = page.get_by_role("link", name="Ver en Alfa")
    expect(store_link).to_have_attribute("href", "https://alfa.example/p/eagle")
    expect(page.locator(".js-plotly-plot").first).to_be_visible(timeout=TIMEOUT)
    expect(page.get_by_text("no disponible para este producto")).to_be_visible()
    _no_exceptions(page)


def test_currency_toggle_shows_pesos(page: Page, app_url: str):
    _open(page, f"{app_url}/catalogo?cat=ram")
    page.get_by_text("Pesos ($)").click()
    expect(page.locator(".hp-price-small")).to_have_text("$ 2.880", timeout=TIMEOUT)  # 72 USD x 40


@pytest.mark.parametrize("path", ["familias", "estado", "modelo"])
def test_secondary_pages_render(page: Page, app_url: str, path: str):
    _open(page, f"{app_url}/{path}")
    expect(page.locator("h1").first).to_be_visible(timeout=TIMEOUT)
    _no_exceptions(page)
