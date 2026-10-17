"""Import calculator in the dashboard, and the bookmarklet on trimmed store pages."""

from __future__ import annotations

import re
import urllib.parse
from pathlib import Path

import pytest
from playwright.sync_api import Page, expect

from src.crossborder.config import load_config
from src.crossborder.intake import parse_intake
from src.dashboard.importing import BOOKMARKLET_PATH
from tests.e2e.test_dashboard import TIMEOUT, _no_exceptions, _open

pytestmark = pytest.mark.e2e

PAGES = Path(__file__).resolve().parents[1] / "fixtures" / "us_pages"


def test_product_page_dialog_computes_a_verdict(page: Page, app_url: str):
    _open(page, f"{app_url}/catalogo?cat=gpu")
    page.get_by_role("link", name=re.compile("RTX 5060 8GB Eagle OC")).first.click()
    expect(page.get_by_text("¿COMPRAR ACÁ O IMPORTAR?")).to_be_visible(timeout=TIMEOUT)
    expect(page.get_by_text("Importarlo conviene si en EE. UU.")).to_be_visible()
    page.get_by_role("button", name="Calcular si conviene importarlo").click()
    dialog = page.get_by_role("dialog")
    price = dialog.get_by_label("Precio en la tienda (U$S, sin impuestos)")
    price.fill("299")
    price.press("Enter")
    expect(dialog.get_by_text("Importado, puesto en Uruguay")).to_be_visible(timeout=TIMEOUT)
    expect(dialog.get_by_text("Conviene importar", exact=False).first).to_be_visible()
    _no_exceptions(page)


def test_bookmarklet_params_prefill_the_calculator(page: Page, app_url: str):
    query = urllib.parse.urlencode(
        {
            "src": "bm",
            "t": "Gigabyte GeForce RTX 5060 Eagle OC 8G Graphics Card",
            "p": "$299.99",
            "u": "https://www.amazon.com/dp/B0TEST",
            "sold": "Amazon.com",
            "w": "2.9 pounds",
        }
    )
    _open(page, f"{app_url}/importar?{query}")
    expect(page.get_by_text("Es el mismo producto que", exact=False)).to_be_visible(timeout=TIMEOUT)
    expect(page.get_by_text("Importado, puesto en Uruguay")).to_be_visible(timeout=TIMEOUT)
    _no_exceptions(page)


def test_build_warns_about_incompatible_parts_and_computes_a_plan(page: Page, app_url: str):
    _open(page, f"{app_url}/importar")
    page.get_by_role("tab", name="Armado (varios productos)").click()
    page.get_by_text("Mostrar solo lo compatible con lo elegido").click()

    def choose(slot: str, option: str) -> None:
        box = page.get_by_role("combobox", name=slot, exact=True)
        box.scroll_into_view_if_needed()
        box.click()
        page.get_by_role("option", name=re.compile(option)).click()
        expect(page.locator('[data-testid="stStatusWidget"]')).to_have_count(0, timeout=TIMEOUT)

    choose("Procesador", "Ryzen 7 7800X3D")
    choose("Motherboard", "B550M-A")
    expect(page.get_by_text("no son compatibles", exact=False)).to_be_visible(timeout=TIMEOUT)
    choose("Motherboard", "B650M-E")
    expect(page.get_by_text("no son compatibles", exact=False)).to_have_count(0, timeout=TIMEOUT)
    choose("Monitor", "MO27Q28G")

    page.get_by_role("button", name="Calcular el mejor plan").click()
    expect(page.get_by_text("Todo en Uruguay")).to_be_visible(timeout=TIMEOUT)
    expect(page.get_by_text("Comprá en Uruguay", exact=False)).to_be_visible()
    _no_exceptions(page)


@pytest.mark.parametrize("tab", ["Armado (varios productos)", "Cómo se calcula"])
def test_importar_tabs_render(page: Page, app_url: str, tab: str):
    _open(page, f"{app_url}/importar")
    page.get_by_role("tab", name=tab).click()
    expect(page.locator("h1").first).to_be_visible(timeout=TIMEOUT)
    _no_exceptions(page)


def _extract(page: Page, fixture: str, url: str) -> dict[str, str]:
    page.set_content((PAGES / fixture).read_text(encoding="utf-8"))
    page.evaluate("window.__HP_TEST__ = true")
    source = BOOKMARKLET_PATH.read_text(encoding="utf-8").replace("__APP_URL__", "https://app")
    page.add_script_tag(content=source)
    built = page.evaluate(
        "u => window.hpBookmarklet.buildUrl(window.hpBookmarklet.extract(new URL(u)))", url
    )
    assert built.startswith("https://app/importar?src=bm&")
    return dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(built).query))


def test_bookmarklet_reads_amazon_in_spanish(page: Page):
    params = _extract(page, "amazon_es.html", "https://www.amazon.com/dp/B0BHJJ9Y77?th=1")
    assert params["t"] == "Samsung 990 PRO SSD NVMe M.2 PCIe Gen4 2TB"
    assert params["p"] == "US$169.99"
    assert params["sold"] == "Amazon.com"
    assert params["mpn"] == "MZ-V9P2T0B/AM"
    assert params["u"] == "https://www.amazon.com/dp/B0BHJJ9Y77"
    intake = parse_intake(params, load_config())
    assert intake is not None
    assert intake.store_id == "amazon" and intake.price_usd == 169.99
    assert intake.weight_kg == 0.05  # 1.76 oz
    assert intake.dims_cm == (10.2, 7.5, 1.2)


def test_bookmarklet_reads_amazon_third_party_offer(page: Page):
    params = _extract(page, "amazon_en.html", "https://www.amazon.com/dp/B0DS6S98ZG")
    intake = parse_intake(params, load_config())
    assert intake is not None
    assert intake.store_id == "amazon_marketplace"
    assert intake.price_usd == 749.99 and intake.weight_kg == 2.2
    assert intake.mpn == "TUF-RTX5070TI-O16G-GAMING"


def test_bookmarklet_reads_json_ld(page: Page):
    params = _extract(page, "jsonld.html", "https://www.newegg.com/p/N82E1")
    intake = parse_intake(params, load_config())
    assert intake is not None
    assert intake.store_id == "newegg"
    assert intake.price_usd == 94.99
    assert intake.mpn == "KF556C40BBK2-32"
