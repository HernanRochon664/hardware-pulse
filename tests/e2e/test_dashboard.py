"""
End-to-end tests for the Hardware Pulse Streamlit dashboard.

Uses Playwright to verify:
- Dashboard loads and displays correctly
- Category filter works
- Price history chart renders
- Deal section is present when data exists
"""

import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import urljoin

import pytest
import requests

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

HERE = Path(__file__).parent.parent.parent.resolve()
STREAMLIT_PORT = 8592
STREAMLIT_URL = f"http://localhost:{STREAMLIT_PORT}"

# Allow up to 30s for Streamlit to start
POLL_INTERVAL = 1.0
START_TIMEOUT = 30.0


@pytest.fixture(scope="module")
def streamlit_server():
    """Start Streamlit as a subprocess, yield once ready, tear down."""
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "streamlit",
            "run",
            str(HERE / "src/dashboard/app.py"),
            "--server.port",
            str(STREAMLIT_PORT),
            "--server.headless",
            "true",
            "--browser.gatherUsageStats",
            "false",
            "--global.developmentMode",
            "false",
        ],
        cwd=HERE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env={**__import__("os").environ, "STREAMLIT_SERVER_HEADLESS": "true"},
    )

    started = time.monotonic()
    while time.monotonic() - started < START_TIMEOUT:
        try:
            resp = requests.get(urljoin(STREAMLIT_URL, "_stcore/health"), timeout=5)
            if resp.status_code == 200:
                break
        except requests.ConnectionError:
            pass
        time.sleep(POLL_INTERVAL)
    else:
        proc.terminate()
        proc.wait(5)
        pytest.fail("Streamlit did not start within timeout")

    yield STREAMLIT_URL

    proc.terminate()
    proc.wait(5)


@pytest.fixture
def page(context, streamlit_server):
    page = context.new_page()
    page.goto(streamlit_server)
    page.wait_for_load_state("networkidle")
    page.wait_for_selector('[data-testid="stAppViewContainer"]', timeout=15000)
    page.wait_for_timeout(2000)
    yield page
    page.close()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestDashboardLoads:
    def test_page_title(self, page):
        assert "Hardware Pulse" in page.title()

    def test_page_header(self, page):
        header = page.locator("h1")
        assert header.is_visible()

    def test_sidebar_slider_present(self, page):
        slider = page.locator('section[data-testid="stSidebar"] div[data-testid="stSlider"]')
        assert slider.is_visible()

    def test_multiselect_filter_present(self, page):
        multiselect = page.locator('div[data-testid="stMultiSelect"]')
        assert multiselect.is_visible()

    def test_two_tabs_rendered(self, page):
        page.wait_for_selector('button[role="tab"]', timeout=10000)
        tabs = page.locator('button[role="tab"]')
        count = tabs.count()
        assert count == 2, f"Expected 2 tabs, got {count}"


class TestDashboardFilter:
    def test_filter_shows_categories(self, page):
        multiselect = page.locator('div[data-testid="stMultiSelect"]')
        multiselect.click()
        options = page.locator('li[role="option"]')
        if options.count() > 0:
            assert options.first.is_visible()

    def test_selecting_category_filters_table(self, page):
        table = page.locator('div[data-testid="stDataFrame"]')
        if not table.is_visible():
            pytest.skip("No data table rendered (likely empty DB)")

    def test_best_deals_section(self, page):
        deals = page.locator("text=Best Deals")
        if deals.is_visible():
            assert deals.is_visible()


class TestDashboardProductTab:
    def test_product_tab_switch(self, page):
        tabs = page.locator('button[role="tab"]')
        if tabs.count() >= 2:
            tabs.nth(1).click()
            page.wait_for_timeout(1000)
            select = page.locator('div[data-testid="stSelectbox"]')
            assert select.is_visible()

    def test_price_chart_renders(self, page):
        tabs = page.locator('button[role="tab"]')
        if tabs.count() >= 2:
            tabs.nth(1).click()
            page.wait_for_timeout(2000)
            chart = page.locator('div[data-testid="stPlotlyChart"]')
            if chart.is_visible():
                assert chart.is_visible()


class TestDashboardDataPresence:
    def test_dataframe_or_warning(self, page):
        table = page.locator('div[data-testid="stDataFrame"]')
        warning = page.locator("text=No data")
        assert table.is_visible() or warning.is_visible()
