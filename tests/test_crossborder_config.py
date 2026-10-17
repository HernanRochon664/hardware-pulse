"""Import calculator configuration: the real files load and stay consistent."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from src.crossborder.config import (
    RegistryStatus,
    WeightBand,
    load_config,
    load_couriers,
    load_rules,
)


@pytest.fixture(scope="module")
def cfg():
    return load_config()


def test_rules_in_force_from_may_2026(cfg):
    rules = cfg.rules.for_date(date(2026, 10, 30))
    assert rules.franchise.annual_usd_cap == 800
    assert rules.franchise.max_shipments == 3
    assert rules.franchise.iva_rate == 0.22
    assert rules.franchise.iva_min_usd == 20
    assert rules.treaty.max_invoice_usd == 200
    assert rules.treaty.registry_required_from == date(2026, 11, 3)
    assert rules.single_tax.rate == 0.60
    assert rules.single_tax.min_usd == 20
    assert rules.single_tax.max_invoice_usd == 800
    for key in ("decree", "iva_postal", "franchise", "registry"):
        assert rules.source_url(key), key
    with pytest.raises(ValueError):
        cfg.rules.for_date(date(2026, 4, 30))


def test_every_courier_is_documented(cfg):
    assert set(cfg.couriers) >= {"gripper", "grinbox", "aerobox", "urubox"}
    for courier in cfg.couriers.values():
        assert courier.source_url.startswith("https://")
        assert courier.verified_at <= date.today()
        assert courier.max_kg >= 20  # every shipment allowed by the rules has a price
        assert 0 < courier.sales_tax_rate < 0.1


def test_registry_status_of_the_main_stores(cfg):
    stores = cfg.stores.stores
    for store_id in ("amazon", "newegg", "bhphoto", "bestbuy", "microcenter", "assisted"):
        assert stores[store_id].registry_status is RegistryStatus.LISTED
    assert stores["amazon_marketplace"].registry_status is RegistryStatus.UNCERTAIN
    assert stores["other"].registry_status is RegistryStatus.NOT_LISTED
    assert stores["amazon"].search_link("KF556C40BBK2-32") == (
        "https://www.amazon.com/s?k=KF556C40BBK2-32"
    )
    store = cfg.stores.store_for_host("www.amazon.com")
    assert store is not None and store.id == "amazon"
    assert cfg.stores.store_for_host("example.com") is None


@pytest.mark.parametrize(
    ("family", "category", "profile"),
    [
        ("gpu:rtx-5090-32gb", "gpu", "gpu_xl"),
        ("gpu:rtx-5070-ti-16gb", "gpu", "gpu_l"),  # before "rtx-5070"
        ("gpu:rtx-5070-12gb", "gpu", "gpu_m"),
        ("gpu:rtx-5060-ti-16gb", "gpu", "gpu_m"),  # before "rtx-5060"
        ("gpu:rtx-5060-8gb", "gpu", "gpu_s"),
        ("gpu:rx-5600-6gb", "gpu", "gpu_s"),
        ("gpu:rx-550-4gb", "gpu", "gpu_xs"),
        ("gpu:gt-730-2gb", "gpu", "gpu_xs"),
        ("gpu:some-new-chip", "gpu", "gpu_m"),
        ("cpu:intel-xeon-w9-3475x", "cpu", "cpu_workstation"),
        ("cpu:amd-ryzen-7-7800x3d", "cpu", "cpu"),
        ("ram:ddr5-16gb-5600-sodimm", "ram", "ram_sodimm"),
        ("ram:ddr5-32gb-6000-dimm", "ram", "ram"),
        ("ssd:external-1tb", "ssd", "ssd_external"),
        ("ssd:sata-1tb", "ssd", "ssd_sata"),
        ("ssd:nvme4-2tb", "ssd", "ssd_m2"),
        ("motherboard:am5-b650-matx", "motherboard", "mobo_matx"),
        ("motherboard:lga1700-h610-itx", "motherboard", "mobo_itx"),
        ("motherboard:am5-x870e-atx", "motherboard", "mobo_atx"),
        ("motherboard:am5-x870e-e-atx", "motherboard", "mobo_atx"),
        ("cooler:aio-240", "cooler", "cooler_aio_s"),
        ("cooler:aio-360", "cooler", "cooler_aio_m"),
        ("cooler:aio-420", "cooler", "cooler_aio_l"),
        ("cooler:air-dual", "cooler", "cooler_air_l"),
        ("cooler:air-tower", "cooler", "cooler_air_m"),
        ("cooler:air-low", "cooler", "cooler_air_s"),
        ("psu:650w-bronze", "psu", "psu_s"),
        ("psu:750w-gold", "psu", "psu_s"),
        ("psu:850w-gold", "psu", "psu_m"),
        ("psu:1000w-platinum", "psu", "psu_m"),
        ("psu:1200w-platinum", "psu", "psu_l"),
        ("case:cougar-fv270", "case", "case"),
        ("monitor:24-fhd-100-120hz-ips", "monitor", "monitor_s"),
        ("monitor:27-qhd-144-180hz-ips", "monitor", "monitor_m"),
        ("monitor:32-uhd-144-180hz-va", "monitor", "monitor_l"),
        ("monitor:34-uwqhd-144-180hz-va", "monitor", "monitor_xl"),
        ("monitor:49-dqhd-144-180hz-oled", "monitor", "monitor_xxl"),
        (None, "monitor", "monitor_m"),
        (None, "gpu", "gpu_m"),
        (None, None, "generic"),
    ],
)
def test_shipping_profiles(cfg, family, category, profile):
    assert cfg.profiles.for_product(family, category).id == profile


def test_bands_must_have_one_price():
    with pytest.raises(ValueError):
        WeightBand(up_to_kg=1.0)
    with pytest.raises(ValueError):
        WeightBand(up_to_kg=1.0, flat_usd=10.0, per_kg_usd=20.0)


def test_unsorted_courier_bands_are_rejected(tmp_path: Path):
    path = tmp_path / "couriers.yaml"
    path.write_text(
        """
couriers:
  bad:
    name: Bad
    site: https://bad.example
    source_url: https://bad.example
    source_kind: official
    verified_at: 2026-10-16
    warehouse: Miami
    sales_tax_rate: 0.07
    transit_days: [3, 7]
    weight_step_kg: 0.1
    bands:
      - {up_to_kg: 5.0, per_kg_usd: 20}
      - {up_to_kg: 1.0, flat_usd: 10}
    handling_usd: 5
    tfspu_rate: 0.1
""",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="sorted"):
        load_couriers(path)


def test_missing_rules_file(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        load_rules(tmp_path / "missing.yaml")
