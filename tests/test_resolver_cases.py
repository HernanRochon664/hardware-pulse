"""
Hand-written resolution cases, starting with every misclassification found in
the v1 audit (2026-10-14). These are independent of the generated gold set.
"""

from __future__ import annotations

import pytest

from src.domain.models import Category, Condition
from src.entities.mpn import extract_mpn
from src.entities.resolver import resolve_title

GPU, CPU, RAM, SSD = Category.GPU, Category.CPU, Category.RAM, Category.SSD


def family(category, title, brand=None):
    r = resolve_title(category, title, brand)
    return r.parsed.family_name if r.parsed else None


@pytest.mark.parametrize(
    ("title", "expected"),
    [
        # v1 put all of these in the wrong bucket
        ("GPU Gigabyte GeForce RTX 5070 Ti Eagle OC SFF 16GB", "RTX 5070 Ti 16GB"),
        ("TARJETA DE VIDEO PALIT GEFORCE RTX 5060TI INFINITY 3 OC 16GB", "RTX 5060 Ti 16GB"),
        ("GIGABYTE GEFORCE RTX3080 GV-N3080IXEB-10GD GA20- REF", "RTX 3080 10GB"),
        ("GPU ASrock Intel Arc A380 Challenger 6Gb DDR6", "Arc A380 6GB"),
        ("GPU XFX Radeon RX9060 XT Swift Dual OC 8Gb", "RX 9060 XT 8GB"),
        ("Tarjeta de video Gigabyte RX 9070 GRE 12GB Gaming OC", "RX 9070 GRE 12GB"),
        # compact forms and Gigabyte part numbers
        ("TARJETA DE VIDEO GIGABYTE GV-N506TWF2OC-8GD 5060TI WINDFORCE OC 8G", "RTX 5060 Ti 8GB"),
        ("TARJETA DE VIDEO GIGABYTE RADEON GV-R906XGAMINGOCEICE-16GB", "RX 9060 XT 16GB"),
        ("TARJETA DE VIDEO GIGABYTE RADEON GV-R9070XTAORUS E-16GD", "RX 9070 XT 16GB"),
        ("TARJETA DE VIDEO GIGABYTE RADEON 7600 GAMING OC GV-R76GAMING OC-8GD", "RX 7600 8GB"),
        ("Tarjeta de video Inno3d RTX 5070 12Bb Twin X2", "RTX 5070 12GB"),  # typo, VRAM inferred
        ("TARJETA DE VIDEO MSI GEFORCE N 210-MD1G/D3", "GT 210 1GB"),
    ],
)
def test_gpu_families(title, expected):
    assert family(GPU, title) == expected


def test_gpu_same_product_across_stores_and_part_numbers():
    titles = [
        "GPU Gigabyte GeForce RTX5060 Eagle OC ICE 8Gb",
        "Tarjeta de Video GIGABYTE RTX 5060 8Gb EAGLE OC ICE blanca",
        "TARJETA DE VIDEO GIGABYTE GEFORCE RTX 5060 EAGLE OC GV-N5060EAGLEOC ICE-8GD 8GB GDDR6",
    ]
    fingerprints = {resolve_title(GPU, t).parsed.fingerprint for t in titles}  # type: ignore[union-attr]
    assert len(fingerprints) == 1


def test_gpu_variants_stay_separate():
    def fp(title):
        return resolve_title(GPU, title).parsed.fingerprint  # type: ignore[union-attr]

    assert fp("GPU Gigabyte GeForce RTX5060 Eagle OC 8Gb") != fp(
        "GPU Gigabyte GeForce RTX5060 Eagle OC ICE 8Gb"
    )
    assert fp("Tarjeta Inno3d Geforce RTX 5060 8GB Twin X2") != fp(
        "Tarjeta Inno3d Geforce Rtx 5060 8gb Twin X2 Wh"
    )
    assert fp("GPU Palit Geforce RTX5060 Ti Dual 8Gb") != fp(
        "GPU Palit Geforce RTX5060 Ti Dual 16Gb"
    )


def test_gpu_unknown_line_is_never_merged():
    r = resolve_title(GPU, "GPU PNY GeForce RTX 5070 OC 12Gb GDDR7")
    assert r.status == "family_only" and r.parsed is not None and r.parsed.fingerprint is None


@pytest.mark.parametrize(
    ("title", "expected"),
    [
        (
            "Procesador CPU Intel Core I3-4130 3.4Ghz Socket LGA1150 2 Nucleos (Tray)",
            "Intel Core i3-4130",
        ),
        ("CPU AMD Ryzen 7 5800X3D AM4", "AMD Ryzen 7 5800X3D"),
        ("PROCESADOR AMD RYZEN 7 9850X3D AM5 8C/16T 5.6GHZ X3D", "AMD Ryzen 7 9850X3D"),
        ("Intel Core i5 12400 Alder Lake 1700", "Intel Core i5-12400"),
        ("Intel Core i5 12400F", "Intel Core i5-12400F"),
        ("CPU Intel Core i7 12700KF Alder Lake 1700", "Intel Core i7-12700KF"),
        ("CPU AMD Ryzen 5 7600X3D AM5", "AMD Ryzen 5 7600X3D"),
        ("CPU AMD Ryzen 5 5600GE AM4", "AMD Ryzen 5 5600GE"),
        ("CPU AMD Ryzen 9 9950X3D2 Dual Edition AM5", "AMD Ryzen 9 9950X3D2"),
        ("Intel Core Ultra i5 245K", "Intel Core Ultra 5 245K"),
        ("Procesador Intel i5 225F", "Intel Core Ultra 5 225F"),
        ("CPU Intel Core Ultra 7 270K Plus Arrow Lake 1851", "Intel Core Ultra 7 270K Plus"),
        ("PROCESADOR AMD RYZEN 9950X3D", "AMD Ryzen 9 9950X3D"),  # tier inferred
        ("PROCESADOR INTEL DUAL CORE G7400 (1700) 12VA", "Intel Pentium Gold G7400"),
    ],
)
def test_cpu_models(title, expected):
    assert family(CPU, title) == expected


def test_cpu_typos_and_accessories_are_not_guessed():
    assert resolve_title(CPU, "Procesador CPU AMD Ryzen 5 86006 6 Nucleos AM5").status == "unparsed"
    assert resolve_title(CPU, "Pasta térmica Aerocool Baraf").status == "rejected"


@pytest.mark.parametrize(
    ("title", "expected"),
    [
        ("NETAC MEMORIA SHADOW RGB 2X8GB 16GB C16 3600 SILVER", "DDR4 16GB 3600 MT/s DIMM"),
        (
            "Memoria DDR4 Corsair Vengeance LPX 32Gb (2x16Gb) 3200MHz DIMM",
            "DDR4 32GB 3200 MT/s DIMM",
        ),
        ("Memoria Sodimm Kingston Fury Impact 32gb DDR4 3200Mhz", "DDR4 32GB 3200 MT/s SODIMM"),
        (
            "Memoria RAM SODIMM Kingston 16GB DDR4 3200MHz PC4-25600 CL22",
            "DDR4 16GB 3200 MT/s SODIMM",
        ),
        ("MEMORIA DDR5 KINGSTON FURY KF556C40BBA-16GB DDR5 RGB", "DDR5 16GB 5600 MT/s DIMM"),
        ("MEMORIA DDR5 NETAC SHADOW II 32GB (2X16) NTSWD5P60DP-32W", "DDR5 32GB 6000 MT/s DIMM"),
        ("MEMORIA DDR5 R-DIMM KINGSTON FURY 16GB KF560R32RB-16GB", "DDR5 16GB 6000 MT/s RDIMM"),
        ("Memoria RAM 2GB DDR3 PC3-10600U 1333MHZ Usadas con Garantia", "DDR3 2GB 1333 MT/s DIMM"),
    ],
)
def test_ram_families(title, expected):
    assert family(RAM, title) == expected


def test_ram_conditions():
    assert (
        resolve_title(RAM, "Memoria RAM SODIMM 4GB DDR4 2666MHz - New Pulled").condition
        == Condition.USED
    )
    assert (
        resolve_title(RAM, "Memoria RAM 2GB DDR3 PC3-12800 1600MHZ Usadas con Garantia").condition
        == Condition.USED
    )
    assert resolve_title(RAM, "MEMORIA MICRO SD 64GB KINGSTON CLASE 10").status == "rejected"


@pytest.mark.parametrize(
    ("title", "expected"),
    [
        ("Kingston SSD PCIe NVMe NV3 4Tb GEN 4.0", "SSD NVMe Gen4 4TB"),
        ("Hiksemi Wave SSD 1024Gb", "SSD SATA 1TB"),
        ("Hiksemi Wave SSD 960Gb", "SSD SATA 960GB"),
        ("Disco SSD Hiksemi Wave 1Tb M.2 NVMe PCIe 3.0", "SSD NVMe Gen3 1TB"),
        ("DISCO SSD 2.5 PATRIOT BURST ELITE 1.92TB SATA III PBE192TS25SSDR", "SSD SATA 1.92TB"),
        ("Disco Externo SSD Kingston XS2000 2Tb", "SSD externo 2TB"),
        (
            "DISCO SSD M.2 CORSAIR MP600 PRO 2TB PCIE 4.0 GEN4 CSSD-F2000GBMP600PXT",
            "SSD NVMe Gen4 2TB",
        ),
    ],
)
def test_ssd_families(title, expected):
    assert family(SSD, title) == expected


@pytest.mark.parametrize(
    "title",
    [
        "Disco externo Toshiba Canvio 1tb usb 3.0",
        "DISCO 4TB SEAGATE SATA SKYHAWK (VIGILANCIA)",
        "BANDEJA DISCO M.2 NVME Y SATA USB C 3.2 UGREEN",
        "SSD FLASH NETAC US2 128GB USB 3.2 NT03US2N-128G-32SL",
        "DISCO EXTERNO 1TB TOSHIBA 2.5 USB 3.0",
    ],
)
def test_ssd_category_rejects_non_ssds(title):
    assert resolve_title(SSD, title).status == "rejected"


def test_ssd_models_with_different_capacity_or_interface_differ():
    def fp(title):
        return resolve_title(SSD, title).parsed.fingerprint  # type: ignore[union-attr]

    assert fp("Kingston SSD PCIe NVMe NV3 1Tb GEN 4.0") != fp(
        "Kingston SSD PCIe NVMe NV3 4Tb GEN 4.0"
    )
    assert fp("Disco SSD Hiksemi Wave 1Tb 2.5 SATA") != fp(
        "Disco SSD Hiksemi Wave 1Tb M.2 NVMe PCIe 3.0"
    )


def test_open_box_is_detected():
    r = resolve_title(GPU, "MSI GEFORCE 3060 VENTUS 2X 12G OC OPEN BOX")
    assert r.condition == Condition.OPEN_BOX


@pytest.mark.parametrize(
    ("title", "expected"),
    [
        (
            "TARJETA DE VIDEO GIGABYTE GEFORCE RTX 5060 EAGLE OC GV-N5060EAGLEOC ICE-8GD 8GB",
            "GVN5060EAGLEOCICE8GD",
        ),
        ("TARJETA VIDEO MSI GEFORCE RTX5060 8GB GAMING OC 912-V537-023", "912V537023"),
        ("DISCO SSD M.2 KINGSTON NV3 1TB PCIE 4.0 NVME SNV3S/1000G", "SNV3S1000G"),
        ("Memoria RAM DDR4 16GB 3200MHz Hiksemi Armor HSC416U32Z2 1.2V CL22 UDIMM", "HSC416U32Z2"),
        ("GPU Palit Geforce RTX5060 Dual 8Gb GDDR7", None),
    ],
)
def test_mpn_extraction(title, expected):
    assert extract_mpn(title) == expected
