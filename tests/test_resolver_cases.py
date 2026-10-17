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


# ---------------------------------------------------------------------------
# Catalog expansion (2026-10-16): motherboard, cooler, psu, case, monitor.
# ---------------------------------------------------------------------------

MOBO, COOLER, PSU, CASE, MONITOR = (
    Category.MOTHERBOARD,
    Category.COOLER,
    Category.PSU,
    Category.CASE,
    Category.MONITOR,
)


def parsed(category, title, brand=None):
    r = resolve_title(category, title, brand)
    assert r.parsed is not None, (title, r.status, r.notes)
    return r.parsed


@pytest.mark.parametrize(
    ("title", "socket", "igpu"),
    [
        ("Procesador Intel Core i5-12400F", "LGA1700", False),
        ("Procesador Intel Core i3-10105 LGA1200", "LGA1200", True),
        ("Intel Core Ultra 7 265KF", "LGA1851", False),
        ("AMD Ryzen 5 5600 AM4", "AM4", False),
        ("AMD Ryzen 5 5600GT", "AM4", True),
        ("AMD Ryzen 7 7800X3D", "AM5", True),
        ("AMD Ryzen 5 7500F", "AM5", False),
        ("AMD Ryzen 7 8700G", "AM5", True),
    ],
)
def test_cpu_socket_and_integrated_graphics(title, socket, igpu):
    specs = parsed(CPU, title).specs
    assert (specs["socket"], specs["igpu"]) == (socket, igpu)


@pytest.mark.parametrize(
    ("title", "family"),
    [
        ("Motherboard ASUS TUF Gaming B650M-E WiFi DDR5 AM5", "B650 mATX (AM5)"),
        ("Mother Asus Prime B650EM-A AM5", "B650E mATX (AM5)"),
        ("Motherboard Gigabyte H610I DDR4 LGA1700 Mini-ITX", "H610 ITX (LGA1700)"),
        ("Motherboard Asrock X870e Nova Wifi Am5", "X870E ATX (AM5)"),
        ("Motherboard ASUS Prime H510M-A DDR4 LGA1200", "H510 mATX (LGA1200)"),
    ],
)
def test_motherboard_family(title, family):
    assert parsed(MOBO, title).family_name == family


def test_motherboard_memory_is_part_of_the_identity_on_lga1700():
    d4 = parsed(MOBO, "Motherboard ASUS Prime B760M-A D4 DDR4 LGA1700")
    d5 = parsed(MOBO, "Motherboard Asus Prime B760M-A DDR5 Socket LGA1700 Intel M.2")
    assert d4.fingerprint and d5.fingerprint and d4.fingerprint != d5.fingerprint
    # Same name without the memory type: family only, never merged with either.
    unknown = resolve_title(MOBO, "Motherboard Gigabyte Z790M AORUS ELITE AX")
    assert unknown.status == "family_only" and "memory_unknown" in unknown.notes


def test_motherboard_merges_store_spellings():
    a = parsed(MOBO, "Motherboard ASRock B760M Steel Legend WiFi DDR5 LGA1700")
    b = parsed(MOBO, "Motherboard Asrock B760m St Legend Wifi Ddr5 S1700")
    assert a.fingerprint == b.fingerprint


def test_motherboard_unknown_word_is_not_merged():
    r = resolve_title(MOBO, "Motherboard MSI B760 Gaming PLUS WIFI/A DDR5")
    assert r.status == "family_only"


def test_cooler_with_a_chipset_like_code_is_not_a_motherboard():
    assert resolve_title(MOBO, "Cooler Master HYPER H410R Red").status == "rejected"


@pytest.mark.parametrize(
    ("title", "family"),
    [
        ("Monitor Samsung Odyssey G5 27″ Curvo VA 2k 165Hz", '27" QHD 1440p (2K) 144–180 Hz VA'),
        ("Monitor ASUS VA249HG 23.8″ 1080p 120Hz IPS 1ms", '24" Full HD 1080p 100–120 Hz IPS'),
        (
            "Monitor Asus TUF VG34VQL3A 34″ WQHD Curvo 180Hz",
            '34" Ultrawide 1440p 144–180 Hz panel sin dato',
        ),
        ("Monitor Teros TE-2790G 27″ 1440p 240Hz QD-OLED", '27" QHD 1440p (2K) 200–280 Hz OLED'),
        ("Monitor Samsung Odyssey G7 27″ IPS 4K 180hz/360hz", '27" 4K UHD 144–180 Hz IPS'),
        ("Monitor Gigabyte GS25F2 25″ 200hz IPS 1Ms", '25" Full HD 1080p 200–280 Hz IPS'),
        ("Monitor Xiaomi 4K 27 UHD Profesional", '27" 4K UHD Hz sin dato panel sin dato'),
        # "400H" is part of the model code, not 400 Hz
        ('MONITOR 20" LG 20MK400H-B (HD/HDMI/VGA/VESA)', '20" HD Hz sin dato panel sin dato'),
        ("Monitor Samsung G9 49″OLED DQHD5120X1440 240H 0.03", '49" Doble QHD 200–280 Hz OLED'),
    ],
)
def test_monitor_family(title, family):
    assert parsed(MONITOR, title).family_name == family


def test_monitor_needs_a_model_code_to_be_an_exact_product():
    coded = parsed(MONITOR, "Monitor Gigabyte MO27Q28G QHD 27″ 280Hz OLED")
    other_store = parsed(MONITOR, "Monitor Gigabyte MO27Q28G 27″ QHD OLED 280Hz 0.03ms")
    assert coded.fingerprint and coded.fingerprint == other_store.fingerprint
    series = resolve_title(MONITOR, "Monitor Samsung Odyssey G5 Curvo 27″ 2K 165Hz")
    assert series.status == "family_only"


def test_samsung_monitor_codes_are_normalized():
    a = parsed(MONITOR, "Monitor Samsung Odyssey G9 Curvo 49″ OLED DQHD 144hz S49DG910SN")
    b = parsed(
        MONITOR, "Monitor Curvo Gaming OLED Samsung Odyssey G9 49'' Doble QHD 144Hz LS49DG910SNXZA"
    )
    assert a.fingerprint == b.fingerprint


def test_case_title_is_not_a_monitor():
    assert resolve_title(MONITOR, "Gabinete DeepCool Matrexx 50 ADD-RGB 4F").status == "rejected"


@pytest.mark.parametrize(
    ("title", "family", "series"),
    [
        (
            "Fuente Gigabyte UD750GM PG5 V2 ICE 80 Plus Gold Full Modular",
            "750 W 80 Plus Gold",
            "UD GM",
        ),
        ("Fuente Cougar Atlas 600 80 Plus Bronce", "600 W 80 Plus Bronze", "Atlas"),
        (
            "Fuente ASUS TUF Gaming 1000G 1000W 80 Plus Gold ATX 3.1 Blanca",
            "1000 W 80 Plus Gold",
            "TUF Gaming",
        ),
        (
            "Fuente be quiet! Power Zone 2 750W Platium Modular",
            "750 W 80 Plus Platinum",
            "Power Zone 2",
        ),
        ("Fuente Cougar STC 600W 80 Plus White", "600 W 80 Plus", "STC"),
        ("Fuente ATX Xtreme 500w 24+4 pin", "500 W certificación sin dato", None),
    ],
)
def test_psu_family_and_series(title, family, series):
    p = parsed(PSU, title)
    assert (p.family_name, p.line) == (family, series)


def test_psu_rating_is_part_of_the_identity():
    gold = parsed(PSU, "Fuente ASRock PRO 650G 650W 80 Plus Gold ATX 3.1")
    bronze = parsed(PSU, "Fuente ATX ASRock PRO 650G 650W 80 Plus Bronze")
    assert gold.fingerprint != bronze.fingerprint


def test_case_with_psu_is_a_case():
    assert resolve_title(PSU, "Gabinete NSX 902M 3 fanes + fuente 650W 80+ Bronce").status == (
        "rejected"
    )
    p = parsed(CASE, "Gabinete NSX 902M 3 fanes + fuente 650W 80+ Bronce")
    assert p.specs["psu_w"] == 650


@pytest.mark.parametrize(
    ("title", "family"),
    [
        ("Water Cooling DeepCool LE360 V2 360mm ARGB blanco", "Refrigeración líquida 360 mm"),
        ("Water Cooling DeepCool LS720S Zero Dark 360mm", "Refrigeración líquida 360 mm"),
        ("Cooler DeepCool AK400 G2 blanco", "Cooler de aire de una torre"),
        ("Cooler DeepCool AK620 Digital SE", "Cooler de aire de doble torre"),
        ("Cooler AMD Wraith Stealth", "Cooler de aire top-flow / bajo perfil"),
    ],
)
def test_cooler_family(title, family):
    assert parsed(COOLER, title).family_name == family


@pytest.mark.parametrize(
    "title",
    [
        "Fan DeepCool FL12 120mm Blanco",
        "Fan Cooler be quiet! Pure Wings 3 White",
        "Hub Deepcool SC790",
    ],
)
def test_case_fans_are_not_cpu_coolers(title):
    assert resolve_title(COOLER, title).status == "rejected"


def test_cooler_color_and_rgb_are_products_of_the_same_family():
    black = parsed(COOLER, "Watercooling Cougar Poseidon Elite 360 ARGB BK")
    white = parsed(COOLER, "Water Cooling Cougar Poseidon Elite 360 Argb Wh")
    assert black.family_id == white.family_id and black.fingerprint != white.fingerprint


def test_case_family_is_the_model():
    black = parsed(CASE, "Gabinete Cougar Fv270 Rgb Black")
    white = parsed(CASE, "Gabinete Cougar Fv270 White")
    assert black.family_name == white.family_name == "Cougar FV270"
    assert black.fingerprint != white.fingerprint


def test_case_model_code_fallback_keeps_the_edition():
    assert (
        parsed(CASE, "Gabinete Cooler Master HAF700").fingerprint
        != parsed(CASE, "Gabinete Cooler Master HAF700 EVO").fingerprint
    )
