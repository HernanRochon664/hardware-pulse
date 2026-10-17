"""Build compatibility warnings (armado por componente)."""

from __future__ import annotations

from src.domain.models import Category
from src.entities.compat import BuildPart, check_build, fits
from src.entities.resolver import resolve_title


def part(category: Category, title: str) -> BuildPart:
    parsed = resolve_title(category, title).parsed
    assert parsed is not None, title
    return BuildPart(category, parsed.name, parsed.specs)


RYZEN_AM5 = part(Category.CPU, "AMD Ryzen 7 7800X3D")
RYZEN_AM4_NO_IGPU = part(Category.CPU, "AMD Ryzen 5 5600 AM4")
B650 = part(Category.MOTHERBOARD, "Motherboard ASUS TUF Gaming B650M-E WiFi DDR5 AM5")
B550 = part(Category.MOTHERBOARD, "Motherboard MSI B550M-A PRO DDR4 AM4")
H610_NO_DDR = part(Category.MOTHERBOARD, "MOTHERBOARD GIGABYTE H610M H (1700)")
DDR5 = part(Category.RAM, "Memoria Kingston Fury Beast 32GB (2x16GB) DDR5 6000MHz")
DDR4 = part(Category.RAM, "Memoria Kingston Fury Beast 16GB DDR4 3200MHz")
SODIMM = part(Category.RAM, "Memoria Kingston 16GB DDR5 5600 SODIMM Notebook")
GPU = part(Category.GPU, "GPU Gigabyte GeForce RTX5060 Eagle OC 8Gb")


def levels(parts):
    return [i.level for i in check_build(parts)]


def test_matching_build_has_no_issues():
    assert check_build([RYZEN_AM5, B650, DDR5, GPU]) == []


def test_socket_mismatch_is_a_warning():
    issues = check_build([RYZEN_AM5, B550])
    assert [i.level for i in issues] == ["warning"]
    assert "AM5" in issues[0].message and "AM4" in issues[0].message


def test_memory_generation_mismatch_and_sodimm():
    assert levels([B650, DDR4]) == ["warning"]
    assert "SODIMM" in check_build([B650, SODIMM])[0].message


def test_lga1700_board_without_memory_type_asks_to_confirm():
    assert levels([H610_NO_DDR, DDR4]) == ["info"]


def test_cpu_without_integrated_graphics_needs_a_gpu():
    assert levels([RYZEN_AM4_NO_IGPU, B550, DDR4]) == ["info"]
    assert check_build([RYZEN_AM4_NO_IGPU, B550, DDR4, GPU]) == []


def test_board_larger_than_a_case_that_says_its_form():
    case = BuildPart(Category.CASE, "Gabinete chico", {"form": "Micro ATX"})
    atx = part(Category.MOTHERBOARD, "Motherboard Asrock X870 Riptide Wifi D5 Am5")
    assert levels([atx, case]) == ["warning"]
    assert check_build([B650, case]) == []
    assert check_build([atx, BuildPart(Category.CASE, "Sin dato", {})]) == []


def test_fits_filters_options_against_chosen_parts():
    assert fits(B650, [RYZEN_AM5])
    assert not fits(B550, [RYZEN_AM5])
    assert fits(RYZEN_AM5, [B650, DDR5])
    # Replacing the part of the same category is not a clash.
    assert fits(B550, [B650, RYZEN_AM4_NO_IGPU])
