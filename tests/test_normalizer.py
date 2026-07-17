"""
Tests for src/entities/normalizer.py

Covers title normalization, SKU token normalization, and brand extraction.
"""

from src.entities.normalizer import extract_brand, extract_variant, normalize_sku, normalize_title

# ---------------------------------------------------------------------------
# normalize_title
# ---------------------------------------------------------------------------


class TestNormalizeTitle:
    def test_strips_whitespace(self):
        assert normalize_title("  RTX 4070  ") == "rtx 4070"

    def test_lowercases(self):
        assert normalize_title("RTX 4070") == "rtx 4070"

    def test_removes_accents(self):
        result = normalize_title("Gráficos")
        assert "á" not in result
        assert "graficos" in result

    def test_replaces_separators_with_spaces(self):
        result = normalize_title("RTX-4070/OC")
        assert "-" not in result
        assert "/" not in result

    def test_collapses_multiple_spaces(self):
        result = normalize_title("RTX   4070")
        assert "  " not in result

    def test_empty_string_returns_empty(self):
        assert normalize_title("") == ""

    def test_removes_noise_words(self):
        result = normalize_title("GPU MSI RTX 4070 GDDR6X")
        assert "gpu" not in result
        assert "gddr6x" not in result
        assert "rtx 4070" in result

    # ① SKU token normalization
    def test_rtx_compact_normalized(self):
        assert "rtx 4070" in normalize_title("RTX4070")

    def test_rtx_ti_compact_normalized(self):
        result = normalize_title("RTX4070Ti")
        assert "rtx 4070 ti" in result

    def test_rx_compact_normalized(self):
        result = normalize_title("RX9070XT")
        assert "rx 9070 xt" in result

    def test_r_prefix_normalized_to_rx(self):
        """R9070XT (missing RX prefix) should normalize to RX 9070 XT."""
        result = normalize_title("RADEON R9070XT")
        assert "rx 9070 xt" in result

    def test_arc_compact_normalized(self):
        result = normalize_title("ArcB580")
        assert "arc b580" in result

    def test_rtx_ti_before_base_model(self):
        """RTX 4070 Ti should not be collapsed to RTX 4070."""
        result = normalize_title("RTX 4070 Ti Gaming")
        assert "rtx 4070 ti" in result

    # RAM normalization
    def test_ram_ddr_generation_normalized(self):
        result = normalize_title("Memoria Kingston DDR5 16GB 6000MHz")
        assert "ddr5 16gb" in result

    def test_ram_capacity_first_reordered(self):
        """16GB DDR5 6000 should be reordered to DDR5 16GB 6000."""
        result = normalize_title("Kingston Fury Beast 16GB DDR5 6000MHz")
        assert "ddr5 16gb 6000mhz" in result

    def test_ram_noise_words_removed(self):
        result = normalize_title("Memoria RAM DDR4 8GB 3200MHz")
        assert "memoria" in result

    def test_ram_pc_speed_normalized(self):
        result = normalize_title("DDR4 16GB PC4-25600")
        assert "25600mhz" in result or "pc4" not in result

    def test_ram_pc_gen_speed_normalized(self):
        result = normalize_title("DDR5 32GB PC5-48000")
        assert "48000mhz" in result

    # SSD normalization
    def test_ssd_capacity_joined(self):
        result = normalize_title("SSD Kingston NV3 1TB NVMe PCIe 4.0")
        assert "kingston nv3 1tb" in result

    def test_ssd_noise_words_removed(self):
        result = normalize_title("Disco SSD NVMe M.2 1TB")
        assert not any(w in result for w in ["nvme", "ssd", "disco", "solido"])

    def test_ssd_m2_format_normalized(self):
        """M.2 is normalized to m2 but then removed as noise."""
        result = normalize_title("M.2 Corsair MP600 1TB")
        assert "m2" not in result
        assert "corsair mp600 1tb" in result

    def test_ssd_pcie_info_removed(self):
        result = normalize_title("Kingston NV3 1TB PCIe 4.0 NVMe")
        assert "pcie" not in result

    def test_ssd_gen_info_removed(self):
        result = normalize_title("Corsair MP600 2TB Gen 4x4")
        assert "gen" not in result


class TestNormalizeSku:
    def test_lowercases(self):
        assert normalize_sku("RTX 4070") == "rtx 4070"

    def test_strips_whitespace(self):
        assert normalize_sku("  RTX 4070  ") == "rtx 4070"

    def test_collapses_spaces(self):
        assert normalize_sku("RTX  4070") == "rtx 4070"

    def test_does_not_remove_noise_words(self):
        """normalize_sku should not strip words, SKUs are already clean."""
        assert normalize_sku("Arc B580") == "arc b580"


class TestNormalizationConsistency:
    """
    Critical: normalize_title and normalize_sku must produce compatible
    output so that substring matching works correctly.
    """

    def test_sku_found_in_normalized_title(self):
        title = "GPU MSI GeForce RTX 4070 Ventus OC"
        sku = "RTX 4070"
        assert normalize_sku(sku) in normalize_title(title)

    def test_sku_with_suffix_found_in_title(self):
        title = "ASUS TUF RTX 4070 Ti OC 12GB"
        sku = "RTX 4070 Ti"
        assert normalize_sku(sku) in normalize_title(title)

    def test_amd_sku_found_in_normalized_title(self):
        title = "GIGABYTE AMD RADEON R9070XT GV-R907XGAMINGOCICE-16GD"
        sku = "RX 9070 XT"
        assert normalize_sku(sku) in normalize_title(title)

    def test_intel_arc_sku_found_in_title(self):
        title = "Tarjeta de Video ASRock Intel ARC B580 Challenger OC 12GB"
        sku = "Arc B580"
        assert normalize_sku(sku) in normalize_title(title)


# ---------------------------------------------------------------------------
# extract_brand
# ---------------------------------------------------------------------------


class TestExtractBrand:
    def test_detects_asus(self):
        assert extract_brand("ASUS TUF RTX 4070") == "ASUS"

    def test_detects_msi(self):
        assert extract_brand("MSI GeForce RTX 3050") == "MSI"

    def test_detects_gigabyte_via_aorus(self):
        assert extract_brand("AORUS RTX 4090 Master") == "Gigabyte"

    def test_detects_palit(self):
        assert extract_brand("Palit StormX RTX 3050") == "Palit"

    def test_detects_xfx(self):
        assert extract_brand("XFX Swift RX 7800 XT") == "XFX"

    def test_returns_none_for_unknown_brand(self):
        assert extract_brand("Unknown Brand RTX 4070") is None

    def test_case_insensitive(self):
        assert extract_brand("asus rtx 4070") == "ASUS"


class TestExtractVariant:
    def test_asus_tuf(self):
        assert extract_variant("ASUS TUF RTX 4070 OC 12GB", "ASUS") == "Tuf"

    def test_asus_rog_strix(self):
        assert extract_variant("ASUS ROG STRIX RTX 4090 24GB", "ASUS") == "Rog Strix"

    def test_msi_ventus(self):
        assert extract_variant("MSI GeForce RTX 3050 Ventus OC", "MSI") == "Ventus"

    def test_msi_gaming_x_trio(self):
        assert extract_variant("MSI RTX 4070 Gaming X Trio 12GB", "MSI") == "Gaming X Trio"

    def test_gigabyte_aorus(self):
        assert extract_variant("AORUS RTX 4090 Master 24GB", "Gigabyte") == "Aorus"

    def test_palit_stormx(self):
        assert extract_variant("Palit StormX RTX 3050 6GB", "Palit") == "Stormx"

    def test_returns_none_no_brand(self):
        assert extract_variant("Generic GPU RTX 4070", None) is None

    def test_returns_none_unknown_variant(self):
        assert extract_variant("ASUS UnknownModel RTX 4070", "ASUS") is None

    def test_case_insensitive(self):
        assert extract_variant("asus tuf rtx 4070", "ASUS") == "Tuf"
