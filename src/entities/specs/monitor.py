"""
Monitor title parser.

Family  = size class + resolution + refresh-rate range + panel
          ('27" QHD 1440p (2K) 144–180 Hz IPS'): what a buyer compares across
          brands. A title without the refresh rate or the panel type goes to a
          family "sin dato" of its own; it is never put with the others.
Product = brand + model code + size + resolution + refresh rate. Only titles
          with a real model code (VG34VQL3A, MO27Q28G, LS27DG602S) identify the
          product; "Samsung Odyssey G5 27 2K 165Hz" names a series that has had
          several models, so it stays family_only.

Sizes are rounded to the nearest inch for the family (23.8" and 24" are the
same class); the exact size, refresh rate, curvature and portability are
specs.
"""

from __future__ import annotations

import re

from src.domain.models import Category
from src.entities.specs import Parsed, ParseFailure, join_name
from src.entities.taxonomy import Taxonomy, match_model
from src.entities.text import fold

_SIZE_MARKED = re.compile(
    r"(?<![\d.,])(\d{2}(?:[.,]\d{1,2})?) ?(?:\"|′′|′|''|’’|\?|pulgadas|pulg\b|inch(?:es)?\b|in\b|')"
)
_BARE_NUMBER = re.compile(r"^\d{2}(?:[.,]\d{1,2})?$")
_MARKER = re.compile(r"\bmonitor|\bpantalla\b|\bdisplay\b")
# Other products whose titles also carry a screen size, resolution or Hz.
_OTHER = re.compile(
    r"\bnotebook|\blaptop|\bportatil gamer|\btablet|\bcelular|\bsmartphone|\bmotherboard"
    r"|\bmother\b|\bgabinete|\bfuente\b|\bcooler\b|\bproyector|\btelevisor|\bsmart tv\b"
    r"|\btarjeta de video|\bgpu\b"
)
_PIXELS = re.compile(r"(\d{3,4}) ?[x×*] ?(\d{3,4})")
_HZ = re.compile(r"(?<![\d.])(\d{2,3}) ?h(?:z|\b)")
_CODE_TOKEN = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*")
_SAMSUNG_CODE = re.compile(r"([scfu]\d{2}[a-z]{1,2}\d{2,3})")
_MSI_SUFFIX = re.compile(r"^[xe]\d{2}[a-z]?$")
# Resolution letter of the model code: Gigabyte GS27Q / M27U / MO34WQC2, MSI 272F / 321UPD.
_CODE_RESOLUTION = {
    "Gigabyte": (re.compile(r"^(?:gs|mo|fo|go|m|g)\d{2}(wq|q|u|f)"), {"wq": "uwqhd"}),
    "MSI": (re.compile(r"^(?:mp|g)?\d{3,4}c?(q|u|f)"), {}),
}
_CODE_CLASSES = {"q": "qhd", "u": "uhd", "f": "fhd"}
_NOT_A_CODE = re.compile(
    r"\d{2,4}x\d{2,4}|hz|^\d+(?:h|ms|m|mm|cd|nits|p|k|w|gb|r)$|^(?:hdr|dp|hdmi|usb|vesa|r)\d"
)

# key: (pixels, short label, family label)
RESOLUTIONS = {
    "hd": (1600 * 900, "HD", "HD"),
    "fhd": (1920 * 1080, "Full HD", "Full HD 1080p"),
    "uwfhd": (2560 * 1080, "UW Full HD", "Ultrawide 1080p"),
    "qhd": (2560 * 1440, "QHD", "QHD 1440p (2K)"),
    "uwqhd": (3440 * 1440, "UWQHD", "Ultrawide 1440p"),
    "uhd": (3840 * 2160, "4K", "4K UHD"),
    "dqhd": (5120 * 1440, "DQHD", "Doble QHD"),
    "5k2k": (5120 * 2160, "5K2K", "5K2K"),
    "5k": (5120 * 2880, "5K", "5K"),
}
_PIXEL_CLASSES = {
    (1366, 768): "hd",
    (1600, 900): "hd",
    (1280, 720): "hd",
    (1920, 1080): "fhd",
    (1920, 1200): "fhd",
    (2560, 1080): "uwfhd",
    (2560, 1440): "qhd",
    (2560, 1600): "qhd",
    (3440, 1440): "uwqhd",
    (3840, 2160): "uhd",
    (5120, 1440): "dqhd",
    (5120, 2160): "5k2k",
    (5120, 2880): "5k",
}
_LABELS = [
    ("dqhd", re.compile(r"\bd(?:ual |oble )?qhd")),
    ("uwqhd", re.compile(r"\buwqhd\b|\bultra ?wide ?(?:quad hd|w?qhd)|\bultra wqhd\b")),
    ("5k2k", re.compile(r"\bwuhd\b")),
    ("5k", re.compile(r"\b5k\b")),
    ("uhd", re.compile(r"\b4k\b|\buhd|\b2160p\b|\bultra hd\b")),
    ("uwfhd", re.compile(r"\bu?wfhd\b|\bultra ?wide full hd\b")),
    ("qhd", re.compile(r"\b(?:w?qhd|quad hd|1440p|2k)\b|\bqhd")),
    ("fhd", re.compile(r"\bfull ?hd\b|\bfhd|\b1080p?\b")),
    ("hd", re.compile(r"\bhd\b|\b720p\b")),
]
_PANELS = [
    ("OLED", re.compile(r"oled")),
    ("IPS", re.compile(r"\bips\b|ips\b|\bpls\b|\bahva\b")),
    ("VA", re.compile(r"\b[mr]?va\b")),
    ("TN", re.compile(r"\btn\b")),
]
_MINI_LED = re.compile(r"\bmini ?led\b")
_CURVED = re.compile(r"\bcurv[oa]s?\b|\bcurved\b|\bcurvado\b|\b\d{4} ?r\b|\br ?\d{4}\b")
_PORTABLE = re.compile(r"portatil|portable|zenscreen")
_TOUCH = re.compile(r"tactil|touch")


def hz_range(hz: int | None) -> tuple[str, str]:
    """(slug, label) of the refresh-rate range of a family."""
    if hz is None:
        return "hz-na", "Hz sin dato"
    for limit, slug, label in (
        (90, "60-75hz", "60–75 Hz"),
        (140, "100-120hz", "100–120 Hz"),
        (190, "144-180hz", "144–180 Hz"),
        (300, "200-280hz", "200–280 Hz"),
    ):
        if hz < limit:
            return slug, label
    return "300hz", "300+ Hz"


def _size(folded: str) -> float | ParseFailure:
    marked = [float(m.group(1).replace(",", ".")) for m in _SIZE_MARKED.finditer(folded)]
    marked = [s for s in marked if 10 <= s <= 65]
    if marked:
        return marked[0]
    bare = {
        float(t.replace(",", "."))
        for t in re.split(r"[\s/|()]+", folded)
        if _BARE_NUMBER.match(t) and 13 <= float(t.replace(",", ".")) <= 65
    }
    if len(bare) == 1:
        return bare.pop()
    return ParseFailure("ambiguous_size" if bare else "no_size")


def _code_resolution(code: str | None, brand: str | None) -> str | None:
    if not code or brand not in _CODE_RESOLUTION:
        return None
    pattern, extra = _CODE_RESOLUTION[brand]
    m = pattern.match(code)
    return (extra.get(m.group(1)) or _CODE_CLASSES[m.group(1)]) if m else None


def _resolution(folded: str, size: float) -> str | None:
    found: set[str] = set()
    for m in _PIXELS.finditer(folded):
        key = _PIXEL_CLASSES.get((int(m.group(1)), int(m.group(2))))
        if key:
            found.add(key)
    explicit_qhd = "qhd" in found
    for key, pattern in _LABELS:
        if pattern.search(folded):
            found.add(key)
    if "hd" in found and len(found) > 1:
        found.discard("hd")  # "Full HD", "HD 400Hz" in dual-mode titles
    if not found:
        return None
    best = max(found, key=lambda k: RESOLUTIONS[k][0])
    if best == "qhd" and size >= 34 and not explicit_qhd:
        best = "uwqhd"  # "34'' WQHD" is 3440x1440
    return best


def _panel(folded: str) -> str | None:
    panels = {name for name, pattern in _PANELS if pattern.search(folded)}
    if not panels and _MINI_LED.search(folded):
        return "Mini LED"
    return panels.pop() if len(panels) == 1 else None


def _model_code(folded: str, brand: str | None) -> str | None:
    brand_tokens = set(fold(brand or "").split())
    codes: list[str] = []
    for token in _CODE_TOKEN.findall(folded):
        packed = token.replace("-", "")
        if codes and _MSI_SUFFIX.match(packed):
            codes.append(packed)  # MSI "MAG 272F X24", "PRO MP271 E14A"
            continue
        digits = sum(c.isdigit() for c in packed)
        if (
            len(packed) < 4
            or digits < 2
            or digits == len(packed)
            or packed in brand_tokens
            or _NOT_A_CODE.search(packed)
        ):
            continue
        if brand == "Samsung" and (m := _SAMSUNG_CODE.search(packed)):
            packed = m.group(1)
        if not codes:
            codes.append(packed)
    return "".join(codes) or None


def parse_monitor(title: str, store_brand: str | None, tax: Taxonomy) -> Parsed | ParseFailure:
    folded = fold(title)
    size = _size(folded)
    if isinstance(size, ParseFailure):
        return size
    resolution = _resolution(folded, size)
    hz_values = sorted({int(m.group(1)) for m in _HZ.finditer(folded) if int(m.group(1)) >= 48})
    hz = hz_values[0] if hz_values else None  # dual-mode titles list the native rate first
    notes = ["dual_mode"] if len(hz_values) > 1 else []
    panel = _panel(folded)
    curved = bool(_CURVED.search(folded))
    portable = bool(_PORTABLE.search(folded))

    allowed = tax.brands_for(Category.MONITOR, tax.monitor_lines)
    brand = tax.detect_brand(title, allowed=allowed)
    if brand is None and store_brand in allowed:
        brand = store_brand
    line = None
    if brand in tax.monitor_lines and (hit := match_model(tax.monitor_lines[brand], title)):
        line = hit[0]
    code = _model_code(folded, brand)
    if resolution is None and (resolution := _code_resolution(code, brand)):
        notes.append("resolution_from_code")
    if resolution and size >= 34 and resolution == "qhd" and "resolution_from_code" in notes:
        resolution = "uwqhd"
    if _OTHER.search(folded) or not (_MARKER.search(folded) or (resolution and hz)):
        return ParseFailure("not_a_monitor")

    size_class = int(size + 0.5)
    hz_slug, hz_label = hz_range(hz)
    res_short, res_family = RESOLUTIONS[resolution][1:] if resolution else (None, None)
    panel_slug = (panel or "panel-na").lower().replace(" ", "-")
    family_name = " ".join(
        [f'{size_class}"', res_family or "resolución sin dato", hz_label, panel or "panel sin dato"]
    )
    family_id = f"monitor:{size_class}-{resolution or 'res-na'}-{hz_slug}-{panel_slug}"
    name = join_name(
        brand,
        line,
        code.upper() if code else None,
        f'{size:g}"',
        res_short,
        f"{hz} Hz" if hz else None,
        panel,
        "Curvo" if curved else None,
        "Portátil" if portable else None,
    )

    fingerprint = None
    if brand and code:
        fingerprint = "|".join(
            ["monitor", brand.lower(), code, str(size_class), resolution or "", str(hz or "")]
        )
    else:
        notes.append("unknown_brand" if brand is None else "no_model_code")

    family_specs = {"size_in": size_class, "resolution": res_family, "panel": panel}
    return Parsed(
        category=Category.MONITOR,
        family_id=family_id,
        family_name=family_name,
        name=name,
        brand=brand,
        line=line,
        fingerprint=fingerprint,
        specs={
            "size_in": size,
            "resolution": res_family,
            "refresh_hz": hz,
            "panel": panel,
            "curved": curved,
            "portable": portable,
            "touch": bool(_TOUCH.search(folded)),
            "model_code": code.upper() if code else None,
        },
        family_specs={**family_specs, "refresh": hz_label},
        notes=notes,
    )
