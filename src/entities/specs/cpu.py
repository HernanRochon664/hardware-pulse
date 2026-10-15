"""
CPU title parser.

For CPUs the exact product is the processor model (a Ryzen 7 7800X3D is the
same item in every store), so family and product coincide. The model must be
read with its exact suffix: 12400 != 12400F, 7700X != 7700X3D, 5600GT != 5600.
Box/Tray packaging is not part of the identity.
"""

from __future__ import annotations

import re

from src.domain.models import Category
from src.entities.specs import Parsed, ParseFailure
from src.entities.taxonomy import Taxonomy
from src.entities.text import slug, spaced

_ULTRA = re.compile(r"\bultra (?:core )?(?:i )?([579]) (2\d{2})(?: (kf|k|f))?(?: (plus))?\b")
_CORE_I_ULTRA = re.compile(r"\bi ([579]) (2\d{2})(?: (kf|k|f))?(?: (plus))?\b")
_CORE_I = re.compile(r"\bi ([3579]) (\d{4,5})(?: (ks|kf|k|f|te|t))?\b")
_PENTIUM = re.compile(r"\b(pentium gold|pentium|celeron|dual core) ([ge]) (\d{4})\b")
_CORE2 = re.compile(r"\bcore 2 (quad|duo) ([qe]) (\d{4})\b")
_XEON = re.compile(r"\bxeon (w) ([3579]) (\d{4})(?: (x))?\b")
_RYZEN = re.compile(r"\bryzen(?: ([3579]))?(?: pro)? (\d{4})(?: (x 3 d 2|x 3 d|xt|x|gt|ge|g|f))?\b")
_THREADRIPPER = re.compile(r"\bthreadripper(?: (pro))? (\d{4}) (wx|x)\b")
_ATHLON = re.compile(r"\bathlon(?: gold| silver)? (\d{4}) (ge|g)\b")


def _ryzen_tier(number: str) -> str:
    hundreds = int(number[1])
    return "3" if hundreds <= 3 else "5" if hundreds <= 6 else "7" if hundreds <= 8 else "9"


def _models(text: str) -> set[tuple[str, str]]:
    """All (vendor, model name) readings found in the title."""
    found: set[tuple[str, str]] = set()
    for pattern in (_ULTRA, _CORE_I_ULTRA):
        for m in pattern.finditer(text):
            suffix = (m.group(3) or "").upper()
            plus = " Plus" if m.group(4) else ""
            found.add(("Intel", f"Intel Core Ultra {m.group(1)} {m.group(2)}{suffix}{plus}"))
    for m in _CORE_I.finditer(text):
        found.add(("Intel", f"Intel Core i{m.group(1)}-{m.group(2)}{(m.group(3) or '').upper()}"))
    for m in _PENTIUM.finditer(text):
        family = (
            "Celeron"
            if m.group(1) == "celeron"
            else ("Pentium Dual-Core" if m.group(2) == "e" else "Pentium Gold")
        )
        found.add(("Intel", f"Intel {family} {m.group(2).upper()}{m.group(3)}"))
    for m in _CORE2.finditer(text):
        found.add(("Intel", f"Intel Core 2 {m.group(1).title()} {m.group(2).upper()}{m.group(3)}"))
    for m in _XEON.finditer(text):
        found.add(("Intel", f"Intel Xeon W{m.group(2)}-{m.group(3)}{(m.group(4) or '').upper()}"))
    for m in _RYZEN.finditer(text):
        number = m.group(2)
        tier = m.group(1) or _ryzen_tier(number)
        if tier != _ryzen_tier(number):
            found.add(("AMD", f"!tier_mismatch:{tier}:{number}"))
            continue
        suffix = (m.group(3) or "").replace(" ", "").upper()
        found.add(("AMD", f"AMD Ryzen {tier} {number}{suffix}"))
    for m in _THREADRIPPER.finditer(text):
        pro = " PRO" if m.group(1) or m.group(3) == "wx" else ""
        found.add(("AMD", f"AMD Ryzen Threadripper{pro} {m.group(2)}{m.group(3).upper()}"))
    for m in _ATHLON.finditer(text):
        found.add(("AMD", f"AMD Athlon {m.group(1)}{m.group(2).upper()}"))
    return found


def parse_cpu(title: str, store_brand: str | None, tax: Taxonomy) -> Parsed | ParseFailure:
    text = spaced(title)
    models = _models(text)
    if not models:
        return ParseFailure("no_model")
    bad = [name for _, name in models if name.startswith("!")]
    if bad:
        return ParseFailure(bad[0].lstrip("!"))
    if len(models) > 1:
        return ParseFailure("ambiguous_model:" + "/".join(sorted(n for _, n in models)))
    vendor, name = next(iter(models))
    family_id = f"cpu:{slug(name)}"
    return Parsed(
        category=Category.CPU,
        family_id=family_id,
        family_name=name,
        name=name,
        brand=vendor,
        line=None,
        fingerprint=family_id,
        specs={"vendor": vendor, "model": name},
        family_specs={"vendor": vendor, "model": name},
    )
