"""
Motherboard title parser.

Family  = socket + chipset + form factor ("B650 mATX (AM5)"): what a buyer
          compares across brands.
Product = brand + chipset + form + sub-model code + name words + Wi-Fi +
          memory type + color ("ASUS Prime B650M-A Wi-Fi").

The chipset is the category marker: titles without a known chipset are not
parsed. The board name is read with a closed vocabulary (taxonomy
``board_words``); a word outside it may be a variant we do not know, so the
board stays family_only instead of being merged with a similar name. LGA1700
boards exist with DDR4 and DDR5 under the same name, so the memory type is
part of the identity and is never inferred for them.
"""

from __future__ import annotations

import re

from src.domain.models import Category
from src.entities.specs import Parsed, ParseFailure, join_name
from src.entities.taxonomy import Taxonomy
from src.entities.text import fold, slug

_TOKENS = re.compile(r"[a-z0-9][a-z0-9.+\-]*")
# B650M-A, X870E, B650EM-A, H610M-HVS, EX-B760M-V5 (one token each)
_CHIPSET = re.compile(r"(?:^|-)([abhxz])(\d{3})(e?)([a-z0-9.+\-]*)$")
_FORM_WORDS = [
    (re.compile(r"\be-?atx\b"), "E-ATX"),
    (re.compile(r"\bmini[- ]?itx\b|\bitx\b"), "ITX"),
    (re.compile(r"\bmicro[- ]?atx\b|\bm-?atx\b|\bmicroatx\b"), "mATX"),
    (re.compile(r"\batx\b"), "ATX"),
]
_WIFI = re.compile(r"\b(?:wifi|wf) ?(6e|6|7)?\b")
_MEMORY = re.compile(r"\b(?:ddr|d)([45])\b")
_SOCKET = re.compile(r"\b(?:am([45])|(?:lga|s)? ?(1151|1200|1700|1851))\b")
_COLORS = {"white", "blanca", "blanco", "wt", "w", "wh", "ice"}
_DEFAULT_COLORS = {"black", "negra", "negro", "bk"}
# Titles of other categories that may contain a chipset-looking code (H410R cooler).
_OTHER = re.compile(r"\bcooler\b|\bdisipador|\bwater ?cool|\bfuente\b|\bgabinete|\bmonitor\b")
_BOARD_WORD = re.compile(r"\bmother|\bmainboard|\bplaca madre|\bplaca base")
_IGNORED = re.compile(
    r"(?:wifi|wf)(?:6e|6|7)?|6e|ddr[345]|d[45]|am[45]|(?:lga|s)?1[0-9]{3}"
    r"|[0-9.,x+]+|[0-9.]+(?:gb|gbe|mhz|w)|e?atx|matx|m-atx|itx|mini|micro"
    r"|[0-9]+(?:ma|va|na|ta|ra|da|ro|do|to)"
)


def _tokens(folded: str) -> list[str]:
    text = re.sub(
        r"\br (\d)\.(\d)\b", r"r\1.\2", folded.replace("wi-fi", "wifi").replace("p/", "para ")
    )
    for ch in "/[](),|:;":
        text = text.replace(ch, " ")
    return _TOKENS.findall(text)


def _form(rest: str, folded: str) -> tuple[str, str]:
    """Form factor from the code suffix ("m-a" -> mATX), else from explicit words."""
    for prefix in ("am", "m"):
        if rest.startswith(prefix):
            return "mATX", rest[len(prefix) :]
    if rest.startswith("i") and not rest.startswith("ii"):
        return "ITX", rest[1:]
    for pattern, form in _FORM_WORDS:
        if pattern.search(folded):
            return form, rest
    return "ATX", rest


def _find_chipset(tokens: list[str], tax: Taxonomy) -> tuple[int, str, str] | ParseFailure:
    found: list[tuple[int, str, str]] = []  # (token index, chipset, rest of the code)
    for i, token in enumerate(tokens):
        m = _CHIPSET.search(token)
        if not m:
            continue
        base = f"{m.group(1)}{m.group(2)}".upper()
        if m.group(3) and base + "E" in tax.board_chipsets:
            chipset, rest = base + "E", m.group(4)
        else:
            chipset, rest = base, m.group(3) + m.group(4)
        if chipset in tax.board_chipsets:
            found.append((i, chipset, rest))
    chipsets = {c for _, c, _ in found}
    if not chipsets:
        return ParseFailure("no_chipset")
    if len(chipsets) > 1:
        return ParseFailure("ambiguous_chipset:" + "/".join(sorted(chipsets)))
    return found[0]


def parse_motherboard(title: str, store_brand: str | None, tax: Taxonomy) -> Parsed | ParseFailure:
    folded = fold(title)
    if _OTHER.search(folded) and not _BOARD_WORD.search(folded):
        return ParseFailure("not_a_motherboard")
    tokens = _tokens(folded)
    joined = " ".join(tokens)
    chip = _find_chipset(tokens, tax)
    if isinstance(chip, ParseFailure):
        return chip
    index, chipset, rest = chip
    socket, fixed_memory = tax.board_chipsets[chipset]
    notes: list[str] = []

    mentioned = {
        f"AM{m.group(1)}" if m.group(1) else f"LGA{m.group(2)}" for m in _SOCKET.finditer(joined)
    }
    if mentioned - {socket}:
        return ParseFailure("socket_mismatch")

    form, rest = _form(rest, folded)
    sub = re.sub(r"[^a-z0-9+]", "", rest)
    code = tokens[index].upper()
    skip = {index}
    if not sub and index + 1 < len(tokens) and re.fullmatch(r"[a-z]", tokens[index + 1]):
        sub = tokens[index + 1]  # Gigabyte "H610M K", "B760M H"
        skip.add(index + 1)
        code = f"{code} {sub.upper()}"

    memories = {f"DDR{m.group(1)}" for m in _MEMORY.finditer(joined)}
    if len(memories) > 1:
        return ParseFailure("ambiguous_memory")
    memory = next(iter(memories), None)
    if fixed_memory:
        if memory and memory != fixed_memory:
            notes.append("memory_conflict")
        memory = fixed_memory

    wifi = None
    for m in _WIFI.finditer(joined):
        wifi = f"Wi-Fi {m.group(1).upper()}" if m.group(1) else (wifi or "Wi-Fi")

    allowed = tax.brands_for(Category.MOTHERBOARD)
    brand = tax.detect_brand(title, allowed=allowed)
    if brand is None and store_brand in allowed:
        brand = store_brand
    brand_tokens = set(fold(brand or "").split())

    before: list[str] = []
    after: list[str] = []
    unknown: list[str] = []
    color = False
    previous = ""
    for i, token in enumerate(tokens):
        if i in skip:
            previous = token
            continue
        whole = token in tax.board_words or token in tax.board_noise
        for part in [token] if whole else token.split("-"):
            if not part or part in brand_tokens:
                pass
            elif part in tax.board_words:
                (before if i < index else after).append(tax.board_words[part])
            elif part in _COLORS:
                color = True
            elif part.isdigit() and len(part) == 1 and previous == "gaming":
                after.append(part)  # ASRock "Phantom Gaming 4"
            elif part in _DEFAULT_COLORS or part in tax.board_noise or _IGNORED.fullmatch(part):
                pass
            else:
                unknown.append(part)
            previous = part

    words = list(dict.fromkeys(before + after))
    name = join_name(
        brand,
        *dict.fromkeys(before),
        code,
        *[w for w in dict.fromkeys(after) if w not in before],
        wifi,
        memory if fixed_memory is None else None,
        "White" if color else None,
    )

    fingerprint = None
    if fixed_memory is None and memory is None:
        notes.append("memory_unknown")  # the same name exists with DDR4 and DDR5
    elif brand and not unknown:
        fingerprint = "|".join(
            [
                "motherboard",
                brand.lower(),
                chipset.lower(),
                form.lower(),
                sub,
                "+".join(sorted(w.lower() for w in words)),
                (wifi or "").lower().replace(" ", ""),
                (memory or "").lower(),
                "white" if color else "",
            ]
        )
    elif brand is None:
        notes.append("unknown_brand")
    else:
        notes.append("unknown_words:" + ",".join(unknown[:5]))

    family_specs = {"socket": socket, "chipset": chipset, "form": form}
    return Parsed(
        category=Category.MOTHERBOARD,
        family_id=f"motherboard:{slug(f'{socket}-{chipset}-{form}')}",
        family_name=f"{chipset} {form} ({socket})",
        name=name,
        brand=brand,
        line=" ".join(words) or None,
        fingerprint=fingerprint,
        specs={**family_specs, "memory": memory, "wifi": wifi, "white": color},
        family_specs=family_specs,
        notes=notes,
    )
