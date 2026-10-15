"""
Text normalization helpers for entity resolution.

Three views of a title are used by the parsers:

- ``fold``:    lowercase, no accents, typographic quotes removed.
- ``spaced``:  fold + every non-alphanumeric run becomes a space and letters
               are split from digits ("RTX5060Ti" -> "rtx 5060 ti",
               "O16G" -> "o 16 g"), so regexes can work on tokens.
- ``compact``: fold with everything except [a-z0-9] removed, for matching
               product-line names written with or without spaces
               ("Infinity3" == "Infinity 3", "EAGLEOC" contains "eagle").
"""

from __future__ import annotations

import re
import unicodedata

_NON_ALNUM = re.compile(r"[^a-z0-9]+")
_LETTER_DIGIT = re.compile(r"(?<=[a-z])(?=\d)|(?<=\d)(?=[a-z])")
_SPACES = re.compile(r"\s+")


def fold(text: str) -> str:
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text.lower().replace("″", '"').replace("”", '"').replace("“", '"')


def spaced(text: str) -> str:
    out = _NON_ALNUM.sub(" ", fold(text))
    out = _LETTER_DIGIT.sub(" ", out)
    return _SPACES.sub(" ", out).strip()


def compact(text: str) -> str:
    return _NON_ALNUM.sub("", fold(text))


def tokens(text: str) -> list[str]:
    return spaced(text).split()


def slug(text: str) -> str:
    return _NON_ALNUM.sub("-", fold(text)).strip("-")
