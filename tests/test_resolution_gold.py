"""Precision/recall gates of entity resolution on the reviewed gold set."""

from __future__ import annotations

import pytest
import yaml

from src.domain.models import LEGACY_CATEGORIES, Category
from src.entities.resolver import resolve_title
from tests.conftest import FIXTURES_DIR

GOLD = yaml.safe_load((FIXTURES_DIR / "resolution_gold.yaml").read_text(encoding="utf-8"))


def _predict(entry):
    r = resolve_title(Category(entry["category"]), entry["title"], entry.get("store_brand"))
    return {
        "status": r.status,
        "condition": r.condition.value,
        "family": r.parsed.family_id if r.parsed else None,
        "product": r.parsed.fingerprint if r.parsed else None,
    }


PREDICTIONS = [(entry, _predict(entry)) for entry in GOLD]


def test_gold_set_is_large_and_covers_every_category():
    assert len(GOLD) >= 250
    assert {e["category"] for e in GOLD} == {c.value for c in Category}


def test_family_precision_and_recall():
    predicted = [(e, p) for e, p in PREDICTIONS if p["family"] is not None]
    correct = sum(1 for e, p in predicted if p["family"] == e["family"])
    expected = [(e, p) for e, p in PREDICTIONS if e["family"] is not None]
    recalled = sum(1 for e, p in expected if p["family"] == e["family"])
    assert correct / len(predicted) >= 0.98, f"family precision {correct}/{len(predicted)}"
    assert recalled / len(expected) >= 0.90, f"family recall {recalled}/{len(expected)}"


def test_product_precision():
    predicted = [(e, p) for e, p in PREDICTIONS if p["product"] is not None]
    correct = sum(1 for e, p in predicted if p["product"] == e["product"])
    assert correct / len(predicted) >= 0.95, f"product precision {correct}/{len(predicted)}"


def test_no_listing_lands_in_a_different_category_family():
    for entry, pred in PREDICTIONS:
        if pred["family"]:
            assert pred["family"].startswith(entry["category"] + ":"), entry["title"]


@pytest.mark.parametrize("kind", ["rejected", "condition"])
def test_rejections_and_conditions_are_stable(kind):
    if kind == "rejected":
        mismatches = [
            e["title"]
            for e, p in PREDICTIONS
            if (e["status"] == "rejected") != (p["status"] == "rejected")
        ]
    else:
        mismatches = [e["title"] for e, p in PREDICTIONS if e["condition"] != p["condition"]]
    assert not mismatches, mismatches[:10]


# ---------------------------------------------------------------------------
# Catalog expansion (ADR-09): the first four categories must resolve exactly as
# before, and each new category passes the gates on its own (so it cannot hide
# behind the ~800 entries of the others).
# ---------------------------------------------------------------------------

NEW_CATEGORIES = [c for c in Category if c not in LEGACY_CATEGORIES]


def test_legacy_categories_resolve_exactly_as_labeled():
    changed = [
        (e["title"], p)
        for e, p in PREDICTIONS
        if Category(e["category"]) in LEGACY_CATEGORIES
        and (p["status"], p["family"], p["product"]) != (e["status"], e["family"], e["product"])
    ]
    assert not changed, changed[:10]


@pytest.mark.parametrize("category", NEW_CATEGORIES, ids=str)
def test_new_category_gates(category):
    rows = [(e, p) for e, p in PREDICTIONS if e["category"] == category.value]
    assert len(rows) >= 40
    assert len({e["store"] for e, _ in rows}) >= 3
    predicted = [(e, p) for e, p in rows if p["family"] is not None]
    expected = [(e, p) for e, p in rows if e["family"] is not None]
    assert sum(p["family"] == e["family"] for e, p in predicted) / len(predicted) >= 0.98
    assert sum(p["family"] == e["family"] for e, p in expected) / len(expected) >= 0.90
    products = [(e, p) for e, p in rows if p["product"] is not None]
    assert sum(p["product"] == e["product"] for e, p in products) / len(products) >= 0.95


def test_titles_are_accepted_by_one_category_only():
    """The import calculator matches a US title only when exactly one parser accepts it."""
    clashes = []
    for entry, pred in PREDICTIONS:
        own = Category(entry["category"])
        if pred["status"] not in ("matched", "family_only"):
            continue
        for other in Category:
            if other is own or (own in LEGACY_CATEGORIES and other in LEGACY_CATEGORIES):
                continue
            r = resolve_title(other, entry["title"], entry.get("store_brand"))
            if r.status in ("matched", "family_only"):
                clashes.append((entry["title"], other.value))
    assert not clashes, clashes[:10]
