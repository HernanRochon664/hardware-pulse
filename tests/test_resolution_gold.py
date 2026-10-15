"""Precision/recall gates of entity resolution on the reviewed gold set."""

from __future__ import annotations

import pytest
import yaml

from src.domain.models import Category
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
