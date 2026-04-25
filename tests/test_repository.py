"""
Tests for src/storage/repository.py

Covers upsert semantics, deduplication logic, and key derivation.
Uses an in-memory SQLite connection via the db_conn fixture.
"""
from datetime import datetime

from src.domain.models import Currency, RawListing, Source
from src.storage.repository import _compute_listing_key, upsert_raw_listing


# ---------------------------------------------------------------------------
# Upsert - insert path
# ---------------------------------------------------------------------------


class TestUpsertInsert:
    def test_new_listing_is_inserted(self, db_conn, sample_listing):
        result = upsert_raw_listing(sample_listing, db_conn)
        assert result.inserted is True
        assert result.updated is False
        assert result.id is not None

    def test_inserted_listing_exists_in_db(self, db_conn, sample_listing):
        upsert_raw_listing(sample_listing, db_conn)
        row = db_conn.execute("SELECT * FROM raw_listings").fetchone()
        assert row is not None
        assert row["title"] == sample_listing.title
        assert row["price"] == sample_listing.price

    def test_listing_with_optional_nulls_inserts(self, db_conn):
        listing = RawListing(
            source=Source.THOT,
            url="https://thotcomputacion.com.uy/producto/rtx-4060",
            timestamp=datetime.fromisoformat("2026-04-25T12:00:00+00:00"),
            title="GPU RTX 4060",
            price=400.0,
            currency=Currency.USD,
            seller="thot",
            # All optional fields left as None
        )
        result = upsert_raw_listing(listing, db_conn)
        assert result.inserted is True


# ---------------------------------------------------------------------------
# Upsert - no-op path
# ---------------------------------------------------------------------------


class TestUpsertNoOp:
    def test_same_listing_twice_is_noop(self, db_conn, sample_listing):
        first = upsert_raw_listing(sample_listing, db_conn)
        second = upsert_raw_listing(sample_listing, db_conn)

        assert first.inserted is True
        assert second.inserted is False
        assert second.updated is False

    def test_noop_does_not_duplicate_rows(self, db_conn, sample_listing):
        upsert_raw_listing(sample_listing, db_conn)
        upsert_raw_listing(sample_listing, db_conn)

        count = db_conn.execute("SELECT COUNT(*) FROM raw_listings").fetchone()[0]
        assert count == 1


# ---------------------------------------------------------------------------
# Upsert - update path
# ---------------------------------------------------------------------------


class TestUpsertUpdate:
    def test_price_change_triggers_update(self, db_conn, sample_listing):
        upsert_raw_listing(sample_listing, db_conn)

        updated_listing = sample_listing.model_copy(update={"price": 699.0})
        result = upsert_raw_listing(updated_listing, db_conn)

        assert result.updated is True
        assert result.inserted is False

    def test_updated_price_persisted_in_db(self, db_conn, sample_listing):
        upsert_raw_listing(sample_listing, db_conn)

        updated_listing = sample_listing.model_copy(update={"price": 699.0})
        upsert_raw_listing(updated_listing, db_conn)

        row = db_conn.execute("SELECT price FROM raw_listings").fetchone()
        assert row["price"] == 699.0

    def test_update_does_not_add_new_row(self, db_conn, sample_listing):
        upsert_raw_listing(sample_listing, db_conn)
        updated = sample_listing.model_copy(update={"price": 699.0})
        upsert_raw_listing(updated, db_conn)

        count = db_conn.execute("SELECT COUNT(*) FROM raw_listings").fetchone()[0]
        assert count == 1


# ---------------------------------------------------------------------------
# Deduplication - URL-based
# ---------------------------------------------------------------------------


class TestDeduplicationByUrl:
    def test_tracking_params_deduplicated(self, db_conn, sample_listing):
        """Same product URL with different UTM params → same listing_key."""
        listing_with_utm = sample_listing.model_copy(
            update={"url": str(sample_listing.url) + "?utm_source=google&utm_medium=cpc"}
        )
        result1 = upsert_raw_listing(sample_listing, db_conn)
        result2 = upsert_raw_listing(listing_with_utm, db_conn)

        assert result1.inserted is True
        assert result2.inserted is False  # treated as same listing

    def test_different_urls_are_different_listings(self, db_conn, sample_listing):
        listing2 = sample_listing.model_copy(
            update={"url": "https://thotcomputacion.com.uy/producto/rtx-4060"}
        )
        result1 = upsert_raw_listing(sample_listing, db_conn)
        result2 = upsert_raw_listing(listing2, db_conn)

        assert result1.inserted is True
        assert result2.inserted is True


# ---------------------------------------------------------------------------
# Deduplication - item_id-based
# ---------------------------------------------------------------------------


class TestDeduplicationByItemId:
    def test_same_item_id_different_url_is_duplicate(self, db_conn, sample_listing_with_item_id):
        """item_id takes precedence over URL for ML listings."""
        listing2 = sample_listing_with_item_id.model_copy(
            update={"url": "https://www.mercadolibre.com.uy/producto/MLU123456-v2"}
        )
        result1 = upsert_raw_listing(sample_listing_with_item_id, db_conn)
        result2 = upsert_raw_listing(listing2, db_conn)

        assert result1.inserted is True
        assert result2.inserted is False


# ---------------------------------------------------------------------------
# Listing key derivation
# ---------------------------------------------------------------------------


class TestListingKeyDerivation:
    def test_key_is_deterministic(self, sample_listing):
        key1 = _compute_listing_key(sample_listing)
        key2 = _compute_listing_key(sample_listing)
        assert key1 == key2

    def test_different_sources_produce_different_keys(self, sample_listing):
        listing_banifox = sample_listing.model_copy(update={"source": Source.BANIFOX})
        key1 = _compute_listing_key(sample_listing)
        key2 = _compute_listing_key(listing_banifox)
        assert key1 != key2

    def test_item_id_used_over_url_for_ml(self, sample_listing_with_item_id):
        """ML listings with same item_id but different URL → same key."""
        listing2 = sample_listing_with_item_id.model_copy(
            update={"url": "https://www.mercadolibre.com.uy/otro-url"}
        )
        assert _compute_listing_key(sample_listing_with_item_id) == _compute_listing_key(listing2)