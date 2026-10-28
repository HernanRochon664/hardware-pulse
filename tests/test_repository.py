from datetime import UTC, datetime, timedelta

from src.domain.models import Category
from src.storage import repository
from src.storage.db import connect, migrate
from tests.conftest import make_listing

T0 = datetime(2026, 10, 14, 12, 0, tzinfo=UTC)


def _periods(conn):
    return conn.execute(
        "SELECT price, in_stock, valid_from, last_seen FROM price_periods ORDER BY id"
    ).fetchall()


def test_migrations_are_idempotent():
    conn = connect(":memory:")
    assert migrate(conn) == []
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"listings", "price_periods", "products", "scrape_runs", "signals"} <= tables


def test_canonical_url_strips_tracking_and_fragment_but_keeps_path_case():
    url = "HTTPS://WWW.Banifox.com/Asus-Dual?utm_source=x&id=3#top"
    assert repository.canonical_url(url) == "https://www.banifox.com/Asus-Dual?id=3"


def test_same_price_extends_period_and_is_idempotent(db_conn):
    with db_conn:
        first = repository.record_observation(db_conn, make_listing(at=T0), 1050)
        repository.record_observation(db_conn, make_listing(at=T0 + timedelta(hours=8)), 1050)
        repository.record_observation(db_conn, make_listing(at=T0 + timedelta(hours=8)), 1050)
    assert first.is_new_listing
    periods = _periods(db_conn)
    assert len(periods) == 1
    assert periods[0]["last_seen"] == "2026-10-14T20:00:00Z"


def test_price_or_stock_change_opens_new_period(db_conn):
    with db_conn:
        repository.record_observation(db_conn, make_listing(at=T0), 1050)
        changed = repository.record_observation(
            db_conn, make_listing(price=999, at=T0 + timedelta(days=1)), 999
        )
        repository.record_observation(
            db_conn, make_listing(price=999, in_stock=False, at=T0 + timedelta(days=2)), 999
        )
    assert changed.price_changed
    assert [p["price"] for p in _periods(db_conn)] == [1050, 999, 999]
    assert _periods(db_conn)[-1]["in_stock"] == 0


def test_unseen_listings_are_deactivated_and_relisting_starts_new_period(db_conn):
    other = "https://thotcomputacion.com.uy/producto/rx-9070/"
    with db_conn:
        repository.record_observation(db_conn, make_listing(at=T0), 1050)
        repository.record_observation(db_conn, make_listing(url=other, price=980, at=T0), 980)
    run2 = T0 + timedelta(days=1)
    with db_conn:
        seen = repository.record_observation(db_conn, make_listing(at=run2), 1050)
        delisted = repository.deactivate_unseen(db_conn, "thot", Category.GPU, {seen.listing_id})
    assert delisted == 1
    active = db_conn.execute("SELECT url, is_active FROM listings ORDER BY id").fetchall()
    assert [r["is_active"] for r in active] == [1, 0]

    # Same price comes back after being delisted: the gap must not be bridged.
    with db_conn:
        repository.record_observation(
            db_conn, make_listing(url=other, price=980, at=run2 + timedelta(days=3)), 980
        )
    prices = [p["price"] for p in _periods(db_conn)]
    assert prices.count(980) == 2


def test_regular_price_not_above_price_is_ignored():
    listing = make_listing(price=100, regular_price=100)
    assert listing.regular_price is None


def test_classify_run_detects_empty_and_anomalous_counts(db_conn):
    assert repository.classify_run(db_conn, "thot", Category.GPU, 0) == "empty"
    assert repository.classify_run(db_conn, "thot", Category.GPU, 5) == "ok"
    for i in range(3):
        run = repository.start_scrape_run(db_conn, f"r{i}", "thot", Category.GPU, T0 + timedelta(i))
        repository.finish_scrape_run(
            db_conn, run, status="ok", finished_at=T0 + timedelta(i), n_found=60
        )
    assert repository.classify_run(db_conn, "thot", Category.GPU, 25) == "anomaly"
    assert repository.classify_run(db_conn, "thot", Category.GPU, 55) == "ok"


def test_classify_run_accepts_a_drop_that_held_for_three_runs(db_conn):
    def finish(i: int, status: str, n_found: int) -> None:
        run = repository.start_scrape_run(db_conn, f"r{i}", "thot", Category.GPU, T0 + timedelta(i))
        repository.finish_scrape_run(
            db_conn, run, status=status, finished_at=T0 + timedelta(i), n_found=n_found
        )

    for i in range(3):
        finish(i, "ok", 35)
    assert repository.classify_run(db_conn, "thot", Category.GPU, 13) == "anomaly"
    finish(3, "anomaly", 13)
    assert repository.classify_run(db_conn, "thot", Category.GPU, 13) == "anomaly"
    finish(4, "anomaly", 14)
    assert repository.classify_run(db_conn, "thot", Category.GPU, 13) == "ok"
    assert repository.classify_run(db_conn, "thot", Category.GPU, 8) == "anomaly"
    finish(5, "ok", 13)  # still below half the median of the ok runs, but held
    assert repository.classify_run(db_conn, "thot", Category.GPU, 13) == "ok"


def test_fx_rate_falls_back_to_latest_earlier_date(db_conn):
    repository.save_fx_rate(db_conn, "2026-10-10", 40.1, "test", T0)
    assert repository.get_fx_rate(db_conn, "2026-10-14") == 40.1
    assert repository.get_fx_rate(db_conn, "2026-10-01") is None
