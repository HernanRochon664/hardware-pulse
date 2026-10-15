"""Data health summary: paused stores are reported apart from failing ones."""

from __future__ import annotations

from datetime import UTC, datetime

from src.domain.models import Category
from src.pipelines.status import build_status, paused_stores
from src.storage import repository

T0 = datetime(2026, 10, 15, 12, 0, tzinfo=UTC)


def _run(conn, store_id: str, status: str) -> None:
    run = repository.start_scrape_run(conn, "r1", store_id, Category.GPU, T0)
    repository.finish_scrape_run(
        conn, run, status=status, finished_at=T0, n_found=0 if status == "error" else 5
    )


def test_paused_store_is_not_counted_as_failing(db_conn):
    _run(db_conn, "thot", "ok")
    _run(db_conn, "banifox", "error")
    status = build_status(db_conn, T0, paused={"banifox": "Bloquea servidores en la nube"})
    assert (status["stores_ok"], status["stores_total"], status["stores_paused"]) == (1, 1, 1)
    assert status["stores"]["banifox"]["paused"] == "Bloquea servidores en la nube"
    assert status["stores"]["thot"]["paused"] is None


def test_failing_store_still_counts_when_not_paused(db_conn):
    _run(db_conn, "thot", "ok")
    _run(db_conn, "banifox", "error")
    status = build_status(db_conn, T0, paused={})
    assert (status["stores_ok"], status["stores_total"]) == (1, 2)


def test_paused_stores_come_from_the_config():
    assert "banifox" in paused_stores()
