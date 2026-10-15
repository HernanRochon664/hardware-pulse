from datetime import UTC, date, datetime

from src.domain.models import Category
from src.pipelines.legacy_import import _write_periods, fix_price, guess_category, store_ok_days


def test_fix_price_restores_pccompu_thousands():
    assert fix_price("pccompu", 1.049) == (1049.0, True)
    assert fix_price("pccompu", 2.369) == (2369.0, True)
    assert fix_price("pccompu", 11.0) == (11.0, False)  # a real USD 11 CPU
    assert fix_price("thot", 1.5) == (1.5, False)


def test_store_ok_days_reads_both_log_formats(tmp_path):
    (tmp_path / "scheduler.log").write_text(
        "2026-10-12T17:01:06 [INFO] src.pipelines.ingest:   thot → fetched 84 listings\n"
        "2026-10-12T17:01:46 [INFO] src.pipelines.ingest:   banifox → fetched 0 listings\n"
        "2026-05-08T19:40:00 [INFO] src.scrapers.base: Fetched 12 listings from pccompu\n",
        encoding="utf-8",
    )
    ok = store_ok_days(tmp_path)
    assert ok is not None
    assert ok["thot"] == {date(2026, 10, 12)}
    assert "banifox" not in ok
    assert ok["pccompu"] == {date(2026, 5, 8)}


def test_guess_category_from_title():
    assert guess_category("GPU MSI GeForce RTX 5060 Shadow 2X OC 8Gb") == Category.GPU
    assert guess_category("AMD PROCESADOR RYZEN 5 5500 AM4") == Category.CPU
    assert guess_category("Memoria Kingston Fury Beast 16GB DDR5 5600Mhz") == Category.RAM
    assert guess_category("KINGSTON A400 disco ssd 480Gb") == Category.SSD


class _Recorder:
    def __init__(self):
        self.rows = []

    def execute(self, sql, params):
        self.rows.append(params)


def test_write_periods_compresses_constant_prices_and_splits_gaps():
    def t(day):
        return datetime(2026, 5, day, 22, 0, tzinfo=UTC)

    obs = [
        (t(1), 100.0, "USD", None),
        (t(2), 100.0, "USD", None),
        (t(3), 90.0, "USD", "2026-05-03T10:00:00Z"),  # exact change time from updated_at
        (t(10), 90.0, "USD", None),  # 7-day gap: unknown in between
    ]
    rec = _Recorder()
    assert _write_periods(rec, 1, obs, first_v2=None) == 3  # type: ignore[arg-type]
    starts = [r[4] for r in rec.rows]
    assert starts == ["2026-05-01T22:00:00Z", "2026-05-03T10:00:00Z", "2026-05-10T22:00:00Z"]


def test_write_periods_never_overlap_v2_history():
    obs = [(datetime(2026, 5, d, 22, 0, tzinfo=UTC), 100.0, "USD", None) for d in (1, 2, 3)]
    rec = _Recorder()
    _write_periods(rec, 1, obs, first_v2="2026-05-02T00:00:00Z")  # type: ignore[arg-type]
    assert len(rec.rows) == 1 and rec.rows[0][5] == "2026-05-01T22:00:00Z"
