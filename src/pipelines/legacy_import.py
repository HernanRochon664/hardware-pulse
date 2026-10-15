"""
One-off import of the v1 price history (2026-04 .. 2026-10) from git.

v1 committed the whole SQLite file after every daily run, so each commit that
touches ``data/hardware_pulse.db`` is a snapshot of ``raw_listings`` with the
price every listing had that day. v1's own ``price_snapshots`` table is not
usable (it re-inserted every listing daily with its first-seen timestamp).

Rules:
- PCCompu prices >= 1000 were parsed as decimals by v1 ("1.049" -> 1.049);
  values below 10 with a fractional part are multiplied by 1000.
- A store's snapshot rows are only used on days the v1 logs show that store
  actually returned listings (PCCompu broke on 2026-08-23, Banifox on
  2026-10-06); otherwise v1 kept re-publishing stale prices.
- Periods are stored with ``origin='legacy'`` and unknown stock (NULL). They
  never overlap the v2 history of the same listing.
- Re-running the import replaces previously imported legacy periods.
"""

from __future__ import annotations

import logging
import re
import sqlite3
import subprocess
import tempfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from src.domain.models import Category
from src.entities.resolver import resolve_title
from src.settings import PROJECT_ROOT, from_iso, local_date, to_iso
from src.storage.repository import canonical_url

logger = logging.getLogger(__name__)

DB_GIT_PATH = "data/hardware_pulse.db"
LEGACY_STORES = {
    "thot": ("Thot Computación", "https://thotcomputacion.com.uy", "woocommerce"),
    "banifox": ("Banifox", "https://www.banifox.com", "nopcommerce"),
    "pccompu": ("PCCompu", "https://www.pccompu.com.uy", "sme"),
}
_FETCHED = [
    re.compile(r"^(\d{4}-\d{2}-\d{2})T\S+ .*?(\w+) → fetched (\d+) listings"),
    re.compile(r"^(\d{4}-\d{2}-\d{2})T\S+ .*Fetched (\d+) listings from (\w+)"),
]
_CATEGORY_HINTS = [
    (Category.GPU, re.compile(r"gpu|tarjeta|geforce|radeon|rtx|gtx|\barc\b", re.I)),
    (Category.CPU, re.compile(r"procesador|\bcpu\b|ryzen|core i\d|athlon|pentium|celeron", re.I)),
    (Category.RAM, re.compile(r"memoria|\bddr[345]|sodimm", re.I)),
    (Category.SSD, re.compile(r"\bssd\b|nvme|disco|solido|m\.2", re.I)),
]


@dataclass
class LegacyStats:
    commits: int = 0
    listings: int = 0
    periods: int = 0
    skipped_stale_rows: int = 0
    fixed_prices: int = 0


def store_ok_days(logs_dir: Path) -> dict[str, set[date]] | None:
    """Days on which each store returned listings, from the v1 scheduler log."""
    log = logs_dir / "scheduler.log"
    if not log.exists():
        logger.warning("No v1 scheduler log at %s: every snapshot day is trusted", log)
        return None
    ok: dict[str, set[date]] = defaultdict(set)
    for line in log.read_text(encoding="utf-8", errors="replace").splitlines():
        for i, pattern in enumerate(_FETCHED):
            m = pattern.search(line)
            if not m:
                continue
            day = date.fromisoformat(m.group(1))
            store, count = (
                (m.group(2), int(m.group(3))) if i == 0 else (m.group(3), int(m.group(2)))
            )
            if count > 0:
                ok[store].add(day)
            break
    return ok


def fix_price(store: str, price: float) -> tuple[float, bool]:
    if store == "pccompu" and price < 10 and price != int(price):
        return round(price * 1000, 2), True
    return price, False


def guess_category(title: str) -> Category | None:
    for category, hint in _CATEGORY_HINTS:
        if hint.search(title):
            status = resolve_title(category, title).status
            if status in ("matched", "family_only"):
                return category
    for category in Category:
        if resolve_title(category, title).status in ("matched", "family_only"):
            return category
    return None


def _git(*args: str, repo: Path) -> str:
    return subprocess.run(
        ["git", *args], cwd=repo, check=True, capture_output=True, text=True
    ).stdout


def _snapshots(repo: Path) -> list[tuple[str, datetime]]:
    # Every branch (the laptop job keeps committing to main) and only commits
    # where the file exists (the v2 branch deletes it).
    out = _git(
        "log",
        "--all",
        "--reverse",
        "--diff-filter=AM",
        "--format=%H %cI",
        "--",
        DB_GIT_PATH,
        repo=repo,
    )
    commits = []
    for line in out.splitlines():
        sha, stamp = line.split()
        commits.append((sha, datetime.fromisoformat(stamp).astimezone(UTC)))
    return commits


def import_legacy_history(
    conn: sqlite3.Connection, *, logs_dir: Path, repo: Path | None = None
) -> LegacyStats:
    repo = repo or PROJECT_ROOT
    ok_days = store_ok_days(logs_dir)
    stats = LegacyStats()
    # url -> list of (snapshot time, price, currency, title, created_at, updated_at)
    observations: dict[tuple[str, str], list[tuple]] = defaultdict(list)
    titles: dict[tuple[str, str], str] = {}
    first_seen: dict[tuple[str, str], str] = {}

    commits = _snapshots(repo)
    with tempfile.TemporaryDirectory() as tmp:
        snapshot = Path(tmp) / "v1.db"
        for sha, committed_at in commits:
            with snapshot.open("wb") as fh:
                subprocess.run(
                    ["git", "show", f"{sha}:{DB_GIT_PATH}"], cwd=repo, check=True, stdout=fh
                )
            day = local_date(committed_at)
            try:
                old = sqlite3.connect(f"file:{snapshot}?mode=ro", uri=True)
                rows = old.execute(
                    "SELECT source, url, title, price, currency, created_at, updated_at "
                    "FROM raw_listings"
                ).fetchall()
                old.close()
            except sqlite3.DatabaseError as exc:
                logger.warning("Skipping unreadable snapshot %s: %s", sha[:8], exc)
                continue
            stats.commits += 1
            for source, url, title, price, currency, created_at, updated_at in rows:
                if source not in LEGACY_STORES or currency != "USD":
                    continue  # every v1 listing was in USD; skip anything else
                if ok_days is not None and day not in ok_days.get(source, set()):
                    stats.skipped_stale_rows += 1
                    continue
                price, fixed = fix_price(source, float(price))
                stats.fixed_prices += fixed
                key = (source, canonical_url(url))
                observations[key].append((committed_at, price, currency, updated_at))
                titles[key] = title
                first_seen.setdefault(key, created_at)

    with conn:
        for store_id, (name, base_url, platform) in LEGACY_STORES.items():
            conn.execute(
                "INSERT OR IGNORE INTO stores (id, name, base_url, platform) VALUES (?, ?, ?, ?)",
                (store_id, name, base_url, platform),
            )
        conn.execute("DELETE FROM price_periods WHERE origin = 'legacy'")
        for key, obs in observations.items():
            store_id, url = key
            category = guess_category(titles[key])
            if category is None:
                continue
            listing_id, first_v2 = _ensure_listing(
                conn, store_id, url, titles[key], category, obs, first_seen[key]
            )
            stats.listings += 1
            stats.periods += _write_periods(conn, listing_id, obs, first_v2)
    logger.info("Legacy import: %s", stats)
    return stats


def _ensure_listing(
    conn: sqlite3.Connection,
    store_id: str,
    url: str,
    title: str,
    category: Category,
    obs: list[tuple],
    created_at: str,
) -> tuple[int, str | None]:
    """Reuse the v2 listing for the same URL, or create an inactive one."""
    row = conn.execute(
        "SELECT id FROM listings WHERE store_id = ? AND url = ?", (store_id, url)
    ).fetchone()
    first_seen = to_iso(min(from_iso(created_at), obs[0][0]))
    last_seen = to_iso(obs[-1][0])
    if row:
        listing_id = int(row["id"])
        conn.execute(
            "UPDATE listings SET first_seen_at = MIN(first_seen_at, ?) WHERE id = ?",
            (first_seen, listing_id),
        )
    else:
        cur = conn.execute(
            """
            INSERT INTO listings (store_id, url, title, category, first_seen_at, last_seen_at,
                                  is_active)
            VALUES (?, ?, ?, ?, ?, ?, 0)
            """,
            (store_id, url, title, category.value, first_seen, last_seen),
        )
        assert cur.lastrowid is not None
        listing_id = cur.lastrowid
    first_v2 = conn.execute(
        "SELECT MIN(valid_from) FROM price_periods WHERE listing_id = ? AND origin = 'scrape'",
        (listing_id,),
    ).fetchone()[0]
    return listing_id, first_v2


def _write_periods(
    conn: sqlite3.Connection, listing_id: int, obs: list[tuple], first_v2: str | None
) -> int:
    """Compress daily snapshots into periods of constant price (gaps > 3 days split)."""
    periods: list[list] = []
    for committed_at, price, currency, updated_at in obs:
        stamp = to_iso(committed_at)
        if first_v2 and stamp >= first_v2:
            break
        current = periods[-1] if periods else None
        gap = current is not None and committed_at - from_iso(current[3]) > timedelta(days=3)
        if current and current[0] == price and not gap:
            current[3] = stamp
            continue
        start = stamp
        if updated_at:  # exact change time when it happened since the previous snapshot
            changed = from_iso(updated_at)
            previous = from_iso(current[3]) if current else None
            if changed <= committed_at and (previous is None or changed > previous):
                start = to_iso(changed)
        periods.append([price, currency, start, stamp])
    for price, currency, start, last in periods:
        conn.execute(
            """
            INSERT INTO price_periods (listing_id, price, regular_price, currency, price_usd,
                                       in_stock, valid_from, last_seen, origin)
            VALUES (?, ?, NULL, ?, ?, NULL, ?, ?, 'legacy')
            """,
            (listing_id, price, currency, price, start, last),
        )
    return len(periods)
