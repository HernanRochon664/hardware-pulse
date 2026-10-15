"""
Data health summary, written to ``status.json`` next to the published DB and
shown on the dashboard's "Estado de datos" page.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

from src.settings import to_iso, utcnow


def build_status(conn: sqlite3.Connection, now: datetime | None = None) -> dict[str, Any]:
    now = now or utcnow()
    latest_runs = conn.execute(
        """
        SELECT r.* FROM scrape_runs r
        WHERE r.id = (
            SELECT r2.id FROM scrape_runs r2
            WHERE r2.store_id = r.store_id AND r2.category = r.category
            ORDER BY r2.started_at DESC, r2.id DESC LIMIT 1
        )
        ORDER BY r.store_id, r.category
        """
    ).fetchall()
    last_ok = {
        (row["store_id"], row["category"]): row["started_at"]
        for row in conn.execute(
            "SELECT store_id, category, MAX(started_at) AS started_at FROM scrape_runs "
            "WHERE status = 'ok' GROUP BY store_id, category"
        )
    }
    stores: dict[str, Any] = {}
    names = {r["id"]: r for r in conn.execute("SELECT id, name, base_url FROM stores")}
    for run in latest_runs:
        store = stores.setdefault(
            run["store_id"],
            {
                "name": names[run["store_id"]]["name"]
                if run["store_id"] in names
                else run["store_id"],
                "url": names[run["store_id"]]["base_url"] if run["store_id"] in names else None,
                "categories": {},
            },
        )
        store["categories"][run["category"]] = {
            "status": run["status"],
            "started_at": run["started_at"],
            "n_found": run["n_found"],
            "error": run["error"],
            "last_ok_at": last_ok.get((run["store_id"], run["category"])),
        }
    for store in stores.values():
        statuses = [c["status"] for c in store["categories"].values()]
        store["ok"] = all(s == "ok" for s in statuses)

    def scalar(sql: str) -> int:
        return int(conn.execute(sql).fetchone()[0] or 0)

    resolution = {
        row["match_status"] or "pending": row["n"]
        for row in conn.execute(
            "SELECT match_status, COUNT(*) AS n FROM listings WHERE is_active = 1 GROUP BY 1"
        )
    }
    return {
        "generated_at": to_iso(now),
        "last_scrape_at": conn.execute("SELECT MAX(started_at) FROM scrape_runs").fetchone()[0],
        "stores": stores,
        "stores_ok": sum(1 for s in stores.values() if s["ok"]),
        "stores_total": len(stores),
        "products_with_offers": scalar(
            "SELECT COUNT(*) FROM product_stats WHERE best_price_usd IS NOT NULL"
        ),
        "offers": scalar("SELECT COALESCE(SUM(n_offers), 0) FROM product_stats"),
        "active_listings": scalar("SELECT COUNT(*) FROM listings WHERE is_active = 1"),
        "families": scalar("SELECT COUNT(DISTINCT family_id) FROM products"),
        "resolution": resolution,
        "quality_issues": scalar("SELECT COUNT(*) FROM quality_issues"),
    }


def write_status(status: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(status, indent=2, ensure_ascii=False), encoding="utf-8")
