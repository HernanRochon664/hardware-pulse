from pathlib import Path
from sqlite3 import Connection


def get_connection(db_path: Path = Path("data/hardware_pulse.db")) -> Connection:
    import sqlite3

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def get_all_skus(conn: Connection) -> list[str]:
    cursor = conn.execute(
        """
        SELECT DISTINCT canonical_product_id
        FROM price_snapshots
        ORDER BY canonical_product_id
        """
    )
    return [row[0] for row in cursor.fetchall()]


def get_current_prices(conn: Connection, sku: str) -> list[dict]:
    cursor = conn.execute(
        """
        SELECT source, seller, price_usd, timestamp
        FROM price_snapshots
        WHERE canonical_product_id = ?
        ORDER BY timestamp DESC
        LIMIT 10
        """,
        (sku,),
    )
    rows = cursor.fetchall()
    seen_sources = set()
    result = []
    for row in rows:
        if row["source"] not in seen_sources:
            seen_sources.add(row["source"])
            result.append(
                {
                    "source": row["source"],
                    "seller": row["seller"],
                    "price_usd": row["price_usd"],
                    "timestamp": row["timestamp"],
                }
            )
    return result


def get_price_history(conn: Connection, sku: str) -> list[dict]:
    cursor = conn.execute(
        """
        SELECT timestamp, source, seller, price_usd
        FROM price_snapshots
        WHERE canonical_product_id = ?
        ORDER BY timestamp ASC
        """,
        (sku,),
    )
    return [dict(row) for row in cursor.fetchall()]


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    mid = n // 2
    if n % 2 == 0:
        return (sorted_vals[mid - 1] + sorted_vals[mid]) / 2
    return sorted_vals[mid]


def get_market_summary(conn: Connection) -> list[dict]:
    cursor = conn.execute(
        """
        SELECT
            canonical_product_id,
            price_usd,
            timestamp
        FROM price_snapshots
        ORDER BY canonical_product_id, timestamp DESC
        """
    )
    rows = cursor.fetchall()

    by_sku: dict[str, list[float]] = {}
    latest_by_sku: dict[str, float] = {}

    for row in rows:
        sku = row["canonical_product_id"]
        price = row["price_usd"]

        if sku not in by_sku:
            by_sku[sku] = []
            latest_by_sku[sku] = price

        by_sku[sku].append(price)

    result = []
    for sku, prices in by_sku.items():
        current_price = latest_by_sku[sku]
        median_price = _median(prices)
        pct_diff = ((current_price - median_price) / median_price) * 100 if median_price else 0

        result.append(
            {
                "sku": sku,
                "current_price": current_price,
                "median_price": round(median_price, 2),
                "pct_diff": round(pct_diff, 1),
            }
        )

    result.sort(key=lambda x: x["pct_diff"])
    return result
