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


def get_current_prices(conn: Connection, sku: str, hours: int = 48) -> list[dict]:
    cursor = conn.execute(
        """
        SELECT source, seller, price_usd, timestamp
        FROM price_snapshots
        WHERE canonical_product_id = ?
          AND timestamp >= datetime('now', '-' || ? || ' hours')
        ORDER BY timestamp DESC
        LIMIT 10
        """,
        (sku, hours),
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


def get_market_summary(conn: Connection, hours: int = 48) -> list[dict]:
    cursor = conn.execute(
        """
        WITH latest_per_source AS (
            -- Get the most recent price per source within the time window
            SELECT
                canonical_product_id,
                source,
                price_usd,
                timestamp,
                ROW_NUMBER() OVER (
                    PARTITION BY canonical_product_id, source
                    ORDER BY timestamp DESC
                ) AS rn
            FROM price_snapshots
            WHERE timestamp >= datetime('now', '-' || ? || ' hours')
        )
        SELECT
            canonical_product_id AS sku,
            MIN(price_usd) AS current_price,
            timestamp AS latest_timestamp
        FROM latest_per_source
        WHERE rn = 1
        GROUP BY canonical_product_id
        """,
        (hours,),
    )
    latest_rows = {
        row["sku"]: (row["current_price"], row["latest_timestamp"]) for row in cursor.fetchall()
    }

    skus = list(latest_rows.keys())
    if not skus:
        return []

    placeholders = ",".join("?" * len(skus))
    cursor = conn.execute(
        f"""
        SELECT canonical_product_id, price_usd
        FROM price_snapshots
        WHERE canonical_product_id IN ({placeholders})
        """,
        skus,
    )

    by_sku: dict[str, list[float]] = {}
    for row in cursor.fetchall():
        sku = row["canonical_product_id"]
        if sku not in by_sku:
            by_sku[sku] = []
        by_sku[sku].append(row["price_usd"])

    result = []
    for sku in skus:
        current_price, latest_timestamp = latest_rows[sku]
        historical_prices = by_sku.get(sku, [])
        median_price = _median(historical_prices)
        pct_diff = ((current_price - median_price) / median_price) * 100 if median_price else 0

        result.append(
            {
                "sku": sku,
                "current_price": current_price,
                "latest_timestamp": latest_timestamp,
                "median_price": round(median_price, 2),
                "pct_diff": round(pct_diff, 1),
            }
        )

    result.sort(key=lambda x: x["pct_diff"])
    return result
