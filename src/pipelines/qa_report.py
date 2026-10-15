"""
Static HTML report of how listings were grouped, for manual review after a
taxonomy change: every multi-store product with its offers, plus unresolved
and family-only listings.
"""

from __future__ import annotations

import html
import sqlite3
from pathlib import Path


def write_qa_report(conn: sqlite3.Connection, out: Path) -> Path:
    rows = conn.execute(
        """
        SELECT p.id, p.name, p.category, f.name AS family, l.store_id, l.title, l.match_status,
               (SELECT price_usd FROM price_periods pp WHERE pp.listing_id = l.id
                ORDER BY valid_from DESC LIMIT 1) AS price
        FROM listings l
        JOIN products p ON p.id = l.product_id
        LEFT JOIN families f ON f.id = p.family_id
        WHERE l.is_active = 1
        ORDER BY p.category, f.name, p.name, price
        """
    ).fetchall()
    groups: dict[int, list[sqlite3.Row]] = {}
    for row in rows:
        groups.setdefault(row["id"], []).append(row)
    unresolved = conn.execute(
        "SELECT store_id, category, title, match_status, match_notes FROM listings "
        "WHERE is_active = 1 AND match_status IN ('unparsed', 'rejected') ORDER BY category, title"
    ).fetchall()

    def esc(value: object) -> str:
        return html.escape(str(value if value is not None else ""))

    parts = [
        "<!doctype html><meta charset='utf-8'><title>Hardware Pulse QA</title>",
        "<style>body{font:14px system-ui;margin:24px}h2{margin-top:28px}"
        "td{padding:2px 8px;border-bottom:1px solid #ddd}.m{color:#555}</style>",
        "<h1>Agrupación de productos</h1>",
    ]
    multi = [g for g in groups.values() if len({r["store_id"] for r in g}) > 1]
    parts.append(
        f"<p>{len(groups)} productos activos, {len(multi)} con ofertas de varias tiendas.</p>"
    )
    for group in multi:
        head = group[0]
        prices = [r["price"] for r in group if r["price"] is not None]
        spread = f"U$S {min(prices):.0f} – {max(prices):.0f}" if prices else ""
        family = f"<span class='m'>({esc(head['family'])}) {spread}</span>"
        parts.append(f"<h3>{esc(head['name'])} {family}</h3><table>")
        for r in group:
            cells = (r["store_id"], r["price"], r["title"])
            parts.append("<tr>" + "".join(f"<td>{esc(c)}</td>" for c in cells) + "</tr>")
        parts.append("</table>")
    parts.append("<h2>Sin resolver / descartados</h2><table>")
    for r in unresolved:
        cells = (r["category"], r["store_id"], r["match_status"], r["match_notes"], r["title"])
        parts.append("<tr>" + "".join(f"<td>{esc(c)}</td>" for c in cells) + "</tr>")
    parts.append("</table>")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(parts), encoding="utf-8")
    return out
