"""
Hardware Pulse command line.

    python -m src.cli run                    # scrape -> resolve -> aggregate -> signals
    python -m src.cli run --stores thot,laaca --categories gpu
    python -m src.cli scrape --dry-run       # live scrape into a throwaway DB
    python -m src.cli resolve [--full]
    python -m src.cli aggregate
    python -m src.cli train | signals
    python -m src.cli status [--json data/status.json]
    python -m src.cli import-legacy          # one-off: v1 history from git
    python -m src.cli pull-data              # download the published DB
    python -m src.cli discover-categories thot
    python -m src.cli qa-report [--out data/qa_report.html]

Exit codes: 0 = ok, 2 = degraded (some store/category failed), 1 = fatal error.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid
from pathlib import Path

from src.config import load_stores_config
from src.domain.models import Category
from src.settings import DATA_DIR, db_path, to_iso, utcnow
from src.storage.db import connect

logger = logging.getLogger("hardware_pulse")

EXIT_OK, EXIT_FATAL, EXIT_DEGRADED = 0, 1, 2


def _csv(value: str | None) -> list[str] | None:
    return [v.strip() for v in value.split(",") if v.strip()] if value else None


def _categories(value: str | None) -> list[Category] | None:
    names = _csv(value)
    return [Category(n) for n in names] if names else None


def cmd_scrape(args: argparse.Namespace) -> int:
    from src.pipelines.scrape import scrape_stores

    conn = connect(":memory:" if args.dry_run else args.db)
    run_id = args.run_id or _new_run_id()
    results = scrape_stores(
        conn,
        load_stores_config(),
        run_id=run_id,
        stores=_csv(args.stores),
        categories=_categories(args.categories),
    )
    failed = [r for r in results if r.failed]
    for r in failed:
        logger.warning("FAILED %s/%s: %s %s", r.store_id, r.category.value, r.status, r.error or "")
    return EXIT_DEGRADED if failed else EXIT_OK


def cmd_resolve(args: argparse.Namespace) -> int:
    from src.pipelines.resolve import resolve_listings

    resolve_listings(connect(args.db), full=args.full)
    return EXIT_OK


def cmd_aggregate(args: argparse.Namespace) -> int:
    from src.pipelines.aggregate import aggregate

    aggregate(connect(args.db))
    return EXIT_OK


def cmd_train(args: argparse.Namespace) -> int:
    from src.ml.signals import train_and_register

    train_and_register(connect(args.db))
    return EXIT_OK


def cmd_signals(args: argparse.Namespace) -> int:
    from src.ml.signals import publish_signals

    publish_signals(connect(args.db))
    return EXIT_OK


def cmd_status(args: argparse.Namespace) -> int:
    from src.pipelines.status import build_status, write_status

    status = build_status(connect(args.db))
    if args.json:
        write_status(status, Path(args.json))
    print(json.dumps(status, indent=2, ensure_ascii=False))
    return EXIT_OK


def cmd_run(args: argparse.Namespace) -> int:
    from src.ml.signals import publish_signals, train_if_stale
    from src.pipelines.aggregate import aggregate
    from src.pipelines.resolve import resolve_listings
    from src.pipelines.scrape import scrape_stores
    from src.pipelines.status import build_status, write_status

    conn = connect(args.db)
    run_id = args.run_id or _new_run_id()
    logger.info("=== run %s started (db=%s) ===", run_id, args.db)
    failed = []
    if not args.skip_scrape:
        results = scrape_stores(
            conn,
            load_stores_config(),
            run_id=run_id,
            stores=_csv(args.stores),
            categories=_categories(args.categories),
        )
        failed = [r for r in results if r.failed]
    resolve_listings(conn)
    aggregate(conn)
    if not args.skip_ml:
        try:
            train_if_stale(conn)
            publish_signals(conn)
        except Exception:  # the ML signal is optional; prices must still be published
            logger.exception("ML stage failed; prices are still published")
    status = build_status(conn)
    status["run_id"] = run_id
    status["degraded"] = [f"{r.store_id}/{r.category.value}: {r.status}" for r in failed]
    write_status(status, Path(args.status_json))
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()
    for line in status["degraded"]:
        logger.warning("DEGRADED %s", line)
    logger.info(
        "=== run %s finished: %d products with offers, %d/%d stores ok ===",
        run_id,
        status["products_with_offers"],
        status["stores_ok"],
        status["stores_total"],
    )
    return EXIT_DEGRADED if failed else EXIT_OK


def cmd_import_legacy(args: argparse.Namespace) -> int:
    from src.pipelines.legacy_import import import_legacy_history

    import_legacy_history(connect(args.db), logs_dir=Path(args.logs))
    return EXIT_OK


def cmd_pull_data(args: argparse.Namespace) -> int:
    from src.storage.remote import download_database

    path = download_database(Path(args.db), url=args.url)
    print(f"Base de datos descargada en {path}")
    return EXIT_OK


def cmd_discover(args: argparse.Namespace) -> int:
    from src.scrapers.discovery import discover_categories

    store = load_stores_config().stores[args.store]
    for line in discover_categories(store):
        print(line)
    return EXIT_OK


def cmd_qa_report(args: argparse.Namespace) -> int:
    from src.pipelines.qa_report import write_qa_report

    out = write_qa_report(connect(args.db), Path(args.out))
    print(f"Reporte escrito en {out}")
    return EXIT_OK


def _new_run_id() -> str:
    return f"{to_iso(utcnow())}-{uuid.uuid4().hex[:6]}"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m src.cli", description="Hardware Pulse: comparador de precios de hardware"
    )
    parser.add_argument("--db", default=str(db_path()), help="SQLite database path")
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    def store_filters(p: argparse.ArgumentParser) -> None:
        p.add_argument("--stores", help="comma separated store ids (default: all enabled)")
        p.add_argument(
            "--categories", help="comma separated: " + ",".join(c.value for c in Category)
        )
        p.add_argument("--run-id")

    p = sub.add_parser("run", help="full pipeline")
    store_filters(p)
    p.add_argument("--skip-scrape", action="store_true")
    p.add_argument("--skip-ml", action="store_true")
    p.add_argument("--status-json", default=str(DATA_DIR / "status.json"))
    p.set_defaults(func=cmd_run)

    p = sub.add_parser("scrape", help="scrape stores only")
    store_filters(p)
    p.add_argument("--dry-run", action="store_true", help="use a throwaway in-memory DB")
    p.set_defaults(func=cmd_scrape)

    p = sub.add_parser("resolve", help="resolve listings to products")
    p.add_argument("--full", action="store_true", help="re-resolve every listing")
    p.set_defaults(func=cmd_resolve)

    sub.add_parser("aggregate", help="rebuild derived tables").set_defaults(func=cmd_aggregate)
    sub.add_parser("train", help="train the buy/wait model").set_defaults(func=cmd_train)
    sub.add_parser("signals", help="publish buy/wait signals").set_defaults(func=cmd_signals)

    p = sub.add_parser("status", help="print data health")
    p.add_argument("--json")
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("import-legacy", help="import v1 price history from git")
    p.add_argument("--logs", default="logs", help="v1 logs directory (scheduler/alerts logs)")
    p.set_defaults(func=cmd_import_legacy)

    p = sub.add_parser("pull-data", help="download the published database")
    p.add_argument("--url")
    p.set_defaults(func=cmd_pull_data)

    p = sub.add_parser("discover-categories", help="list a store's categories")
    p.add_argument("store")
    p.set_defaults(func=cmd_discover)

    p = sub.add_parser("qa-report", help="HTML report of product groups for review")
    p.add_argument("--out", default=str(DATA_DIR / "qa_report.html"))
    p.set_defaults(func=cmd_qa_report)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    try:
        return int(args.func(args))
    except Exception:
        logger.exception("Fatal error in %s", args.command)
        return EXIT_FATAL


if __name__ == "__main__":
    sys.exit(main())
