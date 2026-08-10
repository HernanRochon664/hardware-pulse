"""
Master pipeline entrypoint that runs all three stages sequentially:
    ingest → resolve → features

Optionally runs training and inference after feature engineering.

If any stage fails, the pipeline stops immediately with a non-zero exit code
so Windows Task Scheduler (or any caller) can detect the failure.

Usage:
    uv run scripts/run_pipeline.py
    uv run scripts/run_pipeline.py --since 2026-04-01
    uv run scripts/run_pipeline.py --with-train
    uv run scripts/run_pipeline.py --with-train --with-inference

This replaces the three separate Task Scheduler entries at 17:00 / 17:15 / 17:30
with a single scheduled task.
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import load_config
from src.entities.catalog import load_catalog
from src.pipelines.features import build_features
from src.pipelines.ingest import ingest
from src.pipelines.resolve import resolve
from src.scrapers.banifox import BanifoxScraper
from src.scrapers.pccompu import PCCompuScraper
from src.scrapers.thot import ThotScraper
from src.storage.schema import init_db

logger = logging.getLogger(__name__)

DB_PATH = Path("data/hardware_pulse.db")
LOG_PATH = Path("logs/scheduler.log")
ALERTS_PATH = Path("logs/alerts.log")


def setup_logging() -> None:
    log_dir = LOG_PATH.parent
    log_dir.mkdir(parents=True, exist_ok=True)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    root_logger.addHandler(console)

    file_handler = logging.FileHandler(LOG_PATH)
    file_handler.setFormatter(fmt)
    root_logger.addHandler(file_handler)


def build_scrapers(config):
    scrapers = []

    if config.thot.enabled:
        for job in config.thot.jobs:
            scrapers.append(
                ThotScraper(
                    urls=job.urls,
                    delay=config.resolve_request_delay(
                        config.thot.defaults,
                        job.request_delay,
                    ),
                    max_pages_per_url=config.resolve_max_pages(
                        config.thot.defaults,
                        job.max_pages_per_url,
                    ),
                    timeout=config.global_.timeout,
                )
            )

    if config.banifox.enabled:
        for job in config.banifox.jobs:
            scrapers.append(
                BanifoxScraper(
                    urls=job.urls,
                    delay=config.resolve_request_delay(
                        config.banifox.defaults,
                        job.request_delay,
                    ),
                    max_pages_per_url=config.resolve_max_pages(
                        config.banifox.defaults,
                        job.max_pages_per_url,
                    ),
                    timeout=config.global_.timeout,
                )
            )

    if config.pccompu.enabled:
        for job in config.pccompu.jobs:
            scrapers.append(
                PCCompuScraper(
                    urls=job.urls,
                    delay=config.resolve_request_delay(
                        config.pccompu.defaults,
                        job.request_delay,
                    ),
                    max_pages_per_url=config.resolve_max_pages(
                        config.pccompu.defaults,
                        job.max_pages_per_url,
                    ),
                    timeout=config.global_.timeout,
                )
            )

    return scrapers


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the full hardware-pulse pipeline (ingest → resolve → features)."
    )
    parser.add_argument(
        "--since",
        type=str,
        default=None,
        help="Only process snapshots at or after this date (YYYY-MM-DD). "
        "Passed through to the feature-engineering stage.",
    )
    parser.add_argument(
        "--with-train",
        action="store_true",
        default=False,
        help="Run model training after feature engineering.",
    )
    parser.add_argument(
        "--with-inference",
        action="store_true",
        default=False,
        help="Run inference after training (or after features if --with-train is not set).",
    )
    return parser.parse_args()


def main() -> None:
    setup_logging()

    args = parse_args()

    since: datetime | None = None
    if args.since:
        try:
            since = datetime.fromisoformat(args.since).replace(tzinfo=UTC)
        except ValueError:
            logger.error("Invalid --since format. Expected YYYY-MM-DD, got: %s", args.since)
            sys.exit(1)

    run_ts = datetime.now(UTC)
    logger.info("=== Pipeline start at %s ===", run_ts.isoformat())

    # -----------------------------------------------------------------------
    # Stage 1 — Ingestion
    # -----------------------------------------------------------------------
    logger.info("--- Stage 1/3: Ingestion ---")
    config = load_config()
    logger.info("Initializing database at %s...", DB_PATH)
    conn = init_db(DB_PATH)
    scrapers = build_scrapers(config)

    if not scrapers:
        logger.warning("No scrapers enabled. Skipping ingestion.")
    else:
        ingest_result = ingest(conn=conn, scrapers=scrapers)
        logger.info("Ingestion complete: %s", ingest_result)
        successful_scrapers = sum(1 for count in ingest_result.per_scraper.values() if count > 0)
        total_scrapers = len(ingest_result.per_scraper)
        if ingest_result.errors and successful_scrapers == 0:
            logger.error("Ingestion failed: all %d scraper(s) errored", total_scrapers)
            sys.exit(1)
        if ingest_result.errors:
            logger.warning(
                "Ingestion completed with %d error(s) from %d/%d scraper(s) "
                "— continuing with partial data",
                ingest_result.errors,
                total_scrapers - successful_scrapers,
                total_scrapers,
            )

        for scraper_name, count in ingest_result.per_scraper.items():
            if count == 0:
                ALERTS_PATH.parent.mkdir(parents=True, exist_ok=True)
                with open(ALERTS_PATH, "a") as f:
                    f.write(
                        f"{datetime.now(UTC).isoformat()} [CRITICAL] "
                        f"Scraper '{scraper_name}' returned 0 rows\n"
                    )
                logger.critical(
                    "Scraper '%s' returned 0 rows — alert written to %s",
                    scraper_name,
                    ALERTS_PATH,
                )

    conn.close()

    # -----------------------------------------------------------------------
    # Stage 2 — Entity Resolution
    # -----------------------------------------------------------------------
    logger.info("--- Stage 2/3: Entity Resolution ---")
    conn = init_db(DB_PATH)
    catalog = load_catalog()
    resolve_result = resolve(conn=conn, catalog=catalog)
    logger.info("Resolution complete: %s", resolve_result)
    if resolve_result.errors:
        logger.error("Resolution failed with %d error(s)", resolve_result.errors)
        sys.exit(1)

    conn.close()

    # -----------------------------------------------------------------------
    # Stage 3 — Feature Engineering
    # -----------------------------------------------------------------------
    logger.info("--- Stage 3/3: Feature Engineering ---")
    conn = init_db(DB_PATH)
    feature_result = build_features(conn, since=since)

    logger.info(
        "Feature pipeline finished | weeks=%d | skus=%d | rows=%d | fx_fetched=%d | errors=%d",
        feature_result.weeks_processed,
        feature_result.skus_processed,
        feature_result.rows_written,
        feature_result.fx_rates_fetched,
        len(feature_result.errors),
    )

    if feature_result.errors:
        logger.warning("Errors during feature pipeline:")
        for err in feature_result.errors:
            logger.warning("  - %s", err)
        sys.exit(1)

    conn.close()

    # -----------------------------------------------------------------------
    # Stage 4 — Training (optional)
    # -----------------------------------------------------------------------
    if args.with_train:
        logger.info("--- Stage 4/5: Training ---")
        train_script = Path(__file__).parent / "run_train.py"
        result = subprocess.run(
            [sys.executable, str(train_script)],
        )
        if result.returncode != 0:
            logger.error("Training failed with exit code %d", result.returncode)
            sys.exit(1)
        logger.info("Training complete.")

    # -----------------------------------------------------------------------
    # Stage 5 — Inference (optional)
    # -----------------------------------------------------------------------
    if args.with_inference:
        logger.info("--- Stage 5/5: Inference ---")
        inference_script = Path(__file__).parent / "run_inference.py"
        result = subprocess.run(
            [sys.executable, str(inference_script)],
        )
        if result.returncode != 0:
            logger.error("Inference failed with exit code %d", result.returncode)
            sys.exit(1)
        logger.info("Inference complete.")

    logger.info("=== Pipeline complete at %s ===", datetime.now(UTC).isoformat())

    # -----------------------------------------------------------------------
    # Stage 6 — Auto-commit & push (only runs after full success)
    # -----------------------------------------------------------------------
    git_script = Path(__file__).parent / "run_git.py"
    result = subprocess.run([sys.executable, str(git_script)], text=True)
    for line in result.stdout.splitlines():
        logger.info("git: %s", line)
    for line in result.stderr.splitlines():
        logger.error("git: %s", line)
    if result.returncode != 0:
        logger.error("Auto-commit/push failed with exit code %d", result.returncode)
        sys.exit(result.returncode)
    logger.info("Auto-commit/push complete.")


if __name__ == "__main__":
    main()
