"""Buy/wait signal orchestration (implemented in src/ml/)."""

from __future__ import annotations

import logging
import sqlite3

logger = logging.getLogger(__name__)


def train_and_register(conn: sqlite3.Connection) -> None:
    logger.info("ML training not implemented yet")


def train_if_stale(conn: sqlite3.Connection) -> None:
    logger.info("ML training not implemented yet")


def publish_signals(conn: sqlite3.Connection) -> None:
    logger.info("ML signals not implemented yet")
