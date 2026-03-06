"""
scripts/setup_db.py
Connects to Postgres and applies the schema (creates tables if missing).
Safe to run multiple times — uses CREATE TABLE IF NOT EXISTS.
"""

import logging
import os
import sys

# Allow running from repo root: python scripts/setup_db.py
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from crawler.db import Database

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        logger.error("DATABASE_URL environment variable is not set.")
        sys.exit(1)

    db = Database(dsn)
    db.connect()
    try:
        db.apply_schema()
        logger.info("Database setup complete.")
    finally:
        db.close()


if __name__ == "__main__":
    main()