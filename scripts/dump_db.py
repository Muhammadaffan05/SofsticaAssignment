"""
scripts/dump_db.py
Exports the repositories table to a CSV file for upload as a GitHub Actions artifact.
"""

import csv
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from crawler.db import Database

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

OUTPUT_PATH = os.environ.get("DUMP_OUTPUT", "repositories.csv")


def main() -> None:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        logger.error("DATABASE_URL is not set.")
        sys.exit(1)

    db = Database(dsn)
    db.connect()
    try:
        rows = db.get_all_repositories()
    finally:
        db.close()

    if not rows:
        logger.warning("No rows found in database — is the crawl complete?")
        sys.exit(1)

    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    logger.info("Dumped %d rows to %s", len(rows), OUTPUT_PATH)


if __name__ == "__main__":
    main()