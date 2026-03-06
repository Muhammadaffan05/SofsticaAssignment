"""
scripts/crawl_stars.py
Main entry point: crawls GitHub for 100,000 repos and saves stars to Postgres.

Design goals:
  - Speed: bulk upserts in batches of 500, minimising round-trips to DB.
  - Safety: respects rate limits, retries on transient failures.
  - Idempotency: re-running updates existing rows (upsert), never duplicates.
"""

import logging
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from crawler.db import Database
from crawler.github_client import GitHubClient
from crawler.models import CrawlCursor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

TARGET       = int(os.environ.get("CRAWL_TARGET", 100_000))  # repos to fetch
DB_BATCH     = 500    # repos per DB write
LOG_INTERVAL = 5_000  # log progress every N repos


def main() -> None:
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        logger.error("GITHUB_TOKEN is not set.")
        sys.exit(1)

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        logger.error("DATABASE_URL is not set.")
        sys.exit(1)

    client = GitHubClient(token)
    db     = Database(dsn)
    db.connect()

    cursor     = CrawlCursor.initial()
    buffer     = []        # holds repos until DB_BATCH is reached
    total_saved = 0
    start_time  = time.time()

    logger.info("Starting crawl — target: %d repositories.", TARGET)

    try:
        while cursor.repos_fetched < TARGET and cursor.has_next_page:
            page   = client.fetch_page(cursor)
            cursor = page.next_cursor

            buffer.extend(page.repositories)

            # Flush to DB when buffer is full
            if len(buffer) >= DB_BATCH:
                saved       = db.bulk_upsert_repositories(buffer[:DB_BATCH])
                total_saved += saved
                buffer       = buffer[DB_BATCH:]

            fetched = cursor.repos_fetched
            if fetched % LOG_INTERVAL == 0 or fetched >= TARGET:
                elapsed = time.time() - start_time
                rate    = fetched / elapsed if elapsed > 0 else 0
                logger.info(
                    "Progress: %d/%d fetched | %.0f repos/sec | %.1fs elapsed",
                    fetched, TARGET, rate, elapsed,
                )

        # Flush remaining buffer
        if buffer:
            saved        = db.bulk_upsert_repositories(buffer)
            total_saved += saved

    finally:
        db.close()

    elapsed = time.time() - start_time
    logger.info(
        "Crawl complete. Fetched: %d | DB rows changed: %d | Time: %.1fs",
        cursor.repos_fetched, total_saved, elapsed,
    )


if __name__ == "__main__":
    main()