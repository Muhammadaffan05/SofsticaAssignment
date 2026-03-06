"""
scripts/crawl_stars.py
Main entry point: crawls GitHub for 100,000 repos and saves stars to Postgres.

WHY MULTIPLE SEARCH WINDOWS?
GitHub Search API has a hard limit of 1,000 results per query.
We split into star-count ranges so each window yields up to 1,000 repos.
60+ windows x ~1,000 = enough to hit 100k target.
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
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

TARGET   = int(os.environ.get("CRAWL_TARGET", 100_000))
DB_BATCH = 500

STAR_WINDOWS = [
    (1, 1), (2, 2), (3, 3), (4, 4), (5, 5),
    (6, 7), (8, 9), (10, 11), (12, 13), (14, 15),
    (16, 18), (19, 21), (22, 24), (25, 27), (28, 30),
    (31, 34), (35, 38), (39, 42), (43, 47), (48, 52),
    (53, 57), (58, 63), (64, 69), (70, 76), (77, 83),
    (84, 91), (92, 100), (101, 110), (111, 120), (121, 135),
    (136, 150), (151, 170), (171, 190), (191, 215), (216, 240),
    (241, 270), (271, 305), (306, 345), (346, 390), (391, 440),
    (441, 500), (501, 570), (571, 650), (651, 740), (741, 850),
    (851, 980), (981, 1150), (1151, 1350), (1351, 1600), (1601, 1900),
    (1901, 2300), (2301, 2800), (2801, 3500), (3501, 4500), (4501, 6000),
    (6001, 8000), (8001, 11000), (11001, 15000), (15001, 21000),
    (21001, 30000), (30001, 999999999),
]


def crawl_window(client, db, low, high, total_saved, grand_total, start_time):
    cursor = CrawlCursor.initial()
    buffer = []
    logger.info("Window stars:%d..%d - starting", low, high)

    while cursor.has_next_page and grand_total < TARGET:
        page   = client.fetch_page(cursor, low=low, high=high)
        cursor = page.next_cursor
        buffer.extend(page.repositories)
        grand_total += page.count

        if len(buffer) >= DB_BATCH:
            saved        = db.bulk_upsert_repositories(buffer[:DB_BATCH])
            total_saved += saved
            buffer       = buffer[DB_BATCH:]

        elapsed = time.time() - start_time
        rate    = grand_total / elapsed if elapsed > 0 else 0
        logger.info("  page=%d  total=%d/%d  %.0f repos/s", page.count, grand_total, TARGET, rate)

    if buffer:
        total_saved += db.bulk_upsert_repositories(buffer)

    logger.info("Window stars:%d..%d done - grand_total=%d", low, high, grand_total)
    return total_saved, grand_total


def main():
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        logger.error("GITHUB_TOKEN is not set.")
        sys.exit(1)

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        logger.error("DATABASE_URL is not set.")
        sys.exit(1)

    client      = GitHubClient(token)
    db          = Database(dsn)
    db.connect()
    total_saved = 0
    grand_total = 0
    start_time  = time.time()

    logger.info("Starting crawl - target: %d repositories.", TARGET)

    try:
        for (low, high) in STAR_WINDOWS:
            if grand_total >= TARGET:
                break
            total_saved, grand_total = crawl_window(
                client, db, low, high, total_saved, grand_total, start_time,
            )
    finally:
        db.close()

    elapsed = time.time() - start_time
    logger.info(
        "Crawl complete. Fetched: %d | DB rows changed: %d | Time: %.1fs",
        grand_total, total_saved, elapsed,
    )


if __name__ == "__main__":
    main()