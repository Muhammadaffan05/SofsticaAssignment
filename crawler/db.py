"""
db.py - Database access layer (separation of concerns).
ONLY this file knows about SQL or PostgreSQL.
The rest of the app works only with model objects.
"""

import logging
from contextlib import contextmanager
from typing import Generator, List

import psycopg2
import psycopg2.extras

from crawler.models import Repository

logger = logging.getLogger(__name__)


SCHEMA_SQL = """
-- Core repositories table.
-- ON CONFLICT ... DO UPDATE = efficient "upsert":
-- if repo already exists, only the changed columns are written.
-- This means daily re-crawls touch MINIMAL rows.
CREATE TABLE IF NOT EXISTS repositories (
    id          SERIAL          PRIMARY KEY,
    github_id   TEXT            NOT NULL UNIQUE,   -- GitHub node ID (stable)
    owner       TEXT            NOT NULL,
    name        TEXT            NOT NULL,
    full_name   TEXT            NOT NULL UNIQUE,   -- "owner/name"
    stars       INTEGER         NOT NULL DEFAULT 0,
    first_seen  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- Index for fast lookups when upserting by github_id
CREATE INDEX IF NOT EXISTS idx_repos_github_id  ON repositories (github_id);
-- Index for sorting/filtering by stars (common query)
CREATE INDEX IF NOT EXISTS idx_repos_stars      ON repositories (stars DESC);

-- --------------------------------------------------------------------------
-- Future-proof metadata tables (schema evolution)
-- These are created now but stay empty until the crawler is extended.
-- Adding new tables never touches existing rows — zero downtime migrations.
-- --------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS repo_issues (
    id              SERIAL      PRIMARY KEY,
    repo_id         INTEGER     NOT NULL REFERENCES repositories(id) ON DELETE CASCADE,
    github_id       TEXT        NOT NULL UNIQUE,
    number          INTEGER     NOT NULL,
    title           TEXT,
    state           TEXT,
    created_at      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS repo_pull_requests (
    id              SERIAL      PRIMARY KEY,
    repo_id         INTEGER     NOT NULL REFERENCES repositories(id) ON DELETE CASCADE,
    github_id       TEXT        NOT NULL UNIQUE,
    number          INTEGER     NOT NULL,
    title           TEXT,
    state           TEXT,
    created_at      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Comments belong to either a PR or an Issue (nullable FK pattern).
-- Adding a new comment tomorrow = INSERT one row, no existing rows touched.
CREATE TABLE IF NOT EXISTS comments (
    id              SERIAL      PRIMARY KEY,
    github_id       TEXT        NOT NULL UNIQUE,
    pr_id           INTEGER     REFERENCES repo_pull_requests(id) ON DELETE CASCADE,
    issue_id        INTEGER     REFERENCES repo_issues(id)        ON DELETE CASCADE,
    body            TEXT,
    created_at      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pr_reviews (
    id              SERIAL      PRIMARY KEY,
    pr_id           INTEGER     NOT NULL REFERENCES repo_pull_requests(id) ON DELETE CASCADE,
    github_id       TEXT        NOT NULL UNIQUE,
    state           TEXT,       -- APPROVED / CHANGES_REQUESTED / COMMENTED
    submitted_at    TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ci_checks (
    id              SERIAL      PRIMARY KEY,
    pr_id           INTEGER     NOT NULL REFERENCES repo_pull_requests(id) ON DELETE CASCADE,
    github_id       TEXT        NOT NULL UNIQUE,
    name            TEXT,
    status          TEXT,
    conclusion      TEXT,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

UPSERT_REPO_SQL = """
INSERT INTO repositories (github_id, owner, name, full_name, stars, first_seen, updated_at)
VALUES (%(github_id)s, %(owner)s, %(name)s, %(full_name)s, %(stars)s, NOW(), NOW())
ON CONFLICT (github_id) DO UPDATE
    SET stars      = EXCLUDED.stars,
        updated_at = NOW()
WHERE repositories.stars IS DISTINCT FROM EXCLUDED.stars;
"""

# Bulk upsert using execute_values for performance
BULK_UPSERT_SQL = """
INSERT INTO repositories (github_id, owner, name, full_name, stars, first_seen, updated_at)
VALUES %s
ON CONFLICT (github_id) DO UPDATE
    SET stars      = EXCLUDED.stars,
        updated_at = NOW()
WHERE repositories.stars IS DISTINCT FROM EXCLUDED.stars;
"""


class Database:
    """
    Thin wrapper around a psycopg2 connection.
    All SQL lives here — no SQL anywhere else in the codebase.
    """

    def __init__(self, dsn: str):
        """
        dsn: PostgreSQL connection string
        e.g. "postgresql://user:pass@localhost:5432/github_crawler"
        """
        self._dsn = dsn
        self._conn = None

    def connect(self) -> None:
        self._conn = psycopg2.connect(self._dsn)
        self._conn.autocommit = False
        logger.info("Connected to PostgreSQL.")

    def close(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()
            logger.info("PostgreSQL connection closed.")

    @contextmanager
    def transaction(self) -> Generator:
        """Context manager: commits on success, rolls back on any exception."""
        try:
            yield self._conn
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

    def apply_schema(self) -> None:
        """Create all tables if they don't exist yet."""
        with self.transaction():
            with self._conn.cursor() as cur:
                cur.execute(SCHEMA_SQL)
        logger.info("Schema applied successfully.")

    def bulk_upsert_repositories(self, repos: List[Repository]) -> int:
        """
        Insert or update a batch of repositories in ONE round-trip.
        Uses psycopg2's execute_values for maximum throughput.
        Returns the number of rows actually changed.
        """
        if not repos:
            return 0

        values = [
            (r.github_id, r.owner, r.name, r.full_name, r.stars)
            for r in repos
        ]

        with self.transaction():
            with self._conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    BULK_UPSERT_SQL,
                    values,
                    template="(%s, %s, %s, %s, %s, NOW(), NOW())",
                    page_size=500,   # send 500 rows per round-trip
                )
                return cur.rowcount

    def count_repositories(self) -> int:
        with self._conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM repositories;")
            return cur.fetchone()[0]

    def get_all_repositories(self) -> List[dict]:
        """Return all rows as a list of plain dicts (for CSV export)."""
        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT github_id, owner, name, full_name, stars, first_seen, updated_at "
                "FROM repositories ORDER BY stars DESC;"
            )
            return [dict(row) for row in cur.fetchall()]