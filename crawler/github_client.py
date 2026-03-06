"""
github_client.py - GitHub GraphQL API client.
Handles: requests, pagination, rate-limit detection, and retries.
Nothing outside this file knows about HTTP or GitHub API details.
"""

import logging
import time
from typing import Optional

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
)

from crawler.models import CrawlCursor, PageResult, Repository

logger = logging.getLogger(__name__)

# We search for repos with >0 stars, fetching 100 per page (GitHub's max).
# Using `search` instead of `repositories` gives us cursor-based pagination
# over a large result set.

_SEARCH_QUERY = """
query($cursor: String) {
  search(
    query: "stars:>0 sort:stars-asc"
    type: REPOSITORY
    first: 100
    after: $cursor
  ) {
    pageInfo {
      endCursor
      hasNextPage
    }
    nodes {
      ... on Repository {
        id
        name
        owner { login }
        stargazerCount
      }
    }
  }
  rateLimit {
    remaining
    resetAt
    cost
  }
}
"""

GITHUB_GRAPHQL_URL = "https://api.github.com/graphql"
RATE_LIMIT_BUFFER = 50   # pause when fewer than this many points remain


class RateLimitExceeded(Exception):
    """Raised when GitHub rate limit is hit so tenacity can retry."""


class GitHubClient:
    """
    Stateless GraphQL client for GitHub.
    One instance per crawl session.
    """

    def __init__(self, token: str):
        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/vnd.github+json",
            "X-Github-Next-Global-ID": "1",   # opt into new global node IDs
        })

    def fetch_page(self, cursor: CrawlCursor) -> PageResult:
        """
        Fetch one page of up to 100 repositories.
        Automatically handles rate limits and retries.
        """
        return self._fetch_with_retry(cursor)

    @retry(
        retry=retry_if_exception_type((RateLimitExceeded, requests.ConnectionError, requests.Timeout)),
        wait=wait_exponential(multiplier=2, min=4, max=120),   # 4s, 8s, 16s … up to 120s
        stop=stop_after_attempt(7),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def _fetch_with_retry(self, cursor: CrawlCursor) -> PageResult:
        variables = {"cursor": cursor.end_cursor}

        response = self._session.post(
            GITHUB_GRAPHQL_URL,
            json={"query": _SEARCH_QUERY, "variables": variables},
            timeout=30,
        )

        # HTTP-level errors
        if response.status_code == 403:
            self._handle_rate_limit(response)
        response.raise_for_status()

        payload = response.json()

        # GraphQL-level errors
        if "errors" in payload:
            errors = payload["errors"]
            if any(e.get("type") == "RATE_LIMITED" for e in errors):
                logger.warning("GraphQL rate limit hit.")
                raise RateLimitExceeded(str(errors))
            raise RuntimeError(f"GraphQL errors: {errors}")

        return self._parse_response(payload, cursor)


    @staticmethod
    def _parse_response(payload: dict, cursor: CrawlCursor) -> PageResult:
        search = payload["data"]["search"]
        rate   = payload["data"]["rateLimit"]

        # Log rate limit status every page
        logger.debug(
            "Rate limit — remaining: %d, cost: %d, reset: %s",
            rate["remaining"], rate["cost"], rate["resetAt"]
        )

        # Proactively pause if we're running low on points
        if rate["remaining"] < RATE_LIMIT_BUFFER:
            logger.warning(
                "Rate limit low (%d remaining). Sleeping 60s...", rate["remaining"]
            )
            time.sleep(60)

        # Convert raw nodes → immutable Repository models
        repos = tuple(
            Repository.from_graphql_node(node)
            for node in search["nodes"]
            if node  # nodes can be None if repo was deleted mid-crawl
        )

        page_info   = search["pageInfo"]
        next_cursor = cursor.advance(
            new_end_cursor=page_info["endCursor"],
            has_next_page=page_info["hasNextPage"],
            batch_size=len(repos),
        )

        return PageResult(repositories=repos, next_cursor=next_cursor)


    @staticmethod
    def _handle_rate_limit(response: requests.Response) -> None:
        """Parse Retry-After header and sleep accordingly, then raise to trigger retry."""
        retry_after = int(response.headers.get("Retry-After", 60))
        logger.warning("HTTP 403 — sleeping %ds before retry.", retry_after)
        time.sleep(retry_after)
        raise RateLimitExceeded("HTTP 403 rate limit")