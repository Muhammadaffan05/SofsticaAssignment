"""
models.py - Immutable data models (anti-corruption layer).
These dataclasses are the ONLY representation of GitHub data
inside our application. All external API responses must be
converted to these models before touching any other layer.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)  # frozen = immutable after creation
class Repository:
    """
    Represents a single GitHub repository.
    'frozen=True' makes this immutable - values can never be changed
    after the object is created. This prevents accidental mutations.
    """
    github_id: str          # GitHub's internal node ID (globally unique)
    owner: str              # Repository owner login e.g. "facebook"
    name: str               # Repository name e.g. "react"
    full_name: str          # "owner/name" e.g. "facebook/react"
    stars: int              # Current star count
    crawled_at: datetime = field(default_factory=datetime.utcnow)

    def __post_init__(self):
        """Validate data immediately on creation."""
        if not self.github_id:
            raise ValueError("github_id cannot be empty")
        if not self.full_name:
            raise ValueError("full_name cannot be empty")
        if self.stars < 0:
            raise ValueError(f"stars cannot be negative, got {self.stars}")

    @staticmethod
    def from_graphql_node(node: dict) -> "Repository":
        """
        Anti-corruption layer: converts raw GraphQL API response
        into our clean internal model. All messy API details stay HERE.
        """
        owner = node.get("owner", {}).get("login", "")
        name = node.get("name", "")
        return Repository(
            github_id=node["id"],
            owner=owner,
            name=name,
            full_name=f"{owner}/{name}",
            stars=node.get("stargazerCount", 0),
        )


@dataclass(frozen=True)
class CrawlCursor:
    """
    Tracks pagination state for a crawl session.
    Immutable - to 'advance' the cursor you create a new one.
    """
    end_cursor: Optional[str]   # GitHub's pagination cursor (base64 string)
    has_next_page: bool         # Whether more pages exist
    repos_fetched: int = 0      # Running total fetched so far

    @staticmethod
    def initial() -> "CrawlCursor":
        """Starting cursor before any requests have been made."""
        return CrawlCursor(end_cursor=None, has_next_page=True, repos_fetched=0)

    def advance(self, new_end_cursor: Optional[str], has_next_page: bool, batch_size: int) -> "CrawlCursor":
        """Return a NEW cursor advanced by one page."""
        return CrawlCursor(
            end_cursor=new_end_cursor,
            has_next_page=has_next_page,
            repos_fetched=self.repos_fetched + batch_size,
        )


@dataclass(frozen=True)
class PageResult:
    """
    The result of one paginated API call.
    Contains the repos from that page + the next cursor.
    """
    repositories: tuple          # tuple (immutable) of Repository objects
    next_cursor: CrawlCursor

    @property
    def count(self) -> int:
        return len(self.repositories)