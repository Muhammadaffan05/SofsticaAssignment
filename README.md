

## Q1 — 500 Million Repositories?

| Problem | Solution |
|---------|----------|
| Single process too slow | Distributed workers via AWS SQS / Redis Streams |
| GitHub rate limits | Rotate multiple GitHub App tokens |
| Postgres won't scale | Columnar store (ClickHouse / Redshift / Parquet on S3) |
| Re-crawling everything daily | Incremental crawl via GitHub Events API — only fetch changed repos |
| Sequential HTTP requests | Async I/O with `httpx` + `asyncio` for parallel requests |
| Single DB node | Shard by `github_id` hash or use CockroachDB |

---

## Q2 — Schema Evolution for Richer Metadata?

**Rule: every new entity = new table. Existing tables never change.**

```sql
repositories       (github_id UNIQUE, owner, name, stars, updated_at)
repo_issues        (github_id UNIQUE, repo_id FK, number, title, state)
repo_pull_requests (github_id UNIQUE, repo_id FK, number, title, state)
comments           (github_id UNIQUE, pr_id FK nullable, issue_id FK nullable, body)
pr_reviews         (github_id UNIQUE, pr_id FK, state, submitted_at)
ci_checks          (github_id UNIQUE, pr_id FK, name, status, conclusion)
```

**Why this is efficient:**
- PR gets 10 new comments tomorrow → 10 INSERTs only, zero existing rows touched
- Star count unchanged → row not written at all (`IS DISTINCT FROM` check)
- New metadata (e.g. Discussions) → add new table, zero downtime, zero existing queries affected

**Upsert pattern used throughout:**
```sql
INSERT INTO repositories (github_id, stars, ...)
VALUES (...)
ON CONFLICT (github_id) DO UPDATE
  SET stars = EXCLUDED.stars, updated_at = NOW()
WHERE stars IS DISTINCT FROM EXCLUDED.stars;
```