# GitHub Crawler — Written Answers

## Q1 — What would you do differently for 500 million repositories?

### Problem at Scale
100k repos at 100/page = ~1,000 API requests.  
500M repos at 100/page = ~5,000,000 requests → impossible in a single process or within GitHub's rate limits.

### Changes

**1. Distributed crawling with a work queue**  
Use a message queue (e.g. AWS SQS, Redis Streams) to distribute work across many worker processes/containers. Each worker pulls a cursor range from the queue, crawls it, and pushes results.

**2. Partition by repository characteristics**  
Split the search space across multiple parallel queries:
- By language: `language:python stars:>0`, `language:javascript stars:>0`, etc.
- By creation date range: `created:2020-01-01..2020-06-30`, etc.
Each partition runs concurrently on a separate worker.

**3. GitHub Enterprise or higher-tier tokens**  
Higher rate limits via multiple GitHub App tokens, rotating between them to multiply throughput.

**4. Columnar storage for analytics**  
Switch from Postgres rows to a columnar store (e.g. Amazon Redshift, ClickHouse, or Parquet files on S3) for efficient star-count aggregations across 500M rows.

**5. Incremental crawling**  
Don't re-crawl everything daily. Track `updated_at` on GitHub and only re-fetch repos that have changed. Use GitHub's Events API or webhooks for near-real-time updates on popular repos.

**6. Horizontal DB scaling**  
Partition the `repositories` table by `github_id` hash across multiple Postgres shards, or use a distributed DB like CockroachDB.

**7. Async I/O**  
Replace `requests` with `httpx` + `asyncio` to fire many concurrent HTTP requests from a single process, reducing idle wait time during API calls.

---

## Q2 — How will the schema evolve for richer metadata?

### Core Design Rule
> Every new entity type gets its own table. Rows are only added or updated — never restructured.

### Schema Additions

```sql
-- Already created in setup_db.py:

repo_issues          (id, repo_id FK, github_id UNIQUE, number, title, state, ...)
repo_pull_requests   (id, repo_id FK, github_id UNIQUE, number, title, state, ...)
comments             (id, github_id UNIQUE, pr_id FK nullable, issue_id FK nullable, body, ...)
pr_reviews           (id, pr_id FK, github_id UNIQUE, state, submitted_at, ...)
ci_checks            (id, pr_id FK, github_id UNIQUE, name, status, conclusion, ...)
```

### Why this is efficient for updates

**PR with 10 comments today, 20 tomorrow:**
- The 10 existing comment rows are untouched (no UPDATE)
- 10 new rows are INSERTed into `comments`
- Only 10 rows are written — minimal I/O

**Star count changes:**
- `ON CONFLICT (github_id) DO UPDATE SET stars = EXCLUDED.stars WHERE stars IS DISTINCT FROM EXCLUDED.stars`
- The `WHERE` clause means rows with unchanged star counts are not written at all

**Adding a new metadata type in future (e.g. Discussions):**
- Add a new `repo_discussions` table
- Zero changes to existing tables
- Zero downtime — existing queries are unaffected

### Efficient upsert pattern (used throughout)

```sql
INSERT INTO comments (github_id, pr_id, body, ...)
VALUES (...)
ON CONFLICT (github_id) DO UPDATE
    SET body       = EXCLUDED.body,
        updated_at = NOW()
WHERE comments.body IS DISTINCT FROM EXCLUDED.body;
```

The `IS DISTINCT FROM` check ensures rows are only written when data actually changed — critical at scale.
