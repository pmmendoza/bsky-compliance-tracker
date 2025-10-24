# Compliance Tracker

This directory contains tooling used to collect engagement and feed-retrieval compliance data for the Newsflows Bluesky bots.

## Overview
- `collect_engagements.py` is a thin wrapper around the `compliance_tracker` package. The package splits responsibilities into modules for API calls, SQLite storage, repair routines, hydration utilities, and the CLI entrypoint.
- `compliance_tracker/cli.py` resolves each bot handle in `BOT_HANDLES`, fetches recent posts, and records likes, reposts, replies, and quotes into the shared SQLite database `compliance.db`. At the end of every run the CLI hydrates newly observed feed payload posts into the `posts` table.
- Every engagement row stores whether the engaging DID was part of the subscriber list at collection time (`is_subscriber`), so activity from all accounts is retained with the relevant context.
- Comment and quote engagements capture the text that was posted, stored alongside the engagement metadata for auditability.
- Feed generator compliance events (feed retrievals) are persisted in the same database, enabling joins between engagements and feed requests on requester/engager DIDs.
- Subscriber snapshots are change-driven: repeated checks update `last_checked_ts` on the current state and only add a new row when the endpoint returns different data.
- The collector appends JSON lines to `db_update_log.jsonl` whenever it writes to the database for traceability.
- `finalize_schema.py` can be executed once to back up the on-disk database and ensure all columns/indexes are present. Fresh runs of the CLI also verify the schema, but the helper is convenient for migrating historic files.

## Engagement Collection Flow
1. Load environment variables from `compliance-tracker/.env` (fallback to `blueskyranker/blueskyranker/.env`). Requires `FEEDGEN_LISTENHOST` and `PRIORITIZE_API_KEY`.
2. Query the feed generator `/api/subscribers` endpoint to enumerate subscriber DIDs and handles.
3. Resolve each Newsflows bot handle to a DID via the Bluesky AppView API and fetch posts newer than the requested window. When `--days` is omitted, the collector automatically resumes from the most recent engagement stored in `compliance.db`.
4. For each post, retrieve likes, reposts, replies (via thread traversal), and quotes. All engagements are recorded; the `is_subscriber` flag indicates whether the actor was in the subscriber set when collected.
5. Persist engagements with uniqueness on `(timestamp, did_engagement, post_uri, engagement_type)`.

## Feed Retrieval Compliance
- When `--skip-feed` is omitted, the collector calls the feed generator `/api/compliance` endpoint for the requested window (`--feed-days` override) and optional requester DID filter (`--feed-did`).
- Feed requests are stored in `feed_requests`; individual posts returned to each requester are stored in `feed_request_posts`.
- Foreign key constraints ensure post rows are removed when their parent request is replaced.

## Environment Configuration
Provide a `.env` file in this directory with at least:
```
FEEDGEN_LISTENHOST=<host or https://domain for feed generator>
PRIORITIZE_API_KEY=<api key for subscriber/compliance endpoints>
```
The script will also respect the same variables already present in the process environment.

## Running the Collector
Install dependencies (`requests`, `pytest`, and optionally `tqdm`) in your Python environment, then run for example:
```
python collect_engagements.py --days 2 --db compliance.db
```
Key CLI options:
- `--days`: Engagement lookback window (fractional days allowed). If omitted, the collector resumes where the last engagement run left off.
- `--since`: Absolute timestamp (UTC) or numeric days-ago offset for the engagement window, overriding `--days`.
- `--db`: Path to the unified compliance SQLite database.
- `--log-level`: Logging verbosity (`INFO` default).
- `--timeout` / `--max-retries`: HTTP resilience tuning.
- `--skip-likes`, `--skip-reposts`, `--skip-comments`, `--skip-quotes`: Disable specific engagement types.
- `--feed-days`, `--feed-did`, `--skip-feed`: Control feed compliance collection. Without `--feed-days`, the collector resumes from the latest stored feed retrieval.
- `--feed-since`: Absolute timestamp or numeric offset for feed retrieval backfills, overriding `--feed-days`.
- `--post-repair-window`: Look back this many days for feed snapshots that came back empty and retry fetching their payloads.
- `--position-days` / `--position-since`: Force re-evaluation of subscriber post positions for the supplied window (accepts ISO timestamps or numeric offsets), independent of the engagement lookback.
Run `python compliance-tracker/collect_engagements.py --help` for the full list.

The CLI emits progress bars (via `tqdm` when available) for bot processing, feed retrieval ingestion, and post hydration batches.

## Database Schema (`compliance.db`)

### engagements
| Column | Type | Description | Source / Context |
| --- | --- | --- | --- |
| timestamp | TEXT | UTC ISO timestamp when the engagement occurred | Bluesky engagement APIs (`app.bsky.feed.getLikes`, thread traversal) |
| did_engagement | TEXT | DID performing the engagement | Engagement payload |
| post_uri | TEXT | AT URI of the engaged post | Engagement payload |
| post_author_handle | TEXT | Handle of the post author at collection time | Populated at ingest |
| engagement_type | TEXT | One of `like`, `comment`, `repost`, `quote` | Determined by the API endpoint |
| is_subscriber | INTEGER | `1` if the engaging DID was subscribed when collected | Snapshot membership |
| engagement_text | TEXT | Text body for comment/quote engagements (NULL otherwise) | Thread/quote payload |
| post_position | INTEGER | Zero-based index of the engaged post | `match_post_positions` |
| position_feed_request_id | INTEGER | `feed_requests.request_id` anchoring the position | `match_post_positions` |
| position_age_seconds | REAL | Delay (seconds) between feed retrieval and engagement | `match_post_positions` |
| position_status | TEXT | Match result (`POSITION_STATUS_*`) | `match_post_positions` |

Indexes: `idx_engagements_did_time`, `idx_engagements_post`, `idx_engagements_time`, `idx_engagements_subscriber_time`, `idx_engagements_position_status`.

### feed_requests
| Column | Type | Description | Source / Context |
| --- | --- | --- | --- |
| request_id | INTEGER | Primary key from the compliance endpoint | `/api/compliance` payload |
| requester_did | TEXT | DID that retrieved the feed | Compliance payload (`requester_did`/`user_did`) |
| algo | TEXT | Feed algorithm identifier (nullable) | Compliance payload |
| timestamp | TEXT | UTC ISO timestamp of the request | Compliance payload |
| posts_json | TEXT | JSON array snapshot of posts returned | Stored verbatim for auditing |

Indexes: `idx_feed_requests_did_time`, `idx_feed_requests_time`.

### feed_request_posts
| Column | Type | Description | Source / Context |
| --- | --- | --- | --- |
| id | INTEGER | Surrogate primary key | Auto-increment |
| request_id | INTEGER | Foreign key to `feed_requests.request_id` | Populated in `store_feed_retrievals` |
| post_index | INTEGER | Payload position for the post (nullable) | Parsed via `_coerce_payload_position`; remains NULL when the feed omits the field |
| post_uri | TEXT | AT URI of the delivered post | Feed payload (`uri`/`postUri`) |
| post_json | TEXT | Raw JSON payload | Stored verbatim |

Unique constraint `(request_id, post_uri)` ensures the latest entry replaces older copies. Index: `idx_feed_request_posts_request_uri`. Foreign key `request_id` → `feed_requests(request_id)` with `ON DELETE CASCADE`.

### posts
| Column | Type | Description | Source / Context |
| --- | --- | --- | --- |
| post_uri | TEXT | Primary key; AT URI of the post | Seeded from feed payloads |
| cid | TEXT | Content ID when known | Feed payload or hydration fetch |
| author_did | TEXT | DID of the post author | `hydrate_posts` (`app.bsky.feed.getPosts`) |
| author_handle | TEXT | Handle of the post author | Hydration fetch |
| indexed_at | TEXT | AppView index timestamp | Hydration fetch |
| created_at | TEXT | Original `createdAt` timestamp | Hydration fetch |
| last_hydrated_at | TEXT | UTC timestamp of the most recent hydrate attempt | `hydrate_posts` |
| hydration_status | TEXT | `ok`, `pending`, `not_found`, or `error` | Managed by `hydrate_posts` |
| hydration_error | TEXT | Last error message (nullable) | Populated on failure |

Indexes: `idx_posts_author_did`, `idx_posts_hydration_status`.

### subscriber_snapshots
| Column | Type | Description | Source / Context |
| --- | --- | --- | --- |
| snapshot_ts | TEXT | UTC ISO timestamp when a new subscriber state was first observed | `store_subscriber_snapshot` |
| last_checked_ts | TEXT | UTC timestamp of the most recent verification for that state | Updated on every run that sees the same state |
| did | TEXT | Subscriber DID present in the snapshot | `/api/subscribers` endpoint |
| handle | TEXT | Handle from the subscriber endpoint (nullable) | `/api/subscribers` endpoint |

Primary key `(did, snapshot_ts)`. Index: `idx_subscriber_snapshots_did`.

### subscriber_follow_counts
| Column | Type | Description | Source / Context |
| --- | --- | --- | --- |
| did | TEXT | Subscriber DID | `/xrpc/app.bsky.actor.getProfile` |
| following_count | INTEGER | Number of accounts the DID follows | Hydrated profile data |
| snapshot_ts | TEXT | UTC ISO timestamp when the count was collected | `store_subscriber_follow_counts` |

Primary key `(did, following_count)` keeps only distinct counts per DID. Index: `idx_follow_counts_snapshot`.

## Logging
- `db_update_log.jsonl` contains a JSON entry per database write recording the table, counts, and affected identifiers.
- At startup the collector logs the latest engagement and feed-retrieval timestamps (including days since) and, when more than four days of data would be fetched or no history exists, asks for confirmation before proceeding.

## Follower tracking
- Latest follower counts per subscriber DID:
  ```sql
  SELECT did, following_count
  FROM subscriber_follow_counts
  WHERE (did, snapshot_ts) IN (
      SELECT did, MAX(snapshot_ts)
      FROM subscriber_follow_counts
      GROUP BY did
  )
  ORDER BY following_count DESC;
  ```
- To inspect history for a specific subscriber, run `./follower_history.py --did <DID>` (defaults to the project `compliance.db`). Rows only advance when the subscriber state changes; otherwise `last_checked_ts` is extended in place.
- API prerequisites: the follower fetcher requires `FEEDGEN_LISTENHOST` and `PRIORITIZE_API_KEY` to be present in the environment or `.env` file; the collector enforces a 0.2 s pause between profile requests to stay within the public Bluesky rate limit (~3,000 calls / 5 minutes).
- The collector prints progress bars using `tqdm` when available, otherwise falls back to a stderr progress indicator.
- SQLite files and the log file are created automatically; ensure the process has write access to this directory.
- Failures to reach APIs or decode JSON raise descriptive errors after retry backoff controlled by the CLI flags.
- During each run the CLI resolves the bot DIDs and logs the associated PDS host, aiding troubleshooting when a bot migrates to a different provider.
- After each run the CLI reports how many subscriber engagements received a post position, how many did not, the breakdown of reasons, and the average delay between feed retrieval and engagement. The same payload is appended to `db_update_log.jsonl` under the `engagement_positions` table key.
- Feed payloads with missing posts are automatically retried for the configured repair window. Repair statistics are logged to stdout and the update log.

## Maintenance Scripts
- `hydrate_posts.py` seeds the `posts` table from historical payloads and hydrates any URIs still marked `pending` or `error`.
- `backfill_post_indices.py` recalculates `feed_request_posts.post_index` from stored JSON snapshots—useful after a feed bug is fixed.
- `follower_history.py` prints the latest follower counts per DID or the full change history for a single subscriber.

## Tests

Unit tests live under `tests/` and cover feed storage, post-position matching, repair routines, and utility helpers. Execute them with:
```
pytest
```
All tests operate on in-memory SQLite databases and do not require network access.

## Example SQL Queries
Connect to `compliance.db` with `sqlite3` (or your preferred client) and adjust the placeholders before running these queries.

### a) Single user's compliance in a timeframe
```sql
WITH params AS (
  SELECT 
    'did:example-user' AS target_did,
    '2024-09-01T00:00:00Z' AS start_ts,
    '2024-09-30T23:59:59Z' AS end_ts
),
retrievals AS (
  SELECT fr.requester_did AS user_did, COUNT(*) AS retrievals
  FROM feed_requests fr, params p
  WHERE fr.requester_did = p.target_did
    AND fr.timestamp BETWEEN p.start_ts AND p.end_ts
  GROUP BY fr.requester_did
),
engagements AS (
  SELECT e.did_engagement AS user_did,
         SUM(CASE WHEN e.engagement_type = 'like' THEN 1 ELSE 0 END) AS likes,
         SUM(CASE WHEN e.engagement_type = 'comment' THEN 1 ELSE 0 END) AS comments,
         SUM(CASE WHEN e.engagement_type = 'repost' THEN 1 ELSE 0 END) AS reposts,
         SUM(CASE WHEN e.engagement_type = 'quote' THEN 1 ELSE 0 END) AS quotes
  FROM engagements e, params p
  WHERE e.did_engagement = p.target_did
    AND e.timestamp BETWEEN p.start_ts AND p.end_ts
  GROUP BY e.did_engagement
)
SELECT
  p.target_did AS user_did,
  COALESCE(r.retrievals, 0) AS retrievals,
  COALESCE(e.likes, 0) AS likes,
  COALESCE(e.comments, 0) AS comments,
  COALESCE(e.reposts, 0) AS reposts,
  COALESCE(e.quotes, 0) AS quotes
FROM params p
LEFT JOIN retrievals r ON r.user_did = p.target_did
LEFT JOIN engagements e ON e.user_did = p.target_did;
```

### b) All subscribers' compliance in a timeframe
```sql
WITH params AS (
  SELECT 
    '2024-09-01T00:00:00Z' AS start_ts,
    '2024-09-30T23:59:59Z' AS end_ts
),
active_subscribers AS (
  SELECT s.did, MAX(s.snapshot_ts) AS last_snapshot
  FROM subscriber_snapshots s, params p
  WHERE s.snapshot_ts <= p.end_ts
  GROUP BY s.did
),
retrievals AS (
  SELECT fr.requester_did AS user_did, COUNT(*) AS retrievals
  FROM feed_requests fr, params p
  WHERE fr.timestamp BETWEEN p.start_ts AND p.end_ts
    AND fr.requester_did IN (SELECT did FROM active_subscribers)
  GROUP BY fr.requester_did
),
engagements AS (
  SELECT e.did_engagement AS user_did,
         SUM(CASE WHEN e.engagement_type = 'like' THEN 1 ELSE 0 END) AS likes,
         SUM(CASE WHEN e.engagement_type = 'comment' THEN 1 ELSE 0 END) AS comments,
         SUM(CASE WHEN e.engagement_type = 'repost' THEN 1 ELSE 0 END) AS reposts,
         SUM(CASE WHEN e.engagement_type = 'quote' THEN 1 ELSE 0 END) AS quotes
  FROM engagements e, params p
  WHERE e.timestamp BETWEEN p.start_ts AND p.end_ts
    AND e.is_subscriber = 1
    AND e.did_engagement IN (SELECT did FROM active_subscribers)
  GROUP BY e.did_engagement
)
SELECT
  s.did AS user_did,
  COALESCE(r.retrievals, 0) AS retrievals,
  COALESCE(e.likes, 0) AS likes,
  COALESCE(e.comments, 0) AS comments,
  COALESCE(e.reposts, 0) AS reposts,
  COALESCE(e.quotes, 0) AS quotes
FROM active_subscribers s
LEFT JOIN retrievals r ON r.user_did = s.did
LEFT JOIN engagements e ON e.user_did = s.did
ORDER BY s.did;
```

### c) Per-post subscriber engagement metrics
```sql
WITH params AS (
  SELECT 
    '2024-09-01T00:00:00Z' AS start_ts,
    '2024-09-30T23:59:59Z' AS end_ts
)
SELECT
  e.post_uri,
  SUM(CASE WHEN e.engagement_type = 'like' THEN 1 ELSE 0 END) AS likes,
  SUM(CASE WHEN e.engagement_type = 'comment' THEN 1 ELSE 0 END) AS comments,
  SUM(CASE WHEN e.engagement_type = 'repost' THEN 1 ELSE 0 END) AS reposts,
  SUM(CASE WHEN e.engagement_type = 'quote' THEN 1 ELSE 0 END) AS quotes
FROM engagements e, params p
WHERE e.timestamp BETWEEN p.start_ts AND p.end_ts
  AND e.is_subscriber = 1
GROUP BY e.post_uri
ORDER BY e.post_uri;
```
