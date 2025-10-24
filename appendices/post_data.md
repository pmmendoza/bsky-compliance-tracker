# Post Metadata Backfill Project

## Goals
- Centrally store post metadata (author DID/handle, text snapshot, etc.) for every post URI observed in feed compliance events.
- Remove duplicated author fields from `feed_request_posts` and rely on a canonical `posts` table, improving storage efficiency and hydration consistency.
- Ensure ingestion and downstream analytics transparently use the new structure without regressions.

## Requirements

### Necessary (Minimum Viable Refactor)
- Introduce a `posts` table in `compliance.db` keyed by `post_uri`, storing at minimum `author_did`, `author_handle`, `cid`, and `hydration_status`.
- Modify `store_feed_retrievals` to upsert into `posts` for unseen URIs and to stop writing `post_author_did` / `post_author_handle` into `feed_request_posts`.
- Provide a hydration job that batches unresolved URIs through `app.bsky.feed.getPosts` and updates the `posts` table, capturing missing or deleted posts gracefully.
- Update existing queries and helpers (e.g., position matching, analytics notebooks) to join `feed_request_posts` against `posts` for author data.

### Important (Reliability & Observability)
- Record hydration timestamps and failure reasons (`posts.last_hydrated_at`, `posts.hydration_error`) for troubleshooting.
- Add indexes to support common joins (`posts.post_uri`, `posts.author_did`).
- Emit structured logs/statistics when hydration runs (counts of fetched posts, failures, skipped cached URIs).
- Provide migration tooling to backfill legacy data: extract distinct URIs from `feed_request_posts` and populate `posts` before dropping columns.

### Optional (Future Enhancements)
- Cache additional metadata (post text, language, embed info) to support richer analytics.
- Integrate with engagement collection to hydrate posts on-demand when an unseen URI appears.
- Surface a dashboard/report summarizing hydration coverage (e.g., % of posts with metadata, failure trends).
- Support incremental rehydration when post metadata changes (e.g., author handle updates).

## Design Overview
- New table (`posts`):
  ```sql
  CREATE TABLE posts (
      post_uri TEXT PRIMARY KEY,
      cid TEXT,
      author_did TEXT,
      author_handle TEXT,
      indexed_at TEXT,
      created_at TEXT,
      last_hydrated_at TEXT,
      hydration_status TEXT,
      hydration_error TEXT
  );
  CREATE INDEX idx_posts_author_did ON posts(author_did);
  ```
- `feed_request_posts` retains `request_id`, `post_uri`, `post_index`, `post_json`, etc., but drops `post_author_did` and `post_author_handle` after migration.
- Hydration pipeline:
  1. Gather URIs missing `author_did`.
  2. Batch <=25 URIs per `app.bsky.feed.getPosts` call (use the `post_json.uri` values that originate from the feed bot DIDs, **not** `feed_requests.requester_did`; subscriber DIDs will return empty results) and throttle requests to stay under ~3,000 API calls per five minutes (e.g., default pause ≥0.2s).
  3. Upsert metadata into `posts`; set `hydration_status` to `ok`, `not_found`, etc.
- Migration order:
  1. Add `posts` table.
  2. Populate from existing `feed_request_posts.post_json`.
  3. Update ingestion to write through `posts`.
  4. Backfill/hydrate remaining URIs.
  5. Drop redundant columns from `feed_request_posts`.

## Tasks & Definitions of Done

### 1. Schema Migration & Data Layer (Necessary)
- **Task**: Create `posts` table and access helpers; adjust `store_feed_retrievals` to upsert URIs.
- **DoD**:
  - [x] Migration runs via `setup_database`; `posts` appears in `sqlite_master`.
  - [x] `store_feed_retrievals` writes URIs to `posts` (insert or ignore) and no longer populates author columns in `feed_request_posts`.
  - [x] Unit tests verify new helper behavior (insert/update logic and join queries).

### 2. Hydration Job (Necessary)
- **Task**: Implement `hydrate_posts` routine to resolve missing metadata using the AppView API.
- **DoD**:
  - [x] Batch hydration fetches posts in groups (<=25 URIs); respects rate limits and logs failures.
  - [x] Updated rows reflect author DID/handle and timestamp (`last_hydrated_at`).
  - [x] Job returns stats (hydrated count, not-found count, errors); covered by unit/integration test with mocked API.

### 3. Backfill & Migration Script (Important)
- **Task**: Build a script/command that extracts existing URIs, hydrates them, and drops redundant columns.
- **DoD**:
  - [x] Script populates `posts` from historical `feed_request_posts` data (using stored `post_json`).
  - [x] After running, `feed_request_posts.post_author_*` columns are removed without breaking tests (handled via automatic migration in `setup_database`).
  - [x] README/docs updated with migration steps and rollback guidance.

### 4. Extensions
- Integrate with engagement collection to hydrate posts on-demand when an unseen URI appears.
  - [x] Add minimal post hydration report to existing log of pipeline.

### Optional (Future Enhancements)
- Surface a dashboard/report summarizing hydration coverage (e.g., % of posts with metadata, failure trends).
- Cache additional metadata (post text, language, embed info) to support richer analytics.
- ~~Support incremental rehydration when post metadata changes (e.g., author handle updates).~~

### Open Questions
- Should `posts` include mutable fields (e.g., author handle updates) or do we rely on latest fetch each time?
>A: we rely on the data at time of fetching
- How aggressively should hydration retry 404s (deleted posts)? Keep a `not_found` status to avoid repeated API calls?
>A: indeed keep such a status but allow for a forced re-fetching via a flag e.g., `--forcefetch-failedhydration` 
- Do we need separate jobs for initial backfill vs. incremental hydration (to control API load)?
