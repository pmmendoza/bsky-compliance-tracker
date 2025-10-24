# Follower Tracking Initiative

## Goals
- Provide time-aware visibility into how many accounts each feed subscriber follows.
- Detect meaningful changes in subscriber following behavior to correlate with engagement and feed-refresh patterns.
- Preserve storage efficiency by avoiding redundant rows when follower counts remain unchanged.

## Requirements
### Necessary (Minimum Viable Tracking)
- [x] Extend the daily collector to capture `followsCount` for every subscriber DID.
- [x] Persist follower counts to a new table in `compliance.db` with one row per subscriber DID and distinct follower count, updating the most recent row’s `snapshot_ts` when the count is unchanged.
- [x] Ensure the ingestion flow handles API errors gracefully and logs failures for retry.
- [x] Provide a simple query/helper to fetch the latest follower count per subscriber.
- [x] Throttle Bluesky AppView requests to remain well within the 3,000 calls / 5 minute limit (default pause now 0.2s ≈ 1,500 calls / 5 min; monitor for regressions).

### Important (Strengthens Reliability & Insight)
- Batch or rate-limit profile fetches to respect upstream quotas; cache results within a run.
- Record fetch metadata (e.g., status, error message) for observability and alerting.
- Add unit tests covering the new persistence logic and unchanged-count update behavior.
- Extend analytics scripts/notebooks to join follower history with retrieval/engagement data.

### Optional (Future Enhancements)
- Track detailed follow lists (not just counts) for high-value subscribers.
- Surface follower-count trends in dashboards or automated reports.
- Add anomaly detection (e.g., sudden spikes/drops) with notifications.
- Support configurable snapshot cadence (multiple runs per day) with deduplication across runs.

## Design Overview
- **New table**: `subscriber_follow_counts(did TEXT, snapshot_ts TEXT, following_count INTEGER, PRIMARY KEY (did, following_count))`.
    - On insert, if `following_count` matches the existing row for that DID, update its `snapshot_ts` to the new timestamp instead of inserting.
    - Otherwise insert a new row capturing the new count.
- Update the collector (`cli.py`) to, after fetching subscribers, request follower counts (via `app.bsky.actor.getProfile` or equivalent) and persist through a helper (e.g., `store_follow_counts`).
- Extend the data access layer (`compliance_tracker/database.py`) with transactional helpers encapsulating the insert/update semantics.
- Provide analytical convenience views or SQL snippets for “latest count” (e.g., `SELECT did, MAX(snapshot_ts)`).

## Tasks & Definitions of Done

### 1. Schema Migration & Helper
- **Task**: Add the `subscriber_follow_counts` table and the helper function to insert/update rows. ✅
- **DoD**:
  - [x] Migration executed via `setup_database`; table appears in `sqlite_master`.
  - [x] Helper updates existing rows’ `snapshot_ts` when counts match, inserts otherwise.
  - [x] Unit tests (or sqlite scratch script) cover unchanged vs. changed count behavior.

### 2. API Integration
- **Task**: Extend subscriber ingestion to fetch follower counts with rate limiting/backoff. ✅
- **DoD**:
  - [x] Successful run logs “Recorded follower counts for N subscribers”.
  - [x] Handles per-request failure with retry/skip; failures captured in logs.
  - [x] Respects configurable max concurrency or delay to avoid rate-limit responses.

### 3. Data Persistence Wiring
- **Task**: Invoke the new persistence helper during `cli.py` execution. ✅
- **DoD**:
  - [x] Local run populates `subscriber_follow_counts` alongside snapshots.
  - [x] Transactions ensure counts and subscriber snapshots share the same timestamp.
  - [x] Error handling rolls back on failure and surfaces a useful error.

### 4. Analytics Support
- **Task**: Expose convenience access (SQL view or helper function) for the latest follower count per subscriber.
- **DoD**:
  - [x] Query or helper documented in README/docs.
  - [x] Sanity check script shows follower history for at least one DID (`follower_history.py`).

### 5. Observability (Important tier)
- **Task**: Add structured logging/metrics for follower-fetch successes and failures.
- **DoD**:
  - [x] Logs include counts of successful fetches, skips, and errors (`follower_counts_fetch` entries in `db_update_log.jsonl`).
  - [ ] Optional (if metrics infrastructure exists): gauge on current subscriber coverage.

### 6. Documentation & Runbooks
- **Task**: Update README / internal docs with follower-tracking workflow.
- **DoD**:
  - [x] Instructions describe prerequisites (API keys, rate-limit expectations).
  - [x] Example queries included (README section “Follower tracking”).

### Optional Backlog Tasks
- Differential alerting when follower counts change beyond a threshold.
- Historical trend dashboard (e.g., in Metabase/Data Studio).
- Full follow-graph capture for select subscribers.

## Open Questions
- Which Bluesky endpoint should we prefer for `followsCount`, and what rate limits apply?
- What is the acceptable staleness? (Daily snapshots vs. more frequent.)
- Should unchanged counts retain multiple timestamps (e.g., all run timestamps) or just latest? Current plan keeps only the latest, but analytics might prefer a history of run timestamps even for unchanged counts.
- Do we need to capture “following count fetch failed” separately from “subscriber absent from API” cases?
