# Timeline Position Matching

This document explains how subscriber engagements are enriched with the position each post occupied in the engaging user's timeline.

## Data Sources
- **Engagements (`engagements` table)** – created in `store_engagements` when the CLI records likes, reposts, comments, and quotes. Each row is keyed by engagement timestamp, engaging DID, post URI, and engagement type, and notes whether the engager was a subscriber.
- **Feed retrievals (`feed_requests` table)** – captured via `store_feed_retrievals`, which ingests feed compliance events and keeps a JSON snapshot of the posts returned for each request.
- **Feed request posts (`feed_request_posts` table)** – populated alongside `feed_requests`. Each post from a retrieval gets a zero-based `post_index`, preserving the order provided by the feed generator for later lookup.

## Matching Workflow
1. `compliance_tracker.cli.main` orchestrates collection. After fetching engagements and feed retrievals (and optional empty-feed repairs), it calls `match_post_positions`.
2. `match_post_positions` (in `compliance_tracker/database.py`) processes subscriber engagements that have no `position_status` yet. To avoid rework, the query filters out rows already marked `POSITION_STATUS_MATCHED`.
3. For each engagement, the matcher:
   - Parses and normalises the engagement timestamp.
   - Loads (and caches) the engager's feed retrieval history via `_load_feed_cache`, ordering requests by timestamp for efficient lookup.
   - Finds the most recent feed request at or before the engagement using `bisect_right` over the cached timestamps.
   - Ensures the feed snapshot is usable (valid timestamp, not empty, timestamp not in the future relative to the engagement).
   - Builds (and caches) a map of post URI → `post_index` for the chosen request using `_load_post_mapping`.
   - Looks up the engaged post's URI. If found, it updates the engagement row with the post position, the feed request ID that supplied the position, the age in seconds between feed retrieval and engagement, and marks `position_status` as `POSITION_STATUS_MATCHED`.

## Failure Modes
When a position cannot be assigned, `_update_position` records a descriptive `position_status` while leaving `post_position` null. Common statuses include:
- `POSITION_STATUS_NO_FEED` – no feed retrieval exists for the engager before the engagement time.
- `POSITION_STATUS_POST_MISSING` – the engaged post was not present in the nearest feed snapshot.
- `POSITION_STATUS_EMPTY_FEED` – the feed request contained no posts.
- `POSITION_STATUS_INVALID_TS` / `POSITION_STATUS_INVALID_FEED_TS` – timestamps could not be parsed.
- `POSITION_STATUS_FEED_IN_FUTURE` – the only available feed snapshot is timestamped after the engagement.
- `POSITION_STATUS_MISSING_URI` – the engagement lacks a post URI.

These codes are surfaced in CLI output and logged via `log_db_update`, making it straightforward to monitor matching success.

## Outputs
Successful matches populate the following `engagements` columns:
- `post_position` – zero-based index from the feed snapshot.
- `position_feed_request_id` – identifier of the feed request supplying the position.
- `position_age_seconds` – delay between feed retrieval and engagement.
- `position_status` – set to `POSITION_STATUS_MATCHED`.

The CLI summarises results (matched vs processed, unmatched reasons, and average delay) before exiting, providing operators with quick feedback on timeline coverage.
