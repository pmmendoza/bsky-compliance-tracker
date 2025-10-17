"""Static constants used across the compliance tracker."""

from __future__ import annotations

from pathlib import Path
from typing import Tuple

PACKAGE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = PACKAGE_ROOT.parent

APPVIEW_BASE = "https://public.api.bsky.app"
BOT_HANDLES: Tuple[str, ...] = (
    "news-flows-nl.bsky.social",
    "news-flows-ir.bsky.social",
    "news-flows-cz.bsky.social",
    "news-flows-fr.bsky.social",
)
DEFAULT_DAYS = 3
DEFAULT_DB_PATH = PROJECT_ROOT / "compliance.db"
ENV_DEFAULT_PATH = PROJECT_ROOT / ".env"
LEGACY_ENV_PATH = PROJECT_ROOT / "blueskyranker" / "blueskyranker" / ".env"
UPDATE_LOG_PATH = PROJECT_ROOT / "db_update_log.jsonl"
USER_AGENT = "newsflows-compliance-tracker/0.1"
SUBSCRIBERS_ENDPOINT = "/api/subscribers"
FEED_COMPLIANCE_ENDPOINT = "/api/compliance"
SUBSCRIBER_HOST_ENV = "FEEDGEN_LISTENHOST"
SUBSCRIBER_KEY_ENV = "PRIORITIZE_API_KEY"

POSITION_STATUS_MATCHED = "matched"
POSITION_STATUS_NO_FEED = "no_feed_request"
POSITION_STATUS_EMPTY_FEED = "empty_feed_posts"
POSITION_STATUS_POST_MISSING = "post_not_in_feed"
POSITION_STATUS_INVALID_TS = "invalid_engagement_timestamp"
POSITION_STATUS_FEED_IN_FUTURE = "feed_after_engagement"
POSITION_STATUS_MISSING_URI = "missing_post_uri"
POSITION_STATUS_INVALID_FEED_TS = "invalid_feed_timestamp"

POSITION_STATUS_LABELS = {
    POSITION_STATUS_MATCHED: "matched",
    POSITION_STATUS_NO_FEED: "no feed request before engagement",
    POSITION_STATUS_EMPTY_FEED: "feed retrieval missing posts",
    POSITION_STATUS_POST_MISSING: "post not present in feed payload",
    POSITION_STATUS_INVALID_TS: "invalid engagement timestamp",
    POSITION_STATUS_INVALID_FEED_TS: "invalid feed timestamp",
    POSITION_STATUS_FEED_IN_FUTURE: "feed timestamp newer than engagement",
    POSITION_STATUS_MISSING_URI: "engagement missing post URI",
}
