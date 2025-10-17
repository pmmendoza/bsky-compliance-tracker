"""SQLite persistence helpers."""

from __future__ import annotations

import datetime as dt
import json
import logging
import sqlite3
from bisect import bisect_right
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Set, Tuple

from .constants import (
    POSITION_STATUS_EMPTY_FEED,
    POSITION_STATUS_FEED_IN_FUTURE,
    POSITION_STATUS_INVALID_FEED_TS,
    POSITION_STATUS_INVALID_TS,
    POSITION_STATUS_LABELS,
    POSITION_STATUS_MATCHED,
    POSITION_STATUS_MISSING_URI,
    POSITION_STATUS_NO_FEED,
    POSITION_STATUS_POST_MISSING,
)
from .engagements import EngagementRecord
from .schema import ensure_database
from .utils import ensure_utc, log_db_update, parse_datetime

logger = logging.getLogger(__name__)


def setup_database(conn: sqlite3.Connection) -> None:
    ensure_database(conn)


def store_engagements(conn: sqlite3.Connection, rows: Iterable[EngagementRecord]) -> int:
    insert_sql = (
        "INSERT OR IGNORE INTO engagements (timestamp, did_engagement, post_uri, post_author_handle, engagement_type, is_subscriber, engagement_text)"
        " VALUES (?, ?, ?, ?, ?, ?, ?)"
    )
    prepared_rows = list(rows)
    to_insert = [
        (
            row.timestamp,
            row.did_engagement,
            row.post_uri,
            row.post_author_handle,
            row.engagement_type,
            int(row.is_subscriber),
            row.engagement_text,
        )
        for row in prepared_rows
    ]
    if not to_insert:
        return 0
    before_changes = conn.total_changes
    conn.executemany(insert_sql, to_insert)
    conn.commit()
    inserted = conn.total_changes - before_changes
    if inserted:
        subscriber_count = sum(1 for row in prepared_rows if row.is_subscriber)
        text_count = sum(1 for row in prepared_rows if row.engagement_text)
        log_db_update(
            "engagements",
            {
                "action": "insert_or_ignore",
                "attempted": len(prepared_rows),
                "inserted": inserted,
                "unique_dids": sorted({row.did_engagement for row in prepared_rows}),
                "engagement_types": sorted({row.engagement_type for row in prepared_rows}),
                "subscriber_rows": subscriber_count,
                "non_subscriber_rows": len(prepared_rows) - subscriber_count,
                "rows_with_text": text_count,
            },
        )
    return inserted


def store_feed_retrievals(conn: sqlite3.Connection, retrievals: Iterable[Mapping]) -> int:
    """Persist feed requests; `post_index` is stored zero-based as received."""

    requests_sql = (
        "INSERT OR REPLACE INTO feed_requests (request_id, requester_did, algo, timestamp, posts_json) "
        "VALUES (?, ?, ?, ?, ?)"
    )
    posts_sql = (
        "INSERT OR REPLACE INTO feed_request_posts (request_id, post_index, post_uri, post_author_did, post_author_handle, post_json) "
        "VALUES (?, ?, ?, ?, ?, ?)"
    )
    inserted = 0
    processed_ids: List[str] = []
    post_rows = 0
    for retrieval in retrievals:
        request_id = retrieval.get("id")
        if request_id is None:
            continue
        requester_did = retrieval.get("requester_did") or retrieval.get("user_did") or ""
        algo = retrieval.get("algo")
        timestamp = retrieval.get("timestamp")
        posts = retrieval.get("posts") or []
        posts_json = json.dumps(posts)
        conn.execute(requests_sql, (request_id, requester_did, algo, timestamp, posts_json))
        conn.execute("DELETE FROM feed_request_posts WHERE request_id = ?", (request_id,))
        for index, post in enumerate(posts):
            if not isinstance(post, dict):
                post = {}
            post_uri = post.get("uri") or post.get("postUri")
            post_author = post.get("author") or {}
            post_author_did = post_author.get("did") or post.get("authorDid")
            post_author_handle = post_author.get("handle") or post.get("authorHandle")
            conn.execute(
                posts_sql,
                (
                    request_id,
                    index,  # zero-based position as supplied by feed generator
                    post_uri,
                    post_author_did,
                    post_author_handle,
                    json.dumps(post),
                ),
            )
        inserted += 1
        processed_ids.append(str(request_id))
        post_rows += len(posts)
    conn.commit()
    if inserted:
        log_db_update(
            "feed_requests",
            {
                "action": "insert_or_replace",
                "requests": inserted,
                "request_ids": processed_ids,
                "posts_written": post_rows,
            },
        )
    return inserted


def store_subscriber_snapshot(
    conn: sqlite3.Connection,
    subscriber_dids: Set[str],
    subscriber_handles: Dict[str, str],
    snapshot_dt: dt.datetime,
) -> None:
    if not subscriber_dids:
        return
    snapshot_ts = ensure_utc(snapshot_dt).isoformat()
    rows = [
        (snapshot_ts, did, subscriber_handles.get(did))
        for did in sorted(subscriber_dids)
    ]
    conn.executemany(
        "INSERT OR REPLACE INTO subscriber_snapshots (snapshot_ts, did, handle) VALUES (?, ?, ?)",
        rows,
    )
    conn.commit()
    log_db_update(
        "subscriber_snapshots",
        {
            "snapshot_ts": snapshot_ts,
            "subscriber_count": len(rows),
        },
    )


def get_latest_timestamp(conn: sqlite3.Connection, table: str, column: str) -> Optional[dt.datetime]:
    query = f"SELECT MAX({column}) FROM {table}"
    try:
        result = conn.execute(query).fetchone()
    except sqlite3.Error:
        return None
    raw_value = result[0] if result else None
    if not raw_value:
        return None
    parsed = parse_datetime(raw_value)
    if parsed is None:
        return None
    return ensure_utc(parsed)


@dataclass
class PositionMatchStats:
    processed: int = 0
    matched: int = 0
    age_seconds_sum: float = 0.0
    age_samples: int = 0
    status_counts: Dict[str, int] = field(default_factory=lambda: defaultdict(int))

    def record(self, status: str, *, age_seconds: Optional[float] = None) -> None:
        self.processed += 1
        self.status_counts[status] += 1
        if status == POSITION_STATUS_MATCHED and age_seconds is not None:
            self.matched += 1
            self.age_seconds_sum += age_seconds
            self.age_samples += 1

    def to_dict(self) -> Dict[str, float]:
        payload: Dict[str, float] = {
            "processed": float(self.processed),
            "matched": float(self.matched),
            "age_seconds_sum": self.age_seconds_sum,
            "age_seconds_count": float(self.age_samples),
        }
        if self.age_samples:
            payload["avg_age_seconds"] = self.age_seconds_sum / self.age_samples
        for code, count in self.status_counts.items():
            payload[f"status_{code}"] = float(count)
        return payload


def match_post_positions(
    conn: sqlite3.Connection,
    *,
    since: Optional[dt.datetime] = None,
    chunk_size: int = 500,
) -> PositionMatchStats:
    params: List = [POSITION_STATUS_MATCHED]
    query = (
        "SELECT rowid, timestamp, did_engagement, post_uri "
        "FROM engagements "
        "WHERE is_subscriber = 1 "
        "AND (position_status IS NULL OR position_status != ?) "
    )
    if since is not None:
        params.append(ensure_utc(since).isoformat())
        query += "AND timestamp >= ? "
    query += "ORDER BY timestamp ASC"

    cursor = conn.execute(query, params)
    stats = PositionMatchStats()
    feed_cache: Dict[str, "FeedCache"] = {}
    post_cache: Dict[int, Dict[str, int]] = {}

    while True:
        batch = cursor.fetchmany(chunk_size)
        if not batch:
            break
        for rowid, ts_raw, did_engagement, post_uri in batch:
            if not ts_raw:
                _update_position(
                    conn,
                    rowid,
                    status=POSITION_STATUS_INVALID_TS,
                    request_id=None,
                )
                stats.record(POSITION_STATUS_INVALID_TS)
                continue

            engagement_dt = parse_datetime(ts_raw)
            if engagement_dt is None:
                _update_position(
                    conn,
                    rowid,
                    status=POSITION_STATUS_INVALID_TS,
                    request_id=None,
                )
                stats.record(POSITION_STATUS_INVALID_TS)
                continue

            engagement_dt = ensure_utc(engagement_dt)

            if not post_uri:
                _update_position(
                    conn,
                    rowid,
                    status=POSITION_STATUS_MISSING_URI,
                    request_id=None,
                )
                stats.record(POSITION_STATUS_MISSING_URI)
                continue

            cache = feed_cache.get(did_engagement)
            if cache is None:
                cache = _load_feed_cache(conn, did_engagement)
                feed_cache[did_engagement] = cache

            if not cache.timestamps:
                _update_position(
                    conn,
                    rowid,
                    status=POSITION_STATUS_NO_FEED,
                    request_id=None,
                )
                stats.record(POSITION_STATUS_NO_FEED)
                continue

            ts_value = engagement_dt.timestamp()
            idx = bisect_right(cache.timestamps, ts_value) - 1
            if idx < 0:
                _update_position(
                    conn,
                    rowid,
                    status=POSITION_STATUS_NO_FEED,
                    request_id=None,
                )
                stats.record(POSITION_STATUS_NO_FEED)
                continue

            request = cache.entries[idx]
            if request.timestamp is None:
                _update_position(
                    conn,
                    rowid,
                    status=POSITION_STATUS_INVALID_FEED_TS,
                    request_id=request.request_id,
                )
                stats.record(POSITION_STATUS_INVALID_FEED_TS)
                continue

            if request.timestamp > engagement_dt:
                _update_position(
                    conn,
                    rowid,
                    status=POSITION_STATUS_FEED_IN_FUTURE,
                    request_id=request.request_id,
                )
                stats.record(POSITION_STATUS_FEED_IN_FUTURE)
                continue

            age_seconds = (engagement_dt - request.timestamp).total_seconds()

            if not request.has_posts:
                _update_position(
                    conn,
                    rowid,
                    status=POSITION_STATUS_EMPTY_FEED,
                    request_id=request.request_id,
                )
                stats.record(POSITION_STATUS_EMPTY_FEED)
                continue

            post_mapping = post_cache.get(request.request_id)
            if post_mapping is None:
                post_mapping = _load_post_mapping(conn, request.request_id)
                post_cache[request.request_id] = post_mapping

            post_index = post_mapping.get(post_uri)
            if post_index is None:
                _update_position(
                    conn,
                    rowid,
                    status=POSITION_STATUS_POST_MISSING,
                    request_id=request.request_id,
                )
                stats.record(POSITION_STATUS_POST_MISSING)
                continue

            _update_position(
                conn,
                rowid,
                status=POSITION_STATUS_MATCHED,
                request_id=request.request_id,
                post_position=int(post_index),
                age_seconds=age_seconds,
            )
            stats.record(POSITION_STATUS_MATCHED, age_seconds=age_seconds)

    conn.commit()
    return stats


@dataclass
class FeedCacheEntry:
    request_id: int
    timestamp: Optional[dt.datetime]
    has_posts: bool


@dataclass
class FeedCache:
    entries: List[FeedCacheEntry]
    timestamps: List[float]


def _load_feed_cache(conn: sqlite3.Connection, did: str) -> FeedCache:
    rows = conn.execute(
        """
        SELECT fr.request_id,
               fr.timestamp,
               COUNT(frp.post_index) AS post_count
        FROM feed_requests fr
        LEFT JOIN feed_request_posts frp ON fr.request_id = frp.request_id
        WHERE fr.requester_did = ?
        GROUP BY fr.request_id, fr.timestamp
        ORDER BY fr.timestamp ASC
        """,
        (did,),
    ).fetchall()

    entries: List[FeedCacheEntry] = []
    timestamps: List[float] = []
    for request_id, raw_ts, post_count in rows:
        feed_dt = parse_datetime(raw_ts)
        if feed_dt is not None:
            feed_dt = ensure_utc(feed_dt)
            timestamps.append(feed_dt.timestamp())
        else:
            timestamps.append(float("-inf"))
        entries.append(
            FeedCacheEntry(
                request_id=request_id,
                timestamp=feed_dt,
                has_posts=post_count > 0,
            )
        )
    return FeedCache(entries=entries, timestamps=timestamps)


def _load_post_mapping(conn: sqlite3.Connection, request_id: int) -> Dict[str, int]:
    rows = conn.execute(
        "SELECT post_uri, post_index FROM feed_request_posts WHERE request_id = ?",
        (request_id,),
    ).fetchall()
    return {post_uri: int(post_index) for post_uri, post_index in rows if post_uri}


def _update_position(
    conn: sqlite3.Connection,
    rowid: int,
    *,
    status: str,
    request_id: Optional[int],
    post_position: Optional[int] = None,
    age_seconds: Optional[float] = None,
) -> None:
    conn.execute(
        """
        UPDATE engagements
        SET post_position = ?, position_feed_request_id = ?, position_age_seconds = ?, position_status = ?
        WHERE rowid = ?
        """,
        (post_position, request_id, age_seconds, status, rowid),
    )
