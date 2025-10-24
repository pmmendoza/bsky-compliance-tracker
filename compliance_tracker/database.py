"""SQLite persistence helpers."""

from __future__ import annotations

import datetime as dt
import json
import logging
import math
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


def _coerce_payload_position(value: object) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value) or not value.is_integer():
            return None
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return int(stripped)
        except ValueError:
            try:
                float_value = float(stripped)
            except ValueError:
                return None
            if not math.isfinite(float_value) or not float_value.is_integer():
                return None
            return int(float_value)
    return None


def _upsert_post_placeholder(conn: sqlite3.Connection, post: Mapping) -> None:
    post_uri = post.get("uri") or post.get("postUri")
    if not post_uri:
        return
    cid = post.get("cid")
    if not cid:
        record = post.get("record")
        if isinstance(record, Mapping):
            cid = record.get("cid")
    conn.execute(
        """
        INSERT INTO posts (post_uri, cid, hydration_status)
        VALUES (?, ?, 'pending')
        ON CONFLICT(post_uri) DO UPDATE SET
            cid = COALESCE(posts.cid, excluded.cid),
            hydration_status = CASE
                WHEN posts.hydration_status IS NULL THEN 'pending'
                ELSE posts.hydration_status
            END
        """,
        (post_uri, cid),
    )


def store_feed_retrievals(conn: sqlite3.Connection, retrievals: Iterable[Mapping]) -> int:
    """Persist feed requests; `post_index` mirrors the payload's position when available."""

    requests_sql = (
        "INSERT OR REPLACE INTO feed_requests (request_id, requester_did, algo, timestamp, posts_json) "
        "VALUES (?, ?, ?, ?, ?)"
    )
    posts_sql = (
        "INSERT OR REPLACE INTO feed_request_posts (request_id, post_index, post_uri, post_json) "
        "VALUES (?, ?, ?, ?)"
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
            _upsert_post_placeholder(conn, post)
            post_uri = post.get("uri") or post.get("postUri")
            position_value = _coerce_payload_position(post.get("position"))
            post_index_value = position_value if position_value is not None else None
            conn.execute(
                posts_sql,
                (
                    request_id,
                    post_index_value,
                    post_uri,
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


def seed_posts_from_feed(conn: sqlite3.Connection) -> int:
    """Insert placeholders for all URIs seen in feed_request_posts."""

    cursor = conn.execute(
        """
        INSERT OR IGNORE INTO posts (post_uri, cid, hydration_status)
        SELECT DISTINCT post_uri,
               json_extract(post_json, '$.cid') AS cid,
               'pending'
        FROM feed_request_posts
        WHERE post_uri IS NOT NULL
        """
    )
    conn.commit()
    return cursor.rowcount


def get_post_uris_pending_hydration(conn: sqlite3.Connection, limit: Optional[int] = None) -> List[str]:
    sql = (
        "SELECT post_uri FROM posts "
        "WHERE (author_did IS NULL OR author_handle IS NULL) "
        "AND COALESCE(hydration_status, 'pending') != 'not_found' "
        "ORDER BY COALESCE(last_hydrated_at, '') ASC, post_uri ASC"
    )
    params: Tuple = ()
    if limit is not None:
        sql += " LIMIT ?"
        params = (limit,)
    rows = conn.execute(sql, params).fetchall()
    return [row[0] for row in rows]


def update_posts_metadata(
    conn: sqlite3.Connection,
    metadata: Sequence[Mapping[str, Optional[str]]],
    *,
    hydrated_at: Optional[dt.datetime],
) -> None:
    if not metadata:
        return
    hydrated_ts = ensure_utc(hydrated_at).isoformat() if hydrated_at else None
    rows = []
    for item in metadata:
        post_uri = item.get("post_uri")
        if not post_uri:
            continue
        rows.append(
            (
                item.get("cid"),
                item.get("author_did"),
                item.get("author_handle"),
                item.get("indexed_at"),
                item.get("created_at"),
                hydrated_ts,
                item.get("hydration_status"),
                item.get("hydration_error"),
                post_uri,
            )
        )
    if not rows:
        return
    conn.executemany(
        """
        UPDATE posts
        SET
            cid = COALESCE(?, cid),
            author_did = ?,
            author_handle = ?,
            indexed_at = COALESCE(?, indexed_at),
            created_at = COALESCE(?, created_at),
            last_hydrated_at = ?,
            hydration_status = ?,
            hydration_error = ?
        WHERE post_uri = ?
        """,
        rows,
    )
    conn.commit()


def store_subscriber_snapshot(
    conn: sqlite3.Connection,
    subscriber_dids: Set[str],
    subscriber_handles: Dict[str, str],
    snapshot_dt: dt.datetime,
) -> None:
    if not subscriber_dids:
        return
    snapshot_ts = ensure_utc(snapshot_dt).isoformat()
    did_list = sorted(subscriber_dids)
    placeholders = ",".join(["?"] * len(did_list))
    existing: Dict[str, Tuple[str, str]] = {}
    if did_list:
        query = (
            f"SELECT did, handle, snapshot_ts FROM subscriber_snapshots "
            f"WHERE did IN ({placeholders}) ORDER BY did, snapshot_ts DESC"
        )
        rows = conn.execute(query, did_list).fetchall()
        for did, handle, existing_snapshot_ts in rows:
            if did not in existing:
                existing[did] = (handle, existing_snapshot_ts)

    inserted = 0
    updated = 0
    for did in did_list:
        handle = subscriber_handles.get(did)
        current = existing.get(did)
        if current and current[0] == handle:
            conn.execute(
                "UPDATE subscriber_snapshots SET last_checked_ts = ? WHERE did = ? AND snapshot_ts = ?",
                (snapshot_ts, did, current[1]),
            )
            updated += 1
        else:
            conn.execute(
                "INSERT INTO subscriber_snapshots (snapshot_ts, last_checked_ts, did, handle) VALUES (?, ?, ?, ?)",
                (snapshot_ts, snapshot_ts, did, handle),
            )
            inserted += 1

    conn.commit()
    log_db_update(
        "subscriber_snapshots",
        {
            "snapshot_ts": snapshot_ts,
            "subscriber_count": len(did_list),
            "inserted": inserted,
            "updated": updated,
        },
    )


def store_subscriber_follow_counts(
    conn: sqlite3.Connection,
    follow_counts: Mapping[str, int],
    snapshot_dt: dt.datetime,
) -> Tuple[int, int]:
    if not follow_counts:
        return 0, 0
    snapshot_ts = ensure_utc(snapshot_dt).isoformat()
    updated = 0
    inserted = 0
    for did, count in follow_counts.items():
        if count is None:
            continue
        cursor = conn.execute(
            "UPDATE subscriber_follow_counts SET snapshot_ts = ? WHERE did = ? AND following_count = ?",
            (snapshot_ts, did, count),
        )
        if cursor.rowcount:
            updated += cursor.rowcount
            continue
        conn.execute(
            "INSERT INTO subscriber_follow_counts (did, following_count, snapshot_ts) VALUES (?, ?, ?)",
            (did, count, snapshot_ts),
        )
        inserted += 1
    conn.commit()
    if inserted or updated:
        log_db_update(
            "subscriber_follow_counts",
            {
                "snapshot_ts": snapshot_ts,
                "updated": updated,
                "inserted": inserted,
                "distinct_dids": sorted(follow_counts.keys()),
            },
        )
    return inserted, updated


def get_latest_follower_counts(conn: sqlite3.Connection) -> Dict[str, int]:
    rows = conn.execute(
        """
        SELECT sfc.did, sfc.following_count
        FROM subscriber_follow_counts sfc
        JOIN (
            SELECT did, MAX(snapshot_ts) AS latest_ts
            FROM subscriber_follow_counts
            GROUP BY did
        ) latest ON latest.did = sfc.did AND latest.latest_ts = sfc.snapshot_ts
        """
    ).fetchall()
    return {did: count for did, count in rows}


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


@dataclass
class PostIndexRebuildStats:
    scanned: int = 0
    updated: int = 0
    missing_position: int = 0
    invalid_position: int = 0
    parse_errors: int = 0

    def to_dict(self) -> Dict[str, int]:
        return {
            "scanned": self.scanned,
            "updated": self.updated,
            "missing_position": self.missing_position,
            "invalid_position": self.invalid_position,
            "parse_errors": self.parse_errors,
        }


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


def rebuild_post_indices_from_payload(conn: sqlite3.Connection) -> PostIndexRebuildStats:
    stats = PostIndexRebuildStats()
    updates: List[Tuple[Optional[int], int]] = []
    rows = conn.execute(
        "SELECT rowid, post_json, post_index FROM feed_request_posts"
    ).fetchall()
    for rowid, post_json, existing_index in rows:
        stats.scanned += 1
        if not post_json:
            stats.missing_position += 1
            continue
        try:
            payload = json.loads(post_json)
        except (TypeError, json.JSONDecodeError):
            stats.parse_errors += 1
            continue
        if not isinstance(payload, dict):
            stats.parse_errors += 1
            continue
        position = payload.get("position")
        coerced = _coerce_payload_position(position)
        if coerced is None:
            if position is None:
                stats.missing_position += 1
            else:
                stats.invalid_position += 1
            continue
        if existing_index == coerced:
            continue
        updates.append((coerced, rowid))
    if updates:
        conn.executemany(
            "UPDATE feed_request_posts SET post_index = ? WHERE rowid = ?",
            updates,
        )
        stats.updated = len(updates)
        conn.commit()
        log_db_update("feed_request_posts_rebuild", stats.to_dict())
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
               COUNT(frp.id) AS post_count
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
    mapping: Dict[str, int] = {}
    for post_uri, post_index in rows:
        if post_uri and post_index is not None:
            mapping[post_uri] = int(post_index)
    return mapping


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
