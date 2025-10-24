"""Database schema helpers."""

from __future__ import annotations

import sqlite3


def ensure_engagements_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS engagements (
            timestamp TEXT NOT NULL,
            did_engagement TEXT NOT NULL,
            post_uri TEXT NOT NULL,
            post_author_handle TEXT NOT NULL,
            engagement_type TEXT NOT NULL,
            is_subscriber INTEGER NOT NULL DEFAULT 0,
            engagement_text TEXT,
            post_position INTEGER,
            position_feed_request_id INTEGER,
            position_age_seconds REAL,
            position_status TEXT,
            UNIQUE (timestamp, did_engagement, post_uri, engagement_type)
        )
        """
    )
    columns = {row[1] for row in conn.execute("PRAGMA table_info(engagements)")}
    expected = {
        "timestamp",
        "did_engagement",
        "post_uri",
        "post_author_handle",
        "engagement_type",
        "is_subscriber",
        "engagement_text",
        "post_position",
        "position_feed_request_id",
        "position_age_seconds",
        "position_status",
    }
    missing = expected.difference(columns)
    for column in missing:
        if column == "engagement_text":
            conn.execute("ALTER TABLE engagements ADD COLUMN engagement_text TEXT")
        elif column == "post_position":
            conn.execute("ALTER TABLE engagements ADD COLUMN post_position INTEGER")
        elif column == "position_feed_request_id":
            conn.execute("ALTER TABLE engagements ADD COLUMN position_feed_request_id INTEGER")
        elif column == "position_age_seconds":
            conn.execute("ALTER TABLE engagements ADD COLUMN position_age_seconds REAL")
        elif column == "position_status":
            conn.execute("ALTER TABLE engagements ADD COLUMN position_status TEXT")
        elif column == "is_subscriber":
            conn.execute("ALTER TABLE engagements ADD COLUMN is_subscriber INTEGER NOT NULL DEFAULT 0")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_engagements_did_time ON engagements(did_engagement, timestamp)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_engagements_post ON engagements(post_uri)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_engagements_time ON engagements(timestamp)")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_engagements_subscriber_time ON engagements(is_subscriber, timestamp)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_engagements_position_status ON engagements(position_status)"
    )


def ensure_feed_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS feed_requests (
            request_id INTEGER PRIMARY KEY,
            requester_did TEXT NOT NULL,
            algo TEXT,
            timestamp TEXT NOT NULL,
            posts_json TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS feed_request_posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            request_id INTEGER NOT NULL,
            post_index INTEGER,
            post_uri TEXT,
            post_json TEXT,
            UNIQUE (request_id, post_uri),
            FOREIGN KEY (request_id) REFERENCES feed_requests(request_id) ON DELETE CASCADE
        )
        """
    )
    conn.execute("PRAGMA foreign_keys = ON")
    columns = {row[1] for row in conn.execute("PRAGMA table_info(feed_request_posts)")}
    if "id" not in columns or "post_author_did" in columns or "post_author_handle" in columns:
        _migrate_feed_request_posts(conn)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_feed_requests_did_time ON feed_requests(requester_did, timestamp)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_feed_requests_time ON feed_requests(timestamp)")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_feed_request_posts_request_uri ON feed_request_posts(request_id, post_uri)"
    )


def ensure_posts_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS posts (
            post_uri TEXT PRIMARY KEY,
            cid TEXT,
            author_did TEXT,
            author_handle TEXT,
            indexed_at TEXT,
            created_at TEXT,
            last_hydrated_at TEXT,
            hydration_status TEXT,
            hydration_error TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_posts_author_did ON posts(author_did)")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_posts_hydration_status ON posts(hydration_status, last_hydrated_at)"
    )


def ensure_follow_counts_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS subscriber_follow_counts (
            did TEXT NOT NULL,
            following_count INTEGER NOT NULL,
            snapshot_ts TEXT NOT NULL,
            PRIMARY KEY (did, following_count)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_follow_counts_snapshot
        ON subscriber_follow_counts(snapshot_ts)
        """
    )


def _migrate_feed_request_posts(conn: sqlite3.Connection) -> None:
    conn.execute("ALTER TABLE feed_request_posts RENAME TO feed_request_posts_old")
    conn.execute(
        """
        CREATE TABLE feed_request_posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            request_id INTEGER NOT NULL,
            post_index INTEGER,
            post_uri TEXT,
            post_json TEXT,
            UNIQUE (request_id, post_uri),
            FOREIGN KEY (request_id) REFERENCES feed_requests(request_id) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        """
        INSERT OR REPLACE INTO feed_request_posts (request_id, post_index, post_uri, post_json)
        SELECT request_id, post_index, post_uri, post_json
        FROM feed_request_posts_old
        """
    )
    conn.execute("DROP TABLE feed_request_posts_old")


def ensure_database(conn: sqlite3.Connection) -> None:
    ensure_engagements_schema(conn)
    ensure_feed_schema(conn)
    ensure_posts_schema(conn)
    ensure_follow_counts_schema(conn)
    conn.commit()
