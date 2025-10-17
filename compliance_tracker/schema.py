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
            request_id INTEGER NOT NULL,
            post_index INTEGER NOT NULL,
            post_uri TEXT,
            post_author_did TEXT,
            post_author_handle TEXT,
            post_json TEXT,
            PRIMARY KEY (request_id, post_index),
            FOREIGN KEY (request_id) REFERENCES feed_requests(request_id) ON DELETE CASCADE
        )
        """
    )
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_feed_requests_did_time ON feed_requests(requester_did, timestamp)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_feed_requests_time ON feed_requests(timestamp)")


def ensure_database(conn: sqlite3.Connection) -> None:
    ensure_engagements_schema(conn)
    ensure_feed_schema(conn)
    conn.commit()
