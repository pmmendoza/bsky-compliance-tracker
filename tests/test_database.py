import datetime as dt
import io
import sqlite3
import sys

from compliance_tracker.constants import POSITION_STATUS_MATCHED, POSITION_STATUS_NO_FEED
from compliance_tracker.database import (
    match_post_positions,
    setup_database,
    store_engagements,
    store_feed_retrievals,
)
from compliance_tracker.engagements import EngagementRecord
from compliance_tracker.progress import progress_iter
from compliance_tracker.repair import repair_empty_feed_requests


def make_connection():
    conn = sqlite3.connect(":memory:")
    setup_database(conn)
    return conn


def test_store_feed_retrievals_zero_based_positions():
    conn = make_connection()
    feed_ts = dt.datetime(2024, 10, 1, 12, 0, tzinfo=dt.timezone.utc).isoformat()
    retrieval = {
        "id": 1,
        "requester_did": "did:example:123",
        "timestamp": feed_ts,
        "posts": [
            {"uri": "at://example/post1"},
            {"uri": "at://example/post2"},
        ],
    }
    store_feed_retrievals(conn, [retrieval])
    indices = [row[0] for row in conn.execute("SELECT post_index FROM feed_request_posts ORDER BY post_index")]
    assert indices == [0, 1]


def test_match_post_positions_matches_and_flags():
    conn = make_connection()
    feed_ts = dt.datetime(2024, 10, 1, 12, 0, tzinfo=dt.timezone.utc)
    store_feed_retrievals(
        conn,
        [
            {
                "id": 1,
                "requester_did": "did:example:123",
                "timestamp": feed_ts.isoformat(),
                "posts": [{"uri": "at://example/post1"}],
            }
        ],
    )
    store_engagements(
        conn,
        [
            EngagementRecord(
                timestamp=(feed_ts + dt.timedelta(minutes=5)).isoformat(),
                did_engagement="did:example:123",
                post_uri="at://example/post1",
                post_author_handle="author.handle",
                engagement_type="like",
                is_subscriber=True,
            ),
            EngagementRecord(
                timestamp=(feed_ts + dt.timedelta(minutes=10)).isoformat(),
                did_engagement="did:example:456",
                post_uri="at://example/post2",
                post_author_handle="author.handle",
                engagement_type="like",
                is_subscriber=True,
            ),
        ],
    )

    stats = match_post_positions(conn, since=feed_ts - dt.timedelta(minutes=1))
    stats_dict = stats.to_dict()

    assert stats_dict[f"status_{POSITION_STATUS_MATCHED}"] == 1
    assert stats_dict[f"status_{POSITION_STATUS_NO_FEED}"] == 1

    rows = conn.execute(
        "SELECT post_position, position_status FROM engagements ORDER BY did_engagement"
    ).fetchall()
    assert rows[0] == (0, POSITION_STATUS_MATCHED)
    assert rows[1][1] == POSITION_STATUS_NO_FEED


def test_repair_empty_feed_requests_stores_posts():
    conn = make_connection()
    feed_ts = dt.datetime(2024, 10, 1, 12, 0, tzinfo=dt.timezone.utc)
    conn.execute(
        "INSERT INTO feed_requests (request_id, requester_did, algo, timestamp, posts_json) VALUES (?, ?, ?, ?, ?)",
        (21, "did:example:789", None, feed_ts.isoformat(), "[]"),
    )
    conn.commit()

    def fake_fetch(env, user_did, min_date, timeout, max_retries, backoff):
        return [
            {
                "id": 21,
                "requester_did": "did:example:789",
                "timestamp": feed_ts.isoformat(),
                "posts": [{"uri": "at://example/post99"}],
            }
        ]

    stats = repair_empty_feed_requests(
        conn,
        {},
        since=feed_ts - dt.timedelta(days=1),
        timeout=30.0,
        max_retries=3,
        backoff=1.5,
        fetch_fn=fake_fetch,
    )

    assert stats.repaired_requests == 1
    rows = conn.execute(
        "SELECT post_uri FROM feed_request_posts WHERE request_id = ?", (21,)
    ).fetchall()
    assert rows == [("at://example/post99",)]


def test_progress_iter_handles_generators_without_len(monkeypatch):
    captured = io.StringIO()
    monkeypatch.setattr(sys, "stderr", captured)
    generator = (i for i in range(3))
    assert list(progress_iter(generator, desc="gen")) == [0, 1, 2]
