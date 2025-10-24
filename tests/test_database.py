import datetime as dt
import io
import json
import sqlite3
import sys

from compliance_tracker.constants import (
    POSITION_STATUS_MATCHED,
    POSITION_STATUS_NO_FEED,
    POSITION_STATUS_POST_MISSING,
)
from compliance_tracker.database import (
    get_latest_follower_counts,
    get_post_uris_pending_hydration,
    match_post_positions,
    rebuild_post_indices_from_payload,
    seed_posts_from_feed,
    setup_database,
    store_engagements,
    store_feed_retrievals,
    store_subscriber_follow_counts,
)
from compliance_tracker.engagements import EngagementRecord
from compliance_tracker.progress import progress_iter
from compliance_tracker.repair import repair_empty_feed_requests


def make_connection():
    conn = sqlite3.connect(":memory:")
    setup_database(conn)
    return conn


def test_store_feed_retrievals_respects_payload_position():
    conn = make_connection()
    feed_ts = dt.datetime(2024, 10, 1, 12, 0, tzinfo=dt.timezone.utc).isoformat()
    retrieval = {
        "id": 1,
        "requester_did": "did:example:123",
        "timestamp": feed_ts,
        "posts": [
            {"uri": "at://example/post1", "position": 7},
            {"uri": "at://example/post2", "position": "15"},
        ],
    }
    store_feed_retrievals(conn, [retrieval])
    rows = conn.execute(
        "SELECT post_uri, post_index FROM feed_request_posts ORDER BY post_uri"
    ).fetchall()
    assert rows == [
        ("at://example/post1", 7),
        ("at://example/post2", 15),
    ]

    post_rows = conn.execute("SELECT post_uri, hydration_status FROM posts ORDER BY post_uri").fetchall()
    assert post_rows == [
        ("at://example/post1", "pending"),
        ("at://example/post2", "pending"),
    ]


def test_store_feed_retrievals_leaves_position_null_when_missing():
    conn = make_connection()
    feed_ts = dt.datetime(2024, 10, 1, 12, 0, tzinfo=dt.timezone.utc).isoformat()
    retrieval = {
        "id": 2,
        "requester_did": "did:example:124",
        "timestamp": feed_ts,
        "posts": [
            {"uri": "at://example/post3"},
            {"uri": "at://example/post4"},
        ],
    }
    store_feed_retrievals(conn, [retrieval])
    rows = conn.execute(
        "SELECT post_uri, post_index FROM feed_request_posts WHERE request_id = ? ORDER BY post_uri",
        (2,),
    ).fetchall()
    assert rows == [
        ("at://example/post3", None),
        ("at://example/post4", None),
    ]


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
                "posts": [{"uri": "at://example/post1", "position": 42}],
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
        "SELECT post_position, position_feed_request_id, position_status FROM engagements ORDER BY did_engagement"
    ).fetchall()
    assert rows[0] == (42, 1, POSITION_STATUS_MATCHED)
    assert rows[1] == (None, None, POSITION_STATUS_NO_FEED)


def test_match_post_positions_flags_missing_index():
    conn = make_connection()
    feed_ts = dt.datetime(2024, 10, 1, 12, 0, tzinfo=dt.timezone.utc)
    store_feed_retrievals(
        conn,
        [
            {
                "id": 2,
                "requester_did": "did:example:999",
                "timestamp": feed_ts.isoformat(),
                "posts": [{"uri": "at://example/postX"}],
            }
        ],
    )
    store_engagements(
        conn,
        [
            EngagementRecord(
                timestamp=(feed_ts + dt.timedelta(minutes=2)).isoformat(),
                did_engagement="did:example:999",
                post_uri="at://example/postX",
                post_author_handle="author.handle",
                engagement_type="like",
                is_subscriber=True,
            )
        ],
    )

    stats = match_post_positions(conn, since=feed_ts - dt.timedelta(minutes=1))
    stats_dict = stats.to_dict()

    assert stats_dict.get(f"status_{POSITION_STATUS_POST_MISSING}", 0) == 1
    row = conn.execute(
        "SELECT post_position, position_status FROM engagements"
    ).fetchone()
    assert row == (None, POSITION_STATUS_POST_MISSING)


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


def test_rebuild_post_indices_from_payload_updates_rows():
    conn = make_connection()
    feed_ts = dt.datetime(2024, 10, 1, 12, 0, tzinfo=dt.timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO feed_requests (request_id, requester_did, algo, timestamp, posts_json) VALUES (?, ?, ?, ?, ?)",
        (9, "did:example:789", None, feed_ts, "[]"),
    )
    conn.execute(
        "INSERT INTO feed_request_posts (request_id, post_index, post_uri, post_json) VALUES (?, ?, ?, ?)",
        (
            9,
            0,
            "at://example/postA",
            json.dumps({"uri": "at://example/postA", "position": 6}),
        ),
    )
    conn.execute(
        "INSERT INTO feed_request_posts (request_id, post_index, post_uri, post_json) VALUES (?, ?, ?, ?)",
        (
            9,
            3,
            "at://example/postB",
            json.dumps({"uri": "at://example/postB"}),
        ),
    )
    conn.commit()

    stats = rebuild_post_indices_from_payload(conn)

    assert stats.updated == 1
    assert (
        conn.execute(
            "SELECT post_index FROM feed_request_posts WHERE post_uri = ?",
            ("at://example/postA",),
        ).fetchone()[0]
        == 6
    )
    assert (
        conn.execute(
            "SELECT post_index FROM feed_request_posts WHERE post_uri = ?",
            ("at://example/postB",),
        ).fetchone()[0]
        == 3
    )


def test_store_subscriber_follow_counts_updates_timestamp_for_same_count():
    conn = make_connection()
    did = "did:example:sub"
    ts1 = dt.datetime(2025, 10, 24, 12, 0, tzinfo=dt.timezone.utc)

    store_subscriber_follow_counts(conn, {did: 42}, ts1)
    row = conn.execute(
        "SELECT did, following_count, snapshot_ts FROM subscriber_follow_counts"
    ).fetchone()
    assert row == (did, 42, ts1.isoformat())

    ts2 = ts1 + dt.timedelta(hours=1)
    store_subscriber_follow_counts(conn, {did: 42}, ts2)
    rows = conn.execute(
        "SELECT did, following_count, snapshot_ts FROM subscriber_follow_counts"
    ).fetchall()
    assert rows == [(did, 42, ts2.isoformat())]


def test_store_subscriber_follow_counts_inserts_new_row_on_change():
    conn = make_connection()
    did = "did:example:sub2"
    ts1 = dt.datetime(2025, 10, 24, 12, 0, tzinfo=dt.timezone.utc)
    ts2 = ts1 + dt.timedelta(hours=2)

    store_subscriber_follow_counts(conn, {did: 10}, ts1)
    store_subscriber_follow_counts(conn, {did: 15}, ts2)

    rows = conn.execute(
        "SELECT did, following_count, snapshot_ts FROM subscriber_follow_counts ORDER BY following_count"
    ).fetchall()
    assert rows == [
        (did, 10, ts1.isoformat()),
        (did, 15, ts2.isoformat()),
    ]
    latest = get_latest_follower_counts(conn)
    assert latest == {did: 15}


def test_seed_posts_from_feed_populates_posts_table():
    conn = make_connection()
    conn.execute(
        "INSERT INTO feed_requests (request_id, requester_did, timestamp, posts_json) VALUES (?, ?, ?, ?)",
        (1, "did:example:123", dt.datetime.now(dt.timezone.utc).isoformat(), "[]"),
    )
    conn.execute(
        "INSERT INTO feed_requests (request_id, requester_did, timestamp, posts_json) VALUES (?, ?, ?, ?)",
        (2, "did:example:124", dt.datetime.now(dt.timezone.utc).isoformat(), "[]"),
    )
    conn.execute(
        "INSERT INTO feed_request_posts (request_id, post_index, post_uri, post_json) VALUES (?, ?, ?, ?)",
        (1, 0, "at://example/postA", json.dumps({"uri": "at://example/postA", "cid": "cidA"})),
    )
    conn.execute(
        "INSERT INTO feed_request_posts (request_id, post_index, post_uri, post_json) VALUES (?, ?, ?, ?)",
        (2, 1, "at://example/postB", json.dumps({"uri": "at://example/postB"})),
    )
    conn.commit()

    inserted = seed_posts_from_feed(conn)
    assert inserted == 2
    rows = conn.execute(
        "SELECT post_uri, cid, hydration_status FROM posts ORDER BY post_uri"
    ).fetchall()
    assert rows == [
        ("at://example/postA", "cidA", "pending"),
        ("at://example/postB", None, "pending"),
    ]


def test_get_post_uris_pending_hydration_returns_only_unhydrated():
    conn = make_connection()
    conn.execute(
        "INSERT INTO posts (post_uri, hydration_status) VALUES (?, ?)",
        ("at://example/post1", "pending"),
    )
    conn.execute(
        "INSERT INTO posts (post_uri, author_did, author_handle, hydration_status) VALUES (?, ?, ?, ?)",
        ("at://example/post2", "did:author", "author.handle", "ok"),
    )
    conn.execute(
        "INSERT INTO posts (post_uri, hydration_status) VALUES (?, ?)",
        ("at://example/post3", "not_found"),
    )
    conn.commit()

    pending = get_post_uris_pending_hydration(conn)
    assert pending == ["at://example/post1"]
