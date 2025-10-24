import datetime as dt
import sqlite3

from compliance_tracker.hydration import hydrate_posts
from compliance_tracker.database import setup_database


class DummyClient:
    def __init__(self, responses):
        self._responses = responses

    def get_posts(self, uris):
        return [self._responses[uri] for uri in uris if uri in self._responses]


def make_connection():
    conn = sqlite3.connect(":memory:")
    setup_database(conn)
    return conn


def test_hydrate_posts_updates_metadata():
    conn = make_connection()
    now = dt.datetime(2025, 10, 24, tzinfo=dt.timezone.utc)
    conn.execute(
        "INSERT INTO posts (post_uri, cid, hydration_status) VALUES (?, ?, ?)",
        ("at://example/post1", None, "pending"),
    )
    conn.execute(
        "INSERT INTO posts (post_uri, cid, hydration_status) VALUES (?, ?, ?)",
        ("at://example/post2", None, "pending"),
    )
    conn.commit()

    responses = {
        "at://example/post1": {
            "uri": "at://example/post1",
            "cid": "cid1",
            "author": {"did": "did:author:1", "handle": "author1"},
            "indexedAt": "2025-10-24T10:00:00.000Z",
            "record": {"createdAt": "2025-10-24T09:55:00.000Z"},
        }
    }
    client = DummyClient(responses)

    stats = hydrate_posts(conn, client, batch_size=10, pause_seconds=0.0)

    assert stats == {"attempted": 2, "hydrated": 1, "not_found": 1, "errors": 0}

    rows = conn.execute(
        "SELECT post_uri, author_did, author_handle, hydration_status FROM posts ORDER BY post_uri"
    ).fetchall()
    assert rows == [
        ("at://example/post1", "did:author:1", "author1", "ok"),
        ("at://example/post2", None, None, "not_found"),
    ]
