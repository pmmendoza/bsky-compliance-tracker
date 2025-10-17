import datetime as dt

from compliance_tracker.engagements import build_quote_records, extract_quoted_uri


def test_extract_quoted_uri_record():
    embed = {
        "$type": "app.bsky.embed.record",
        "record": {"uri": "at://example/post1"},
    }
    assert extract_quoted_uri(embed) == "at://example/post1"


def test_extract_quoted_uri_record_with_media():
    embed = {
        "$type": "app.bsky.embed.recordWithMedia",
        "record": {
            "record": {"uri": "at://example/post2"},
        },
    }
    assert extract_quoted_uri(embed) == "at://example/post2"


def test_build_quote_records_skips_mismatched_targets():
    window_start = dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)
    subscriber_dids = {"did:example:123"}
    quotes = [
        {
            "author": {"did": "did:example:123"},
            "record": {
                "createdAt": "2024-01-02T00:00:00Z",
                "embed": {
                    "$type": "app.bsky.embed.record",
                    "record": {"uri": "at://target/post"},
                },
                "text": "quoted text",
            },
        },
        {
            "author": {"did": "did:example:123"},
            "record": {
                "createdAt": "2024-01-02T01:00:00Z",
                "embed": {
                    "$type": "app.bsky.embed.record",
                    "record": {"uri": "at://other/post"},
                },
                "text": "should be skipped",
            },
        },
    ]

    records = build_quote_records(
        quotes,
        subscriber_dids,
        window_start,
        post_uri="at://target/post",
        post_author_handle="handle",
    )

    assert len(records) == 1
    assert records[0].post_uri == "at://target/post"
    assert records[0].engagement_text == "quoted text"
