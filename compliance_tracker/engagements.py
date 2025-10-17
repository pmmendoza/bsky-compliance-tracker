"""Engagement record utilities."""

from __future__ import annotations

import datetime as dt
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

from .client import BlueskyClient, HttpError
from .utils import ensure_utc, parse_datetime


@dataclass(frozen=True)
class EngagementOptions:
    include_likes: bool = True
    include_reposts: bool = True
    include_comments: bool = True
    include_quotes: bool = True


@dataclass(frozen=True)
class EngagementRecord:
    timestamp: str
    did_engagement: str
    post_uri: str
    post_author_handle: str
    engagement_type: str
    is_subscriber: bool
    engagement_text: Optional[str] = None


def extract_post_text(record: Dict) -> Optional[str]:
    text = record.get("text")
    if text:
        return text
    value = record.get("value")
    if isinstance(value, dict):
        text = value.get("text")
        if text:
            return text
    return None


def extract_quoted_uri(embed: Optional[Dict]) -> Optional[str]:
    """Return the quoted post URI for embeds handling record and recordWithMedia variants."""

    if not isinstance(embed, dict):
        return None
    embed_type = embed.get("$type")
    if embed_type == "app.bsky.embed.record":
        target = embed.get("record") or {}
        uri = target.get("uri") or target.get("$link")
        if isinstance(uri, str) and uri:
            return uri
    elif embed_type == "app.bsky.embed.recordWithMedia":
        record_container = embed.get("record") or {}
        if isinstance(record_container, dict):
            record = record_container.get("record") or {}
            uri = record.get("uri") or record.get("$link")
            if isinstance(uri, str) and uri:
                return uri
    for key in ("record", "media", "embed"):
        nested = embed.get(key)
        if isinstance(nested, dict):
            nested_uri = extract_quoted_uri(nested)
            if nested_uri:
                return nested_uri
    return None


def iter_thread_replies(node: Dict) -> Iterator[Dict]:
    for child in node.get("replies", []) or []:
        post = child.get("post")
        if post:
            yield post
        yield from iter_thread_replies(child)


def build_like_records(
    likes: Iterable[Dict],
    subscriber_dids,
    window_start: dt.datetime,
    post_uri: str,
    post_author_handle: str,
) -> List[EngagementRecord]:
    records: List[EngagementRecord] = []
    for like in likes:
        actor = like.get("actor") or {}
        actor_did = actor.get("did")
        if not actor_did:
            continue
        ts = parse_datetime(like.get("createdAt") or like.get("indexedAt"))
        if ts is None or ts < window_start:
            continue
        records.append(
            EngagementRecord(
                timestamp=ts.isoformat(),
                did_engagement=actor_did,
                post_uri=post_uri,
                post_author_handle=post_author_handle,
                engagement_type="like",
                is_subscriber=actor_did in subscriber_dids,
                engagement_text=None,
            )
        )
    return records


def build_repost_records(
    reposts: Iterable[Dict],
    subscriber_dids,
    window_start: dt.datetime,
    post_uri: str,
    post_author_handle: str,
) -> List[EngagementRecord]:
    records: List[EngagementRecord] = []
    for repost in reposts:
        actor_did = repost.get("did") or repost.get("actor", {}).get("did")
        if not actor_did:
            continue
        ts = parse_datetime(repost.get("createdAt") or repost.get("indexedAt"))
        if ts is None or ts < window_start:
            continue
        records.append(
            EngagementRecord(
                timestamp=ts.isoformat(),
                did_engagement=actor_did,
                post_uri=post_uri,
                post_author_handle=post_author_handle,
                engagement_type="repost",
                is_subscriber=actor_did in subscriber_dids,
                engagement_text=None,
            )
        )
    return records


def build_comment_records(
    replies: Iterable[Dict],
    subscriber_dids,
    window_start: dt.datetime,
    post_uri: str,
    post_author_handle: str,
) -> List[EngagementRecord]:
    records: List[EngagementRecord] = []
    for reply in replies:
        author = reply.get("author") or {}
        author_did = author.get("did")
        if not author_did:
            continue
        record = reply.get("record", {})
        ts = parse_datetime(record.get("createdAt") or reply.get("indexedAt"))
        if ts is None or ts < window_start:
            continue
        records.append(
            EngagementRecord(
                timestamp=ts.isoformat(),
                did_engagement=author_did,
                post_uri=post_uri,
                post_author_handle=post_author_handle,
                engagement_type="comment",
                is_subscriber=author_did in subscriber_dids,
                engagement_text=extract_post_text(record) or None,
            )
        )
    return records


def build_quote_records(
    quotes: Iterable[Dict],
    subscriber_dids,
    window_start: dt.datetime,
    post_uri: str,
    post_author_handle: str,
) -> List[EngagementRecord]:
    records: List[EngagementRecord] = []
    for quote in quotes:
        author = quote.get("author") or {}
        author_did = author.get("did")
        if not author_did:
            continue
        record = quote.get("record", {})
        ts = parse_datetime(record.get("createdAt") or quote.get("indexedAt"))
        if ts is None or ts < window_start:
            continue
        quoted_uri = extract_quoted_uri(record.get("embed") or quote.get("embed"))
        if quoted_uri and quoted_uri != post_uri:
            continue
        records.append(
            EngagementRecord(
                timestamp=ts.isoformat(),
                did_engagement=author_did,
                post_uri=post_uri,
                post_author_handle=post_author_handle,
                engagement_type="quote",
                is_subscriber=author_did in subscriber_dids,
                engagement_text=extract_post_text(record) or None,
            )
        )
    return records


def _collect_posts_text_map(posts: Iterable[Dict]) -> Dict[Tuple[str, str], str]:
    mapping: Dict[Tuple[str, str], str] = {}
    for post in posts:
        author = post.get("author") or {}
        did = author.get("did")
        if not did:
            continue
        record = post.get("record") or {}
        ts_raw = record.get("createdAt") or post.get("indexedAt")
        ts = parse_datetime(ts_raw)
        if ts is None:
            continue
        iso_ts = ensure_utc(ts).isoformat()
        text = extract_post_text(record)
        if not text:
            continue
        mapping[(did, iso_ts)] = text
    return mapping


def build_comment_text_map(client: BlueskyClient, post_uri: str) -> Dict[Tuple[str, str], str]:
    try:
        thread = client.get_post_thread(post_uri)
    except HttpError:
        return {}
    replies: List[Dict] = []
    thread_root = thread.get("thread")
    if isinstance(thread_root, dict):
        replies.extend(iter_thread_replies(thread_root))
    return _collect_posts_text_map(replies)


def build_quote_text_map(client: BlueskyClient, post_uri: str) -> Dict[Tuple[str, str], str]:
    try:
        quotes = list(client.iter_quotes(post_uri))
    except HttpError:
        return {}
    return _collect_posts_text_map(quotes)


def backfill_missing_engagement_texts(conn, client: BlueskyClient) -> int:
    query = (
        "SELECT rowid, post_uri, did_engagement, timestamp, engagement_type "
        "FROM engagements "
        "WHERE engagement_type IN ('comment', 'quote') "
        "AND (engagement_text IS NULL OR engagement_text = '')"
    )
    rows = conn.execute(query).fetchall()
    if not rows:
        return 0

    grouped: Dict[Tuple[str, str], List[Tuple[int, str, str]]] = defaultdict(list)
    for rowid, post_uri, did, ts, engagement_type in rows:
        grouped[(post_uri, engagement_type)].append((rowid, did, ts))

    updated = 0
    for (post_uri, engagement_type), entries in grouped.items():
        if engagement_type == "comment":
            text_map = build_comment_text_map(client, post_uri)
        else:
            text_map = build_quote_text_map(client, post_uri)
        if not text_map:
            continue
        for rowid, did, ts in entries:
            text = text_map.get((did, ts))
            if not text:
                continue
            conn.execute(
                "UPDATE engagements SET engagement_text = ? WHERE rowid = ?",
                (text, rowid),
            )
            updated += 1
    if updated:
        conn.commit()
    return updated


def collect_engagements_for_post(
    client: BlueskyClient,
    subscriber_dids,
    window_start: dt.datetime,
    post: Dict,
    options: EngagementOptions,
) -> List[EngagementRecord]:
    post_uri = post.get("uri")
    if not post_uri:
        return []
    author = post.get("author") or {}
    post_author_handle = author.get("handle") or ""

    records: List[EngagementRecord] = []

    like_count = post.get("likeCount")
    if options.include_likes and (like_count is None or like_count > 0):
        like_records = build_like_records(
            client.iter_likes(post_uri), subscriber_dids, window_start, post_uri, post_author_handle
        )
        records.extend(like_records)

    repost_count = post.get("repostCount")
    if options.include_reposts and (repost_count is None or repost_count > 0):
        repost_records = build_repost_records(
            client.iter_reposts(post_uri), subscriber_dids, window_start, post_uri, post_author_handle
        )
        records.extend(repost_records)

    reply_count = post.get("replyCount")
    if options.include_comments and (reply_count is None or reply_count > 0):
        thread = client.get_post_thread(post_uri)
        replies_iter: List[Dict] = []
        thread_root = thread.get("thread")
        if isinstance(thread_root, dict):
            replies_iter.extend(iter_thread_replies(thread_root))
        comment_records = build_comment_records(
            replies_iter, subscriber_dids, window_start, post_uri, post_author_handle
        )
        records.extend(comment_records)

    quote_count = post.get("quoteCount")
    if options.include_quotes and (quote_count is None or quote_count > 0):
        try:
            quotes_source = list(client.iter_quotes(post_uri))
        except HttpError:
            quotes_source = []
        quote_records = build_quote_records(
            quotes_source, subscriber_dids, window_start, post_uri, post_author_handle
        )
        records.extend(quote_records)

    return records
