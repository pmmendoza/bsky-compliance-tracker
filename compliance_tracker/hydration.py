"""Post metadata hydration utilities."""

from __future__ import annotations

import datetime as dt
import logging
import math
import time
from typing import Dict, Iterable, List, Mapping, Optional

import sqlite3

from .database import (
    get_post_uris_pending_hydration,
    update_posts_metadata,
)
from .progress import progress_iter

logger = logging.getLogger(__name__)


def hydrate_posts(
    conn: sqlite3.Connection,
    client,
    *,
    batch_size: int = 25,
    pause_seconds: float = 0.2,
    limit: Optional[int] = None,
    fetch_fn=None,
) -> Dict[str, int]:
    """Hydrate missing post metadata using the AppView API.

    Parameters
    ----------
    conn : sqlite3.Connection
        Database connection with `posts` table.
    client : BlueskyClient-like
        Must expose `get_posts(uris: Iterable[str]) -> List[Dict]`.
    batch_size : int
        Maximum URIs to include per API request (AppView allows up to 25).
    pause_seconds : float
        Delay between API calls to avoid rate limiting.
    limit : Optional[int]
        Optional cap on number of URIs hydrated in this run.
    fetch_fn : callable
        Optional override for fetching posts; useful in tests.

    Returns
    -------
    Dict[str, int]
        Summary statistics: attempted, hydrated, not_found, errors.
    """

    pending = get_post_uris_pending_hydration(conn, limit=limit)
    stats = {
        "attempted": len(pending),
        "hydrated": 0,
        "not_found": 0,
        "errors": 0,
    }
    if not pending:
        return stats

    fetch = fetch_fn or client.get_posts
    now = dt.datetime.now(dt.timezone.utc)

    total_batches = math.ceil(len(pending) / batch_size)
    batch_iter = progress_iter(
        range(0, len(pending), batch_size),
        total=total_batches,
        desc="Hydrating posts",
    )

    for start in batch_iter:
        chunk = pending[start : start + batch_size]
        try:
            response_posts = fetch(chunk)
        except Exception as exc:  # pragma: no cover - network failure path
            logger.warning("Hydration batch failed for %d URIs: %s", len(chunk), exc)
            failed_metadata = [
                {
                    "post_uri": uri,
                    "cid": None,
                    "author_did": None,
                    "author_handle": None,
                    "indexed_at": None,
                    "created_at": None,
                    "hydration_status": "error",
                    "hydration_error": str(exc),
                }
                for uri in chunk
            ]
            update_posts_metadata(conn, failed_metadata, hydrated_at=now)
            stats["errors"] += len(chunk)
            continue

        posts_by_uri: Dict[str, Mapping] = {}
        if isinstance(response_posts, list):
            for post in response_posts:
                if isinstance(post, Mapping):
                    uri = post.get("uri")
                    if isinstance(uri, str):
                        posts_by_uri[uri] = post

        metadata_updates: List[Dict[str, Optional[str]]] = []
        for uri in chunk:
            post_data = posts_by_uri.get(uri)
            if post_data:
                author = post_data.get("author") or {}
                record = post_data.get("record") or {}
                metadata_updates.append(
                    {
                        "post_uri": uri,
                        "cid": post_data.get("cid"),
                        "author_did": author.get("did"),
                        "author_handle": author.get("handle"),
                        "indexed_at": post_data.get("indexedAt"),
                        "created_at": record.get("createdAt"),
                        "hydration_status": "ok",
                        "hydration_error": None,
                    }
                )
                stats["hydrated"] += 1
            else:
                metadata_updates.append(
                    {
                        "post_uri": uri,
                        "cid": None,
                        "author_did": None,
                        "author_handle": None,
                        "indexed_at": None,
                        "created_at": None,
                        "hydration_status": "not_found",
                        "hydration_error": None,
                    }
                )
                stats["not_found"] += 1

        update_posts_metadata(conn, metadata_updates, hydrated_at=now)

        if pause_seconds and start + batch_size < len(pending):  # avoid sleep after final batch
            time.sleep(pause_seconds)

    return stats
