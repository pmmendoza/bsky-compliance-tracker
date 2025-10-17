"""Functions to repair incomplete feed retrieval payloads."""

from __future__ import annotations

import datetime as dt
import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

from typing import Callable

from .api import fetch_feed_retrievals
from .database import store_feed_retrievals
from .utils import ensure_utc, format_min_date, parse_datetime

logger = logging.getLogger(__name__)


@dataclass
class RepairStats:
    empty_requests: int = 0
    did_attempts: int = 0
    repaired_requests: int = 0
    still_empty: int = 0
    errors: int = 0

    def to_dict(self) -> Dict[str, int]:
        return {
            "empty_requests": self.empty_requests,
            "did_attempts": self.did_attempts,
            "repaired_requests": self.repaired_requests,
            "still_empty": self.still_empty,
            "errors": self.errors,
        }


def repair_empty_feed_requests(
    conn,
    env: Dict[str, str],
    *,
    since: Optional[dt.datetime],
    timeout: float,
    max_retries: int,
    backoff: float,
    fetch_fn: Callable[..., List[Dict]] = fetch_feed_retrievals,
) -> RepairStats:
    """Attempt to re-fetch feed payloads for empty snapshots."""

    params: List = []
    query = (
        "SELECT fr.request_id, fr.requester_did, fr.timestamp "
        "FROM feed_requests fr "
        "LEFT JOIN feed_request_posts frp ON fr.request_id = frp.request_id "
        "WHERE frp.request_id IS NULL "
    )
    if since is not None:
        params.append(ensure_utc(since).isoformat())
        query += "AND fr.timestamp >= ? "

    rows = conn.execute(query, params).fetchall()
    stats = RepairStats(empty_requests=len(rows))
    if not rows:
        return stats

    grouped: Dict[str, List[Tuple[int, str]]] = defaultdict(list)
    for request_id, did, ts_raw in rows:
        grouped[did].append((request_id, ts_raw))

    stats.did_attempts = len(grouped)
    for did, entries in grouped.items():
        min_ts = _calculate_min_ts(entries)
        if min_ts is None:
            stats.errors += 1
            continue
        min_date = format_min_date(min_ts - dt.timedelta(seconds=5))
        try:
            retrievals = fetch_fn(
                env,
                user_did=did,
                min_date=min_date,
                timeout=timeout,
                max_retries=max_retries,
                backoff=backoff,
            )
        except Exception:
            logger.exception("Failed to repair feed requests for %s", did)
            stats.errors += 1
            continue

        if not retrievals:
            stats.still_empty += len(entries)
            continue

        store_feed_retrievals(conn, retrievals)

        repaired = 0
        for request_id, _ in entries:
            has_posts = conn.execute(
                "SELECT 1 FROM feed_request_posts WHERE request_id = ? LIMIT 1",
                (request_id,),
            ).fetchone()
            if has_posts:
                repaired += 1
            else:
                stats.still_empty += 1
        stats.repaired_requests += repaired

    conn.commit()
    return stats


def _calculate_min_ts(entries: Iterable[Tuple[int, str]]) -> Optional[dt.datetime]:
    timestamps: List[dt.datetime] = []
    for _, ts_raw in entries:
        ts = parse_datetime(ts_raw)
        if ts is None:
            continue
        timestamps.append(ensure_utc(ts))
    if not timestamps:
        return None
    return min(timestamps)
