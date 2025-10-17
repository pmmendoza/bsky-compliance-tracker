"""Command-line interface for the compliance tracker."""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import sqlite3
from pathlib import Path
from typing import Dict, Optional, Sequence

from .api import fetch_feed_retrievals, fetch_subscribers
from .client import BlueskyClient, HttpError
from .constants import (
    BOT_HANDLES,
    DEFAULT_DB_PATH,
    ENV_DEFAULT_PATH,
    LEGACY_ENV_PATH,
    POSITION_STATUS_LABELS,
    POSITION_STATUS_MATCHED,
)
from .database import (
    get_latest_timestamp,
    match_post_positions,
    setup_database,
    store_engagements,
    store_feed_retrievals,
    store_subscriber_snapshot,
)
from .engagements import (
    EngagementOptions,
    backfill_missing_engagement_texts,
    collect_engagements_for_post,
)
from .progress import progress_iter
from .repair import repair_empty_feed_requests
from .utils import format_min_date, load_env_from_file, log_db_update, normalize_since

logger = logging.getLogger("engagement_collector")


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=float, default=None, help="Engagement lookback window in days")
    parser.add_argument(
        "--since",
        type=str,
        default=None,
        help="Absolute timestamp or numeric days-ago offset for engagement window (overrides --days)",
    )
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH, help="Path to the compliance SQLite DB")
    parser.add_argument("--env", type=Path, default=ENV_DEFAULT_PATH, help="Path to the .env file with credentials")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity",
    )
    parser.add_argument("--timeout", type=float, default=30.0, help="HTTP request timeout in seconds")
    parser.add_argument("--max-retries", type=int, default=5, help="Retry count for transient HTTP errors")
    parser.add_argument("--skip-likes", action="store_true", help="Skip collecting likes")
    parser.add_argument("--skip-reposts", action="store_true", help="Skip collecting reposts")
    parser.add_argument("--skip-comments", action="store_true", help="Skip collecting replies/comments")
    parser.add_argument("--skip-quotes", action="store_true", help="Skip collecting quotes")
    parser.add_argument("--feed-days", type=float, default=None, help="Feed retrieval lookback window in days")
    parser.add_argument(
        "--feed-since",
        type=str,
        default=None,
        help="Absolute timestamp or numeric days-ago offset for feed retrievals (overrides --feed-days)",
    )
    parser.add_argument("--feed-did", help="Restrict feed retrieval compliance to a specific requester DID")
    parser.add_argument("--skip-feed", action="store_true", help="Skip feed retrieval compliance collection")
    parser.add_argument(
        "--post-repair-window",
        type=float,
        default=0.0,
        help="Days to look back for repairing empty feed payload snapshots",
    )
    parser.add_argument(
        "--position-since",
        type=str,
        default=None,
        help="Timestamp or numeric days-ago offset for recomputing post positions (overrides engagement window)",
    )
    parser.add_argument(
        "--position-days",
        type=float,
        default=None,
        help="Days back from now to recompute post positions (overrides engagement window)",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    env_values = load_env_from_file(args.env)
    if not env_values and args.env == ENV_DEFAULT_PATH and LEGACY_ENV_PATH.exists():
        logger.info("Primary env file missing or empty; trying legacy path %s", LEGACY_ENV_PATH)
        env_values = load_env_from_file(LEGACY_ENV_PATH)
    for key, value in env_values.items():
        os.environ.setdefault(key, value)

    try:
        subscriber_dids, subscriber_handles = fetch_subscribers(env_values)
    except Exception:
        logger.exception("Failed to load subscriber list")
        return 1
    logger.info("Loaded %d subscribers", len(subscriber_dids))
    if not subscriber_dids:
        logger.warning("No subscribers found; continuing with empty subscriber list")

    db_path = args.db
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    setup_database(conn)
    logger.info("Storing results in %s", db_path)

    now = dt.datetime.now(dt.timezone.utc)
    repair_window_days = max(args.post_repair_window or 0.0, 0.0)
    last_engagement_ts = get_latest_timestamp(conn, "engagements", "timestamp")
    last_feed_ts = get_latest_timestamp(conn, "feed_requests", "timestamp")

    _log_latest("engagement", last_engagement_ts, now)
    _log_latest("feed retrieval", last_feed_ts, now)

    if subscriber_dids:
        store_subscriber_snapshot(conn, subscriber_dids, subscriber_handles, now)
        logger.info("Recorded subscriber snapshot for %d DIDs", len(subscriber_dids))

    if not _confirm_large_backlog(args, now, last_engagement_ts, last_feed_ts):
        conn.close()
        return 0

    try:
        engagement_window_start = _compute_engagement_window(
            since_arg=args.since,
            days_arg=args.days,
            now=now,
            last_engagement_ts=last_engagement_ts,
        )
    except ValueError as exc:
        logger.error("Invalid engagement window: %s", exc)
        conn.close()
        return 2
    logger.info("Engagement window start: %s", engagement_window_start.isoformat())

    try:
        positions_since_dt = _compute_position_since(
            engagement_window_start,
            now,
            repair_window_days,
            position_since_arg=args.position_since,
            position_days_arg=args.position_days,
        )
    except ValueError as exc:
        logger.error("Invalid position window: %s", exc)
        conn.close()
        return 2

    feed_min_date = None
    feed_min_dt = None
    if not args.skip_feed:
        try:
            feed_min_dt = _compute_feed_min_dt(
                feed_days_arg=args.feed_days,
                feed_since_arg=args.feed_since,
                now=now,
                last_feed_ts=last_feed_ts,
                repair_window_days=repair_window_days,
            )
        except ValueError as exc:
            logger.error("Invalid feed window: %s", exc)
            conn.close()
            return 2
        feed_min_date = format_min_date(feed_min_dt) if feed_min_dt else None

    options = EngagementOptions(
        include_likes=not args.skip_likes,
        include_reposts=not args.skip_reposts,
        include_comments=not args.skip_comments,
        include_quotes=not args.skip_quotes,
    )

    client = BlueskyClient(timeout=args.timeout, max_retries=args.max_retries)

    backfilled = backfill_missing_engagement_texts(conn, client)
    if backfilled:
        logger.info("Backfilled engagement text for %d existing rows", backfilled)

    total_inserts = 0
    bot_iter = progress_iter(BOT_HANDLES, total=len(BOT_HANDLES), desc="Bots")
    for handle in bot_iter:
        logger.info("Processing bot %s", handle)
        try:
            did = client.resolve_handle(handle)
        except HttpError as exc:
            logger.error("Failed to resolve %s: %s", handle, exc)
            continue
        pds_host = client.resolve_pds_endpoint(did)
        if pds_host:
            logger.info("Resolved PDS host for %s (%s): %s", handle, did, pds_host)
        else:
            logger.warning("Could not resolve PDS host for %s (%s)", handle, did)

        posts = client.get_author_posts(did, engagement_window_start)
        logger.info("Found %d posts for %s", len(posts), handle)
        if not posts:
            continue
        post_iter = progress_iter(posts, total=len(posts), desc=f"Posts for {handle}")
        for post in post_iter:
            post_uri = post.get("uri")
            logger.debug("Collecting engagements for %s", post_uri)
            try:
                records = collect_engagements_for_post(
                    client, subscriber_dids, engagement_window_start, post, options
                )
            except HttpError as exc:
                logger.error("Failed to collect engagements for %s: %s", post_uri, exc)
                continue
            inserted = store_engagements(conn, records)
            total_inserts += inserted
            if inserted:
                logger.debug("Inserted %d records for %s", inserted, post_uri)
        if hasattr(post_iter, "close"):
            post_iter.close()
    if hasattr(bot_iter, "close"):
        bot_iter.close()
    logger.info("Finished engagement collection; inserted %d rows", total_inserts)
    print(f"Inserted {total_inserts} engagement rows into {db_path}")

    feed_inserts = 0
    repair_stats = None
    if args.skip_feed:
        logger.info("Skipping feed retrieval compliance collection as requested")
    else:
        try:
            retrievals = fetch_feed_retrievals(
                env_values,
                user_did=args.feed_did,
                min_date=feed_min_date,
                timeout=args.timeout,
                max_retries=args.max_retries,
                backoff=client.backoff,
            )
        except Exception:
            logger.exception("Failed to fetch feed retrieval compliance events")
        else:
            retrieval_list = list(retrievals)
            if retrieval_list:
                logger.info("Fetched %d feed retrieval events", len(retrieval_list))
                feed_iter = progress_iter(retrieval_list, total=len(retrieval_list), desc="Feed retrievals")
                feed_inserts = store_feed_retrievals(conn, feed_iter)
                if hasattr(feed_iter, "close"):
                    feed_iter.close()
            else:
                logger.info("No feed retrieval events returned for the specified window")
        logger.info("Stored %d feed retrieval events", feed_inserts)

        if repair_window_days > 0:
            repair_since = now - dt.timedelta(days=repair_window_days)
            repair_stats = repair_empty_feed_requests(
                conn,
                env_values,
                since=repair_since,
                timeout=args.timeout,
                max_retries=args.max_retries,
                backoff=client.backoff,
            )
            logger.info(
                "Repair attempted for %d DIDs; repaired %d requests, %d remain empty, %d errors",
                repair_stats.did_attempts,
                repair_stats.repaired_requests,
                repair_stats.still_empty,
                repair_stats.errors,
            )
            repair_line = (
                f"Feed repair: attempted {repair_stats.did_attempts} DIDs; "
                f"repaired {repair_stats.repaired_requests}, remaining empty {repair_stats.still_empty}, "
                f"errors {repair_stats.errors}"
            )
            print(repair_line)
            log_db_update("feed_repair", repair_stats.to_dict())

    position_stats = match_post_positions(conn, since=positions_since_dt)
    stats_dict = position_stats.to_dict()
    matched_positions = int(stats_dict.get("matched", 0))
    processed_positions = int(stats_dict.get("processed", 0))
    unmatched_positions = processed_positions - matched_positions
    avg_age_seconds = stats_dict.get("avg_age_seconds")

    if processed_positions:
        summary_line = (
            f"Post position assignment: matched {matched_positions} of {processed_positions} "
            f"subscriber engagements ({unmatched_positions} unmatched)"
        )
        print(summary_line)
        logger.info(summary_line)
        if unmatched_positions:
            reason_lines = [
                f"{POSITION_STATUS_LABELS.get(code, code)}={int(stats_dict.get(f'status_{code}', 0))}"
                for code in POSITION_STATUS_LABELS
                if code != POSITION_STATUS_MATCHED and stats_dict.get(f"status_{code}", 0)
            ]
            if reason_lines:
                reasons_message = "Unmatched reasons: " + ", ".join(reason_lines)
                print(reasons_message)
                logger.info(reasons_message)
        if avg_age_seconds is not None and stats_dict.get(f"status_{POSITION_STATUS_MATCHED}", 0):
            delay_message = f"Average delay between feed retrieval and engagement: {avg_age_seconds:.1f}s"
            print(delay_message)
            logger.info(delay_message)
    else:
        message = "Post position assignment: no subscriber engagements required matching"
        print(message)
        logger.info(message)

    log_db_update(
        "engagement_positions",
        {
            "processed": processed_positions,
            "matched": matched_positions,
            "unmatched": unmatched_positions,
            "stats": stats_dict,
            "since_timestamp": positions_since_dt.isoformat() if positions_since_dt else None,
        },
    )

    conn.close()
    return 0


def _log_latest(label: str, ts_value: Optional[dt.datetime], now: dt.datetime) -> None:
    if ts_value is None:
        logger.info("Latest %s entry: none recorded", label)
        return
    delta_days = (now - ts_value).total_seconds() / 86400
    logger.info(
        "Latest %s entry: %s (%.2f days ago)",
        label,
        ts_value.isoformat(),
        delta_days,
    )


def _confirm_large_backlog(args, now, last_engagement_ts, last_feed_ts) -> bool:
    catchup_targets = []
    if args.since is None and args.days is None:
        backlog = None if last_engagement_ts is None else (now - last_engagement_ts).total_seconds() / 86400
        catchup_targets.append(("engagements", backlog, last_engagement_ts))
    if not args.skip_feed and args.feed_since is None and args.feed_days is None:
        backlog = None if last_feed_ts is None else (now - last_feed_ts).total_seconds() / 86400
        catchup_targets.append(("feed retrievals", backlog, last_feed_ts))

    if not catchup_targets or not any(backlog is None or backlog > 4 for _, backlog, _ in catchup_targets):
        return True

    detail_parts = []
    for label, backlog, ts_value in catchup_targets:
        if backlog is None:
            detail_parts.append(f"{label}: no previous entries (full history)")
        elif ts_value is None:
            detail_parts.append(f"{label}: ~{backlog:.1f} days awaiting collection")
        else:
            detail_parts.append(f"{label}: ~{backlog:.1f} days since {ts_value.isoformat()}")
    prompt_message = "Detected large backlog - " + "; ".join(detail_parts) + ". Proceed? [y/n]: "
    while True:
        try:
            user_input = input(prompt_message)
        except EOFError:
            user_input = "n"
        decision = user_input.strip().lower()
        if decision in {"y", "yes"}:
            return True
        if decision in {"n", "no", ""}:
            logger.info("Aborting run at user request")
            return False
        print("Please answer with 'y' or 'n'.")


def _compute_engagement_window(
    *,
    since_arg: Optional[str],
    days_arg: Optional[float],
    now: dt.datetime,
    last_engagement_ts: Optional[dt.datetime],
) -> dt.datetime:
    if since_arg is not None:
        return normalize_since(since_arg, now=now)
    if days_arg is not None:
        if days_arg < 0:
            raise ValueError("--days must not be negative")
        return now - dt.timedelta(days=days_arg)
    if last_engagement_ts is not None:
        return last_engagement_ts - dt.timedelta(seconds=1)
    return dt.datetime.fromtimestamp(0, dt.timezone.utc)


def _compute_feed_min_dt(
    *,
    feed_days_arg: Optional[float],
    feed_since_arg: Optional[str],
    now: dt.datetime,
    last_feed_ts: Optional[dt.datetime],
    repair_window_days: float,
) -> Optional[dt.datetime]:
    feed_min_dt: Optional[dt.datetime]
    if feed_since_arg is not None:
        feed_min_dt = normalize_since(feed_since_arg, now=now)
        logger.info(
            "Collecting feed retrievals since %s (explicit window)",
            format_min_date(feed_min_dt),
        )
    elif feed_days_arg is not None:
        feed_min_dt = now - dt.timedelta(days=feed_days_arg)
        logger.info(
            "Collecting feed retrievals for last %.2f days (min_date %s)",
            feed_days_arg,
            format_min_date(feed_min_dt),
        )
    elif last_feed_ts is not None:
        feed_min_dt = last_feed_ts - dt.timedelta(seconds=1)
        logger.info("Collecting feed retrievals since %s", format_min_date(feed_min_dt))
    else:
        feed_min_dt = None
        logger.info("Collecting feed retrievals from the beginning (no previous records)")

    if repair_window_days > 0:
        repair_dt = now - dt.timedelta(days=repair_window_days)
        if feed_min_dt is None or repair_dt < feed_min_dt:
            feed_min_dt = repair_dt
            logger.info(
                "Extending feed retrieval lookup by %.2f days to repair empty payloads (min_date %s)",
                repair_window_days,
                format_min_date(feed_min_dt),
            )
    return feed_min_dt


def _compute_position_since(
    engagement_window_start: dt.datetime,
    now: dt.datetime,
    repair_window_days: float,
    *,
    position_since_arg: Optional[str],
    position_days_arg: Optional[float],
) -> dt.datetime:
    since_candidates = [engagement_window_start]
    if repair_window_days > 0:
        since_candidates.append(now - dt.timedelta(days=repair_window_days))
    if position_days_arg is not None:
        since_candidates.append(now - dt.timedelta(days=position_days_arg))
    if position_since_arg:
        since_candidates.append(normalize_since(position_since_arg, now=now))
    return min(since_candidates)
