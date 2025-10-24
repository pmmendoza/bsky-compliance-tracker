#!/usr/bin/env python3
"""Hydrate post metadata for existing feed retrievals."""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import sqlite3
from pathlib import Path

from compliance_tracker.client import BlueskyClient
from compliance_tracker.database import seed_posts_from_feed, setup_database
from compliance_tracker.hydration import hydrate_posts


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=Path("compliance.db"), help="Path to compliance.db")
    parser.add_argument("--limit", type=int, default=None, help="Optional cap on number of URIs to hydrate")
    parser.add_argument("--batch-size", type=int, default=25, help="URIs per hydration request (max 25)")
    parser.add_argument("--pause", type=float, default=0.2, help="Seconds to sleep between hydration requests")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))

    conn = sqlite3.connect(args.db)
    setup_database(conn)

    seeded = seed_posts_from_feed(conn)
    if seeded:
        logging.info("Seeded %d posts from existing feed_request_posts", seeded)

    client = BlueskyClient(timeout=30.0, max_retries=5)
    stats = hydrate_posts(
        conn,
        client,
        batch_size=args.batch_size,
        pause_seconds=args.pause,
        limit=args.limit,
    )

    logging.info(
        "Hydration complete: attempted=%d hydrated=%d not_found=%d errors=%d",
        stats.get("attempted", 0),
        stats.get("hydrated", 0),
        stats.get("not_found", 0),
        stats.get("errors", 0),
    )

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
