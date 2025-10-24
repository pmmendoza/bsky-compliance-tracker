#!/usr/bin/env python3
"""Rebuild feed_request_posts.post_index values from stored payload positions."""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from compliance_tracker.constants import DEFAULT_DB_PATH
from compliance_tracker.database import rebuild_post_indices_from_payload, setup_database


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help="Path to the compliance SQLite database",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    conn = sqlite3.connect(args.db)
    try:
        setup_database(conn)
        stats = rebuild_post_indices_from_payload(conn)
    finally:
        conn.close()

    print(
        "Rebuild complete: scanned {stats.scanned}, updated {stats.updated}, "
        "missing position {stats.missing_position}, invalid position {stats.invalid_position}, "
        "parse errors {stats.parse_errors}".format(stats=stats)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
