#!/usr/bin/env python3
"""Display follower-count history for subscribers."""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Optional


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=Path("compliance.db"), help="Path to compliance.db")
    parser.add_argument("--did", type=str, default=None, help="Optional subscriber DID to filter")
    parser.add_argument("--limit", type=int, default=20, help="Limit number of rows displayed")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    if args.did:
        query = (
            "SELECT snapshot_ts, following_count FROM subscriber_follow_counts "
            "WHERE did = ? ORDER BY snapshot_ts DESC LIMIT ?"
        )
        rows = conn.execute(query, (args.did, args.limit)).fetchall()
    else:
        query = (
            "SELECT did, snapshot_ts, following_count FROM subscriber_follow_counts "
            "WHERE (did, snapshot_ts) IN ("
            "  SELECT did, MAX(snapshot_ts) FROM subscriber_follow_counts GROUP BY did"
            ") ORDER BY snapshot_ts DESC LIMIT ?"
        )
        rows = conn.execute(query, (args.limit,)).fetchall()

    if not rows:
        print("No follower data found.")
        return 0

    if args.did:
        print(f"Follower history for {args.did} (most recent first):")
        for row in rows:
            print(f"  {row['snapshot_ts']}: {row['following_count']}")
    else:
        print("Latest follower counts (most recent snapshots):")
        for row in rows:
            print(f"  {row['snapshot_ts']} — {row['did']}: {row['following_count']}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
