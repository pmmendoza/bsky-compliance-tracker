#!/usr/bin/env python3
"""One-off helper to ensure the compliance database schema is up to date."""

from __future__ import annotations

import argparse
import datetime as dt
import shutil
import sqlite3
from pathlib import Path

from compliance_tracker.constants import DEFAULT_DB_PATH
from compliance_tracker.database import setup_database


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH, help="Path to compliance.db")
    parser.add_argument(
        "--skip-backup",
        action="store_true",
        help="Do not create compliance-backup_YYYYMMDD.db before migrating",
    )
    return parser.parse_args()


def backup_database(db_path: Path) -> None:
    if not db_path.exists():
        return
    timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d")
    backup_path = db_path.with_name(f"compliance-backup_{timestamp}.db")
    if backup_path.exists():
        return
    shutil.copy2(db_path, backup_path)
    print(f"Created backup at {backup_path}")


def main() -> int:
    args = parse_args()
    db_path = args.db
    if not args.skip_backup:
        backup_database(db_path)
    conn = sqlite3.connect(db_path)
    setup_database(conn)
    conn.close()
    print(f"Schema finalized for {db_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
