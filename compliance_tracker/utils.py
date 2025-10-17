"""Utility helpers shared across modules."""

from __future__ import annotations

import datetime as dt
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional, Union

from .constants import UPDATE_LOG_PATH

logger = logging.getLogger(__name__)


def parse_datetime(raw: Optional[str]) -> Optional[dt.datetime]:
    if not raw:
        return None
    try:
        return dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def ensure_utc(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc)


def normalize_since(
    value: Union[str, int, float, dt.datetime, dt.date, dt.timedelta],
    *,
    now: Optional[dt.datetime] = None,
) -> dt.datetime:
    """Normalise a user-supplied window start to a UTC-aware datetime."""

    reference = ensure_utc(now or dt.datetime.now(dt.timezone.utc))

    if isinstance(value, dt.datetime):
        return ensure_utc(value)
    if isinstance(value, dt.date) and not isinstance(value, dt.datetime):
        return dt.datetime.combine(value, dt.time.min, tzinfo=dt.timezone.utc)
    if isinstance(value, dt.timedelta):
        total_seconds = value.total_seconds()
        if total_seconds < 0:
            raise ValueError("Timedelta window must not be negative")
        return reference - value
    if isinstance(value, (int, float)):
        if value < 0:
            raise ValueError("Numeric window must not be negative")
        return reference - dt.timedelta(days=float(value))
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            raise ValueError("Window string must not be empty")
        parsed = parse_datetime(stripped)
        if parsed is not None:
            return ensure_utc(parsed)
        try:
            numeric = float(stripped)
        except ValueError as exc:
            raise ValueError(f"Unsupported window value {value!r}") from exc
        if numeric < 0:
            raise ValueError("Numeric window must not be negative")
        return reference - dt.timedelta(days=numeric)
    raise ValueError(f"Unsupported window value {value!r}")


def format_min_date(value: dt.datetime) -> str:
    value_utc = ensure_utc(value).replace(microsecond=0)
    return value_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def log_db_update(table: str, details: Dict[str, Any]) -> None:
    entry = {
        "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
        "table": table,
        "details": details,
    }
    UPDATE_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with UPDATE_LOG_PATH.open("a", encoding="utf-8") as log_file:
        json.dump(entry, log_file)
        log_file.write("\n")


def load_env_from_file(path: Path) -> Dict[str, str]:
    values: Dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        values[key.strip()] = value.strip().strip('"')
    if values:
        logger.debug("Loaded %d env vars from %s", len(values), path)
    else:
        logger.warning("Env file %s exists but contained no key/value pairs", path)
    return values
