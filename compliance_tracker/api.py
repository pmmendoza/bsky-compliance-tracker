"""Interactions with feed generator endpoints."""

from __future__ import annotations

import json
import logging
import time
from typing import Dict, Iterable, List, Optional, Tuple

import requests

from .constants import (
    APPVIEW_BASE,
    FEED_COMPLIANCE_ENDPOINT,
    SUBSCRIBER_HOST_ENV,
    SUBSCRIBER_KEY_ENV,
    SUBSCRIBERS_ENDPOINT,
    USER_AGENT,
)
from .utils import format_min_date

logger = logging.getLogger(__name__)


def build_feedgen_endpoint(host_value: str, path: str) -> str:
    if host_value.startswith("http://") or host_value.startswith("https://"):
        base = host_value.rstrip("/")
    else:
        base = f"https://{host_value}:443"
    if not path.startswith("/"):
        path = "/" + path
    return base + path


def fetch_subscribers(env: Dict[str, str]) -> Tuple[set, Dict[str, str]]:
    host = env.get(SUBSCRIBER_HOST_ENV) or _getenv(SUBSCRIBER_HOST_ENV)
    api_key = env.get(SUBSCRIBER_KEY_ENV) or _getenv(SUBSCRIBER_KEY_ENV)
    if not host or not api_key:
        raise RuntimeError("Missing FEEDGEN_LISTENHOST or PRIORITIZE_API_KEY in environment")
    endpoint = build_feedgen_endpoint(host, SUBSCRIBERS_ENDPOINT)
    logger.debug("Fetching subscribers from %s", endpoint)
    session = requests.Session()
    session.headers.setdefault("User-Agent", USER_AGENT)
    response = session.get(endpoint, headers={"api-key": api_key}, timeout=30)
    if not response.ok:
        raise RuntimeError(f"Failed to fetch subscribers: {response.status_code} {response.text[:200]}")
    try:
        data = response.json()
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid subscriber JSON: {exc}") from exc
    subscribers = data.get("subscribers") or []
    dids: set = set()
    handles: Dict[str, str] = {}
    for entry in subscribers:
        did = entry.get("did")
        handle = entry.get("handle")
        if did:
            dids.add(did)
            if handle:
                handles[did] = handle
    return dids, handles


def fetch_follow_counts(
    dids: Iterable[str],
    *,
    timeout: float = 10.0,
    pause_seconds: float = 0.2,
) -> Tuple[Dict[str, int], Dict[str, str]]:
    """Fetch the current followsCount for each DID via the public AppView API."""

    session = requests.Session()
    session.headers.setdefault("User-Agent", USER_AGENT)
    endpoint = f"{APPVIEW_BASE}/xrpc/app.bsky.actor.getProfile"
    counts: Dict[str, int] = {}
    errors: Dict[str, str] = {}
    did_list = list(dids)
    for idx, did in enumerate(did_list):
        try:
            response = session.get(endpoint, params={"actor": did}, timeout=timeout)
            response.raise_for_status()
            payload = response.json()
        except requests.exceptions.RequestException as exc:  # type: ignore[attr-defined]
            errors[did] = str(exc)
            payload = None
        except json.JSONDecodeError as exc:
            errors[did] = f"invalid JSON: {exc}"
            payload = None
        if payload:
            follow_count = payload.get("followsCount")
            if isinstance(follow_count, int):
                counts[did] = follow_count
            else:
                errors[did] = "missing followsCount"
        if pause_seconds and idx + 1 < len(did_list):
            time.sleep(pause_seconds)
    return counts, errors


def fetch_feed_retrievals(
    env: Dict[str, str],
    *,
    user_did: Optional[str],
    min_date: Optional[str],
    timeout: float,
    max_retries: int,
    backoff: float,
) -> List[Dict]:
    host = env.get(SUBSCRIBER_HOST_ENV) or _getenv(SUBSCRIBER_HOST_ENV)
    api_key = env.get(SUBSCRIBER_KEY_ENV) or _getenv(SUBSCRIBER_KEY_ENV)
    if not host or not api_key:
        raise RuntimeError("Missing FEEDGEN_LISTENHOST or PRIORITIZE_API_KEY in environment")
    endpoint = build_feedgen_endpoint(host, FEED_COMPLIANCE_ENDPOINT)
    session = requests.Session()
    session.headers.setdefault("User-Agent", USER_AGENT)
    params: Dict[str, str] = {}
    if user_did:
        params["user_did"] = user_did
    if min_date:
        params["min_date"] = min_date
    headers = {"api-key": api_key}
    last_error: Optional[Exception] = None
    for attempt in range(max_retries):
        try:
            response = session.get(endpoint, params=params, headers=headers, timeout=timeout)
        except requests.exceptions.RequestException as exc:  # type: ignore[attr-defined]
            last_error = exc
            logger.warning(
                "Feed compliance request error (attempt %d/%d): %s", attempt + 1, max_retries, exc
            )
            if attempt + 1 == max_retries:
                break
            time.sleep(max(0.5, (backoff ** (attempt + 1)) + _rand()))
            continue
        if response.status_code in (429, 502, 503, 504):
            logger.warning(
                "Feed compliance returned %s (attempt %d/%d)", response.status_code, attempt + 1, max_retries
            )
            last_error = RuntimeError(f"{response.status_code} from {endpoint}")
            if attempt + 1 == max_retries:
                break
            time.sleep(max(0.5, (backoff ** (attempt + 1)) + _rand()))
            continue
        if not response.ok:
            raise RuntimeError(f"Failed to fetch compliance data: {response.status_code} {response.text[:200]}")
        try:
            data = response.json()
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Invalid compliance JSON: {exc}") from exc
        compliance = data.get("compliance") or []
        if not isinstance(compliance, list):
            raise RuntimeError("Unexpected compliance payload structure")
        return compliance
    raise RuntimeError("Feed compliance request failed after retries") from last_error


def _getenv(name: str) -> Optional[str]:
    import os

    return os.getenv(name)


def _rand() -> float:
    import random

    return random.uniform(0, 0.5)
