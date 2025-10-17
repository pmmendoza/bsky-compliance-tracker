"""HTTP client for Bluesky APIs."""

from __future__ import annotations

import json
import logging
import random
import time
from typing import Dict, Optional

import requests

from .constants import APPVIEW_BASE, USER_AGENT
from .utils import parse_datetime

logger = logging.getLogger(__name__)


class HttpError(RuntimeError):
    """Raised when the Bluesky API returns an unexpected response."""


class BlueskyClient:
    def __init__(
        self,
        session: Optional[requests.Session] = None,
        *,
        user_agent: str = USER_AGENT,
        timeout: float = 30.0,
        max_retries: int = 5,
        backoff: float = 1.5,
    ) -> None:
        self.session = session or requests.Session()
        self.session.headers.setdefault("User-Agent", user_agent)
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff = backoff
        self._pds_cache: Dict[str, Optional[str]] = {}

    def _get_json(self, path: str, params: Optional[Dict[str, str]] = None) -> Dict:
        url = APPVIEW_BASE + path
        last_error: Optional[Exception] = None
        for attempt in range(self.max_retries):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
            except requests.exceptions.RequestException as exc:  # type: ignore[attr-defined]
                last_error = exc
                logger.warning(
                    "Request error for %s (attempt %d/%d): %s", url, attempt + 1, self.max_retries, exc
                )
                if attempt + 1 == self.max_retries:
                    break
                time.sleep(self._compute_backoff(attempt))
                continue
            if response.status_code in (429, 502, 503, 504):
                logger.warning(
                    "Received %s from %s (attempt %d/%d)",
                    response.status_code,
                    url,
                    attempt + 1,
                    self.max_retries,
                )
                last_error = HttpError(f"{response.status_code} from {url}")
                if attempt + 1 == self.max_retries:
                    break
                time.sleep(self._compute_backoff(attempt))
                continue
            if not response.ok:
                raise HttpError(f"GET {url} failed with status {response.status_code}: {response.text[:200]}")
            try:
                return response.json()
            except json.JSONDecodeError as exc:
                raise HttpError(f"Failed to decode JSON from {url}: {exc}") from exc
        raise HttpError(f"GET {url} failed after retries") from last_error

    def _compute_backoff(self, attempt: int) -> float:
        return max(0.5, (self.backoff ** (attempt + 1)) + random.uniform(0, 0.5))

    def resolve_handle(self, handle: str) -> str:
        payload = self._get_json("/xrpc/com.atproto.identity.resolveHandle", {"handle": handle})
        did = payload.get("did")
        if not did:
            raise HttpError(f"No DID for handle {handle}")
        return did

    def resolve_pds_endpoint(self, did: str) -> Optional[str]:
        if did in self._pds_cache:
            return self._pds_cache[did]
        endpoint = self._lookup_pds_endpoint(did)
        self._pds_cache[did] = endpoint
        return endpoint

    def _lookup_pds_endpoint(self, did: str) -> Optional[str]:
        def extract_endpoint(services) -> Optional[str]:
            if not isinstance(services, list):
                return None
            for svc in services:
                if not isinstance(svc, dict):
                    continue
                svc_type = str(svc.get("type") or "")
                svc_id = str(svc.get("id") or "")
                if svc_type in {"#atproto_pds", "AtprotoPersonalDataServer"} or svc_id.lower().endswith("#atproto_pds"):
                    endpoint = svc.get("serviceEndpoint") or svc.get("endpoint")
                    if isinstance(endpoint, str) and endpoint:
                        return endpoint.rstrip("/")
            return None

        # Attempt resolveDid via AppView
        try:
            did_doc_appview = self._get_json("/xrpc/com.atproto.identity.resolveDid", {"did": did})
        except HttpError:
            did_doc_appview = None
        if did_doc_appview:
            services = (
                did_doc_appview.get("didDocument", {}).get("service")
                or did_doc_appview.get("didDoc", {}).get("service")
                or []
            )
            endpoint = extract_endpoint(services)
            if endpoint:
                return endpoint

        # Fallback to PLC directory
        plc_url = f"https://plc.directory/{did}"
        try:
            response = self.session.get(plc_url, headers={"Accept": "application/json"}, timeout=self.timeout)
            if response.ok:
                try:
                    document = response.json()
                except json.JSONDecodeError:
                    document = {}
                services = document.get("service") or []
                endpoint = extract_endpoint(services)
                if endpoint:
                    return endpoint
        except requests.exceptions.RequestException as exc:
            logger.debug("PLC directory lookup failed for %s: %s", did, exc)

        # Final attempt via resolveIdentity
        try:
            identity_doc = self._get_json("/xrpc/com.atproto.identity.resolveIdentity", {"identity": did})
        except HttpError:
            identity_doc = None
        if identity_doc:
            services = (
                identity_doc.get("didDocument", {}).get("service")
                or identity_doc.get("didDoc", {}).get("service")
                or []
            )
            endpoint = extract_endpoint(services)
            if endpoint:
                return endpoint

        return None

    def get_author_posts(self, actor: str, window_start):
        posts = []
        cursor: Optional[str] = None
        reached_window_end = False
        while True:
            params = {"actor": actor, "limit": "100"}
            if cursor:
                params["cursor"] = cursor
            data = self._get_json("/xrpc/app.bsky.feed.getAuthorFeed", params)
            for item in data.get("feed", []):
                post = item.get("post")
                if not post:
                    continue
                record = post.get("record", {})
                if record.get("$type") != "app.bsky.feed.post":
                    continue
                created_at = record.get("createdAt")
                if created_at is None:
                    continue
                ts = parse_datetime(created_at)
                if ts is None:
                    continue
                if ts < window_start:
                    reached_window_end = True
                    break
                posts.append(post)
            if reached_window_end:
                break
            cursor = data.get("cursor")
            if not cursor or not data.get("feed"):
                break
        return posts

    def iter_likes(self, uri: str):
        cursor: Optional[str] = None
        while True:
            params = {"uri": uri, "limit": "100"}
            if cursor:
                params["cursor"] = cursor
            data = self._get_json("/xrpc/app.bsky.feed.getLikes", params)
            for like in data.get("likes", []):
                yield like
            cursor = data.get("cursor")
            if not cursor:
                break

    def iter_reposts(self, uri: str):
        cursor: Optional[str] = None
        while True:
            params = {"uri": uri, "limit": "100"}
            if cursor:
                params["cursor"] = cursor
            data = self._get_json("/xrpc/app.bsky.feed.getRepostedBy", params)
            for repost in data.get("repostedBy", []):
                yield repost
            cursor = data.get("cursor")
            if not cursor:
                break

    def iter_quotes(self, uri: str):
        cursor: Optional[str] = None
        while True:
            params = {"uri": uri, "limit": "100"}
            if cursor:
                params["cursor"] = cursor
            data = self._get_json("/xrpc/app.bsky.feed.getQuotes", params)
            quotes = data.get("quotes")
            if quotes is None:
                quotes = data.get("posts") or []
            for quote in quotes:
                post = quote.get("post") if isinstance(quote, dict) else None
                if post is None and isinstance(quote, dict):
                    post = quote
                if post:
                    yield post
            cursor = data.get("cursor")
            if not cursor:
                break

    def get_post_thread(self, uri: str, depth: int = 15):
        params = {"uri": uri, "depth": str(depth), "parentHeight": "0"}
        return self._get_json("/xrpc/app.bsky.feed.getPostThread", params)
