# app/db.py
"""
Direct Supabase REST (PostgREST) adapter using httpx.

WHY: Avoids SDK/httpx incompatibilities and keeps deploys stable.
Adds a tiny key/value store (app_state) for incremental Fillout sync.

Tables expected (create once in Supabase SQL):
  articles(url unique), subscribers(email unique), digests, deliveries
  app_state(key primary key, value text, updated_at timestamptz)

Env vars:
  SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import httpx
from loguru import logger

# Load .env locally if present
try:
    from dotenv import load_dotenv, find_dotenv
    _DOT = find_dotenv(usecwd=True)
    if _DOT:
        load_dotenv(_DOT, override=False)
except Exception:
    pass


_HTTPX: Optional[httpx.Client] = None
_BASE_REST: Optional[str] = None


def _require(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing environment variable: {name}")
    return val


def _client() -> httpx.Client:
    global _HTTPX, _BASE_REST
    if _HTTPX is not None:
        return _HTTPX

    url = _require("SUPABASE_URL").rstrip("/")
    key = (
        os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("SUPABASE_SERVICE_ROLE")
        or os.getenv("SUPABASE_KEY")
    )
    if not key:
        raise RuntimeError(
            "Provide SUPABASE_SERVICE_ROLE_KEY (preferred) or SUPABASE_SERVICE_ROLE. "
            "Anonymous keys usually fail when RLS is enabled."
        )

    _BASE_REST = f"{url}/rest/v1"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Accept": "application/json",
    }
    _HTTPX = httpx.Client(base_url=_BASE_REST, headers=headers, timeout=30.0)
    return _HTTPX


def _iso(dt: datetime | str | None) -> Optional[str]:
    if dt is None:
        return None
    if isinstance(dt, str):
        return dt
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


# ---------------------------
# Articles
# ---------------------------

def upsert_article(item: Dict[str, Any]) -> Dict[str, Any]:
    c = _client()
    payload = {
        "url": item["url"],
        "title": item.get("title") or "",
        "raw_text": item.get("raw_text") or "",
        "published_at": _iso(item.get("published_at")),
        "hash": item.get("hash"),
    }
    r = c.post(
        "/articles",
        params={"on_conflict": "url", "select": "url,title,raw_text,published_at"},
        headers={"Prefer": "resolution=merge-duplicates,return=representation"},
        json=[payload],
    )
    r.raise_for_status()
    data = r.json() or []
    return {"count": len(data), "data": data}


def list_articles_since(since: datetime, limit: int = 500) -> List[Dict[str, Any]]:
    c = _client()
    r = c.get(
        "/articles",
        params={
            "select": "url,title,raw_text,published_at,inserted_at",
            "published_at": f"gte.{_iso(since)}",
            "order": "published_at.desc",
            "limit": str(limit),
        },
    )
    r.raise_for_status()
    return r.json() or []


def list_recent_articles_days(days: int = 7, limit: int = 500) -> List[Dict[str, Any]]:
    since = datetime.now(timezone.utc) - timedelta(days=days)
    return list_articles_since(since, limit=limit)


# ---------------------------
# Subscribers
# ---------------------------

def upsert_subscriber(*, email: str, full_name: Optional[str], org: Optional[str], regions: Optional[List[str]]):
    c = _client()
    payload = {
        "email": (email or "").strip().lower(),
        "full_name": (full_name or "").strip(),
        "org": (org or "").strip(),
        "regions": regions or [],
        "is_active": True,
    }
    r = c.post(
        "/subscribers",
        params={"on_conflict": "email", "select": "id,email"},
        headers={"Prefer": "resolution=merge-duplicates,return=representation"},
        json=[payload],
    )
    r.raise_for_status()
    return r.json()


def fetch_active_subscribers() -> List[Dict[str, Any]]:
    c = _client()
    r = c.get("/subscribers", params={"select": "id,email", "is_active": "eq.true"})
    r.raise_for_status()
    return r.json() or []


# ---------------------------
# Digests & deliveries
# ---------------------------

def insert_digest(period_label: str, html: str) -> Dict[str, Any]:
    c = _client()
    r = c.post(
        "/digests",
        headers={"Prefer": "return=representation"},
        json=[{"period_label": period_label, "html": html}],
    )
    r.raise_for_status()
    data = r.json() or []
    if not data:
        raise RuntimeError("Digest insert failed")
    return data[0]


def insert_delivery(digest_id: str, subscriber_id: str, status: str = "sent") -> Dict[str, Any]:
    c = _client()
    r = c.post(
        "/deliveries",
        headers={"Prefer": "return=representation"},
        json=[{"digest_id": digest_id, "subscriber_id": subscriber_id, "status": status}],
    )
    r.raise_for_status()
    data = r.json() or []
    return data[0] if data else {}


# ---------------------------
# App state (key/value)
# ---------------------------

def get_state(key: str) -> Optional[str]:
    c = _client()
    r = c.get("/app_state", params={"select": "value", "key": f"eq.{key}", "limit": "1"})
    if r.status_code == 404:
        return None
    r.raise_for_status()
    data = r.json() or []
    if not data:
        return None
    return data[0].get("value")


def set_state(key: str, value: str) -> None:
    c = _client()
    r = c.post(
        "/app_state",
        params={"on_conflict": "key"},
        headers={"Prefer": "resolution=merge-duplicates"},
        json=[{"key": key, "value": value}],
    )
    r.raise_for_status()


def health_check() -> bool:
    try:
        c = _client()
        r = c.get("/articles", params={"select": "id", "limit": "1"})
        r.raise_for_status()
        return True
    except Exception as e:
        logger.error(f"DB health check failed: {e}")
        return False
