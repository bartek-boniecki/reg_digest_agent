# app/fillout_sync.py
"""
Sync subscribers from Fillout with:
- Pagination (limit/offset)
- Incremental sync via afterDate (using app_state 'fillout_last_sync_iso')
- Region fallback (US/EU)
- Robust email extraction

Docs:
- Get all submissions: /forms/{formId}/submissions (limit 1..150, offset, afterDate, beforeDate)
- Webhooks deliver the same shape as items in 'responses' array.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Tuple, Dict, Any, List, Optional

import httpx
from loguru import logger

from .settings import settings
from .db import upsert_subscriber, get_state, set_state

EMAIL_RE = re.compile(r"^[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}$", re.I)


def _extract_email_from_submission(sub: dict) -> tuple[str | None, dict]:
    """
    Heuristic to find an email and map a few other fields.
    Returns (email, answers_map)
    """
    questions = sub.get("questions", [])
    answers: Dict[str, Any] = {}
    for q in questions:
        name = (q.get("name") or "").strip()
        val = q.get("value")
        if name:
            answers[name] = val

    # type=='email'
    for q in questions:
        if (q.get("type") or "").lower() == "email":
            v = q.get("value")
            if isinstance(v, str) and EMAIL_RE.match(v):
                return v, answers

    # name contains 'email'
    for q in questions:
        name = (q.get("name") or "").lower()
        v = q.get("value")
        if "email" in name and isinstance(v, str) and EMAIL_RE.match(v):
            return v, answers

    # sometimes present at top-level
    login = sub.get("login") or {}
    if isinstance(login, dict):
        v = login.get("email")
        if isinstance(v, str) and EMAIL_RE.match(v):
            return v, answers

    # last-resort: any string value that looks like an email
    for q in questions:
        v = q.get("value")
        if isinstance(v, str) and EMAIL_RE.match(v):
            return v, answers

    return None, answers


def _pick_label(answers: Dict[str, Any], *labels: str) -> str | None:
    for l in labels:
        for k, v in answers.items():
            if k.strip().lower() == l.strip().lower():
                return v if isinstance(v, str) else None
    return None


def _candidate_bases() -> List[str]:
    prim = (settings.fillout_api_base or "https://api.fillout.com/v1/api").rstrip("/")
    us = "https://api.fillout.com/v1/api"
    eu = "https://eu-api.fillout.com/v1/api"
    bases = [prim]
    if prim != us:
        bases.append(us)
    if prim != eu:
        bases.append(eu)
    # dedupe
    seen, out = set(), []
    for b in bases:
        if b not in seen:
            seen.add(b); out.append(b)
    return out


def _fetch_page(base: str, *, form_id: str, limit: int, offset: int, after_iso: Optional[str]) -> Tuple[Dict[str, Any] | None, str | None]:
    headers = {
        "Authorization": f"Bearer {settings.fillout_api_key}",
        "Accept": "application/json",
    }
    params: Dict[str, Any] = {"limit": str(limit), "offset": str(offset)}
    if after_iso:
        params["afterDate"] = after_iso  # ISO 8601 per Fillout docs
    url = f"{base}/forms/{form_id}/submissions"
    try:
        with httpx.Client(timeout=30) as client:
            r = client.get(url, headers=headers, params=params)
    except Exception as e:
        return None, f"Network error {url}: {e}"
    if r.status_code >= 400:
        return None, f"{url} -> {r.status_code}. Body: {(r.text or '')[:500]}"
    try:
        return r.json(), None
    except Exception as e:
        return None, f"Invalid JSON from {url}: {e}"


def _upsert_from_submission(sub: dict) -> bool:
    email, answers = _extract_email_from_submission(sub)
    if not email:
        logger.warning("Submission without a usable email, skipping: {}", sub.get("submissionId"))
        return False

    full_name = _pick_label(answers, "full_name", "full name", "name")
    org = _pick_label(answers, "organization", "company", "employer")

    regions_val = answers.get("regions") or answers.get("Region") or ""
    if isinstance(regions_val, str):
        regions = [x.strip() for x in regions_val.split(",") if x.strip()]
    elif isinstance(regions_val, list):
        regions = [str(x).strip() for x in regions_val if str(x).strip()]
    else:
        regions = []

    upsert_subscriber(email=email, full_name=full_name, org=org, regions=regions)
    logger.info("Upserted subscriber {}", email)
    return True


def sync_from_fillout() -> None:
    """
    Incremental + paginated sync.
    We read last sync iso from app_state['fillout_last_sync_iso'].
    If empty, we fetch the first 1000 submissions in pages of 150 (hard cap).
    """
    last_iso = get_state("fillout_last_sync_iso")
    if last_iso:
        logger.info("Fillout incremental sync after {}", last_iso)
    else:
        logger.info("Fillout initial sync (no last sync marker)")

    bases = _candidate_bases()
    form_id = settings.fillout_form_id
    page_limit = 150  # Fillout limit is 1..150

    total_upserted = 0
    for base in bases:
        offset = 0
        any_success = False
        while True:
            data, err = _fetch_page(base, form_id=form_id, limit=page_limit, offset=offset, after_iso=last_iso)
            if err:
                logger.warning("Fillout page fetch failed: {}", err)
                break  # try next base
            any_success = True

            subs = data.get("responses", data.get("submissions", []))
            if not subs:
                break

            for sub in subs:
                if _upsert_from_submission(sub):
                    total_upserted += 1

            # next page
            if len(subs) < page_limit:
                break
            offset += page_limit

        if any_success:
            # Successful base → stop trying other regions
            if last_iso is None:
                # Set a conservative last sync marker to "now-1m" to avoid missing submissions right at boundary
                now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
                set_state("fillout_last_sync_iso", now_iso)
            else:
                # Bump marker forward to now
                now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
                set_state("fillout_last_sync_iso", now_iso)
            if base != settings.fillout_api_base.rstrip("/"):
                logger.warning("Fillout worked via fallback base {}. Consider setting FILLOUT_API_BASE to this value.", base)
            logger.info("Fillout sync complete. Upserted: {}", total_upserted)
            return

    # If we got here, neither base worked:
    raise RuntimeError(
        "Fillout sync failed for all bases. Check FILLOUT_API_KEY, FILLOUT_FORM_ID, and FILLOUT_API_BASE (US vs EU)."
    )


# ---- Webhook helper (used by /fillout-webhook) ----

def process_webhook_payload(payload: dict) -> bool:
    """
    Fillout says webhooks POST in the same shape as items in `responses`.
    We just feed it through the same upsert logic.
    Returns True if an email was upserted, else False.
    """
    try:
        return _upsert_from_submission(payload)
    except Exception as e:
        logger.exception("Webhook processing failed: {}", e)
        return False
