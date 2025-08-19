# app/fillout_sync.py
"""
Sync subscribers from Fillout and process webhooks.

Enhancements in this version:
- Webhook payload unwrapping:
    Accepts any of these shapes and extracts the *submission object*:

    A) Plain submission object:
       { "submissionId": "...", "questions": [ ... ], ... }

    B) Newer wrapper (common):
       { "formId": "...", "formName": "...", "submission": { ...submission... } }

    C) Rare wrapper variant:
       { "formId": "...", "formName": "...", "response": { ...submission... } }

    D) Array-like (some custom relays):
       { "responses": [ { ...submission... } ] }

- Pagination + incremental sync (afterDate) for polling route
- US/EU base fallback for polling route
- Robust email extraction heuristics

Docs:
- Get all submissions: /forms/{formId}/submissions (returns the 'responses' list; each entry is a submission object with questions[]). 
- Webhooks: "receive submissions in the same format as entries in 'responses'"; Fillout also noted adding formId/formName to webhook payloads recently.
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


# ---------------------------
# Helpers to extract answers
# ---------------------------

def _extract_email_from_submission(sub: dict) -> tuple[str | None, dict]:
    """
    Find an email value inside a single *submission object*.
    The submission object must contain 'questions': [{ id, name, type, value }, ...]
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

    # sometimes present at top-level (depends on SSO/login forms)
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


# ---------------------------
# Region handling for polling
# ---------------------------

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


# ---------------------------
# Polling (GET /submissions)
# ---------------------------

def _fetch_page(base: str, *, form_id: str, limit: int, offset: int, after_iso: Optional[str]) -> Tuple[Dict[str, Any] | None, str | None]:
    headers = {
        "Authorization": f"Bearer {settings.fillout_api_key}",
        "Accept": "application/json",
    }
    params: Dict[str, Any] = {"limit": str(limit), "offset": str(offset)}
    if after_iso:
        # Fillout requires ISO 8601 like 2024-05-16T23:20:05.324Z
        params["afterDate"] = after_iso
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
    """
    Given a *submission object*, extract an email and upsert the subscriber.
    """
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
    Incremental + paginated sync when we poll:
    reads last sync iso from app_state['fillout_last_sync_iso'].
    """
    last_iso = get_state("fillout_last_sync_iso")
    if last_iso:
        logger.info("Fillout incremental sync after {}", last_iso)
    else:
        logger.info("Fillout initial sync (no last sync marker)")

    bases = _candidate_bases()
    form_id = settings.fillout_form_id
    page_limit = 150  # API docs: 1..150

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
            # Successful base → set 'now' as the new marker
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


# ---------------------------
# Webhook processing
# ---------------------------

def _unwrap_webhook_payload(payload: dict) -> dict | None:
    """
    Try to extract the *submission object* from various webhook shapes.
    Returns the submission dict or None if not found.
    """
    if not isinstance(payload, dict):
        return None

    # Case A: already a submission object (has 'questions' and 'submissionId')
    if "questions" in payload and ("submissionId" in payload or "submissionTime" in payload):
        return payload

    # Case B: common wrapper { formId, formName, submission: { ... } }
    sub = payload.get("submission")
    if isinstance(sub, dict) and "questions" in sub:
        return sub

    # Case C: rare wrapper { ..., response: { ... } }
    resp = payload.get("response")
    if isinstance(resp, dict) and "questions" in resp:
        return resp

    # Case D: { responses: [ { ... } ] }
    resps = payload.get("responses")
    if isinstance(resps, list) and resps and isinstance(resps[0], dict) and "questions" in resps[0]:
        return resps[0]

    # If nothing matched, return None for clear logging upstream
    return None


def process_webhook_payload(payload: dict) -> bool:
    """
    Accept a Fillout webhook JSON payload and upsert one subscriber if possible.
    Returns True iff an email was found and upserted.
    """
    try:
        sub = _unwrap_webhook_payload(payload)
        if not sub:
            logger.warning("Webhook payload did not contain a recognizable submission object. Keys seen: {}", list(payload.keys()))
            return False
        return _upsert_from_submission(sub)
    except Exception as e:
        logger.exception("Webhook processing failed: {}", e)
        return False
