"""
FastAPI service:
- /health           quick liveness check
- /subscribe-test   add yourself without Fillout (GET/POST)
- /run-now          trigger a digest in background (GET/POST)
- /hf-selftest      verify Hugging Face token/endpoint
- /fillout-webhook  receive Fillout submission webhooks → upsert subscriber
                     and (optionally) trigger an immediate digest run with cooldown

Enable auto-run on webhook by setting env:
  TRIGGER_ON_SUBSCRIBE=true
  AUTO_RUN_COOLDOWN_MIN=10   # minutes (default 15 if not set)

We store last auto-run time in Supabase app_state['webhook_last_run_iso'] to debounce.
"""

import os
import re
import asyncio
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Tuple

from fastapi import FastAPI, Query, HTTPException, Request
from pydantic import BaseModel
from loguru import logger

from .pipeline import run_digest_async
from .db import upsert_subscriber, get_state, set_state
from .summarize import hf_selftest
from .fillout_sync import process_webhook_payload

app = FastAPI(title="Regulatory Digest API")

EMAIL_RE = re.compile(r"^[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}$", re.I)


@app.get("/health")
def health():
    return {"ok": True}


class SubscribeBody(BaseModel):
    email: str
    full_name: Optional[str] = None
    org: Optional[str] = None
    regions: Optional[List[str]] = None


def _validate_email(email: str) -> None:
    if not EMAIL_RE.match(email or ""):
        raise HTTPException(status_code=422, detail="Invalid email format")


@app.get("/subscribe-test")
def subscribe_test_get(
    email: str = Query(..., description="Your email"),
    full_name: Optional[str] = Query(None),
    org: Optional[str] = Query(None),
    regions: Optional[str] = Query(None, description="Comma-separated, e.g. 'EU,US'"),
):
    """
    Quick way to add yourself as a subscriber without using Fillout.
    Example:
      /subscribe-test?email=you@example.com&full_name=You&org=Acme&regions=EU,US
    """
    _validate_email(email)
    regs = [r.strip() for r in regions.split(",")] if regions else []
    upsert_subscriber(email=email, full_name=full_name, org=org, regions=regs)
    return {"status": "ok", "email": email, "full_name": full_name, "org": org, "regions": regs}


@app.post("/subscribe-test")
def subscribe_test_post(body: SubscribeBody):
    _validate_email(body.email)
    upsert_subscriber(email=body.email, full_name=body.full_name, org=body.org, regions=body.regions or [])
    return {"status": "ok", "email": body.email}


async def _schedule_run(period_label: str):
    """
    Common helper to schedule a background digest run and return immediately.
    """
    logger.info("Received trigger for {}", period_label)
    asyncio.create_task(run_digest_async(period_label=period_label))
    return {"status": "accepted", "message": f"Digest '{period_label}' scheduled in background."}


@app.post("/run-now")
async def run_now_post():
    return await _schedule_run("manual-trigger")


@app.get("/run-now")
async def run_now_get():
    return await _schedule_run("manual-trigger")


@app.get("/hf-selftest")
def huggingface_selftest():
    """
    Quick verification of your HF token & endpoint.
    """
    return hf_selftest()


# -------------------------------
# Webhook + optional auto-trigger
# -------------------------------

def _env_truthy(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _maybe_schedule_auto_run() -> Tuple[bool, str]:
    """
    If TRIGGER_ON_SUBSCRIBE is enabled, ensure we don't spam runs:
    - read last time from app_state['webhook_last_run_iso']
    - if older than AUTO_RUN_COOLDOWN_MIN (default 15), schedule a run and update the timestamp.
    Returns (scheduled?, reason).
    """
    if not _env_truthy("TRIGGER_ON_SUBSCRIBE", default=False):
        return (False, "disabled")

    # cooldown minutes (default 15)
    try:
        cooldown_min = int(os.getenv("AUTO_RUN_COOLDOWN_MIN", "15"))
    except Exception:
        cooldown_min = 15

    last_iso = get_state("webhook_last_run_iso")
    now = datetime.now(timezone.utc)

    if last_iso:
        try:
            last_dt = datetime.fromisoformat(last_iso.replace("Z", "+00:00"))
        except Exception:
            last_dt = None
        if last_dt:
            delta = now - last_dt
            if delta < timedelta(minutes=cooldown_min):
                remaining = int((timedelta(minutes=cooldown_min) - delta).total_seconds() // 60)
                return (False, f"cooldown_active_{remaining}m")

    # Passed cooldown → schedule
    set_state("webhook_last_run_iso", _now_utc_iso())
    label = f"auto-webhook-{now.strftime('%Y%m%d-%H%M')}"
    logger.info("Auto-scheduling digest due to new subscription (label={})", label)
    asyncio.create_task(run_digest_async(period_label=label))
    return (True, "scheduled")


@app.post("/fillout-webhook")
async def fillout_webhook(req: Request):
    """
    Configure this URL in Fillout's Webhook settings.

    Behavior:
    - Upsert the subscriber from the submission payload.
    - If TRIGGER_ON_SUBSCRIBE=true, schedule a digest run once per cooldown window.
    """
    try:
        payload = await req.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    ok = process_webhook_payload(payload)

    auto = {"enabled": _env_truthy("TRIGGER_ON_SUBSCRIBE", False), "action": "skipped"}
    if ok:
        scheduled, reason = _maybe_schedule_auto_run()
        auto["action"] = reason
        auto["scheduled"] = scheduled

    return {"ok": ok, "auto_run": auto}
