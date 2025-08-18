"""
FastAPI service:
- /health           quick liveness check
- /subscribe-test   add yourself without Fillout (GET/POST)
- /run-now          trigger a digest in background (GET/POST)
- /hf-selftest      verify Hugging Face token/endpoint
- /fillout-webhook  (NEW) receive Fillout submission webhooks → upsert subscriber

Security: For the webhook, set a random secret and validate a header if you want
(quick start leaves it open, but Railway URL is unguessable; tighten in prod).
"""

import re
import asyncio
from typing import List, Optional

from fastapi import FastAPI, Query, HTTPException, Request
from pydantic import BaseModel
from loguru import logger

from .pipeline import run_digest_async
from .db import upsert_subscriber
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
    return hf_selftest()


# ---- NEW: Fillout webhook endpoint ----
@app.post("/fillout-webhook")
async def fillout_webhook(req: Request):
    """
    Configure this URL in Fillout's Webhook settings.
    We accept the payload and upsert the subscriber immediately.
    """
    try:
        payload = await req.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    ok = process_webhook_payload(payload)
    return {"ok": ok}
