"""
Runs the whole pipeline:

1) Sync subscribers from Fillout
2) Fetch new items from sources
3) Summarize with Hugging Face Inference
4) Render HTML
5) Save digest
6) Email subscribers + record deliveries
"""

from __future__ import annotations

import asyncio
from loguru import logger
from .fillout_sync import sync_from_fillout
from .fetch import fetch_all
from .db import (
    list_recent_articles_days,
    fetch_active_subscribers,
    insert_digest,
    insert_delivery,
)
from .summarize import summarize
from .compose import render_html
from .emailer import send_html_email


async def run_digest_async(period_label: str):
    try:
        # 1) Fillout → Supabase subscribers
        logger.info("1) Syncing subscribers from Fillout …")
        sync_from_fillout()

        # 2) Crawl sources and write articles
        logger.info("2) Fetching latest articles from sources …")
        await fetch_all()

        # 3) Load last 7 days of content for this digest
        logger.info("3) Loading recent articles from DB …")
        items = list_recent_articles_days(days=7)

        if not items:
            logger.warning("No recent items found; sending a short 'no updates' note.")
            summary = "No noteworthy regulatory updates in the last period."
        else:
            logger.info("4) Summarizing {} items with LLM …", len(items))
            summary = summarize(items)

        # 4) Email HTML render
        logger.info("5) Rendering HTML …")
        html = render_html(summary, period=period_label)

        # 5) Persist this digest HTML
        logger.info("6) Persisting digest …")
        digest = insert_digest(period_label, html)

        # 6) Send to all active subscribers
        logger.info("7) Sending email …")
        subs = fetch_active_subscribers()
        to_list = [s["email"] for s in subs]
        send_html_email(to_list, subject=f"Regulatory Digest — {period_label}", html=html)

        # 7) Per-subscriber delivery logs (audit trail)
        for s in subs:
            insert_delivery(digest["id"], s["id"], status="sent")

        logger.info("Done. Period={}", period_label)

    except Exception as e:
        # Make background task failures explicit in logs
        logger.exception("Digest run failed: {}", e)


def run_digest(period_label: str):
    """
    Sync wrapper for CLI/schedulers that aren't async-aware.
    The API uses run_digest_async() scheduled via asyncio.create_task().
    """
    asyncio.run(run_digest_async(period_label))
