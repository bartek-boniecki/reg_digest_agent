"""
Schedule the weekly run every Thursday in your local timezone, AND
optionally trigger a run immediately on startup.

Run this as a separate worker on Railway/Heroku/Fly:
  python -m app.scheduler
"""

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger
from datetime import datetime
from .settings import settings
from .pipeline import run_digest


def main(run_now: bool = True):
    scheduler = BlockingScheduler(timezone=settings.tz)

    # Every Thursday at 09:00 local time (change hour/minute if you prefer)
    trigger = CronTrigger(day_of_week='thu', hour=9, minute=0)
    scheduler.add_job(
        lambda: run_digest(period_label=f"weekly-{datetime.utcnow().date()}"),
        trigger,
        name="weekly-digest",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=3600,  # allow 1h late
    )
    logger.info("Scheduled weekly job: Thursday 09:00 ({})", settings.tz)

    if run_now:
        logger.info("Running on-demand job now …")
        run_digest(period_label=f"on-demand-{datetime.utcnow().isoformat(timespec='minutes')}Z")

    scheduler.start()


if __name__ == "__main__":
    main()
