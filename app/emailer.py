"""
Send HTML emails to all recipients using Resend.
Docs: https://resend.com/docs/send-with-python
"""

from typing import List
import resend
from loguru import logger
from .settings import settings

# Set API key once on import
resend.api_key = settings.resend_api_key


def send_html_email(to_list: List[str], subject: str, html: str) -> None:
    """
    Batches recipients in chunks of 100 to stay friendly with providers.
    If one batch fails, we log it and continue with the next.
    """
    if not to_list:
        logger.warning("No recipients to send to; skipping.")
        return

    for i in range(0, len(to_list), 100):
        chunk = [addr.strip().lower() for addr in to_list[i:i + 100]]
        params = {
            "from": settings.mail_from,
            "to": chunk,
            "subject": subject,
            "html": html,
        }
        try:
            resend.Emails.send(params)  # API call
            logger.info("Sent digest to {} recipients", len(chunk))
        except Exception as e:
            logger.exception("Email send failed for chunk: {}", e)
