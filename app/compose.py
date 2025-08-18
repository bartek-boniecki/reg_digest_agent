"""
Turns the summary string into a clean, responsive(ish) HTML email via Jinja2.
"""

from jinja2 import Template
from datetime import datetime, timezone

HTML_TMPL = Template("""
<!doctype html>
<html>
  <body style="font-family:system-ui,-apple-system,'Segoe UI',Roboto,Arial; color:#111; line-height:1.5; background:#f7f7f8;">
    <div style="max-width:760px; margin:auto; padding:24px;">
      <h1 style="margin:0 0 8px;">Regulatory Digest</h1>
      <div style="color:#555; font-size:14px;">Generated on {{ now }} ({{ period }})</div>
      <hr style="margin:16px 0;">
      <div style="white-space:pre-wrap;">{{ summary }}</div>
      <hr style="margin:16px 0;">
      <div style="color:#666; font-size:12px;">
        You’re receiving this because you subscribed via our Fillout form. Reply "STOP" to unsubscribe.
      </div>
    </div>
  </body>
</html>
""")

def render_html(summary: str, period: str) -> str:
    # stamp in UTC so everyone understands when it was generated
    now = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    return HTML_TMPL.render(now=now, period=period, summary=summary)
