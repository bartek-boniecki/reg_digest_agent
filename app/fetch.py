# app/fetch.py
"""
Asynchronous fetcher that:
- Pulls listings from curated sources (config/sources.yaml)
- Picks likely article links (English-only), follows 1 PDF if needed
- Extracts dates (meta tags, ld+json, URL patterns, Last-Modified)
- Applies a "recent only" gate (last 7 days by default)
- Writes idempotently into Supabase

Tunable via environment:
  FETCH_CONCURRENCY, PER_HOST_CONCURRENCY, MIN_TEXT_LENGTH, MAX_AGE_DAYS,
  HTTP_TIMEOUT, HTTP_RETRIES, HTTP_BACKOFF_BASE, HTTP_ACCEPT_LANGUAGE
"""

from __future__ import annotations

# Load .env early for tunables
try:
    from dotenv import load_dotenv, find_dotenv
    _DOT = find_dotenv(usecwd=True)
    if _DOT:
        load_dotenv(_DOT, override=False)
except Exception:
    pass

import asyncio
import hashlib
import json
import os
import re
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from io import BytesIO
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

import httpx
from loguru import logger
from lxml import html as lxml_html
import yaml
from pdfminer.high_level import extract_text as pdf_extract_text

from .db import upsert_article  # database writer

# ---- Config knobs (with sane defaults) ----
FETCH_CONCURRENCY = int(os.getenv("FETCH_CONCURRENCY", "6"))
PER_HOST_CONCURRENCY = int(os.getenv("PER_HOST_CONCURRENCY", "2"))
MIN_TEXT_LENGTH = int(os.getenv("MIN_TEXT_LENGTH", "600"))
MAX_AGE_DAYS = int(os.getenv("MAX_AGE_DAYS", "7"))
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "20"))
HTTP_RETRIES = int(os.getenv("HTTP_RETRIES", "3"))
HTTP_BACKOFF_BASE = float(os.getenv("HTTP_BACKOFF_BASE", "0.8"))
HTTP_ACCEPT_LANGUAGE = os.getenv("HTTP_ACCEPT_LANGUAGE", "en,en-GB;q=0.9,en-US;q=0.8")

HEADERS_PRIMARY = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept-Language": HTTP_ACCEPT_LANGUAGE,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.google.com/",
}
HEADERS_SECONDARY = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0"
    ),
    "Accept-Language": HTTP_ACCEPT_LANGUAGE,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.google.com/",
    "Upgrade-Insecure-Requests": "1",
    "Sec-CH-UA": '"Not/A)Brand";v="8", "Chromium";v="125", "Microsoft Edge";v="125"',
    "Sec-CH-UA-Platform": '"Windows"',
    "Sec-CH-UA-Mobile": "?0",
}

# Some hosts frequently update header dates (we treat them as evergreen)
EVERGREEN_HEADER_DOMAINS = {"www.iso.org", "iso.org"}

# Non-English path segments we drop to keep English-only signal
NON_EN_PATH_SEGMENTS = [
    "/fr/", "/de/", "/es/", "/it/", "/pt/", "/bg/", "/pl/", "/ru/",
    "/nl/", "/cs/", "/da/", "/fi/", "/sv/", "/ro/", "/sk/", "/sl/",
    "/lt/", "/lv/", "/et/", "/el/", "/hu/", "/ga/", "/mt/",
]


# ---------------- Helpers ----------------
def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _is_recent(dt: Optional[datetime]) -> bool:
    if dt is None:
        return False
    return dt >= (_now_utc() - timedelta(days=MAX_AGE_DAYS))

def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def _same_host(url: str, base: str) -> bool:
    try:
        return urlparse(url).netloc == urlparse(base).netloc
    except Exception:
        return False

def _clean_links(links: List[str]) -> List[str]:
    out, seen = [], set()
    for u in links:
        u = u.split("#")[0].strip()
        if u and u not in seen:
            seen.add(u)
            out.append(u)
    return out

def _is_non_english_url(url: str) -> bool:
    ul = url.lower()
    return any(seg in ul for seg in NON_EN_PATH_SEGMENTS)

def _html_lang_is_english(html: str) -> bool:
    try:
        doc = lxml_html.fromstring(html)
        lang = doc.xpath("string(//html/@lang)") or doc.xpath("string(//html/@xml:lang)")
        return (not lang) or lang.lower().startswith("en")
    except Exception:
        return True

# ---- Date parsing helpers ----
_MONTHS = ("january","february","march","april","may","june","july",
           "august","september","october","november","december")

def _try_parse_human_date(s: str) -> Optional[datetime]:
    s = s.strip()
    m = re.search(r"\b(" + "|".join(_MONTHS) + r")\s+\d{1,2},\s+\d{4}\b", s, flags=re.I)
    if not m:
        return None
    try:
        dt = datetime.strptime(m.group(0), "%B %d, %Y")
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None

def _date_from_url_path(url: str) -> Optional[datetime]:
    # /YYYY/MM/DD or /YYYY/MM
    m = re.search(r"/(20\d{2})/([01]\d)(?:/([0-3]\d))?/", url)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3) or "15")
        try:
            return datetime(y, mo, d, tzinfo=timezone.utc)
        except Exception:
            return None
    # YYYY-MM-DD in slug
    m2 = re.search(r"(20\d{2})-([01]\d)-([0-3]\d)", url)
    if m2:
        try:
            return datetime(int(m2.group(1)), int(m2.group(2)), int(m2.group(3)), tzinfo=timezone.utc)
        except Exception:
            return None
    return None

def _parse_isoish(s: str) -> Optional[datetime]:
    if not s:
        return None
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None

def _guess_published_at_from_html(html: str) -> Optional[datetime]:
    """Try meta tags, ld+json, or visible 'Month DD, YYYY' text."""
    try:
        doc = lxml_html.fromstring(html)
        # meta and common date places
        for xp in [
            "//meta[@property='article:published_time']/@content",
            "//meta[@property='article:modified_time']/@content",
            "//meta[@property='og:updated_time']/@content",
            "//meta[@name='date']/@content",
            "//meta[@name='dcterms.date']/@content",
            "//meta[@name='publish_date']/@content",
            "//meta[@name='publication_date']/@content",
            "//meta[@itemprop='datePublished']/@content",
            "//meta[@itemprop='dateModified']/@content",
            "//time/@datetime",
        ]:
            vals = doc.xpath(xp)
            if vals:
                dt = _parse_isoish(vals[0])
                if dt:
                    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

        # JSON-LD
        for node in doc.xpath("//script[@type='application/ld+json']/text()"):
            try:
                data = json.loads(node)
                items = data if isinstance(data, list) else [data]
                for d in items:
                    if isinstance(d, dict):
                        for key in ("datePublished", "dateModified", "uploadDate"):
                            if key in d:
                                dt = _parse_isoish(str(d[key]))
                                if dt:
                                    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            except Exception:
                continue

        # visible "Month DD, YYYY"
        for xp in [
            "//article//*[contains(@class,'date') or contains(@class,'published') or contains(@class,'time')]/text()",
            "//header//*[contains(@class,'date') or contains(@class,'published') or contains(@class,'time')]/text()",
            "//*[self::h1 or self::h2 or self::h3]/following::text()[position()<20]"
        ]:
            texts = [t.strip() for t in doc.xpath(xp) if isinstance(t, str) and t.strip()]
            for t in texts:
                dt = _try_parse_human_date(t)
                if dt:
                    return dt
    except Exception:
        pass
    return None

def _extract_text_from_html(html: str) -> str:
    # First try trafilatura for a clean article body
    try:
        import trafilatura
        text = trafilatura.extract(html, include_comments=False, favor_precision=True, no_fallback=False) or ""
        text = text.strip()
        if text:
            return text
    except Exception:
        pass
    # Fallback: raw text of <body>, normalized
    try:
        doc = lxml_html.fromstring(html)
        t = doc.xpath("string(//body)")
        t = re.sub(r"\s+\n", "\n", t or "")
        t = re.sub(r"\n{3,}", "\n\n", t)
        return (t or "").strip()
    except Exception:
        return ""

def _find_first_pdf_link(html: str, base_url: str) -> Optional[str]:
    try:
        doc = lxml_html.fromstring(html)
        for href in doc.xpath("//a[@href]/@href"):
            if href.lower().endswith(".pdf"):
                return urljoin(base_url, href)
    except Exception:
        pass
    return None

def _published_from_headers(resp: httpx.Response) -> Optional[datetime]:
    lm = resp.headers.get("Last-Modified")
    if lm:
        try:
            dt = parsedate_to_datetime(lm)
            return dt.astimezone(timezone.utc)
        except Exception:
            return None
    return None

# ---- HTTP & throttling ----
class HostLimiter:
    def __init__(self, per_host: int):
        self.per_host = per_host
        self._locks: Dict[str, asyncio.Semaphore] = {}

    def limiter(self, url: str) -> asyncio.Semaphore:
        host = urlparse(url).netloc
        if host not in self._locks:
            self._locks[host] = asyncio.Semaphore(self.per_host)
        return self._locks[host]

HOST_LIMITER = HostLimiter(PER_HOST_CONCURRENCY)

async def _backoff_sleep(retry_idx: int) -> None:
    delay = (HTTP_BACKOFF_BASE ** retry_idx) * (1.0 + 0.25 * (retry_idx + 1))
    await asyncio.sleep(delay)

async def _http_get(client: httpx.AsyncClient, url: str, *, use_secondary_headers: bool = False) -> httpx.Response:
    headers = HEADERS_SECONDARY if use_secondary_headers else HEADERS_PRIMARY
    sem = HOST_LIMITER.limiter(url)
    async with sem:
        last_exc = None
        for i in range(HTTP_RETRIES + 1):
            try:
                r = await client.get(url, headers=headers, timeout=HTTP_TIMEOUT, follow_redirects=True)
                if r.status_code in (429, 500, 502, 503, 504):
                    last_exc = RuntimeError(f"Transient HTTP {r.status_code}")
                    logger.warning(f"Transient {r.status_code} on {url}, retry {i}/{HTTP_RETRIES}")
                else:
                    return r
            except Exception as e:
                last_exc = e
                logger.warning(f"GET failed {url} ({e}), retry {i}/{HTTP_RETRIES}")
            await _backoff_sleep(i + 1)
        if last_exc:
            raise last_exc
        raise RuntimeError("GET failed without exception")

def _amp_variants(u: str) -> List[str]:
    variants = []
    if not u.rstrip("/").endswith("/amp"):
        variants.append(u.rstrip("/") + "/amp")
    if "?amp" not in u:
        variants.append(u + ("&amp" if "?" in u else "?amp"))
    if "?output=amp" not in u:
        variants.append(u + ("&output=amp" if "?" in u else "?output=amp"))
    return variants

# ---- Listing parsing ----
DENY_ALWAYS = ["/page-not-found", "/404"] + NON_EN_PATH_SEGMENTS

def _parse_listing_html(page_html: str, base_url: str, cfg: Dict[str, Any]) -> List[str]:
    try:
        doc = lxml_html.fromstring(page_html)
    except Exception:
        return []

    # Prefer content cards first
    a1 = [urljoin(base_url, href.strip()) for href in doc.xpath("//article//a[@href]/@href")]
    a2 = [urljoin(base_url, href.strip()) for href in doc.xpath("//a[@href]/@href")]
    links = _clean_links(a1 + a2)

    if cfg.get("same_host_only", True):
        links = [u for u in links if _same_host(u, base_url)]
    links = [u for u in links if not _is_non_english_url(u)]

    allow = cfg.get("allow_substr", []) or []
    deny = list(cfg.get("deny_substr", []) or []) + DENY_ALWAYS
    if allow:
        links = [u for u in links if any(a in u for a in allow)]
    links = [u for u in links if all(d not in u for d in deny)]

    # Drop navigational dead-ends
    drop_suffixes = ("/en", "/news", "/policies", "/about", "/contact", "/events")
    links = [u for u in links if not u.rstrip("/").endswith(drop_suffixes)]

    max_links = int(cfg.get("max_links", 12))
    return links[:max_links]

# ---- Article fetching ----
async def _fetch_article_page(client: httpx.AsyncClient, url: str, cfg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    r = await _http_get(client, url)
    if r.status_code != 200:
        logger.error(f"Article fetch {url} -> {r.status_code}")
        return None

    html = r.text
    if not _html_lang_is_english(html):
        logger.info(f"Skip non-English page via <html lang>: {url}")
        return None

    dt = _guess_published_at_from_html(html)
    host = urlparse(url).netloc.lower()
    if dt is None and host not in EVERGREEN_HEADER_DOMAINS:
        # Try Last-Modified header
        dt = _published_from_headers(r)
        # Try date patterns in URL
        if dt is None:
            dt = _date_from_url_path(url)

    if cfg.get("last_week_only", True) and not _is_recent(dt):
        logger.info(f"Skip old/undated for recency: {url}")
        return None
    published_at = dt or _now_utc()

    text = _extract_text_from_html(html)

    # If page body is small, try first PDF (common for regulators)
    if len(text) < MIN_TEXT_LENGTH and cfg.get("pdf_chase", True):
        pdf_url = _find_first_pdf_link(html, url)
        if pdf_url:
            pr = await _http_get(client, pdf_url)
            if pr.status_code == 200 and "application/pdf" in pr.headers.get("Content-Type", "").lower():
                pdf_text = ""
                try:
                    pdf_text = pdf_extract_text(BytesIO(pr.content)) or ""
                except Exception as e:
                    logger.warning(f"PDF extract failed {pdf_url}: {e}")
                if pdf_text:
                    lm = _published_from_headers(pr)
                    if not cfg.get("last_week_only", True) or _is_recent(lm):
                        text = (text + "\n\n" + pdf_text.strip()).strip()
                        if not dt and lm and host not in EVERGREEN_HEADER_DOMAINS:
                            published_at = lm

    if len(text) < MIN_TEXT_LENGTH:
        logger.info(f"Skip too-short ({len(text)} chars): {url}")
        return None

    # Page title fallback
    title = ""
    try:
        doc = lxml_html.fromstring(html)
        t = doc.xpath("string(//title)")
        title = (t or "").strip()
    except Exception:
        pass
    if not title:
        first_line = (text.splitlines()[0] if text else "").strip()
        title = (first_line[:120] + "…") if len(first_line) > 120 else first_line or "Untitled"

    return {"url": url, "title": title, "published_at": published_at, "raw_text": text}

# ---- Public: fetch_all ----
async def fetch_all() -> None:
    """
    Loads config/sources.yaml, fetches listings/articles concurrently,
    and upserts articles into Supabase.
    """
    cfg_path_env = os.getenv("SOURCES_YAML")  # allow override
    cfg_path = cfg_path_env or os.path.join("config", "sources.yaml")
    with open(cfg_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    defaults = raw.get("defaults", {})
    sources = raw.get("sources", [])

    def merged(s: Dict[str, Any]) -> Dict[str, Any]:
        c = {**defaults, **s}
        if "max_links" not in c:
            c["max_links"] = 12
        return c

    async with httpx.AsyncClient(http2=True) as client:
        sem = asyncio.Semaphore(FETCH_CONCURRENCY)

        async def process_source(cfg: Dict[str, Any]) -> None:
            name = cfg.get("name", cfg.get("url", ""))
            urls = [cfg["url"]] + cfg.get("fallback_urls", [])
            base_html = None
            base_url = None

            # Try main + AMP variants + secondary headers on 403
            for u in urls:
                try:
                    r = await _http_get(client, u, use_secondary_headers=False)
                except Exception as e:
                    logger.warning(f"Listing GET failed {u}: {e}")
                    continue

                if r.status_code == 200:
                    base_html = r.text; base_url = u; break

                if r.status_code == 403:
                    try:
                        r2 = await _http_get(client, u, use_secondary_headers=True)
                        if r2.status_code == 200:
                            base_html = r2.text; base_url = u; break
                        for au in _amp_variants(u):
                            r3 = await _http_get(client, au, use_secondary_headers=True)
                            if r3.status_code == 200:
                                base_html = r3.text; base_url = au; break
                        if base_html: break
                    except Exception as e2:
                        logger.warning(f"Listing 403 fallback failed {u}: {e2}")
                        continue

            if not base_html or not base_url:
                logger.error(f"Listing failed: {name}")
                return

            links = _parse_listing_html(base_html, base_url, cfg)

            async def handle(u: str) -> None:
                async with sem:
                    try:
                        art = await _fetch_article_page(client, u, cfg)
                        if not art:
                            return
                        item = {
                            "url": art["url"],
                            "title": art["title"],
                            "raw_text": art["raw_text"],
                            "published_at": art["published_at"],
                            "hash": _sha1(art["url"]),
                        }
                        upsert_article(item)
                    except Exception as e:
                        logger.error(f"Fetch article failed {u}: {e}")

            await asyncio.gather(*(handle(u) for u in links))

        await asyncio.gather(*(process_source(merged(s)) for s in sources))

    logger.info("Fetch finished.")
