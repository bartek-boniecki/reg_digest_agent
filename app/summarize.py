# app/summarize.py
"""
Summarization via Hugging Face, with two transport modes:

1) HF Inference Providers "router" (OpenAI-compatible):
   base = https://router.huggingface.co/v1
   Requires a fine-grained token with permission:
   "Make calls to Inference Providers".
   Docs: Inference Providers + Chat Completion API.
2) HF Dedicated Inference Endpoint (Pro):
   Set HF_ENDPOINT_URL to your endpoint base (must expose /v1/chat/completions).
   Backed by Text Generation Inference (TGI) which is OpenAI-chat compatible.

We do:
- A quick preflight to catch bad tokens/permissions up-front.
- Chunking + strict max_tokens to keep cost+latency predictable.
- Model fallback only in router mode; endpoints typically host a single model.

ENV:
  HF_TOKEN                  # required (fine-grained with proper scopes)
  HF_ROUTER_BASE            # default: https://router.huggingface.co/v1
  HF_ENDPOINT_URL           # if set, we use this instead of router
  HF_MODEL                  # preferred model id (router mode)
  HF_MAX_TOKENS             # default 600
  HF_TEMPERATURE            # default 0.2
  HF_MAX_ITEMS_PER_CHUNK    # default 8
  HF_MAX_CHARS_PER_CHUNK    # default 5000
"""

from __future__ import annotations

import os
import time
import json
import httpx
from typing import List, Dict, Any
from loguru import logger

# -----------------------
# Configuration
# -----------------------

HF_TOKEN = os.getenv("HF_TOKEN") or os.getenv("HUGGING_FACE_HUB_TOKEN")
ROUTER_BASE = os.getenv("HF_ROUTER_BASE", "https://router.huggingface.co/v1").rstrip("/")
ENDPOINT_BASE = (os.getenv("HF_ENDPOINT_URL") or "").rstrip("/")

# When using router, we try a preference list (you can reorder in .env by setting HF_MODEL).
MODEL_PREFERENCE: List[str] = [
    os.getenv("HF_MODEL") or "meta-llama/Llama-3.1-8B-Instruct",
    "Qwen/Qwen2.5-7B-Instruct",
    "mistralai/Mistral-7B-Instruct-v0.3",
    "HuggingFaceH4/zephyr-7b-beta",
    "google/gemma-2-9b-it",
]

GEN_MAX_TOKENS = int(os.getenv("HF_MAX_TOKENS") or "600")
GEN_TEMPERATURE = float(os.getenv("HF_TEMPERATURE") or "0.2")
REQUEST_TIMEOUT_SECS = int(os.getenv("HF_TIMEOUT_SECS") or "60")
MAX_ITEMS_PER_CHUNK = int(os.getenv("HF_MAX_ITEMS_PER_CHUNK") or "8")
MAX_CHARS_PER_CHUNK = int(os.getenv("HF_MAX_CHARS_PER_CHUNK") or "5000")

# Cache: we verify credentials once per process
_VERIFIED_OK = False
_VERIFIED_MODE = None  # "router" or "endpoint"


# -----------------------
# HTTP helpers
# -----------------------

def _post_json(url: str, headers: Dict[str, str], payload: Dict[str, Any]) -> Dict[str, Any]:
    with httpx.Client(timeout=REQUEST_TIMEOUT_SECS) as client:
        r = client.post(url, headers=headers, json=payload)
        body = (r.text or "")[:1000]
        if r.status_code >= 400:
            # Explain common permission errors clearly
            if r.status_code in (401, 403):
                raise RuntimeError(
                    f"{r.status_code} calling {url}. "
                    f"Likely a token/permission issue. Body: {body}"
                )
            raise RuntimeError(f"POST {url} -> {r.status_code}. Body: {body}")
        try:
            return r.json()
        except Exception as e:
            raise RuntimeError(f"Invalid JSON from {url}: {e}. Body: {body}")


def _get(url: str, headers: Dict[str, str]) -> Dict[str, Any]:
    with httpx.Client(timeout=REQUEST_TIMEOUT_SECS) as client:
        r = client.get(url, headers=headers)
        body = (r.text or "")[:1000]
        if r.status_code >= 400:
            if r.status_code in (401, 403):
                raise RuntimeError(
                    f"{r.status_code} calling {url}. "
                    f"Likely a token/permission issue. Body: {body}"
                )
            raise RuntimeError(f"GET {url} -> {r.status_code}. Body: {body}")
        try:
            return r.json()
        except Exception as e:
            raise RuntimeError(f"Invalid JSON from {url}: {e}. Body: {body}")


# -----------------------
# Preflight: verify we can talk to HF
# -----------------------

def _verify_once() -> None:
    """Run a fast, friendly verification of your HF setup."""
    global _VERIFIED_OK, _VERIFIED_MODE
    if _VERIFIED_OK:
        return
    if not HF_TOKEN:
        raise RuntimeError(
            "HF_TOKEN is not set. Create a Hugging Face access token and put it in your .env"
        )

    headers = {"Authorization": f"Bearer {HF_TOKEN}", "Content-Type": "application/json"}

    if ENDPOINT_BASE:
        # Quick 1-token chat call to your endpoint
        url = f"{ENDPOINT_BASE}/chat/completions"
        payload = {
            "model": os.getenv("HF_MODEL") or "endpoint",
            "messages": [{"role": "user", "content": "ping"}],
            "max_tokens": 1,
            "temperature": 0.0,
        }
        _post_json(url, headers, payload)
        _VERIFIED_MODE = "endpoint"
        _VERIFIED_OK = True
        logger.info("HF preflight OK (endpoint mode): {}", ENDPOINT_BASE)
        return

    # Router mode: check /v1/models (fast, explicit permission check)
    url = f"{ROUTER_BASE}/models"
    _get(url, headers)
    _VERIFIED_MODE = "router"
    _VERIFIED_OK = True
    logger.info("HF preflight OK (router mode): {}", ROUTER_BASE)


# -----------------------
# Chat completion
# -----------------------

def _chat_complete(base: str, model: str, system: str, user: str, *, max_tokens: int, temperature: float) -> str:
    url = f"{base}/chat/completions"
    headers = {
        "Authorization": f"Bearer {HF_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    resp = _post_json(url, headers, payload)
    try:
        return resp["choices"][0]["message"]["content"]
    except Exception as e:
        raise RuntimeError(f"Bad chat response: {e}. Full: {json.dumps(resp)[:800]}")


def _format_items_for_prompt(items: List[Dict[str, Any]]) -> str:
    """Compact list of items for the prompt."""
    out = []
    for it in items:
        title = (it.get("title") or "").strip()
        url = (it.get("url") or "").strip()
        date = (it.get("published_at") or "")[:10]
        text = (it.get("raw_text") or "").strip().replace("\n", " ")
        if len(text) > 400:
            text = text[:400] + "..."
        out.append(f"- [{date}] {title} — {text} | {url}")
    blob = "\n".join(out)
    if len(blob) > MAX_CHARS_PER_CHUNK:
        blob = blob[:MAX_CHARS_PER_CHUNK] + "\n..."
    return blob


def _chunk(items: List[Dict[str, Any]], n: int) -> List[List[Dict[str, Any]]]:
    return [items[i:i+n] for i in range(0, len(items), n)]


def _generate_with_fallbacks(system: str, user: str, *, max_tokens: int, temperature: float) -> str:
    """
    Router mode: try preferred models in order.
    Endpoint mode: call the single endpoint (model name is ignored by most TGI deployments).
    """
    _verify_once()

    if _VERIFIED_MODE == "endpoint":
        model = os.getenv("HF_MODEL") or "endpoint"
        logger.info("HF call (endpoint) model={}", model)
        return _chat_complete(ENDPOINT_BASE, model, system, user, max_tokens=max_tokens, temperature=temperature)

    last_err = None
    for m in MODEL_PREFERENCE:
        try:
            logger.info("HF router call -> model={}", m)
            return _chat_complete(ROUTER_BASE, m, system, user, max_tokens=max_tokens, temperature=temperature)
        except Exception as e:
            last_err = e
            msg = str(e)
            # Friendly guidance for typical permission error
            if "sufficient permissions" in msg or "403" in msg:
                raise RuntimeError(
                    "Hugging Face router denied access (403). "
                    "Create a fine-grained token with the permission “Make calls to Inference Providers”, "
                    "set it as HF_TOKEN, or set HF_ENDPOINT_URL to your dedicated endpoint. "
                    f"Details: {msg}"
                )
            logger.warning("Model {} failed: {}", m, e)
            time.sleep(0.6)
    raise RuntimeError(f"All router models failed. Last error: {last_err}")


# -----------------------
# Public API
# -----------------------

def summarize(items: List[Dict[str, Any]]) -> str:
    """
    Summarize a list of regulatory items into ~6–10 bullets for compliance teams.
    Algorithm:
      - chunk items -> partial summaries
      - synthesize a final digest
    """
    if not items:
        return "No noteworthy regulatory updates in the last period."

    sys_msg = (
        "You are an expert legal/regulatory analyst. "
        "Write for COMPLIANCE TEAMS: precise, actionable, neutral tone."
    )

    parts: List[str] = []
    for group in _chunk(items, MAX_ITEMS_PER_CHUNK):
        user_blob = (
            "Summarize the following regulatory items into 3–6 crisp bullets. "
            "Focus on obligations, who is affected, deadlines (ISO dates). "
            "Group by jurisdiction if relevant. Cite as [n] and provide a Sources list.\n\n"
            f"{_format_items_for_prompt(group)}\n\n"
            "Return:\n"
            "• Bulleted list\n"
            "• Short 'What this means' paragraph\n"
            "• Sources list [n] → URL"
        )
        part = _generate_with_fallbacks(sys_msg, user_blob, max_tokens=GEN_MAX_TOKENS, temperature=GEN_TEMPERATURE)
        parts.append(part)

    if len(parts) == 1:
        return parts[0]

    synth_user = (
        "Combine these partial summaries into a single weekly digest for compliance teams. "
        "Keep 6–10 bullets max, deduplicate, keep only concrete changes. "
        "End with 'Key dates' (if any) and a consolidated Sources list.\n\n"
        + "\n\n--- PART ---\n".join(parts)
    )
    return _generate_with_fallbacks(sys_msg, synth_user, max_tokens=GEN_MAX_TOKENS, temperature=GEN_TEMPERATURE)


def hf_selftest() -> dict:
    """
    Tiny self-test used by /hf-selftest endpoint.
    Returns a dict describing mode and token validity.
    """
    try:
        _verify_once()
        return {"ok": True, "mode": _VERIFIED_MODE, "base": ENDPOINT_BASE or ROUTER_BASE}
    except Exception as e:
        return {"ok": False, "error": str(e)}
