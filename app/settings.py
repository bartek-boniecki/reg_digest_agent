"""
Loads configuration & secrets from environment variables (.env file in dev).

Why this file matters:
- You can switch LLMs by changing HF_MODEL only.
- Your Supabase service role key name confusion is handled here (we accept either).
"""

from pydantic import BaseModel, Field
from dotenv import load_dotenv
import os

# Allow local dev via .env
load_dotenv()


class Settings(BaseModel):
    # ---- LLM (Hugging Face Inference) ----
    hf_token: str = Field(default_factory=lambda: os.environ["HF_TOKEN"])
    # Use the official gated repo id you were granted access to:
    hf_model: str = os.environ.get("HF_MODEL", "meta-llama/Llama-3.1-8B-Instruct")
    # Optional provider routing (Hugging Face Inference Providers)
    hf_provider: str | None = os.environ.get("HF_PROVIDER")

    # ---- Email (Resend) ----
    resend_api_key: str = Field(default_factory=lambda: os.environ["RESEND_API_KEY"])
    mail_from: str = os.environ.get("MAIL_FROM", "Regulatory Digest <no-reply@example.com>")

    # ---- Supabase ----
    supabase_url: str = Field(default_factory=lambda: os.environ["SUPABASE_URL"])
    # Accept either env name; if you set both, SERVICE_ROLE_KEY wins
    supabase_service_role_key: str = Field(
        default_factory=lambda: os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        or os.environ.get("SUPABASE_SERVICE_ROLE")
        or os.environ["SUPABASE_KEY"]  # last resort
    )

    # ---- Fillout ----
    fillout_api_base: str = os.environ.get("FILLOUT_API_BASE", "https://api.fillout.com/v1/api")
    fillout_api_key: str = Field(default_factory=lambda: os.environ["FILLOUT_API_KEY"])
    fillout_form_id: str = Field(default_factory=lambda: os.environ["FILLOUT_FORM_ID"])

    # ---- Timezone ----
    tz: str = os.environ.get("TZ", "Europe/Warsaw")


settings = Settings()
