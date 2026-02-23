"""Application configuration helpers."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(slots=True)
class AppConfig:
    openai_api_key: Optional[str]
    google_sheet_id: Optional[str]
    gcp_service_account: Optional[Dict[str, Any]]
    log_level: str = "DEBUG"

    @classmethod
    def load(cls) -> "AppConfig":
        """Load configuration from environment variables."""

        def _clean_env_value(value: Optional[str]) -> Optional[str]:
            if value is None:
                return None
            cleaned = value.strip()
            if len(cleaned) >= 2 and cleaned[0] == cleaned[-1] and cleaned[0] in {"\"", "'"}:
                cleaned = cleaned[1:-1].strip()
            return cleaned or None

        raw_sa = (os.getenv("GCP_SERVICE_ACCOUNT_JSON") or "").strip()
        sa_file = _clean_env_value(os.getenv("GCP_SERVICE_ACCOUNT_FILE")) or ""
        service_account: Optional[Dict[str, Any]] = None

        if raw_sa:
            try:
                service_account = json.loads(raw_sa)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    "GCP_SERVICE_ACCOUNT_JSON is set but is not valid JSON. "
                    "Provide the full JSON string (or use GCP_SERVICE_ACCOUNT_FILE)."
                ) from exc
        elif sa_file:
            if not os.path.exists(sa_file):
                raise FileNotFoundError(
                    f"GCP_SERVICE_ACCOUNT_FILE is set to '{sa_file}' but the file was not found."
                )
            with open(sa_file, "r", encoding="utf-8") as fh:
                service_account = json.load(fh)

        if service_account is None:
            raise ValueError(
                "Google service account credentials not provided. "
                "Set GCP_SERVICE_ACCOUNT_JSON (full JSON string) or GCP_SERVICE_ACCOUNT_FILE (path to JSON)."
            )

        return cls(
            openai_api_key=_clean_env_value(os.getenv("OPENAI_API_KEY")),
            google_sheet_id=_clean_env_value(os.getenv("GOOGLE_SHEET_ID")),
            gcp_service_account=service_account,
            log_level=os.getenv("LOG_LEVEL", "INFO"),
        )


settings = AppConfig.load()
