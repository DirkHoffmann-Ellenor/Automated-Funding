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
    log_level: str = "INFO"

    @classmethod
    def load(cls) -> "AppConfig":
        """Load configuration from environment variables."""

        raw_sa = os.getenv("GCP_SERVICE_ACCOUNT_JSON")
        sa_file = os.getenv("GCP_SERVICE_ACCOUNT_FILE")
        service_account: Optional[Dict[str, Any]] = None

        if raw_sa:
            service_account = json.loads(raw_sa)
        elif sa_file and os.path.exists(sa_file):
            with open(sa_file, "r", encoding="utf-8") as fh:
                service_account = json.load(fh)

        return cls(
            openai_api_key=os.getenv("OPENAI_API_KEY"),
            google_sheet_id=os.getenv("GOOGLE_SHEET_ID"),
            gcp_service_account=service_account,
            log_level=os.getenv("LOG_LEVEL", "INFO"),
        )


settings = AppConfig.load()
