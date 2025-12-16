"""Shared FastAPI dependencies."""

from __future__ import annotations

import logging
from typing import Callable

from fastapi import Depends

from api.config import settings
from utils import tools


_CONFIGURED = False


def ensure_configured() -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return

    tools.configure_tools(
        openai_api_key=settings.openai_api_key,
        google_service_account=settings.gcp_service_account,
        google_sheet_id=settings.google_sheet_id,
        log_callback=_log_callback,
    )
    logging.getLogger(__name__).info("Tools configured")
    _CONFIGURED = True


def _log_callback(level: str, message: str) -> None:
    logger = logging.getLogger("tools")
    log_fn: Callable[[str], None] | None = getattr(logger, level, None)
    if callable(log_fn):
        log_fn(message)
    else:
        logger.info(message)


def get_tools_module() -> tools:
    ensure_configured()
    return tools


def get_settings():
    return settings
