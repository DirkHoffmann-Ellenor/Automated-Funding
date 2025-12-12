"""Pydantic schemas for the FastAPI service."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, HttpUrl


class HealthResponse(BaseModel):
    status: str = "ok"


class ScrapeRequest(BaseModel):
    fund_url: HttpUrl = Field(..., description="Funding page to scrape")
    fund_name: Optional[str] = Field(None, description="Optional friendly name")


class ScrapeResponse(BaseModel):
    fund_url: HttpUrl
    fund_name: Optional[str]
    pages_scraped: Optional[int]
    visited_urls_count: Optional[int]
    eligibility: Optional[str]
    error: Optional[str] = None
    raw: Dict[str, Any]


class BatchScrapeRequest(BaseModel):
    fund_urls: List[HttpUrl]


class JobCreatedResponse(BaseModel):
    job_id: str
    fund_urls: List[HttpUrl]


class JobError(BaseModel):
    url: str
    message: str


class JobStatusResponse(BaseModel):
    job_id: str
    done: bool
    progress_percent: int
    results: List[Dict[str, Any]]
    errors: List[JobError]


class ResultsResponse(BaseModel):
    results: List[Dict[str, Any]]
