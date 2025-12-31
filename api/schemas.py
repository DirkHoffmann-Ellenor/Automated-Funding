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
    to_scrape: List[HttpUrl]
    already_processed: List[HttpUrl] = Field(default_factory=list)
    duplicates_in_payload: List[HttpUrl] = Field(default_factory=list)


class JobError(BaseModel):
    url: str
    message: str


class UrlTiming(BaseModel):
    url: str
    duration_seconds: float
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    error: Optional[str] = None


class JobStatusResponse(BaseModel):
    job_id: str
    done: bool
    progress_percent: int
    results: List[Dict[str, Any]]
    errors: List[JobError]
    current_url: Optional[str] = None
    current_elapsed_seconds: int = 0
    total_elapsed_seconds: int = 0
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    url_timings: List[UrlTiming] = Field(default_factory=list)
    total_urls: int = 0
    completed_urls: int = 0


class ResultsResponse(BaseModel):
    results: List[Dict[str, Any]]


class PrepareUrlsRequest(BaseModel):
    fund_urls: List[HttpUrl] = Field(..., description="URLs to stage for scraping")


class PrepareUrlsResponse(BaseModel):
    to_scrape: List[HttpUrl]
    already_processed: List[HttpUrl]
    duplicates_in_payload: List[HttpUrl]
    normalized_map: Dict[str, str]


class UpdateOpenAIKeyRequest(BaseModel):
    openai_api_key: str = Field("", description="OpenAI API key to use for this runtime session")


class UpdateOpenAIKeyResponse(BaseModel):
    status: str = "ok"
    openai_api_key_set: bool = True
