from typing import Optional, Set

from fastapi import APIRouter, Depends, HTTPException, status

from api import dependencies
from api.jobs import job_store
from api.schemas import (
    BatchScrapeRequest,
    JobCreatedResponse,
    JobError,
    JobStatusResponse,
    PrepareUrlsRequest,
    PrepareUrlsResponse,
    ScrapeRequest,
    ScrapeResponse,
)
from utils import tools

router = APIRouter(prefix="/scrape", tags=["scrape"])


def _prepare_urls_for_scrape(
    raw_urls: list[str],
    *,
    tools_module: tools,
    allow_rescrape: Optional[Set[str]] = None,
) -> dict:
    """
    Normalize URLs, drop duplicates, and flag any that were already processed.
    Rescrape URLs can be explicitly allowed to bypass the duplicate check.
    """
    processed = tools_module.get_already_processed_urls(force_refresh=True)
    allow_rescrape = allow_rescrape or set()
    allow_rescrape_normalized = {tools_module.normalize_url(u) for u in allow_rescrape}
    normalized_map: dict[str, str] = {}
    already_processed: list[str] = []
    duplicates_in_payload: list[str] = []
    to_scrape: list[str] = []
    seen_normalized: set[str] = set()

    for raw in raw_urls:
        normalized = tools_module.normalize_url(raw)
        normalized_map[raw] = normalized
        if normalized in seen_normalized:
            duplicates_in_payload.append(raw)
            continue
        seen_normalized.add(normalized)
        if normalized in processed and normalized not in allow_rescrape_normalized:
            already_processed.append(raw)
            continue
        to_scrape.append(normalized)

    return {
        "to_scrape": to_scrape,
        "already_processed": already_processed,
        "duplicates_in_payload": duplicates_in_payload,
        "normalized_map": normalized_map,
    }


@router.post("/prepare", response_model=PrepareUrlsResponse, status_code=status.HTTP_200_OK)
def prepare_urls(
    payload: PrepareUrlsRequest,
    tools_module: tools = Depends(dependencies.get_tools_module),
) -> PrepareUrlsResponse:
    prepared = _prepare_urls_for_scrape([str(url) for url in payload.fund_urls], tools_module=tools_module)
    return PrepareUrlsResponse(**prepared)


@router.post("/single", response_model=ScrapeResponse, status_code=status.HTTP_200_OK)
def scrape_single(
    payload: ScrapeRequest,
    tools_module: tools = Depends(dependencies.get_tools_module),
) -> ScrapeResponse:
    prepared = _prepare_urls_for_scrape([str(payload.fund_url)], tools_module=tools_module)
    if not prepared["to_scrape"]:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="URL already exists in results or was provided more than once.",
        )

    scrape_url = prepared["to_scrape"][0]
    result = tools_module.process_single_fund(scrape_url, payload.fund_name)
    return ScrapeResponse(
        fund_url=result.get("fund_url", scrape_url),
        fund_name=result.get("fund_name", payload.fund_name),
        pages_scraped=result.get("pages_scraped"),
        visited_urls_count=result.get("visited_urls_count"),
        eligibility=result.get("eligibility"),
        error=result.get("error"),
        raw=result,
    )


@router.post("/batch", response_model=JobCreatedResponse, status_code=status.HTTP_202_ACCEPTED)
def scrape_batch(
    payload: BatchScrapeRequest,
    tools_module: tools = Depends(dependencies.get_tools_module),
) -> JobCreatedResponse:
    if not payload.fund_urls and not payload.rescrape_urls:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No URLs provided")

    raw_urls = [str(url) for url in payload.fund_urls]
    rescrape_urls = [str(url) for url in payload.rescrape_urls] if payload.rescrape_urls else []
    if rescrape_urls:
        raw_urls.extend(rescrape_urls)
    prepared = _prepare_urls_for_scrape(raw_urls, tools_module=tools_module, allow_rescrape=set(rescrape_urls))
    if not prepared["to_scrape"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="All provided URLs already exist in results or are duplicates.",
        )

    job = job_store.create(prepared["to_scrape"])
    return JobCreatedResponse(
        job_id=job.id,
        fund_urls=payload.fund_urls,
        to_scrape=prepared["to_scrape"],
        already_processed=prepared["already_processed"],
        duplicates_in_payload=prepared["duplicates_in_payload"],
        rescrape_urls=payload.rescrape_urls,
    )


@router.get("/jobs/{job_id}", response_model=JobStatusResponse)
def job_status(job_id: str):
    job = job_store.get(job_id)
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")

    snapshot = job.snapshot()
    errors = [JobError(url=err[0], message=err[1]) for err in snapshot["errors"]]
    # TODO: extend this endpoint (or add websockets/server-sent events) to push live status updates to clients.
    return JobStatusResponse(
        job_id=snapshot["job_id"],
        done=snapshot["done"],
        progress_percent=snapshot["progress_percent"],
        results=snapshot["results"],
        errors=errors,
        current_url=snapshot.get("current_url"),
        current_elapsed_seconds=snapshot.get("current_elapsed_seconds", 0),
        total_elapsed_seconds=snapshot.get("total_elapsed_seconds", 0),
        started_at=snapshot.get("started_at"),
        finished_at=snapshot.get("finished_at"),
        url_timings=snapshot.get("url_timings", []),
        total_urls=snapshot.get("total_urls", 0),
        completed_urls=snapshot.get("completed_urls", 0),
    )
