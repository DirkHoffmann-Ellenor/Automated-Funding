from fastapi import APIRouter, Depends, HTTPException, status

from api import dependencies
from api.jobs import job_store
from api.schemas import (
    BatchScrapeRequest,
    JobCreatedResponse,
    JobError,
    JobStatusResponse,
    ScrapeRequest,
    ScrapeResponse,
)
from utils import tools

router = APIRouter(prefix="/scrape", tags=["scrape"])


@router.post("/single", response_model=ScrapeResponse, status_code=status.HTTP_200_OK)
def scrape_single(
    payload: ScrapeRequest,
    tools_module: tools = Depends(dependencies.get_tools_module),
) -> ScrapeResponse:
    result = tools_module.process_single_fund(str(payload.fund_url), payload.fund_name)
    return ScrapeResponse(
        fund_url=result.get("fund_url", payload.fund_url),
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
    if not payload.fund_urls:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No URLs provided")

    # ensure tools configured (side effect of dependency)
    _ = tools_module
    job = job_store.create([str(url) for url in payload.fund_urls])
    return JobCreatedResponse(job_id=job.id, fund_urls=payload.fund_urls)


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
    )
