from datetime import datetime

from fastapi import APIRouter, Depends, Query, Response

from api import dependencies
from api.schemas import RefreshResultsResponse, ResultsResponse, StaleResultsResponse
from utils import tools

router = APIRouter(prefix="/results", tags=["results"])


@router.get("/", response_model=ResultsResponse)
def list_results(
    response: Response,
    force_refresh: bool = Query(False),
    tools_module: tools = Depends(dependencies.get_tools_module),
) -> ResultsResponse:
    if force_refresh:
        tools_module.clear_results_cache()
    df = tools_module.load_results_csv(force_refresh=force_refresh)
    df = tools_module.latest_results_by_url(df)
    records = df.to_dict(orient="records") if not df.empty else []
    response.headers["Cache-Control"] = "no-store"
    return ResultsResponse(results=records)


@router.get("/stale", response_model=StaleResultsResponse)
def list_stale_results(
    response: Response,
    months: int = Query(3, ge=1, le=24),
    force_refresh: bool = Query(False),
    tools_module: tools = Depends(dependencies.get_tools_module),
) -> StaleResultsResponse:
    if force_refresh:
        tools_module.clear_results_cache()
    df = tools_module.load_results_csv(force_refresh=force_refresh)
    stale_df = tools_module.stale_results_by_url(df, months=months)
    records = stale_df.to_dict(orient="records") if not stale_df.empty else []
    cutoff = tools_module.subtract_months(datetime.now(), months).isoformat()
    response.headers["Cache-Control"] = "no-store"
    return StaleResultsResponse(results=records, months=months, cutoff_timestamp=cutoff)


@router.post("/refresh", response_model=RefreshResultsResponse)
def refresh_results(response: Response, tools_module: tools = Depends(dependencies.get_tools_module)) -> RefreshResultsResponse:
    tools_module.clear_results_cache()
    df = tools_module.load_results_csv(force_refresh=True)
    response.headers["Cache-Control"] = "no-store"
    return RefreshResultsResponse(total_results=len(df.index))
