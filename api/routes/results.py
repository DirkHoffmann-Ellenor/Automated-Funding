from fastapi import APIRouter, Depends

from api import dependencies
from api.schemas import ResultsResponse
from utils import tools

router = APIRouter(prefix="/results", tags=["results"])


@router.get("/", response_model=ResultsResponse)
def list_results(tools_module: tools = Depends(dependencies.get_tools_module)) -> ResultsResponse:
    df = tools_module.load_results_csv()
    records = df.to_dict(orient="records") if not df.empty else []
    return ResultsResponse(results=records)
