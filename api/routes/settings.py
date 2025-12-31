from fastapi import APIRouter, Depends

from api import dependencies
from api.schemas import UpdateOpenAIKeyRequest, UpdateOpenAIKeyResponse
from utils import tools

router = APIRouter(prefix="/settings", tags=["settings"])


@router.post("/openai", response_model=UpdateOpenAIKeyResponse)
def update_openai_key(
    payload: UpdateOpenAIKeyRequest,
    tools_module: tools = Depends(dependencies.get_tools_module),
) -> UpdateOpenAIKeyResponse:
    """
    Override the OpenAI API key for the current runtime session.
    Service account and sheet ID remain server-managed.
    """
    key = (payload.openai_api_key or "").strip()
    dependencies.settings.openai_api_key = key
    tools_module.configure_tools(openai_api_key=key)
    return UpdateOpenAIKeyResponse(status="ok", openai_api_key_set=bool(key))
