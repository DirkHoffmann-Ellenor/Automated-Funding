from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api import dependencies
from api.config import settings
from api.routes import health, results, scrape, settings


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    logging.basicConfig(level=settings.log_level)
    # Temporarily disable startup dependency configuration to avoid long hangs in Azure.
    # Re-enable once secrets/config are confirmed working.
    # dependencies.ensure_configured()
    yield


def create_app() -> FastAPI:
    app = FastAPI(title="Automated Funding API", lifespan=lifespan)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(health.router)
    app.include_router(results.router)
    app.include_router(scrape.router)
    app.include_router(settings.router)

    return app


app = create_app()
