import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.routers import admin, hexagons
from app.state import AppState

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Urban Yield AI backend starting — triggering initial data load...")
    await AppState.refresh()
    yield
    logger.info("Shutting down.")


app = FastAPI(
    title="Urban Yield AI",
    description="Geospatial intelligence platform for City of Montgomery, AL",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(hexagons.router)
app.include_router(admin.router)


@app.get("/health")
async def health():
    from datetime import datetime, timezone
    from app.state import _cells, _census_tract_count
    return {
        "status": "ok",
        "hexagon_count": len(_cells),
        "census_tracts_loaded": _census_tract_count,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
