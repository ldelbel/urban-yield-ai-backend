import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.enrichers.bright_enricher import BrightEnricher
from app.state import AppState

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin", tags=["admin"])

# ---------------------------------------------------------------------------
# Enrichment state (module-level, single-server safe)
# ---------------------------------------------------------------------------

_enrich_running: bool = False
_total_enriched: int = 0
_last_cell_completed_at: Optional[datetime] = None

_SAFE_DEFAULT_MAX_CELLS = 2  # used when max_cells is missing or invalid


class EnrichRequest(BaseModel):
    max_cells: Optional[int] = None
    delay_between_cells: float = 3.0


async def _run_enrichment(max_cells: int, delay_between_cells: float) -> None:
    global _enrich_running, _total_enriched, _last_cell_completed_at
    try:
        cells = AppState.get_cells()

        def _on_cell(enriched_count: int) -> None:
            global _total_enriched, _last_cell_completed_at
            _total_enriched = enriched_count
            _last_cell_completed_at = datetime.now(timezone.utc)

        enriched_cells = await BrightEnricher().enrich_all(
            cells,
            max_cells=max_cells,
            delay_between_cells=delay_between_cells,
            presentation_mode=False,
            on_cell_complete=_on_cell,
        )
        # Final sync in case the last cell didn't trigger the callback
        # (e.g. it was already cached and skipped)
        _total_enriched = sum(
            1 for c in enriched_cells.values() if c.zillow_avg_price_sqft is not None
        )
        _last_cell_completed_at = datetime.now(timezone.utc)
        logger.info(
            f"Admin enrichment complete — {_total_enriched} hexagons now have Zillow data."
        )
    except Exception as exc:
        logger.error(f"Admin enrichment failed: {exc}", exc_info=True)
    finally:
        _enrich_running = False


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("/enrich")
async def trigger_enrichment(request: EnrichRequest):
    """
    Trigger a live BrightData enrichment run in the background.

    - Processes one hexagon at a time (sequential, not concurrent).
    - Returns immediately; poll GET /api/admin/enrich/status for progress.
    - Only one run may be active at a time (409 if already running).
    - max_cells defaults to 2 if not provided or invalid — an explicit
      positive integer is required to enrich more cells.
    """
    global _enrich_running

    if _enrich_running:
        raise HTTPException(
            status_code=409,
            detail="Enrichment is already running. Check GET /api/admin/enrich/status.",
        )

    cells = AppState.get_cells()
    if not cells:
        raise HTTPException(status_code=503, detail="No cell data loaded yet.")

    # Safety: require an explicit positive integer for max_cells.
    # Default to 2 if missing or invalid to prevent accidental full runs.
    max_cells = request.max_cells
    if not isinstance(max_cells, int) or max_cells < 1:
        logger.warning(
            f"POST /api/admin/enrich called without a valid max_cells "
            f"(got {request.max_cells!r}) — defaulting to {_SAFE_DEFAULT_MAX_CELLS} "
            f"as a safety measure."
        )
        max_cells = _SAFE_DEFAULT_MAX_CELLS

    queued = sum(
        1 for c in cells.values()
        if c.zillow_avg_price_sqft is None or c.gmaps_avg_rating is None
    )
    queued = min(queued, max_cells)

    _enrich_running = True
    asyncio.create_task(_run_enrichment(max_cells, request.delay_between_cells))

    return {"status": "started", "queued": queued, "max_cells": max_cells}


@router.get("/enrich/status")
async def get_enrichment_status():
    """Return the current enrichment run status."""
    cells = AppState.get_cells()
    return {
        "is_running": _enrich_running,
        "total_enriched": _total_enriched,
        "total_cells": len(cells),
        "last_cell_completed_at": (
            _last_cell_completed_at.isoformat() if _last_cell_completed_at else None
        ),
    }
