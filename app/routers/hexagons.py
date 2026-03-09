from fastapi import APIRouter, BackgroundTasks, HTTPException

from app.analytics.analytics_engine import AnalyticsEngine
from app.config import settings
from app.h3_utils.indexer import hexcells_to_geojson
from app.models.vitality import HexagonResponse
from app.state import AppState

router = APIRouter(prefix="/api", tags=["hexagons"])
analytics = AnalyticsEngine()


@router.get("/hexagons")
async def get_hexagons():
    """Return a GeoJSON FeatureCollection of all scored H3 hexagons."""
    cells = AppState.get_cells()
    if not cells:
        raise HTTPException(status_code=503, detail="Data not yet loaded. POST /api/refresh to trigger ingestion.")
    return hexcells_to_geojson(cells)


@router.get("/hexagons/{h3_id}", response_model=HexagonResponse)
async def get_hexagon(h3_id: str):
    """Return the full detail of a single H3 hexagon."""
    cells = AppState.get_cells()
    cell = cells.get(h3_id)
    if not cell:
        raise HTTPException(status_code=404, detail=f"Hexagon {h3_id!r} not found.")
    return HexagonResponse(
        h3_index=cell.h3_index,
        uvi_score=cell.uvi_score,
        uvi_rank=cell.uvi_rank,
        uvi_percentile=cell.uvi_percentile,
        yield_score=cell.yield_score,
        yield_label=cell.yield_label,
        permit_count=cell.permit_count,
        active_permit_count=cell.active_permit_count,
        business_count=cell.business_count,
        primary_zoning=cell.primary_zoning,
        vacant_count=cell.vacant_count,
        total_declared_value=cell.total_declared_value,
        avg_declared_value=cell.avg_declared_value,
        zillow_avg_price_sqft=cell.zillow_avg_price_sqft,
        zillow_days_on_market=cell.zillow_days_on_market,
        zillow_price_reduction_count=cell.zillow_price_reduction_count,
        gmaps_avg_rating=cell.gmaps_avg_rating,
        gmaps_review_count=cell.gmaps_review_count,
        gmaps_permanently_closed_count=cell.gmaps_permanently_closed_count,
        service_request_count=cell.service_request_count,
        chronic_case_count=cell.chronic_case_count,
        dominant_311_type_breakdown=cell.dominant_311_type_breakdown,
        is_flood_zone=cell.is_flood_zone,
        is_historic_district=cell.is_historic_district,
        is_infrastructure_priority=cell.is_infrastructure_priority,
        is_infill_opportunity=cell.is_infill_opportunity,
        census_median_income=cell.census_median_income,
        census_median_home_value=cell.census_median_home_value,
        census_median_rent=cell.census_median_rent,
        census_total_population=cell.census_total_population,
        census_vacancy_rate=cell.census_vacancy_rate,
        census_coverage=cell.census_median_income is not None,
    )


@router.get("/alerts")
async def get_alerts():
    """Return hexagons flagged as infrastructure priority or infill opportunity."""
    cells = AppState.get_cells()
    if not cells:
        raise HTTPException(status_code=503, detail="Data not yet loaded.")

    return {
        "infrastructure_priority": [
            {
                "h3_index": h,
                "yield_label": c.yield_label,
                "service_request_count": c.service_request_count,
                "chronic_case_count": c.chronic_case_count,
                "dominant_311_type_breakdown": c.dominant_311_type_breakdown,
                "uvi_score": c.uvi_score,
            }
            for h, c in cells.items() if c.is_infrastructure_priority
        ],
        "infill_opportunities": [
            {
                "h3_index": h,
                "primary_zoning": c.primary_zoning,
                "vacant_count": c.vacant_count,
                "yield_score": c.yield_score,
                "business_count": c.business_count,
            }
            for h, c in cells.items() if c.is_infill_opportunity
        ],
    }


@router.get("/insights/{h3_id}")
async def get_insights(h3_id: str):
    """Return an AI-generated narrative for a specific hexagon."""
    cells = AppState.get_cells()
    cell = cells.get(h3_id)
    if not cell:
        raise HTTPException(status_code=404, detail=f"Hexagon {h3_id!r} not found.")
    narrative = await analytics.generate_narrative(cell)
    return {"h3_index": h3_id, "narrative": narrative}


@router.post("/refresh")
async def refresh_data(background_tasks: BackgroundTasks):
    """Trigger a full re-ingest and re-score cycle in the background."""
    background_tasks.add_task(AppState.refresh)
    return {"status": "refresh started"}
