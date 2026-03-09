from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class HexCell(BaseModel):
    h3_index: str
    resolution: int
    permit_count: int
    total_declared_value: Optional[float] = None  # None if all records null
    avg_declared_value: Optional[float] = None
    active_permit_count: int = 0
    latest_issue_date: Optional[datetime] = None
    business_count: int = 0
    # Land-use audit
    primary_zoning: Optional[str] = None
    vacant_count: int = 0
    # Enrichment signals (populated by BrightEnricher)
    zillow_avg_price_sqft: Optional[float] = None
    zillow_days_on_market: Optional[float] = None
    zillow_price_reduction_count: Optional[int] = None
    gmaps_avg_rating: Optional[float] = None
    gmaps_review_count: Optional[int] = None
    gmaps_permanently_closed_count: Optional[int] = None
    # Risk signals (populated by 311/code-enforcement ingestor)
    service_request_count: int = 0
    chronic_case_count: int = 0
    dominant_311_type_breakdown: dict[str, int] = {}
    # Structural constraint flags
    is_flood_zone: bool = False
    is_historic_district: bool = False
    # Computed scores
    uvi_score: Optional[float] = None
    uvi_rank: int = 0
    uvi_percentile: float = 0.0
    yield_score: Optional[float] = None
    yield_label: Optional[str] = None
    # Derived flags
    is_infrastructure_priority: bool = False
    is_infill_opportunity: bool = False
    # Census ACS signals (populated by assign_census_data_to_hexagons)
    census_median_income: Optional[int] = None
    census_median_home_value: Optional[int] = None
    census_median_rent: Optional[int] = None
    census_total_population: Optional[int] = None
    census_vacancy_rate: Optional[float] = None
