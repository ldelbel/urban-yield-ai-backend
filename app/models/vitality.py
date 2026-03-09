from typing import Optional

from pydantic import BaseModel


class UVIScore(BaseModel):
    h3_index: str
    uvi_score: float
    investment_signal: float
    sentiment_signal: float
    market_signal: float
    risk_signal: float


class YieldScore(BaseModel):
    h3_index: str
    yield_score: float
    yield_label: str  # "Accelerating" | "Stagnating" | "Stable"
    momentum: float
    baseline_density: float


class HexagonResponse(BaseModel):
    h3_index: str
    uvi_score: Optional[float]
    uvi_rank: int
    uvi_percentile: float
    yield_score: Optional[float]
    yield_label: Optional[str]
    permit_count: int
    active_permit_count: int
    business_count: int
    primary_zoning: Optional[str]
    vacant_count: int
    total_declared_value: Optional[float]
    avg_declared_value: Optional[float]
    zillow_avg_price_sqft: Optional[float]
    zillow_days_on_market: Optional[float] = None
    zillow_price_reduction_count: Optional[int] = None
    gmaps_avg_rating: Optional[float]
    gmaps_review_count: Optional[int] = None
    gmaps_permanently_closed_count: Optional[int] = None
    service_request_count: int
    chronic_case_count: int
    dominant_311_type_breakdown: dict[str, int]
    is_flood_zone: bool
    is_historic_district: bool
    is_infrastructure_priority: bool
    is_infill_opportunity: bool
    census_median_income: Optional[int] = None
    census_median_home_value: Optional[int] = None
    census_median_rent: Optional[int] = None
    census_total_population: Optional[int] = None
    census_vacancy_rate: Optional[float] = None
    census_coverage: bool = False
