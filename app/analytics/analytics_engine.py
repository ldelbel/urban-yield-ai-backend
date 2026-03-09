import json
import logging
from pathlib import Path
from typing import Optional

from app.models.hexagon import HexCell

logger = logging.getLogger(__name__)

CACHE_PATH = Path("data/enrichment_cache.json")
NARRATIVE_SCHEMA_VERSION = "v3"


def _load_cache() -> dict:
    try:
        if CACHE_PATH.exists():
            return json.loads(CACHE_PATH.read_text())
    except Exception:
        pass
    return {}


def _fmt_currency(val: Optional[int], suffix="") -> str:
    if val is None:
        return "unavailable"
    return f"${val:,}{suffix}"


def _fmt_pct(val: Optional[float]) -> str:
    if val is None:
        return "unavailable"
    return f"{val * 100:.1f}%"


class AnalyticsEngine:
    async def generate_narrative(self, cell: HexCell) -> str:
        """Return cached narrative if available and schema version matches, otherwise a stub."""
        cache = _load_cache()
        entry = cache.get(cell.h3_index, {})
        if (
            "narrative" in entry
            and entry.get("narrative_schema_version") == NARRATIVE_SCHEMA_VERSION
        ):
            return entry["narrative"]
        return self._stub_narrative(cell)

    def _stub_narrative(self, cell: HexCell) -> str:
        label = cell.yield_label or "Stable"
        val = f"${cell.total_declared_value:,.0f}" if cell.total_declared_value else "N/A"

        # Derive top issue type from breakdown
        top_issue = "general complaints"
        if cell.dominant_311_type_breakdown:
            top_issue = max(cell.dominant_311_type_breakdown, key=cell.dominant_311_type_breakdown.get)

        # Build constraint-aware recommended action
        if cell.is_flood_zone and cell.is_historic_district:
            action = (
                "Contact the Planning Department to evaluate flood mitigation programs. "
                "Coordinate with the Historic Preservation Office before any structural intervention."
            )
        elif cell.is_flood_zone:
            action = (
                "Engage the Engineering Department to assess flood mitigation eligibility. "
                "Prioritize drainage improvements and FEMA floodplain management programs."
            )
        elif cell.is_historic_district:
            action = (
                "Contact the Historic Preservation Office to identify applicable grant programs. "
                "Ensure any rehabilitation plans comply with historic district guidelines."
            )
        elif cell.is_infrastructure_priority:
            action = (
                f"Escalate to Public Works for priority maintenance review of {top_issue} cases. "
                f"The {cell.chronic_case_count} chronic open cases (180d+) warrant a dedicated inspection sweep."
            )
        elif cell.is_infill_opportunity:
            action = (
                "Refer to the Economic Development Department for commercial infill incentive programs. "
                "Vacant parcel data supports rezoning or land bank engagement."
            )
        else:
            action = (
                f"Review {top_issue} service requests with the relevant city department. "
                "Monitor permit activity and business counts for trend changes."
            )

        return (
            f"SITUATION: Hexagon {cell.h3_index} is currently {label} with "
            f"{cell.permit_count} permits (declared value: {val}), "
            f"{cell.service_request_count} service requests, "
            f"median income {_fmt_currency(cell.census_median_income)}, "
            f"median home value {_fmt_currency(cell.census_median_home_value)}, "
            f"and a vacancy rate of {_fmt_pct(cell.census_vacancy_rate)}.\n"
            f"ROOT CAUSE: {cell.chronic_case_count} chronic open cases (180d+) and "
            f"a dominant issue type of '{top_issue}' suggest structural "
            f"{'constraint' if cell.is_flood_zone or cell.is_historic_district else 'service delivery gap'}.\n"
            f"RECOMMENDED ACTION: {action}"
        )
