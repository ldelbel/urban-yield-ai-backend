import logging
import httpx
from typing import Optional

logger = logging.getLogger(__name__)

ACS_URL = "https://api.census.gov/data/2022/acs/acs5"
ACS_FIELDS = "NAME,B19013_001E,B25077_001E,B25064_001E,B25002_002E,B25002_003E,B01003_001E"
CENSUS_NULL = -666666666


class CensusACSIngestor:
    async def fetch(self) -> dict[str, dict]:
        """Fetch ACS 5-year estimates for Montgomery County tracts. Returns {} on failure."""
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.get(ACS_URL, params={
                    "get": ACS_FIELDS,
                    "for": "tract:*",
                    "in": "state:01 county:101",
                })
                resp.raise_for_status()
                rows = resp.json()
        except Exception as exc:
            logger.warning(f"CensusACSIngestor: failed to fetch ACS data: {exc}")
            return {}

        headers, *data_rows = rows
        # header order: NAME, B19013_001E, B25077_001E, B25064_001E, B25002_002E, B25002_003E, B01003_001E, state, county, tract
        result = {}
        for row in data_rows:
            row_dict = dict(zip(headers, row))
            income = _safe_int(row_dict.get("B19013_001E"))
            home_val = _safe_int(row_dict.get("B25077_001E"))
            # Filter sentinel values
            if income == CENSUS_NULL:
                income = None
            if home_val == CENSUS_NULL:
                home_val = None

            occupied = _safe_int(row_dict.get("B25002_002E")) or 0
            vacant = _safe_int(row_dict.get("B25002_003E")) or 0
            total_units = occupied + vacant
            vacancy_rate = vacant / total_units if total_units > 0 else 0.0

            geoid = (row_dict.get("state", "") + row_dict.get("county", "") + row_dict.get("tract", "")).zfill(11)
            result[geoid] = {
                "median_household_income": income,
                "median_home_value": home_val,
                "median_gross_rent": _safe_int(row_dict.get("B25064_001E")),
                "total_population": _safe_int(row_dict.get("B01003_001E")),
                "vacancy_rate": vacancy_rate,
            }

        logger.info(f"CensusACSIngestor: loaded {len(result)} tracts.")
        return result


def _safe_int(val) -> Optional[int]:
    try:
        v = int(val)
        return v if v != CENSUS_NULL else None
    except (TypeError, ValueError):
        return None
