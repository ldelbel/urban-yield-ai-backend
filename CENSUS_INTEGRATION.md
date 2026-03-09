# Census ACS Integration — Urban Yield AI

## Overview

This document describes the Census Bureau ACS 5-Year data integration added to expand the Urban Vitality Index (UVI) from 4 signals to 6 signals. Census data covers all 638 hexagons via free public APIs — no API key required.

---

## API Endpoints

### ACS 5-Year Estimates
- **URL**: `https://api.census.gov/data/2022/acs/acs5`
- **Coverage**: Montgomery County, AL (state: 01, county: 101), all Census tracts
- **No API key required**

| ACS Variable   | Description                       |
|----------------|-----------------------------------|
| B19013_001E    | Median household income           |
| B25077_001E    | Median home value                 |
| B25064_001E    | Median gross rent                 |
| B25002_002E    | Occupied housing units            |
| B25002_003E    | Vacant housing units              |
| B01003_001E    | Total population                  |

Vacancy rate is derived: `vacant / (occupied + vacant)`.

### TIGERweb Tract Boundaries
- **URL**: `https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/Tracts_Blocks/MapServer/0/query`
- **Filter**: `STATE='01' AND COUNTY='101'`
- **Pagination**: 100 records per page until exhausted
- Returns GeoJSON polygons (EPSG:4326)

---

## Field Mapping

| HexCell field               | ACS source                    | Notes                          |
|-----------------------------|-------------------------------|--------------------------------|
| `census_median_income`      | B19013_001E                   | Sentinel -666666666 → None     |
| `census_median_home_value`  | B25077_001E                   | Sentinel -666666666 → None     |
| `census_median_rent`        | B25064_001E                   | Stored but not in UVI formula  |
| `census_total_population`   | B01003_001E                   |                                |
| `census_vacancy_rate`       | B25002_003E / (002E + 003E)   | Derived float [0.0, 1.0]       |
| `census_coverage`           | `census_median_income is not None` | Bool convenience flag     |

---

## New UVI Weights (6 signals, sum = 1.0)

| Signal         | Weight | Source                          |
|----------------|--------|---------------------------------|
| Investment     | 0.30   | Permit declared value / count   |
| Sentiment      | 0.20   | Google Maps avg rating          |
| Market         | 0.20   | Zillow price/sqft               |
| Risk           | 0.15   | 311 service requests (inverted) |
| SocioEconomic  | 0.10   | Census income + home value      |
| Density        | 0.05   | Census population + vacancy     |

### SocioEconomic Signal
`0.6 × norm_income + 0.4 × norm_home_value` (min-max normalized across all hexagons). Falls back gracefully if either component is missing. Defaults to 0.5 when Census API is unreachable.

### Density Signal
`0.5 × norm_population + 0.5 × (1 - vacancy_rate)`. Higher population and lower vacancy rate both indicate denser, more active neighborhoods.

---

## Yield Momentum Update

**Previous formula**: `W_INVESTMENT × inv + W_SENTIMENT × sent` → max = 0.35 + 0.25 = 0.60

**New formula**: `W_INVESTMENT × inv + W_SENTIMENT × sent + W_SOCIOECONOMIC × socio` → max = 0.30 + 0.20 + 0.10 = **0.60**

Same ceiling — label thresholds (`YIELD_ACCELERATING = 1.1`, `YIELD_STAGNATING = 0.8`) are unchanged.

---

## Why Census Data is Additive (Not a Replacement for BrightData)

BrightData (Zillow + Google Maps) captures **current market signals**:
- Zillow price/sqft reflects live buyer/seller demand
- Google Maps ratings reflect current business activity and sentiment

Census ACS captures **long-run neighborhood desirability**:
- Median income reflects structural wealth accumulation over years
- Home values in ACS are owner-reported and lag the live market
- Population density and vacancy patterns reveal underlying demand pressure

Together they give both **trend** (BrightData) and **context** (Census): a hexagon with high Zillow prices but low Census income signals gentrification risk; a hexagon with high Census income but low Zillow activity may be an underserved opportunity.

---

## Graceful Degradation

Both ingestors catch all exceptions and return `{}` on failure. If Census APIs are unreachable:
- `census_data = {}` → all Census fields remain `None`
- `tract_geometries = {}` → spatial join loop exits immediately, 0 hexagons assigned
- UVI formula falls back to 0.5 defaults for SocioEconomic and Density signals
- All BrightData-enriched signals are unaffected
- Server starts and serves data normally

---

## Verification

1. `POST /api/refresh` — logs show `"Census data assigned to ~600+/638 hexagons"`
2. `GET /health` → `"census_tracts_loaded": ~70`
3. `GET /api/hexagons/{h3_id}` → GeoJSON properties include all 5 census fields + `census_coverage`
4. `GET /api/hexagons/{enriched_h3_id}` → Zillow and GMaps values still present
5. Two hexagons in different parts of Montgomery have different `census_median_income` values
6. No hexagon has `census_median_income` set AND `census_coverage: false`
