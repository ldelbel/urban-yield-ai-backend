# Urban Yield AI — Backend

> **Hackathon:** GenAI.Works "World Wide Vibes" — March 2026
> **Track:** Smart Cities
> **City:** Montgomery, Alabama

Urban Yield AI is a geospatial intelligence platform for city planners and economic development staff. It ingests real-time municipal data, aggregates it into H3 hexagonal tiles, and computes composite scores that reveal where investment is accelerating, stagnating, or structurally constrained — surfacing actionable alerts and AI-generated narrative briefings for each neighborhood.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Frontend (Next.js + MapLibre GL)  — port 3000          │
│  https://github.com/ldelbel/urban-yield-ai-frontend     │
└─────────────────────┬───────────────────────────────────┘
                      │ REST API
┌─────────────────────▼───────────────────────────────────┐
│  Backend (FastAPI + Python)  — port 8000                │
│  6-phase data pipeline on startup                       │
│    Phase 1: Concurrent ingestion (9 sources)            │
│    Phase 2: H3 hex aggregation (resolution 8)           │
│    Phase 3: Spatial assignment (zoning, Census tracts)  │
│    Phase 4: UVI + Yield scoring                         │
│    Phase 5: BrightData enrichment (GMaps + Zillow)      │
│    Phase 6: Final rescore with market signals           │
└─────────────────────┬───────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────┐
│  Data Sources                                           │
│  • ArcGIS (Montgomery AL city services)                 │
│  • Census Bureau ACS 5-year API                         │
│  • TIGERweb tract boundaries                            │
│  • BrightData (Google Maps + Zillow via Claude Haiku)   │
└─────────────────────────────────────────────────────────┘
```

---

## Data Sources (9 total)

| # | Source | Data |
|---|--------|------|
| 1 | ArcGIS — Building Permits | Permit counts + declared value |
| 2 | ArcGIS — Business Licenses | Active business locations |
| 3 | ArcGIS — 311 / Code Enforcement | Service request volume + chronic cases |
| 4 | ArcGIS — Vacant Properties | Parcel-level vacancy |
| 5 | ArcGIS — Zoning | Primary zoning classification per hexagon |
| 6 | Census ACS 5-year | Median income, home value, rent, population, vacancy rate |
| 7 | TIGERweb | Census tract boundaries for spatial join |
| 8 | BrightData → Google Maps | Business ratings, review counts, closures |
| 9 | BrightData → Zillow | Price/sqft, days on market, price reductions |

---

## Scoring Model

### Urban Vitality Index (UVI, 0–100)

```
UVI = 0.30 × Investment
    + 0.20 × Sentiment
    + 0.20 × Market
    + 0.15 × Risk (inverted)
    + 0.10 × SocioEconomic
    + 0.05 × Density
```

| Signal | Source |
|--------|--------|
| Investment (30%) | Total declared permit value |
| Sentiment (20%) | GMaps avg rating × review volume, closed-biz penalty |
| Market (20%) | Zillow price/sqft, days on market, price reductions |
| Risk (15%, inverted) | 311 volume + chronic case rate |
| SocioEconomic (10%) | Median household income + median home value |
| Density (5%) | Population + inverse vacancy rate |

### Yield Score

```
momentum    = 0.30×Investment + 0.20×Sentiment + 0.10×SocioEconomic
baseline    = normalized permit_count (min 0.01)
yield_score = momentum / baseline
```

| Condition | Label |
|-----------|-------|
| `permit_count < 5` | Low Confidence |
| `yield_score > 1.1` | Accelerating |
| `0.8 ≤ yield_score ≤ 1.1` | Stable |
| `yield_score < 0.8` | Stagnating |
| Stagnating + flood zone + historic district | Structurally Constrained — Flood + Historic |

### Strategic Flags

| Flag | Condition |
|------|-----------|
| Infrastructure Priority | Accelerating AND 311 load > 1.4× city average |
| Infill Opportunity | B-zoned AND Stagnating AND vacant parcels present |

---

## Local Development

### Prerequisites

- Python ≥ 3.11
- `pip install -r requirements.txt`

### Environment Variables

Copy `.env.example` to `.env` and fill in values:

```bash
cp .env.example .env
```

### Start the server

```bash
uvicorn app.main:app --reload
# → http://localhost:8000
```

The server runs the full 6-phase pipeline on startup (~10–20 seconds depending on ArcGIS response times). After startup, `/health` will report hexagon and tract counts.

### Scripts

```bash
# Rebuild GMaps enrichment from BrightData exports
python scripts/build_gmaps_cache.py

# Rebuild Zillow enrichment from BrightData exports
python scripts/build_zillow_cache.py
```

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Status + hexagon count + tract count |
| `GET` | `/api/hexagons` | GeoJSON FeatureCollection of all scored hexagons |
| `GET` | `/api/hexagons/{h3_id}` | Full detail for one hexagon |
| `GET` | `/api/alerts` | Infrastructure priorities + infill opportunities |
| `GET` | `/api/insights/{h3_id}` | AI narrative briefing (cached or stub) |
| `POST` | `/api/refresh` | Trigger full re-ingest + re-score |
| `POST` | `/api/admin/enrich` | Trigger live BrightData enrichment |
| `GET` | `/api/admin/enrich/status` | Enrichment run status |

---

## Environment Variables

```env
# .env.example

# ArcGIS base URL (Montgomery AL city services)
ARCGIS_SERVICES_BASE_URL=https://services7.arcgis.com/xNUwUjOJqYE54USz/ArcGIS/rest/services

# BrightData MCP token (for live enrichment)
BRIGHTDATA_API_TOKEN=your_token_here

# Anthropic API key (for AI narrative generation)
ANTHROPIC_API_KEY=your_key_here
```

---

## Built With

| Technology | Role |
|------------|------|
| [FastAPI](https://fastapi.tiangolo.com/) | API framework |
| [H3](https://h3geo.org/) | Hexagonal spatial indexing (resolution 8) |
| [Shapely](https://shapely.readthedocs.io/) | Geometry + spatial joins |
| [Anthropic Claude](https://www.anthropic.com/) | AI narrative generation (Claude Haiku) |
| [BrightData](https://brightdata.com/) | Google Maps + Zillow data via MCP |
| [Census ACS](https://www.census.gov/data/developers/data-sets/acs-5year.html) | Socioeconomic signals |
| [ArcGIS REST API](https://developers.arcgis.com/rest/) | Montgomery AL municipal data |

---

## Companion Repository

Frontend: [urban-yield-ai-frontend](https://github.com/ldelbel/urban-yield-ai-frontend)
