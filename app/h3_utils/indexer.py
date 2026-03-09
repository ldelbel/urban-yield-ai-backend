import asyncio
import logging
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Optional

import h3
import httpx

from app.models.business_license import BusinessLicenseRecord
from app.models.hexagon import HexCell
from app.models.permit import PermitRecord
from app.models.service_request import ServiceRequestRecord
from app.models.vacancy import VacancyRecord

logger = logging.getLogger(__name__)

_ZONING_BATCH = 20  # concurrent spatial queries per batch


def point_to_h3(lat: float, lon: float, resolution: int = 8) -> str:
    return h3.latlng_to_cell(lat, lon, resolution)


def aggregate_permits_to_hexagons(
    permits: list[PermitRecord], resolution: int = 8
) -> dict[str, HexCell]:
    cells: dict[str, dict] = {}

    for permit in permits:
        h3_id = point_to_h3(permit.latitude, permit.longitude, resolution)
        permit.h3_index = h3_id

        if h3_id not in cells:
            cells[h3_id] = {
                "permit_count": 0,
                "active_permit_count": 0,
                "declared_values": [],
                "latest_issue_date": None,
            }

        cell = cells[h3_id]
        cell["permit_count"] += 1

        if permit.status and permit.status.upper() in ("ACTIVE", "ISSUED", "OPEN"):
            cell["active_permit_count"] += 1

        if permit.declared_value is not None and permit.declared_value > 0:
            cell["declared_values"].append(permit.declared_value)

        if permit.issue_date:
            if cell["latest_issue_date"] is None or permit.issue_date > cell["latest_issue_date"]:
                cell["latest_issue_date"] = permit.issue_date

    result: dict[str, HexCell] = {}
    for h3_id, data in cells.items():
        vals = data["declared_values"]
        total_val: Optional[float] = sum(vals) if vals else None
        avg_val: Optional[float] = sum(vals) / len(vals) if vals else None

        result[h3_id] = HexCell(
            h3_index=h3_id,
            resolution=resolution,
            permit_count=data["permit_count"],
            total_declared_value=total_val,
            avg_declared_value=avg_val,
            active_permit_count=data["active_permit_count"],
            latest_issue_date=data["latest_issue_date"],
        )

    return result


def aggregate_businesses_to_hexagons(
    businesses: list[BusinessLicenseRecord],
    cells: dict[str, HexCell],
    resolution: int = 8,
) -> dict[str, HexCell]:
    for biz in businesses:
        h3_id = point_to_h3(biz.latitude, biz.longitude, resolution)
        biz.h3_index = h3_id
        if h3_id in cells:
            cells[h3_id].business_count += 1

    return cells


def aggregate_service_requests_to_hexagons(
    requests: list[ServiceRequestRecord],
    cells: dict[str, HexCell],
    resolution: int = 8,
) -> dict[str, HexCell]:
    type_counters: dict[str, Counter] = {}
    chronic_threshold = datetime.now(tz=timezone.utc) - timedelta(days=180)

    for req in requests:
        h3_id = point_to_h3(req.latitude, req.longitude, resolution)
        req.h3_index = h3_id
        if h3_id not in cells:
            continue

        cells[h3_id].service_request_count += 1

        # Chronicity: open case older than 180 days
        if (
            req.status
            and req.status.casefold() == "open"
            and req.case_date is not None
            and req.case_date < chronic_threshold
        ):
            cells[h3_id].chronic_case_count += 1

        if req.case_type:
            if h3_id not in type_counters:
                type_counters[h3_id] = Counter()
            type_counters[h3_id][req.case_type] += 1

    for h3_id, counter in type_counters.items():
        if h3_id in cells and counter:
            cells[h3_id].dominant_311_type_breakdown = dict(counter)

    return cells


def aggregate_vacancies_to_hexagons(
    vacancies: list[VacancyRecord],
    cells: dict[str, HexCell],
    resolution: int = 8,
) -> dict[str, HexCell]:
    for vac in vacancies:
        h3_id = point_to_h3(vac.latitude, vac.longitude, resolution)
        vac.h3_index = h3_id
        if h3_id in cells:
            cells[h3_id].vacant_count += 1

    return cells


async def assign_zoning_to_hexagons(
    cells: dict[str, HexCell],
    zoning_url: str,
) -> dict[str, HexCell]:
    """
    Spatial join: for each hexagon centroid, query ArcGIS to find the
    intersecting zoning polygon and assign its ZoningCode.
    Runs in batches to avoid overwhelming the API.
    """
    h3_ids = list(cells.keys())

    async def _query_zone(client: httpx.AsyncClient, h3_id: str) -> tuple[str, Optional[str]]:
        lat, lng = h3.cell_to_latlng(h3_id)
        try:
            resp = await client.get(
                zoning_url,
                params={
                    "geometry": f"{lng},{lat}",
                    "geometryType": "esriGeometryPoint",
                    "inSR": "4326",
                    "spatialRel": "esriSpatialRelIntersects",
                    "outFields": "ZoningCode",
                    "returnGeometry": "false",
                    "f": "json",
                },
                timeout=10.0,
            )
            data = resp.json()
            features = data.get("features", [])
            if features:
                code = features[0].get("attributes", {}).get("ZoningCode")
                return h3_id, code
        except Exception as exc:
            logger.warning(f"Zoning query failed for {h3_id}: {exc}")
        return h3_id, None

    async with httpx.AsyncClient(timeout=15.0) as client:
        for i in range(0, len(h3_ids), _ZONING_BATCH):
            batch = h3_ids[i : i + _ZONING_BATCH]
            results = await asyncio.gather(*[_query_zone(client, hid) for hid in batch])
            for h3_id, zoning_code in results:
                if zoning_code:
                    cells[h3_id].primary_zoning = zoning_code

    assigned = sum(1 for c in cells.values() if c.primary_zoning)
    logger.info(f"Zoning assigned to {assigned}/{len(cells)} hexagons.")
    return cells


async def assign_flood_zones_to_hexagons(
    cells: dict[str, HexCell],
    flood_url: Optional[str],
) -> dict[str, HexCell]:
    """
    Spatial join: mark hexagons that fall within flood zone polygons.
    If flood_url is None/empty (no catalog match), returns cells unchanged.
    """
    if not flood_url:
        logger.info("assign_flood_zones_to_hexagons: no URL provided, skipping.")
        return cells

    h3_ids = list(cells.keys())

    async def _query_flood(client: httpx.AsyncClient, h3_id: str) -> tuple[str, bool]:
        lat, lng = h3.cell_to_latlng(h3_id)
        try:
            resp = await client.get(
                flood_url,
                params={
                    "geometry": f"{lng},{lat}",
                    "geometryType": "esriGeometryPoint",
                    "inSR": "4326",
                    "spatialRel": "esriSpatialRelIntersects",
                    "outFields": "OBJECTID",
                    "returnGeometry": "false",
                    "f": "json",
                },
                timeout=10.0,
            )
            data = resp.json()
            return h3_id, bool(data.get("features"))
        except Exception as exc:
            logger.warning(f"Flood zone query failed for {h3_id}: {exc}")
        return h3_id, False

    async with httpx.AsyncClient(timeout=15.0) as client:
        for i in range(0, len(h3_ids), _ZONING_BATCH):
            batch = h3_ids[i : i + _ZONING_BATCH]
            results = await asyncio.gather(*[_query_flood(client, hid) for hid in batch])
            for h3_id, in_flood in results:
                if in_flood:
                    cells[h3_id].is_flood_zone = True

    flooded = sum(1 for c in cells.values() if c.is_flood_zone)
    logger.info(f"Flood zones assigned to {flooded}/{len(cells)} hexagons.")
    return cells


async def assign_historic_districts_to_hexagons(
    cells: dict[str, HexCell],
    historic_url: Optional[str],
) -> dict[str, HexCell]:
    """
    Spatial join: mark hexagons that fall within historic district polygons.
    If historic_url is None/empty (no catalog match), returns cells unchanged.
    """
    if not historic_url:
        logger.info("assign_historic_districts_to_hexagons: no URL provided, skipping.")
        return cells

    h3_ids = list(cells.keys())

    async def _query_historic(client: httpx.AsyncClient, h3_id: str) -> tuple[str, bool]:
        lat, lng = h3.cell_to_latlng(h3_id)
        try:
            resp = await client.get(
                historic_url,
                params={
                    "geometry": f"{lng},{lat}",
                    "geometryType": "esriGeometryPoint",
                    "inSR": "4326",
                    "spatialRel": "esriSpatialRelIntersects",
                    "outFields": "OBJECTID",
                    "returnGeometry": "false",
                    "f": "json",
                },
                timeout=10.0,
            )
            data = resp.json()
            return h3_id, bool(data.get("features"))
        except Exception as exc:
            logger.warning(f"Historic district query failed for {h3_id}: {exc}")
        return h3_id, False

    async with httpx.AsyncClient(timeout=15.0) as client:
        for i in range(0, len(h3_ids), _ZONING_BATCH):
            batch = h3_ids[i : i + _ZONING_BATCH]
            results = await asyncio.gather(*[_query_historic(client, hid) for hid in batch])
            for h3_id, in_historic in results:
                if in_historic:
                    cells[h3_id].is_historic_district = True

    historic = sum(1 for c in cells.values() if c.is_historic_district)
    logger.info(f"Historic districts assigned to {historic}/{len(cells)} hexagons.")
    return cells


def assign_census_data_to_hexagons(
    cells: dict[str, HexCell],
    census_data: dict[str, dict],
    tract_geometries: dict[str, object],  # shapely geometries
) -> dict[str, HexCell]:
    from shapely.geometry import Point  # local import for optional dep safety
    assigned = 0
    for h3_id, cell in cells.items():
        lat, lng = h3.cell_to_latlng(h3_id)
        pt = Point(lng, lat)  # shapely: x=lng, y=lat
        for geoid, poly in tract_geometries.items():
            if poly.contains(pt):
                data = census_data.get(geoid, {})
                cell.census_median_income = data.get("median_household_income")
                cell.census_median_home_value = data.get("median_home_value")
                cell.census_median_rent = data.get("median_gross_rent")
                cell.census_total_population = data.get("total_population")
                cell.census_vacancy_rate = data.get("vacancy_rate")
                assigned += 1
                break
    logger.info(f"Census data assigned to {assigned}/{len(cells)} hexagons.")
    return cells


def hexcell_to_geojson_feature(cell: HexCell) -> dict:
    boundary = h3.cell_to_boundary(cell.h3_index)
    # h3 returns (lat, lng) pairs; GeoJSON needs [lng, lat]
    coordinates = [[lng, lat] for lat, lng in boundary]
    coordinates.append(coordinates[0])  # close the ring

    return {
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [coordinates],
        },
        "properties": {
            "h3_index": cell.h3_index,
            "uvi_score": cell.uvi_score,
            "uvi_rank": cell.uvi_rank,
            "uvi_percentile": cell.uvi_percentile,
            "yield_score": cell.yield_score,
            "yield_label": cell.yield_label,
            "permit_count": cell.permit_count,
            "active_permit_count": cell.active_permit_count,
            "business_count": cell.business_count,
            "primary_zoning": cell.primary_zoning,
            "vacant_count": cell.vacant_count,
            "total_declared_value": cell.total_declared_value,
            "avg_declared_value": cell.avg_declared_value,
            "zillow_avg_price_sqft": cell.zillow_avg_price_sqft,
            "zillow_days_on_market": cell.zillow_days_on_market,
            "zillow_price_reduction_count": cell.zillow_price_reduction_count,
            "gmaps_avg_rating": cell.gmaps_avg_rating,
            "gmaps_review_count": cell.gmaps_review_count,
            "gmaps_permanently_closed_count": cell.gmaps_permanently_closed_count,
            "service_request_count": cell.service_request_count,
            "chronic_case_count": cell.chronic_case_count,
            "dominant_311_type_breakdown": cell.dominant_311_type_breakdown,
            "is_flood_zone": cell.is_flood_zone,
            "is_historic_district": cell.is_historic_district,
            "is_infrastructure_priority": cell.is_infrastructure_priority,
            "is_infill_opportunity": cell.is_infill_opportunity,
            "census_median_income": cell.census_median_income,
            "census_median_home_value": cell.census_median_home_value,
            "census_median_rent": cell.census_median_rent,
            "census_total_population": cell.census_total_population,
            "census_vacancy_rate": cell.census_vacancy_rate,
            "census_coverage": cell.census_median_income is not None,
        },
    }


def hexcells_to_geojson(cells: dict[str, HexCell]) -> dict:
    return {
        "type": "FeatureCollection",
        "features": [hexcell_to_geojson_feature(cell) for cell in cells.values()],
    }
