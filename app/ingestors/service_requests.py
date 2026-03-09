import logging
from datetime import datetime, timezone
from typing import Optional

import httpx

from app.config import settings
from app.ingestors.base import BaseIngestor
from app.models.service_request import ServiceRequestRecord

logger = logging.getLogger(__name__)


def _parse_arcgis_date(value: Optional[int | str]) -> Optional[datetime]:
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc)
    except (ValueError, TypeError, OSError):
        return None


def _extract_request(feature: dict) -> Optional[ServiceRequestRecord]:
    props = feature.get("properties") or {}
    geom = feature.get("geometry") or {}

    if geom.get("type") != "Point":
        return None

    coords = geom.get("coordinates", [])
    if len(coords) < 2:
        return None

    lon, lat = float(coords[0]), float(coords[1])
    if lat == 0.0 and lon == 0.0:
        return None

    # Support both 311 field names and Code_Enforcement field names
    case_type = (
        props.get("Service_Request_Type")
        or props.get("CaseType")
    )
    status = props.get("Status") or props.get("CaseStatus")
    address = props.get("Address")
    date_val = props.get("Created_Date") or props.get("CaseDate")

    return ServiceRequestRecord(
        case_type=case_type,
        status=status,
        address=address,
        case_date=_parse_arcgis_date(date_val),
        latitude=lat,
        longitude=lon,
    )


class ServiceRequestsIngestor(BaseIngestor):
    PAGE_SIZE = 2000

    async def fetch(self) -> list[ServiceRequestRecord]:
        url, out_fields = await self._resolve_endpoint()
        return await self._paginate(url, out_fields)

    async def _resolve_endpoint(self) -> tuple[str, str]:
        """Try the 311 endpoint first; fall back to Code_Enforcement_view."""
        async with httpx.AsyncClient(timeout=10.0) as client:
            try:
                probe = await client.get(
                    settings.arcgis_311_url.replace("/query", ""),
                    params={"f": "json"},
                )
                if probe.status_code == 200 and "error" not in probe.json():
                    logger.info("Using Received_311_Service_Request endpoint.")
                    return (
                        settings.arcgis_311_url,
                        "Service_Request_Type,Status,Address,Created_Date",
                    )
            except Exception:
                pass

        logger.warning(
            "Received_311_Service_Request not available — "
            "falling back to Code_Enforcement_view."
        )
        return (
            settings.arcgis_code_enforcement_url,
            "CaseType,CaseStatus,Address,CaseDate,CouncilDistrict",
        )

    async def _paginate(self, url: str, out_fields: str) -> list[ServiceRequestRecord]:
        records: list[ServiceRequestRecord] = []
        offset = 0

        async with httpx.AsyncClient(timeout=30.0) as client:
            while True:
                params = {
                    "where": "1=1",
                    "outFields": out_fields,
                    "returnGeometry": "true",
                    "f": "geojson",
                    "outSR": "4326",
                    "resultRecordCount": self.PAGE_SIZE,
                    "resultOffset": offset,
                }

                resp = await client.get(url, params=params)
                resp.raise_for_status()
                data = resp.json()

                features = data.get("features", [])
                for feature in features:
                    record = _extract_request(feature)
                    if record:
                        records.append(record)

                if len(features) < self.PAGE_SIZE:
                    break

                offset += self.PAGE_SIZE

        return records
