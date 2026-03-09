from datetime import datetime, timezone
from typing import Optional

import httpx

from app.config import settings
from app.ingestors.base import BaseIngestor
from app.models.permit import PermitRecord


def _parse_arcgis_date(value: Optional[int | str]) -> Optional[datetime]:
    if value is None:
        return None
    try:
        ms = int(value)
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    except (ValueError, TypeError, OSError):
        return None


def _extract_permit(feature: dict) -> Optional[PermitRecord]:
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

    permit_num = props.get("PermitNo") or ""
    if not permit_num:
        return None

    raw_cost = props.get("EstimatedCost")
    declared_value = float(raw_cost) if raw_cost is not None else None

    return PermitRecord(
        permit_num=str(permit_num),
        status=props.get("PermitStatus"),
        address=props.get("PhysicalAddress"),
        issue_date=_parse_arcgis_date(props.get("IssuedDate")),
        work_type=props.get("ProjectType"),
        description=props.get("PermitDescription"),
        declared_value=declared_value,
        latitude=lat,
        longitude=lon,
    )


class PermitsIngestor(BaseIngestor):
    PAGE_SIZE = 2000

    async def fetch(self) -> list[PermitRecord]:
        records: list[PermitRecord] = []
        offset = 0

        async with httpx.AsyncClient(timeout=30.0) as client:
            while True:
                params = {
                    "where": "1=1",
                    "outFields": (
                        "PermitNo,PermitStatus,PhysicalAddress,IssuedDate,"
                        "PermitDescription,ProjectType,EstimatedCost,UseType,Council_District"
                    ),
                    "returnGeometry": "true",
                    "f": "geojson",
                    "outSR": "4326",
                    "resultRecordCount": self.PAGE_SIZE,
                    "resultOffset": offset,
                }

                resp = await client.get(settings.arcgis_permits_url, params=params)
                resp.raise_for_status()
                data = resp.json()

                features = data.get("features", [])
                for feature in features:
                    permit = _extract_permit(feature)
                    if permit:
                        records.append(permit)

                if len(features) < self.PAGE_SIZE:
                    break  # last page

                offset += self.PAGE_SIZE

        return records
