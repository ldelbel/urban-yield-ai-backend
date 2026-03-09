from typing import Optional

import httpx

from app.config import settings
from app.ingestors.base import BaseIngestor
from app.models.vacancy import VacancyRecord


def _centroid_from_rings(rings: list) -> Optional[tuple[float, float]]:
    """Compute approximate centroid from the first polygon ring."""
    if not rings or not rings[0]:
        return None
    coords = rings[0]
    lons = [c[0] for c in coords if len(c) >= 2]
    lats = [c[1] for c in coords if len(c) >= 2]
    if not lons or not lats:
        return None
    return sum(lats) / len(lats), sum(lons) / len(lons)


def _extract_vacancy(feature: dict) -> Optional[VacancyRecord]:
    props = feature.get("attributes") or {}
    geom = feature.get("geometry") or {}

    rings = geom.get("rings", [])
    centroid = _centroid_from_rings(rings)
    if centroid is None:
        return None

    lat, lon = centroid
    if lat == 0.0 and lon == 0.0:
        return None

    return VacancyRecord(
        address=props.get("Address"),
        district=props.get("District"),
        latitude=lat,
        longitude=lon,
    )


class VacanciesIngestor(BaseIngestor):
    PAGE_SIZE = 2000

    async def fetch(self) -> list[VacancyRecord]:
        records: list[VacancyRecord] = []
        offset = 0

        async with httpx.AsyncClient(timeout=60.0) as client:
            while True:
                params = {
                    "where": "1=1",
                    "outFields": "Address,District",
                    "returnGeometry": "true",
                    "outSR": "4326",
                    "f": "json",
                    "resultRecordCount": self.PAGE_SIZE,
                    "resultOffset": offset,
                }

                resp = await client.get(settings.arcgis_vacancies_url, params=params)
                resp.raise_for_status()
                data = resp.json()

                features = data.get("features", [])
                for feature in features:
                    record = _extract_vacancy(feature)
                    if record:
                        records.append(record)

                if len(features) < self.PAGE_SIZE:
                    break

                offset += self.PAGE_SIZE

        return records
