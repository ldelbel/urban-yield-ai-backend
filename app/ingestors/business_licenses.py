from typing import Optional

import httpx

from app.config import settings
from app.ingestors.base import BaseIngestor
from app.models.business_license import BusinessLicenseRecord


def _extract_business(feature: dict) -> Optional[BusinessLicenseRecord]:
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

    raw_year = props.get("pvYEAR")
    license_year = int(raw_year) if raw_year is not None else None

    return BusinessLicenseRecord(
        company_name=props.get("custCOMPANY_NAME"),
        industry=props.get("scNAME"),
        license_year=license_year,
        address=props.get("Physical_Add"),
        latitude=lat,
        longitude=lon,
    )


class BusinessLicensesIngestor(BaseIngestor):
    PAGE_SIZE = 2000

    async def fetch(self) -> list[BusinessLicenseRecord]:
        records: list[BusinessLicenseRecord] = []
        offset = 0

        async with httpx.AsyncClient(timeout=30.0) as client:
            while True:
                params = {
                    "where": "1=1",
                    "outFields": "custCOMPANY_NAME,scNAME,pvYEAR,Physical_Add,Council_District",
                    "returnGeometry": "true",
                    "f": "geojson",
                    "outSR": "4326",
                    "resultRecordCount": self.PAGE_SIZE,
                    "resultOffset": offset,
                }

                resp = await client.get(settings.arcgis_business_licenses_url, params=params)
                resp.raise_for_status()
                data = resp.json()

                features = data.get("features", [])
                for feature in features:
                    record = _extract_business(feature)
                    if record:
                        records.append(record)

                if len(features) < self.PAGE_SIZE:
                    break

                offset += self.PAGE_SIZE

        return records
