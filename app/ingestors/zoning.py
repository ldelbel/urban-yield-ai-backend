from typing import Optional

import httpx

from app.config import settings
from app.ingestors.base import BaseIngestor
from app.models.zoning import ZoningRecord


def _extract_zoning(feature: dict) -> Optional[ZoningRecord]:
    props = feature.get("attributes") or {}
    geom = feature.get("geometry") or {}

    zoning_code = props.get("ZoningCode") or ""
    if not zoning_code:
        return None

    rings = geom.get("rings", [])
    if not rings:
        return None

    return ZoningRecord(
        zoning_code=zoning_code,
        zoning_desc=props.get("ZoningDesc"),
        rings=rings,
    )


class ZoningIngestor(BaseIngestor):
    PAGE_SIZE = 2000

    async def fetch(self) -> list[ZoningRecord]:
        records: list[ZoningRecord] = []
        offset = 0

        async with httpx.AsyncClient(timeout=60.0) as client:
            while True:
                params = {
                    "where": "1=1",
                    "outFields": "ZoningCode,ZoningDesc",
                    "returnGeometry": "true",
                    "outSR": "4326",
                    "f": "json",
                    "resultRecordCount": self.PAGE_SIZE,
                    "resultOffset": offset,
                }

                resp = await client.get(settings.arcgis_zoning_url, params=params)
                resp.raise_for_status()
                data = resp.json()

                features = data.get("features", [])
                for feature in features:
                    record = _extract_zoning(feature)
                    if record:
                        records.append(record)

                if len(features) < self.PAGE_SIZE:
                    break

                offset += self.PAGE_SIZE

        return records
