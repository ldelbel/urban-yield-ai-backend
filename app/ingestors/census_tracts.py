import logging
import httpx
from shapely.geometry import shape

logger = logging.getLogger(__name__)

TIGERWEB_URL = (
    "https://tigerweb.geo.census.gov/arcgis/rest/services/"
    "TIGERweb/Tracts_Blocks/MapServer/0/query"
)


class CensusTractIngestor:
    async def fetch(self) -> dict[str, object]:  # values are shapely geometries
        """Fetch tract polygon boundaries. Returns {} on failure."""
        result = {}
        offset = 0
        record_count = 100

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                while True:
                    resp = await client.get(TIGERWEB_URL, params={
                        "where": "STATE='01' AND COUNTY='101'",
                        "outFields": "GEOID,NAME",
                        "outSR": "4326",
                        "f": "geojson",
                        "resultOffset": offset,
                        "resultRecordCount": record_count,
                    })
                    resp.raise_for_status()
                    data = resp.json()
                    features = data.get("features", [])
                    for feat in features:
                        geoid = feat.get("properties", {}).get("GEOID", "")
                        try:
                            geom = shape(feat["geometry"])
                            result[geoid] = geom
                        except Exception:
                            pass
                    if len(features) < record_count:
                        break
                    offset += record_count
        except Exception as exc:
            logger.warning(f"CensusTractIngestor: failed to fetch geometries: {exc}")
            return {}

        logger.info(f"CensusTractIngestor: loaded {len(result)} tract polygons.")
        return result
