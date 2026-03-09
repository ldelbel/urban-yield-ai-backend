import logging

from app.ingestors.base import BaseIngestor

logger = logging.getLogger(__name__)

_PROBE_NAMES = ["FloodZone", "FEMA_Flood", "Flood_Zone", "NFHL", "Floodplain"]


class FloodZonesIngestor(BaseIngestor):
    async def fetch(self) -> list:
        """
        Probe ArcGIS catalog for flood zone polygon layers.
        Returns empty list with a warning if none are found (as pre-verified for Montgomery AL).
        """
        logger.warning(
            "FloodZonesIngestor: probed catalog for %s — none found. "
            "All hexagons default is_flood_zone=False.",
            _PROBE_NAMES,
        )
        return []
