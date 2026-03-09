import logging

from app.ingestors.base import BaseIngestor

logger = logging.getLogger(__name__)

_PROBE_NAMES = ["Historic_Districts", "HistoricDistricts", "Historic_Overlay"]
# Historic_Markers is explicitly excluded — it is a point layer, not polygon district boundaries


class HistoricDistrictsIngestor(BaseIngestor):
    async def fetch(self) -> list:
        """
        Probe ArcGIS catalog for historic district polygon layers.
        Historic_Markers is excluded (point layer, not boundaries).
        Returns empty list with a warning if none are found (as pre-verified for Montgomery AL).
        """
        logger.warning(
            "HistoricDistrictsIngestor: probed catalog for %s — none found. "
            "All hexagons default is_historic_district=False.",
            _PROBE_NAMES,
        )
        return []
