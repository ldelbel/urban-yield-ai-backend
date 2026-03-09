import asyncio
import logging

from app.config import settings
from app.enrichers.bright_enricher import BrightEnricher
from app.h3_utils.indexer import (
    aggregate_businesses_to_hexagons,
    aggregate_permits_to_hexagons,
    aggregate_service_requests_to_hexagons,
    aggregate_vacancies_to_hexagons,
    assign_census_data_to_hexagons,
    assign_flood_zones_to_hexagons,
    assign_historic_districts_to_hexagons,
    assign_zoning_to_hexagons,
)
from app.ingestors.business_licenses import BusinessLicensesIngestor
from app.ingestors.census_acs import CensusACSIngestor
from app.ingestors.census_tracts import CensusTractIngestor
from app.ingestors.flood_zones import FloodZonesIngestor
from app.ingestors.historic_districts import HistoricDistrictsIngestor
from app.ingestors.permits import PermitsIngestor
from app.ingestors.service_requests import ServiceRequestsIngestor
from app.ingestors.vacancies import VacanciesIngestor
from app.ingestors.zoning import ZoningIngestor
from app.models.hexagon import HexCell
from app.scoring.vitality_scorer import score_hexagons

logger = logging.getLogger(__name__)

_cells: dict[str, HexCell] = {}
_census_tract_count: int = 0
_lock = asyncio.Lock()


class AppState:
    @staticmethod
    def get_cells() -> dict[str, HexCell]:
        return _cells

    @staticmethod
    async def refresh() -> None:
        global _cells, _census_tract_count
        logger.info("Starting data refresh — Triple-Layer Audit pipeline...")

        try:
            # Phase 1: Concurrent ingestion of all 9 sources
            (permits, businesses, service_requests, vacancies, zoning,
             _flood_zones, _historic, census_data, tract_geometries) = await asyncio.gather(
                PermitsIngestor().fetch(),
                BusinessLicensesIngestor().fetch(),
                ServiceRequestsIngestor().fetch(),
                VacanciesIngestor().fetch(),
                ZoningIngestor().fetch(),
                FloodZonesIngestor().fetch(),
                HistoricDistrictsIngestor().fetch(),
                CensusACSIngestor().fetch(),
                CensusTractIngestor().fetch(),
            )
            _census_tract_count = len(tract_geometries)
            logger.info(
                f"Ingested — permits: {len(permits)}, businesses: {len(businesses)}, "
                f"service requests: {len(service_requests)}, vacancies: {len(vacancies)}, "
                f"zoning polygons: {len(zoning)}, census tracts: {_census_tract_count}"
            )

            # Phase 2: Aggregate to hexagons
            cells = aggregate_permits_to_hexagons(permits, resolution=settings.h3_resolution)
            cells = aggregate_businesses_to_hexagons(businesses, cells, resolution=settings.h3_resolution)
            cells = aggregate_service_requests_to_hexagons(service_requests, cells, resolution=settings.h3_resolution)
            cells = aggregate_vacancies_to_hexagons(vacancies, cells, resolution=settings.h3_resolution)
            logger.info(f"Aggregated into {len(cells)} H3 hexagons (resolution {settings.h3_resolution}).")

            # Phase 3: Spatial zoning assignment (async batched ArcGIS queries)
            cells = await assign_zoning_to_hexagons(cells, settings.arcgis_zoning_url)
            # Phase 3b/3c: Structural constraint flags (gracefully skip if no catalog URL)
            cells = await assign_flood_zones_to_hexagons(cells, None)
            cells = await assign_historic_districts_to_hexagons(cells, None)
            # Phase 3d: Assign Census ACS data via spatial join
            cells = assign_census_data_to_hexagons(cells, census_data, tract_geometries)

            # Phase 4: Initial scoring + flag computation
            cells = score_hexagons(cells)
            logger.info("Initial scoring complete.")

            # Phase 5: Smart enrichment (BrightData — triggered cells only)
            cells = await BrightEnricher().enrich_all(cells)

            # Phase 6: Rescore with enrichment data (market signal now live)
            cells = score_hexagons(cells)
            logger.info("Final scoring complete.")

            infill = sum(1 for c in cells.values() if c.is_infill_opportunity)
            infra = sum(1 for c in cells.values() if c.is_infrastructure_priority)
            logger.info(
                f"Flags — infill opportunities: {infill}, infrastructure priorities: {infra}"
            )

            async with _lock:
                _cells = cells

        except Exception as e:
            logger.error(f"Refresh failed: {e}", exc_info=True)
            raise
