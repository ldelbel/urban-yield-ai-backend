import asyncio
import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Optional

import h3
import httpx
from anthropic import AsyncAnthropic

from app.config import settings
from app.models.hexagon import HexCell

logger = logging.getLogger(__name__)

CACHE_PATH = Path("data/enrichment_cache.json")

ZILLOW_DATASET_ID = "gd_lfqkr8wm13ixtbd8f5"
ZILLOW_DATASET_URL = "https://api.brightdata.com/datasets/v3/scrape"

GMAPS_PROMPT = (
    "Use the Google Maps search tool via BrightData to find businesses "
    "within 500 meters of lat={lat:.4f}, lon={lng:.4f} in Montgomery, "
    "AL. Find at least 5 nearby businesses.\n\n"
    "Extract these aggregated values:\n"
    "- Average star rating across all businesses found\n"
    "- Total count of reviews across all businesses found\n"
    "- Count of businesses marked as permanently closed\n\n"
    "Reply ONLY with valid JSON, no markdown, no explanation:\n"
    '{{"gmaps_rating": <number or null>, '
    '"review_count": <integer or null>, '
    '"permanently_closed_count": <integer or null>}}'
)


class BrightEnricher:
    def __init__(self) -> None:
        self._cache_path = CACHE_PATH
        self._client: Optional[AsyncAnthropic] = (
            AsyncAnthropic(api_key=settings.claude_api_key)
            if settings.claude_api_key
            else None
        )

    # ------------------------------------------------------------------
    # Cache helpers
    # ------------------------------------------------------------------

    def _apply_cache(self, cells: dict[str, HexCell]) -> dict[str, HexCell]:
        """Load cached enrichment data into cells."""
        try:
            if not self._cache_path.exists():
                return cells
            with open(self._cache_path) as f:
                cache = json.load(f)
        except Exception as exc:
            logger.warning(f"Could not load enrichment cache: {exc}")
            return cells

        for h3_id, cell in cells.items():
            entry = cache.get(h3_id, {})
            if not entry:
                continue

            if "zillow_avg_price_sqft" in entry:
                cell.zillow_avg_price_sqft = entry["zillow_avg_price_sqft"]
            if "zillow_days_on_market" in entry:
                cell.zillow_days_on_market = entry["zillow_days_on_market"]
            if "zillow_price_reduction_count" in entry:
                cell.zillow_price_reduction_count = entry["zillow_price_reduction_count"]
            if "gmaps_avg_rating" in entry:
                cell.gmaps_avg_rating = entry["gmaps_avg_rating"]
            if "gmaps_review_count" in entry:
                cell.gmaps_review_count = entry["gmaps_review_count"]
            if "gmaps_permanently_closed_count" in entry:
                cell.gmaps_permanently_closed_count = entry["gmaps_permanently_closed_count"]

        cached_count = sum(1 for c in cells.values() if c.zillow_avg_price_sqft is not None)
        logger.info(f"BrightEnricher: loaded {cached_count} cells from cache.")
        return cells

    def _save_batch_to_cache(self, cells: list[HexCell]) -> None:
        """Persist enriched cells to the cache file atomically."""
        try:
            if self._cache_path.exists():
                with open(self._cache_path) as f:
                    cache = json.load(f)
            else:
                cache = {}

            for cell in cells:
                if any([
                    cell.zillow_avg_price_sqft is not None,
                    cell.gmaps_avg_rating is not None,
                    cell.gmaps_review_count is not None,
                    cell.zillow_days_on_market is not None,
                ]):
                    existing = cache.get(cell.h3_index, {})
                    cache[cell.h3_index] = {
                        **existing,  # preserve narrative and other fields
                        "zillow_avg_price_sqft": cell.zillow_avg_price_sqft,
                        "zillow_days_on_market": cell.zillow_days_on_market,
                        "zillow_price_reduction_count": cell.zillow_price_reduction_count,
                        "gmaps_avg_rating": cell.gmaps_avg_rating,
                        "gmaps_review_count": cell.gmaps_review_count,
                        "gmaps_permanently_closed_count": cell.gmaps_permanently_closed_count,
                    }

            self._cache_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = tempfile.NamedTemporaryFile(
                mode="w", dir=self._cache_path.parent, delete=False, suffix=".tmp"
            )
            json.dump(cache, tmp, indent=2)
            tmp.close()
            os.replace(tmp.name, self._cache_path)

        except Exception as exc:
            logger.error(f"BrightEnricher: failed to save cache: {exc}")

    # ------------------------------------------------------------------
    # Zillow Dataset API (direct HTTP, no Claude)
    # ------------------------------------------------------------------

    async def _fetch_zillow_dataset(self, cell_id: str = "") -> Optional[list[dict]]:
        """
        Call BrightData Dataset API for Zillow listings in Montgomery, AL.

        The discover_new+input_filters mode is async: initial POST returns 202
        with a snapshot_id. We poll the snapshot endpoint every 5s (max 24
        attempts = 2 minutes) until HTTP 200 with data.
        """
        headers = {
            "Authorization": f"Bearer {settings.bright_data_api_token}",
            "Content-Type": "application/json",
        }
        payload = {
            "input": [{
                "location": "Montgomery, AL",
                "listingCategory": "House for sale",
                "HomeType": "Houses",
                "exact_address": False,
            }],
        }
        params = {
            "dataset_id": ZILLOW_DATASET_ID,
            "notify": "false",
            "include_errors": "true",
            "type": "discover_new",
            "discover_by": "input_filters",
        }

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                ZILLOW_DATASET_URL,
                headers=headers,
                params=params,
                json=payload,
            )

        logger.info(
            f"BrightData Zillow Dataset API — "
            f"status: {response.status_code}, "
            f"h3: {cell_id}"
        )

        if response.status_code == 200:
            # Synchronous response (unlikely for discover_new but handle it)
            return self._extract_listings(response.json(), cell_id)

        if response.status_code != 202:
            logger.warning(
                f"Zillow Dataset API error {response.status_code}: "
                f"{response.text[:500]}"
            )
            return None

        # --- 202: async job started, poll snapshot ---
        snapshot_id = response.json().get("snapshot_id")
        if not snapshot_id:
            logger.warning(f"Zillow Dataset API 202 but no snapshot_id: {response.text[:200]}")
            return None

        logger.info(f"Zillow snapshot started ({cell_id}): {snapshot_id}")
        return await self._poll_snapshot(snapshot_id, cell_id)

    async def _poll_snapshot(self, snapshot_id: str, cell_id: str = "") -> Optional[list[dict]]:
        """Poll a BrightData snapshot until HTTP 200 (ready) or timeout."""
        snapshot_url = f"https://api.brightdata.com/datasets/v3/snapshot/{snapshot_id}"
        headers = {"Authorization": f"Bearer {settings.bright_data_api_token}"}
        max_attempts = 24
        poll_interval = 5.0

        async with httpx.AsyncClient(timeout=30.0) as client:
            for attempt in range(1, max_attempts + 1):
                await asyncio.sleep(poll_interval)
                resp = await client.get(snapshot_url, headers=headers,
                                        params={"format": "json"})
                logger.info(
                    f"Zillow snapshot poll {attempt}/{max_attempts} "
                    f"({snapshot_id}): HTTP {resp.status_code}"
                )
                if resp.status_code == 200:
                    return self._extract_listings(resp.json(), cell_id)
                if resp.status_code != 202:
                    logger.warning(
                        f"Zillow snapshot unexpected status {resp.status_code}: "
                        f"{resp.text[:200]}"
                    )
                    return None
                # 202 → still processing, keep polling

        logger.warning(
            f"Zillow snapshot {snapshot_id} not ready after "
            f"{max_attempts} attempts ({cell_id}) — skipping."
        )
        return None

    def _extract_listings(self, data, cell_id: str = "") -> Optional[list[dict]]:
        """Extract listings list from API response and log first record."""
        listings = None
        if isinstance(data, list):
            listings = data
        elif isinstance(data, dict) and "results" in data:
            listings = data["results"]

        if listings:
            logger.info(
                f"Zillow first listing ({cell_id}): "
                f"{json.dumps(listings[0])[:1000]}"
            )
        else:
            logger.info(f"Zillow raw response ({cell_id}): {json.dumps(data)[:500]}")

        return listings or None

    def _parse_zillow_listings(self, listings: list[dict]) -> dict:
        """Compute averages from a list of Zillow listing dicts."""
        prices_sqft = []
        days_on_market = []
        reduction_count = 0

        for listing in listings:
            # Compute price/sqft from price and livingArea
            price = listing.get("price")
            area = listing.get("livingArea")
            if isinstance(price, (int, float)) and isinstance(area, (int, float)) and area > 0:
                prices_sqft.append(round(float(price) / float(area), 2))

            dom = listing.get("daysOnZillow")
            if isinstance(dom, (int, float)) and dom >= 0:
                days_on_market.append(float(dom))

            # priceReduction is non-null and non-zero when a reduction exists
            pr = listing.get("priceReduction")
            if pr:
                reduction_count += 1

        return {
            "price_sqft": round(sum(prices_sqft) / len(prices_sqft), 2) if prices_sqft else None,
            "days_on_market": round(sum(days_on_market) / len(days_on_market), 1) if days_on_market else None,
            "price_reduction_count": reduction_count if listings else None,
        }

    # ------------------------------------------------------------------
    # GMaps via Claude+MCP (unchanged)
    # ------------------------------------------------------------------

    async def _call_brightdata_gmaps(self, prompt: str, cell_id: str = "") -> Optional[dict]:
        """Make one Claude+BrightData MCP call for GMaps data."""
        if self._client is None:
            return None
        response = await self._client.beta.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=800,
            mcp_servers=[{
                "name": "brightdata",
                "type": "url",
                "url": settings.bright_data_sse_url,
            }],
            betas=["mcp-client-2025-04-04"],
            messages=[{"role": "user", "content": prompt}],
        )
        logger.info(
            f"BrightData GMaps MCP tokens — "
            f"input: {response.usage.input_tokens}, "
            f"output: {response.usage.output_tokens}, "
            f"h3: {cell_id}"
        )
        text_blocks = [b for b in response.content if hasattr(b, "text")]
        if not text_blocks:
            return None
        text = text_blocks[-1].text
        start = text.find("{")
        end = text.rfind("}") + 1
        if start == -1 or end == 0:
            return None
        return json.loads(text[start:end])

    # ------------------------------------------------------------------
    # Single-hexagon enrichment
    # ------------------------------------------------------------------

    async def enrich_hexagon(self, cell: HexCell) -> HexCell:
        """Enrich a HexCell: Zillow via Dataset API, GMaps via MCP."""
        # Skip only if ALL new fields are present
        if all([
            cell.zillow_avg_price_sqft is not None,
            cell.zillow_days_on_market is not None,
            cell.gmaps_avg_rating is not None,
            cell.gmaps_review_count is not None,
        ]):
            return cell  # fully enriched, skip

        lat, lng = h3.cell_to_latlng(cell.h3_index)

        # --- CALL A: Zillow via Dataset API (direct HTTP, no Claude) ---
        try:
            listings = await self._fetch_zillow_dataset(cell_id=cell.h3_index)
            if listings:
                parsed = self._parse_zillow_listings(listings)
                if isinstance(parsed.get("price_sqft"), float):
                    cell.zillow_avg_price_sqft = parsed["price_sqft"]
                if isinstance(parsed.get("days_on_market"), float):
                    cell.zillow_days_on_market = parsed["days_on_market"]
                if isinstance(parsed.get("price_reduction_count"), int):
                    cell.zillow_price_reduction_count = parsed["price_reduction_count"]
                logger.info(
                    f"Zillow parsed ({cell.h3_index}): "
                    f"price_sqft={cell.zillow_avg_price_sqft}, "
                    f"dom={cell.zillow_days_on_market}, "
                    f"reductions={cell.zillow_price_reduction_count}"
                )
        except Exception as exc:
            logger.warning(f"Zillow Dataset API failed for {cell.h3_index}: {exc}")

        # --- CALL B: Google Maps via Claude+MCP ---
        try:
            gmaps_result = await self._call_brightdata_gmaps(
                GMAPS_PROMPT.format(lat=lat, lng=lng),
                cell_id=cell.h3_index,
            )
            if gmaps_result:
                rating = gmaps_result.get("gmaps_rating")
                review_count = gmaps_result.get("review_count")
                closed = gmaps_result.get("permanently_closed_count")
                if isinstance(rating, (int, float)):
                    cell.gmaps_avg_rating = float(rating)
                if isinstance(review_count, int):
                    cell.gmaps_review_count = review_count
                if isinstance(closed, int):
                    cell.gmaps_permanently_closed_count = closed
        except Exception as exc:
            logger.warning(f"GMaps enrichment failed for {cell.h3_index}: {exc}")

        return cell

    # ------------------------------------------------------------------
    # Sequential enrichment
    # ------------------------------------------------------------------

    async def enrich_all(
        self,
        cells: dict[str, HexCell],
        max_cells: Optional[int] = None,
        delay_between_cells: float = 3.0,
        presentation_mode: bool = True,
        on_cell_complete: Optional[callable] = None,
    ) -> dict[str, HexCell]:
        """
        Enrich cells with BrightData market data, one hexagon at a time.

        Args:
            cells: All hexagon cells to process.
            max_cells: Hard cap on cells to enrich in this run. Must be a
                positive integer; validated by the caller before this is invoked.
            delay_between_cells: Seconds to wait between each hexagon (not
                between batches — there are no batches).
            presentation_mode: When True (default), load cache only and make
                no live API calls. Set to False to enable live enrichment.
            on_cell_complete: Optional callback invoked after each hexagon
                completes with the running count of cells that have
                zillow_avg_price_sqft set. Used by the admin router to update
                real-time status without waiting for the full run to finish.
        """
        if not settings.bright_data_api_token:
            logger.info("BRIGHTDATA_API_TOKEN not set — skipping BrightData enrichment.")
            return cells

        # Always load existing cache first (preserves valid cached data)
        cells = self._apply_cache(cells)

        if presentation_mode:
            cached_count = sum(
                1 for c in cells.values() if c.zillow_avg_price_sqft is not None
            )
            logger.info(
                f"BrightEnricher: pre-loaded cache into {cached_count} cells. "
                f"(Presentation Mode — no API calls)"
            )
            return cells

        # --- Live enrichment path (sequential, one cell at a time) ---
        to_enrich = [
            c for c in cells.values()
            if c.zillow_avg_price_sqft is None or c.gmaps_avg_rating is None
        ]
        # max_cells is validated as a positive int by the admin endpoint before
        # calling here, but guard defensively.
        if max_cells and max_cells > 0:
            to_enrich = to_enrich[:max_cells]

        logger.info(
            f"BrightEnricher: {len(to_enrich)} hexagons queued for enrichment "
            f"({len(cells) - len(to_enrich)} already cached)"
        )

        if not to_enrich:
            logger.info("BrightEnricher: all cells already enriched, skipping.")
            return cells

        for i, cell in enumerate(to_enrich):
            logger.info(
                f"BrightEnricher: enriching cell {i + 1}/{len(to_enrich)} "
                f"({cell.h3_index})"
            )

            cell = await self.enrich_hexagon(cell)
            self._save_batch_to_cache([cell])

            if on_cell_complete:
                enriched_so_far = sum(
                    1 for c in cells.values() if c.zillow_avg_price_sqft is not None
                )
                on_cell_complete(enriched_so_far)

            logger.info(
                f"BrightEnricher: {i + 1}/{len(to_enrich)} done, cache saved."
            )

            # Wait between cells (skip after the last one)
            if i < len(to_enrich) - 1:
                logger.info(
                    f"BrightEnricher: waiting {delay_between_cells}s "
                    f"before next cell..."
                )
                await asyncio.sleep(delay_between_cells)

        logger.info(
            f"BrightEnricher: enrichment complete. "
            f"{len(to_enrich)} hexagons processed."
        )
        return cells
