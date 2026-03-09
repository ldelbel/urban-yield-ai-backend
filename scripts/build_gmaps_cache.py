"""
build_gmaps_cache.py

Loads 4 Google Maps BrightData dataset files, deduplicates records, assigns
them to H3 resolution-8 hexagons, computes per-hexagon aggregates, and merges
them into enrichment_cache.json (preserving all existing zillow_* and narrative
fields).

Usage:
    python scripts/build_gmaps_cache.py
"""

import json
import math
import os
import statistics
import tempfile
from collections import defaultdict
from pathlib import Path

import h3

REPO_ROOT = Path(__file__).resolve().parent.parent
GMAPS_FILES = [
    REPO_ROOT / "gmaps_sd_mmj65iwbk2285flcw.json",
    REPO_ROOT / "gmaps_sd_mmj66trm2k5o4bo9ke.json",
    REPO_ROOT / "gmaps_sd_mmj69fh619m3y087dx.json",
    REPO_ROOT / "gmaps_sd_mmj684mes8p9uiq87.json",
]
CACHE_PATH = REPO_ROOT / "data" / "enrichment_cache.json"

H3_RESOLUTION = 8

# Bounding box for Montgomery County, AL
LAT_MIN, LAT_MAX = 32.1, 32.6
LON_MIN, LON_MAX = -86.6, -85.9


def main():
    # --- Load all files ---
    all_records = []
    per_file_counts = []
    for path in GMAPS_FILES:
        with open(path) as f:
            data = json.load(f)
        per_file_counts.append((path.name, len(data)))
        all_records.extend(data)

    print("Records per file:")
    for name, count in per_file_counts:
        print(f"  {name}: {count}")
    total_raw = len(all_records)
    print(f"  Total raw: {total_raw}")

    # --- Deduplicate by (name, lat, lon) ---
    seen: set[tuple] = set()
    deduped = []
    for rec in all_records:
        key = (rec.get("name"), rec.get("lat"), rec.get("lon"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(rec)
    print(f"  After dedup: {len(deduped)}\n")

    # --- Load existing cache ---
    if CACHE_PATH.exists():
        with open(CACHE_PATH) as f:
            cache = json.load(f)
        print(f"Existing cache has {len(cache)} entries.")
    else:
        cache = {}
        print("No existing cache — starting fresh.")

    # --- Assign to hexagons ---
    hex_data: dict[str, list[dict]] = defaultdict(list)
    skipped_coords = 0
    skipped_bounds = 0
    processed = 0

    for biz in deduped:
        lat = biz.get("lat")
        lon = biz.get("lon")

        if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
            skipped_coords += 1
            continue

        if not (LAT_MIN <= lat <= LAT_MAX and LON_MIN <= lon <= LON_MAX):
            skipped_bounds += 1
            continue

        rating = biz.get("rating")
        review_count = biz.get("reviews_count") or 0
        perm_closed = 1 if biz.get("permanently_closed") is True else 0

        if not isinstance(rating, (int, float)):
            skipped_coords += 1
            continue

        cell = h3.latlng_to_cell(lat, lon, H3_RESOLUTION)
        hex_data[cell].append({
            "rating": float(rating),
            "review_count": int(review_count),
            "permanently_closed": perm_closed,
        })
        processed += 1

    print(f"\nProcessed: {processed} businesses → {len(hex_data)} hexagons")
    print(f"Skipped (no coords/rating): {skipped_coords}")
    print(f"Skipped (out of bounds):    {skipped_bounds}")

    # --- Compute per-hexagon aggregates ---
    hexagons_written = 0
    all_hex_ratings = []

    for h3_index, records in hex_data.items():
        reviewed = [(r["rating"], r["review_count"]) for r in records if r["review_count"] > 0]

        if reviewed:
            total_weight = sum(math.log1p(rc) for _, rc in reviewed)
            if total_weight > 0:
                weighted_rating = sum(
                    r * math.log1p(rc) for r, rc in reviewed
                ) / total_weight
            else:
                weighted_rating = statistics.mean(r for r, _ in reviewed)
        else:
            weighted_rating = statistics.mean(r["rating"] for r in records)

        total_reviews = sum(r["review_count"] for r in records)
        perm_closed_count = sum(r["permanently_closed"] for r in records)

        avg_rating = round(weighted_rating, 3)
        all_hex_ratings.append(avg_rating)

        existing = cache.get(h3_index, {})
        cache[h3_index] = {
            **existing,  # preserve zillow_*, narrative, etc.
            "gmaps_avg_rating": avg_rating,
            "gmaps_review_count": total_reviews,
            "gmaps_permanently_closed_count": perm_closed_count,
        }
        hexagons_written += 1

    # --- Atomic write ---
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = tempfile.NamedTemporaryFile(
        mode="w", dir=CACHE_PATH.parent, delete=False, suffix=".tmp"
    )
    json.dump(cache, tmp, indent=2)
    tmp.close()
    os.replace(tmp.name, CACHE_PATH)
    print(f"\nCache saved to {CACHE_PATH}")

    # --- Summary ---
    both_zillow_and_gmaps = sum(
        1 for e in cache.values()
        if e.get("zillow_avg_price_sqft") is not None
        and e.get("gmaps_avg_rating") is not None
    )

    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)
    print(f"  Hexagons enriched with GMaps:    {hexagons_written}")
    print(f"  Total cache entries now:         {len(cache)}")
    print(f"\n  Coverage vs. 638 active hexagons:")
    print(f"    GMaps: {hexagons_written} / 638 = {hexagons_written/638*100:.1f}%")
    print(f"    Both:  {both_zillow_and_gmaps} / 638 = {both_zillow_and_gmaps/638*100:.1f}%   (have both Zillow + GMaps)")

    if all_hex_ratings:
        all_hex_ratings.sort()
        print(f"\n  Per-hexagon avg rating:")
        print(f"    min:    {min(all_hex_ratings):.3f}")
        print(f"    median: {statistics.median(all_hex_ratings):.3f}")
        print(f"    max:    {max(all_hex_ratings):.3f}")

    print("\n  Sample cache entries (first 5 with GMaps data):")
    shown = 0
    for h3_idx, entry in cache.items():
        if entry.get("gmaps_avg_rating") is not None:
            psf = entry.get("zillow_avg_price_sqft")
            print(f"    {h3_idx}: rating={entry['gmaps_avg_rating']:.3f}, "
                  f"reviews={entry['gmaps_review_count']}, "
                  f"closed={entry['gmaps_permanently_closed_count']}, "
                  f"zillow_psf={f'${psf:.2f}' if psf else 'none'}")
            shown += 1
            if shown >= 5:
                break


if __name__ == "__main__":
    main()
