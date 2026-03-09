"""
build_zillow_cache.py

Assigns 886 Montgomery AL Zillow listings to H3 resolution-8 hexagons,
computes per-hexagon aggregates, and merges them into enrichment_cache.json.

Usage:
    python scripts/build_zillow_cache.py
"""

import json
import os
import statistics
import tempfile
from collections import defaultdict
from pathlib import Path

import h3

REPO_ROOT = Path(__file__).resolve().parent.parent
ZILLOW_PATH = REPO_ROOT / "montgomery_zillow.json"
CACHE_PATH = REPO_ROOT / "data" / "enrichment_cache.json"

H3_RESOLUTION = 8
MIN_PRICE_SQF = 10.0
MAX_PRICE_SQF = 1000.0


def has_price_reduction(price_history: list) -> int:
    """Return 1 if any priceHistory entry has priceChangeRate < 0, else 0."""
    if not isinstance(price_history, list):
        return 0
    for entry in price_history:
        rate = entry.get("priceChangeRate")
        if isinstance(rate, (int, float)) and rate < 0:
            return 1
    return 0


def main():
    # --- Load listings ---
    print(f"Loading {ZILLOW_PATH} ...")
    with open(ZILLOW_PATH) as f:
        listings = json.load(f)
    print(f"  {len(listings)} total listings loaded.")

    # --- Load existing cache ---
    if CACHE_PATH.exists():
        with open(CACHE_PATH) as f:
            cache = json.load(f)
        print(f"  Existing cache has {len(cache)} entries.")
    else:
        cache = {}
        print("  No existing cache found — starting fresh.")

    # --- Assign listings to hexagons ---
    # hex_data[h3_index] = list of {price_sqft, days_on_market, has_reduction}
    hex_data: dict[str, list[dict]] = defaultdict(list)
    skipped = 0

    for listing in listings:
        lat = listing.get("latitude")
        lng = listing.get("longitude")
        price = listing.get("price")
        area = listing.get("livingArea")
        dom = listing.get("daysOnZillow")

        # Require valid coordinates, price, and living area
        if not isinstance(lat, (int, float)):
            skipped += 1
            continue
        if not isinstance(lng, (int, float)):
            skipped += 1
            continue
        if not isinstance(price, (int, float)) or price <= 0:
            skipped += 1
            continue
        if not isinstance(area, (int, float)) or area <= 0:
            skipped += 1
            continue

        price_sqft = price / area

        # Outlier filter
        if price_sqft < MIN_PRICE_SQF or price_sqft > MAX_PRICE_SQF:
            skipped += 1
            continue

        cell = h3.latlng_to_cell(lat, lng, H3_RESOLUTION)
        hex_data[cell].append({
            "price_sqft": price_sqft,
            "days_on_market": float(dom) if isinstance(dom, (int, float)) else None,
            "has_reduction": has_price_reduction(listing.get("priceHistory")),
        })

    processed = sum(len(v) for v in hex_data.values())
    print(f"\n  Processed: {processed} listings → {len(hex_data)} hexagons")
    print(f"  Skipped (null/invalid/outlier): {skipped}")

    # --- Compute per-hexagon aggregates and merge into cache ---
    hexagons_written = 0
    all_hex_psf = []

    for h3_index, records in hex_data.items():
        psf_vals = [r["price_sqft"] for r in records]
        dom_vals = [r["days_on_market"] for r in records if r["days_on_market"] is not None]
        reduction_count = sum(r["has_reduction"] for r in records)

        avg_psf = round(statistics.mean(psf_vals), 2)
        avg_dom = round(statistics.mean(dom_vals), 1) if dom_vals else None

        all_hex_psf.append(avg_psf)

        existing = cache.get(h3_index, {})
        cache[h3_index] = {
            **existing,  # preserve gmaps_*, narrative, etc.
            "zillow_avg_price_sqft": avg_psf,
            "zillow_days_on_market": avg_dom,
            "zillow_price_reduction_count": reduction_count,
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
    print(f"\n  Cache saved to {CACHE_PATH}")

    # --- Summary ---
    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)
    print(f"  Total listings processed:        {processed}")
    print(f"  Listings skipped:                {skipped}")
    print(f"  Hexagons enriched with Zillow:   {hexagons_written}")
    print(f"  Total cache entries now:         {len(cache)}")

    # Coverage against 638 known active hexagons
    print(f"\n  Coverage vs. 638 active hexagons:")
    print(f"    {hexagons_written} / 638 = {hexagons_written/638*100:.1f}%")

    if all_hex_psf:
        all_hex_psf.sort()
        print(f"\n  Per-hexagon price/sqft range:")
        print(f"    min:    ${min(all_hex_psf):.2f}")
        print(f"    p25:    ${all_hex_psf[len(all_hex_psf)//4]:.2f}")
        print(f"    median: ${statistics.median(all_hex_psf):.2f}")
        print(f"    mean:   ${statistics.mean(all_hex_psf):.2f}")
        print(f"    p75:    ${all_hex_psf[3*len(all_hex_psf)//4]:.2f}")
        print(f"    max:    ${max(all_hex_psf):.2f}")

    # Show a few sample entries
    print("\n  Sample cache entries (first 5 with Zillow data):")
    shown = 0
    for h3_idx, entry in cache.items():
        if entry.get("zillow_avg_price_sqft") is not None:
            print(f"    {h3_idx}: psf=${entry['zillow_avg_price_sqft']:.2f}, "
                  f"dom={entry['zillow_days_on_market']}, "
                  f"reductions={entry['zillow_price_reduction_count']}")
            shown += 1
            if shown >= 5:
                break


if __name__ == "__main__":
    main()
