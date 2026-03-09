import math

from app.models.hexagon import HexCell

# Scoring weights
W_INVESTMENT = 0.30
W_SENTIMENT = 0.20
W_MARKET = 0.20
W_RISK = 0.15
W_SOCIOECONOMIC = 0.10
W_DENSITY = 0.05

YIELD_ACCELERATING = 1.1
YIELD_STAGNATING = 0.8
YIELD_LOW_CONFIDENCE_THRESHOLD = 5   # permit_count below this triggers penalty
YIELD_LOW_CONFIDENCE_MULTIPLIER = 0.2

INFRASTRUCTURE_PRIORITY_MULTIPLIER = 1.4  # 40% above avg 311 rate

COMMERCIAL_ZONING_CODES = {"B-1-a", "B-1-b", "B-2", "B-3", "B-4", "B-5"}


def _normalize(values: list[float]) -> list[float]:
    """Min-max normalize a list to [0, 1]. Returns 0.5 for single-element lists."""
    if not values:
        return []
    min_v, max_v = min(values), max(values)
    if max_v == min_v:
        return [0.5] * len(values)
    return [(v - min_v) / (max_v - min_v) for v in values]


def _normalize_subset(values: list, default: float = 0.5) -> list[float]:
    """Min-max normalize a list that may contain None values. Nones → default."""
    valid = [v for v in values if v is not None]
    if not valid:
        return [default] * len(values)
    min_v, max_v = min(valid), max(valid)
    if max_v == min_v:
        return [default if v is None else 0.5 for v in values]
    return [default if v is None else (v - min_v) / (max_v - min_v) for v in values]


def score_hexagons(cells: dict[str, HexCell]) -> dict[str, HexCell]:
    """
    Compute UVI, Yield scores, and derived flags for all hexagons.

    Investment signal: total_declared_value, falls back to permit_count.
    Sentiment: gmaps_avg_rating / 5 when available, neutral 0.5 otherwise.
    Market: normalized zillow_avg_price_sqft when available, 0 otherwise.
    Risk: inverse of service_request_count.
    Confidence penalty: yield_score × 0.2 if permit_count < 5.
    Infrastructure Priority: Accelerating + service_request_count > 1.4× avg.
    Infill Opportunity: Commercial zone + Stagnating + vacant_count > 0.
    """
    if not cells:
        return cells

    h3_ids = list(cells.keys())

    # --- Investment signal ---
    raw_investment = []
    for h3_id in h3_ids:
        cell = cells[h3_id]
        raw_investment.append(
            cell.total_declared_value if cell.total_declared_value is not None
            else float(cell.permit_count)
        )
    norm_investment = _normalize(raw_investment)

    # --- Risk signal (chronicity-aware composite) ---
    raw_risk = [float(cells[h3_id].service_request_count) for h3_id in h3_ids]
    raw_chronic = [float(cells[h3_id].chronic_case_count) for h3_id in h3_ids]
    norm_risk_volume = _normalize(raw_risk)
    norm_risk_chronicity = _normalize(raw_chronic)
    # risk_composite = 0.6 × volume + 0.4 × chronicity; then invert for UVI contribution
    norm_risk_composite = [
        0.6 * v + 0.4 * c for v, c in zip(norm_risk_volume, norm_risk_chronicity)
    ]
    norm_risk_inverse = [1.0 - v for v in norm_risk_composite]

    avg_service_requests = sum(raw_risk) / len(raw_risk) if raw_risk else 0.0

    # --- Sentiment signal (review-volume weighted + closed-business penalty) ---
    sentiment_values = []
    for h3_id in h3_ids:
        cell = cells[h3_id]
        if cell.gmaps_avg_rating is not None:
            # Weight rating by log of review count: more reviews = more reliable.
            # log1p(500) ≈ 6.21 caps the weight at 1.0 for 500+ reviews.
            review_weight = math.log1p(cell.gmaps_review_count or 1) / math.log1p(500)
            review_weight = min(review_weight, 1.0)
            raw_sentiment = cell.gmaps_avg_rating / 5.0
            # Blend: low review count pulls sentiment toward neutral (0.5)
            sentiment = review_weight * raw_sentiment + (1 - review_weight) * 0.5
            # Penalty for permanently closed businesses (max 0.15 deduction)
            if cell.gmaps_permanently_closed_count:
                closed_penalty = min(cell.gmaps_permanently_closed_count * 0.01, 0.15)
                sentiment = max(0.0, sentiment - closed_penalty)
            sentiment_values.append(sentiment)
        else:
            sentiment_values.append(0.5)

    # --- Market signal (price + liquidity + reduction pressure) ---
    raw_market = [cells[h3_id].zillow_avg_price_sqft or 0.0 for h3_id in h3_ids]
    raw_dom = [cells[h3_id].zillow_days_on_market for h3_id in h3_ids]
    raw_reductions = [cells[h3_id].zillow_price_reduction_count for h3_id in h3_ids]

    if any(v > 0 for v in raw_market):
        norm_price = _normalize(raw_market)
        norm_dom = _normalize_subset(raw_dom, default=0.5)
        norm_reductions = _normalize_subset(raw_reductions, default=0.5)
        market_values = []
        for i in range(len(h3_ids)):
            if raw_market[i] > 0:
                # Liquidity: lower days on market = more liquid market
                liquidity = (1.0 - norm_dom[i]) if raw_dom[i] is not None else 0.5
                # Reduction pressure: fewer reductions = healthier market
                reduction_pressure = (
                    (1.0 - norm_reductions[i]) if raw_reductions[i] is not None else 0.5
                )
                market_val = (
                    0.60 * norm_price[i]
                    + 0.25 * liquidity
                    + 0.15 * reduction_pressure
                )
                market_values.append(market_val)
            else:
                market_values.append(0.0)
    else:
        market_values = [0.0] * len(h3_ids)

    # --- SocioEconomic signal (Census income + home value) ---
    raw_income = [cells[h].census_median_income for h in h3_ids]
    raw_home_val = [cells[h].census_median_home_value for h in h3_ids]
    norm_income = _normalize_subset(raw_income)
    norm_home_val = _normalize_subset(raw_home_val)
    socioeconomic_values = []
    for i in range(len(h3_ids)):
        inc, hval = raw_income[i], raw_home_val[i]
        if inc is not None and hval is not None:
            socioeconomic_values.append(0.6 * norm_income[i] + 0.4 * norm_home_val[i])
        elif inc is not None:
            socioeconomic_values.append(norm_income[i])
        elif hval is not None:
            socioeconomic_values.append(norm_home_val[i])
        else:
            socioeconomic_values.append(0.5)

    # --- Density signal (Census population + vacancy rate utilization) ---
    raw_pop = [cells[h].census_total_population for h in h3_ids]
    raw_vac = [cells[h].census_vacancy_rate for h in h3_ids]
    norm_pop = _normalize_subset(raw_pop)
    density_values = []
    for i in range(len(h3_ids)):
        pop_n, vac = raw_pop[i], raw_vac[i]
        if pop_n is not None and vac is not None:
            density_values.append(0.5 * norm_pop[i] + 0.5 * (1.0 - vac))
        elif pop_n is not None:
            density_values.append(norm_pop[i])
        elif vac is not None:
            density_values.append(1.0 - vac)
        else:
            density_values.append(0.5)

    # --- Baseline density ---
    permit_counts = [float(cells[h3_id].permit_count) for h3_id in h3_ids]
    norm_density = _normalize(permit_counts)

    # --- Score each cell ---
    for i, h3_id in enumerate(h3_ids):
        cell = cells[h3_id]

        uvi = (
            W_INVESTMENT * norm_investment[i]
            + W_SENTIMENT * sentiment_values[i]
            + W_MARKET * market_values[i]
            + W_RISK * norm_risk_inverse[i]
            + W_SOCIOECONOMIC * socioeconomic_values[i]
            + W_DENSITY * density_values[i]
        )
        cell.uvi_score = round(uvi * 100, 2)

        momentum = (
            W_INVESTMENT * norm_investment[i]
            + W_SENTIMENT * sentiment_values[i]
            + W_SOCIOECONOMIC * socioeconomic_values[i]
        )
        baseline = norm_density[i] if norm_density[i] > 0 else 0.01
        yield_score = momentum / baseline

        # Confidence penalty for low-activity cells
        if cell.permit_count < YIELD_LOW_CONFIDENCE_THRESHOLD:
            yield_score *= YIELD_LOW_CONFIDENCE_MULTIPLIER
            cell.yield_label = "Low Confidence"
        elif yield_score > YIELD_ACCELERATING:
            cell.yield_label = "Accelerating"
        elif yield_score < YIELD_STAGNATING:
            cell.yield_label = "Stagnating"
        else:
            cell.yield_label = "Stable"

        cell.yield_score = round(yield_score, 4)

        # Structural correction: override "Stagnating" for constrained parcels
        if cell.yield_label == "Stagnating":
            if cell.is_flood_zone and cell.is_historic_district:
                cell.yield_label = "Structurally Constrained — Flood + Historic"
                cell.is_infill_opportunity = False
            elif cell.is_flood_zone:
                cell.yield_label = "Structurally Constrained — Flood Zone"
                cell.is_infill_opportunity = False
            elif cell.is_historic_district:
                cell.yield_label = "Structurally Constrained — Historic District"
                cell.is_infill_opportunity = False

        # Infrastructure Priority: high growth zone under disproportionate 311 pressure
        cell.is_infrastructure_priority = (
            cell.yield_label == "Accelerating"
            and avg_service_requests > 0
            and cell.service_request_count > avg_service_requests * INFRASTRUCTURE_PRIORITY_MULTIPLIER
        )

        # Infill Opportunity: commercial zone that's stagnating with vacant parcels
        # yield_label is no longer "Stagnating" for constrained cells, so this
        # naturally evaluates False for those — structural correction takes precedence.
        cell.is_infill_opportunity = (
            cell.primary_zoning in COMMERCIAL_ZONING_CODES
            and cell.yield_label == "Stagnating"
            and cell.vacant_count > 0
        )

    # --- Post-processing: UVI rank and percentile ---
    total_cells = len(h3_ids)
    ranked = sorted(cells.keys(), key=lambda h: cells[h].uvi_score or 0.0, reverse=True)
    for rank, h3_id in enumerate(ranked, start=1):
        cell = cells[h3_id]
        cell.uvi_rank = rank
        cell.uvi_percentile = round(((total_cells - rank) / total_cells) * 100, 1)

    return cells
