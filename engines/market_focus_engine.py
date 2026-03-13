from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


CLARITY_SCORE_MAP = {
    "HIGH": 100.0,
    "MODERATE": 70.0,
    "LOW": 40.0,
}

TIMEFRAME_WEIGHTS = {
    "D1": 0.5,
    "H4": 0.3,
    "M30": 0.2,
}


def _clamp(value: float, min_value: float = 0.0, max_value: float = 100.0) -> float:
    return max(min_value, min(max_value, value))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _clarity_score(clarity: Any) -> float:
    label = str(clarity or "LOW").strip().upper()
    return CLARITY_SCORE_MAP.get(label, 40.0)


def _normalize_opportunity_score(raw_score: Any) -> float:
    return _clamp(_safe_float(raw_score))


def _metric_strength_score(metrics: Optional[Dict[str, Any]]) -> float:
    if not metrics:
        return 50.0

    rsi = metrics.get("rsi")
    pc = metrics.get("pc")

    if rsi is None and pc is None:
        return 50.0

    rsi_score = 50.0 if rsi is None else _clamp((_safe_float(rsi, 50.0) - 50.0) * 1.6 + 50.0)
    pc_score = 50.0 if pc is None else _clamp(50.0 + _safe_float(pc) * 40.0)
    return round(0.7 * rsi_score + 0.3 * pc_score, 2)


def _aggregate_currency_strength(currency_strength_table: Optional[List[Dict[str, Any]]]) -> Dict[str, float]:
    if not currency_strength_table:
        return {}

    score_accumulator: Dict[str, float] = {}
    weight_accumulator: Dict[str, float] = {}

    for row in currency_strength_table:
        timeframe = str(row.get("timeframe") or "").upper()
        weight = TIMEFRAME_WEIGHTS.get(timeframe, 0.0)
        if weight <= 0:
            continue

        currencies = row.get("currencies") or {}
        for currency, metrics in currencies.items():
            metric_score = _metric_strength_score(metrics)
            score_accumulator[currency] = score_accumulator.get(currency, 0.0) + metric_score * weight
            weight_accumulator[currency] = weight_accumulator.get(currency, 0.0) + weight

    aggregated: Dict[str, float] = {}
    for currency, total in score_accumulator.items():
        weight = weight_accumulator.get(currency, 0.0)
        aggregated[currency] = round(total / weight, 2) if weight > 0 else 50.0

    return aggregated


def _split_pair(pair: str) -> Tuple[str, str]:
    clean = str(pair or "").replace("_", "").upper()
    if len(clean) == 6:
        return clean[:3], clean[3:]
    if clean.startswith("XAU") and len(clean) >= 6:
        return "XAU", clean[3:6]
    return clean[:3], clean[3:6] if len(clean) >= 6 else ""


def derive_pair_macro_bias(pair: str, currency_strength_table: Optional[List[Dict[str, Any]]]) -> Dict[str, Any]:
    aggregated = _aggregate_currency_strength(currency_strength_table)
    base, quote = _split_pair(pair)

    base_score = aggregated.get(base, 50.0)
    quote_score = aggregated.get(quote, 50.0)
    delta = base_score - quote_score
    macro_score = round(_clamp(abs(delta)), 2)

    if abs(delta) < 5:
        bias_label = "NEUTRAL"
    elif delta > 0:
        bias_label = f"{base}_STRONG"
    else:
        bias_label = f"{quote}_STRONG"

    return {
        "MacroBias": bias_label,
        "CurrencyStrengthScore": macro_score,
        "BaseStrength": round(base_score, 2),
        "QuoteStrength": round(quote_score, 2),
        "BiasDelta": round(delta, 2),
        "BaseCurrency": base,
        "QuoteCurrency": quote,
    }


def _smc_score(row: Optional[Dict[str, Any]]) -> float:
    if not row:
        return 0.0

    h4_poi = _safe_float(row.get("h4_poi_score"))
    d1_poi = _safe_float(row.get("d1_poi_score"))
    m30_poi = _safe_float(row.get("m30_poi_score"))
    confluence = _safe_float(row.get("entry_confluence"))
    entry_signal = bool(row.get("entry_signal"))

    raw = h4_poi * 10.0 + d1_poi * 4.0 + m30_poi * 4.0 + confluence * 2.0 + (10.0 if entry_signal else 0.0)
    return round(_clamp(raw), 2)


def build_actionable_summary(focus_row: Dict[str, Any]) -> str:
    pair = focus_row.get("Pair") or focus_row.get("pair") or "?"
    cycle = focus_row.get("Cycle") or focus_row.get("CycleState") or "UNDEFINED"
    core = focus_row.get("Core") or "N/A"
    entry = focus_row.get("Entry") or "WAIT"
    macro_bias = focus_row.get("MacroBias") or "NEUTRAL"
    smc_note = focus_row.get("SmcSignalSummary") or "No SMC trigger"
    return f"{pair}: {cycle} | {core} | {entry} | Macro {macro_bias} | {smc_note}"


def merge_focus_inputs(
    core_results: List[Dict[str, Any]],
    opportunity_ranked: Optional[List[Dict[str, Any]]] = None,
    currency_strength_table: Optional[List[Dict[str, Any]]] = None,
    smc_analysis: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    opportunity_map = {
        str(row.get("symbol") or row.get("Pair") or "").upper(): row
        for row in (opportunity_ranked or [])
    }
    smc_map = {
        str(row.get("pair") or row.get("Pair") or "").upper(): row
        for row in (smc_analysis or [])
    }

    merged_rows: List[Dict[str, Any]] = []
    for core_row in core_results:
        pair = str(core_row.get("Pair") or "").upper()
        opportunity_row = opportunity_map.get(pair, {})
        smc_row = smc_map.get(pair, {})
        macro_context = derive_pair_macro_bias(pair, currency_strength_table)

        merged = dict(core_row)
        merged.update({
            "OpportunityState": opportunity_row.get("state"),
            "OpportunityAge": opportunity_row.get("age"),
            "OpportunityDrive": opportunity_row.get("drive"),
            "OpportunityAlignment": opportunity_row.get("alignment"),
            "OpportunityScore": _normalize_opportunity_score(opportunity_row.get("score")),
            "SmcScore": _smc_score(smc_row),
            "SmcEntrySignal": bool(smc_row.get("entry_signal")),
            "SmcEntryDirection": smc_row.get("entry_dir"),
            "SmcPoiScore": _safe_float(smc_row.get("h4_poi_score")),
            "SmcSignalSummary": smc_row.get("entry_reason") or (
                f"POI {smc_row.get('h4_poi_score')}" if smc_row else "No SMC trigger"
            ),
        })
        merged.update(macro_context)
        merged_rows.append(merged)

    return merged_rows


def compute_focus_score(row: Dict[str, Any]) -> Dict[str, Any]:
    trust = _clamp(_safe_float(row.get("Trust")))
    opportunity_score = _clamp(_safe_float(row.get("OpportunityScore")))
    currency_strength_score = _clamp(_safe_float(row.get("CurrencyStrengthScore")))
    clarity_score = _clarity_score(row.get("Clarity"))
    smc_score = _clamp(_safe_float(row.get("SmcScore")))

    focus_score = round(
        trust * 0.35
        + opportunity_score * 0.20
        + currency_strength_score * 0.15
        + clarity_score * 0.15
        + smc_score * 0.15,
        2,
    )

    enriched = dict(row)
    enriched.update({
        "TrustScore": round(trust, 2),
        "ClarityScore": round(clarity_score, 2),
        "FocusScore": focus_score,
    })
    enriched["ActionableSummary"] = build_actionable_summary(enriched)
    return enriched


def select_top_focus_pairs(rows: List[Dict[str, Any]], limit: int = 3) -> List[Dict[str, Any]]:
    return sorted(rows, key=lambda row: row.get("FocusScore", 0), reverse=True)[:limit]


def run_market_focus_engine(
    core_results: List[Dict[str, Any]],
    opportunity_ranked: Optional[List[Dict[str, Any]]] = None,
    currency_strength_table: Optional[List[Dict[str, Any]]] = None,
    smc_analysis: Optional[List[Dict[str, Any]]] = None,
    limit: int = 3,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    merged_rows = merge_focus_inputs(
        core_results=core_results,
        opportunity_ranked=opportunity_ranked,
        currency_strength_table=currency_strength_table,
        smc_analysis=smc_analysis,
    )
    focus_ranking = [compute_focus_score(row) for row in merged_rows]
    focus_ranking = sorted(focus_ranking, key=lambda row: row.get("FocusScore", 0), reverse=True)
    return focus_ranking, select_top_focus_pairs(focus_ranking, limit=limit)