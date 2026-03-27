from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


PROFILE_WEIGHTS: Dict[str, Dict[str, float]] = {
    "D1-H4": {"D1": 0.55, "H4": 0.45},
    "H4-M30": {"H4": 0.55, "M30": 0.45},
    "M30-M5": {"M30": 0.55, "M5": 0.45},
}

PROFILE_ORDER = ["D1-H4", "H4-M30", "M30-M5"]


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _pct_gap(a: Optional[float], b: Optional[float]) -> float:
    if a is None or b is None or b == 0:
        return 0.0
    return max(0.0, ((a - b) / abs(b)) * 100.0)


def classify_indicator_signal(row: Dict[str, Any]) -> Dict[str, Any]:
    rsi = _safe_float(row.get("RSI"))
    rsi_sma = _safe_float(row.get("RSIwSMA"))
    rsi_wma = _safe_float(row.get("RSIwWMA"))
    atr = _safe_float(row.get("ATR"))
    atr_sma = _safe_float(row.get("ATRwSMA"))
    atr_wma = _safe_float(row.get("ATRwWMA"))

    signal = "NEUTRAL"
    atr_ready = False
    score = 0.0

    if None not in {rsi, rsi_sma, rsi_wma, atr, atr_sma, atr_wma}:
        atr_ready = atr > atr_sma > atr_wma
        bullish = rsi > rsi_sma > rsi_wma and atr_ready
        bearish = rsi < rsi_sma < rsi_wma and atr_ready

        if bullish:
            signal = "BULLISH"
        elif bearish:
            signal = "BEARISH"

        if signal != "NEUTRAL":
            rsi_score = abs(rsi - rsi_sma) + abs(rsi_sma - rsi_wma)
            atr_score = _pct_gap(atr, atr_sma) + _pct_gap(atr_sma, atr_wma)
            score = round(rsi_score + atr_score, 4)

    return {
        "Pair": row.get("Pair"),
        "Timeframe": row.get("Timeframe"),
        "Signal": signal,
        "AtrReady": atr_ready,
        "SignalScore": score,
        "RSI": rsi,
        "RSIwSMA": rsi_sma,
        "RSIwWMA": rsi_wma,
        "ATR": atr,
        "ATRwSMA": atr_sma,
        "ATRwWMA": atr_wma,
    }


def _group_rows(indicator_scan_table: Optional[List[Dict[str, Any]]]) -> Dict[str, Dict[str, Dict[str, Any]]]:
    grouped: Dict[str, Dict[str, Dict[str, Any]]] = {}

    for row in indicator_scan_table or []:
        pair = str(row.get("Pair") or "").upper()
        timeframe = str(row.get("Timeframe") or "").upper()
        if not pair or not timeframe:
            continue

        grouped.setdefault(pair, {})[timeframe] = classify_indicator_signal(row)

    return grouped


def _build_profile_candidate(
    pair: str,
    profile_name: str,
    weights: Dict[str, float],
    timeframe_map: Dict[str, Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    signals: Dict[str, str] = {}
    weighted_score = 0.0
    missing_timeframes: List[str] = []
    direction: Optional[str] = None
    matched_rows: List[Dict[str, Any]] = []

    for timeframe, weight in weights.items():
        row = timeframe_map.get(timeframe)
        if not row:
            missing_timeframes.append(timeframe)
            signals[timeframe] = "MISSING"
            continue

        signal = str(row.get("Signal") or "NEUTRAL")
        signals[timeframe] = signal
        if signal == "NEUTRAL":
            return None

        if direction is None:
            direction = signal
        elif direction != signal:
            return None

        weighted_score += float(row.get("SignalScore") or 0.0) * weight
        matched_rows.append(row)

    if missing_timeframes or direction is None:
        return None

    summary = (
        f"{pair} {direction} | {profile_name} đồng bộ | "
        + " / ".join(f"{tf}:{signals[tf]}" for tf in weights.keys())
    )

    return {
        "Pair": pair,
        "Direction": direction,
        "Profile": profile_name,
        "Score": round(weighted_score, 2),
        "MatchedTFs": profile_name,
        "D1Signal": signals.get("D1", "N/A"),
        "H4Signal": signals.get("H4", "N/A"),
        "M30Signal": signals.get("M30", "N/A"),
        "M5Signal": signals.get("M5", "N/A"),
        "Summary": summary,
    }


def select_top_indicator_trends(indicator_scan_table: Optional[List[Dict[str, Any]]], limit: int = 3) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    grouped = _group_rows(indicator_scan_table)
    ranking: List[Dict[str, Any]] = []

    for pair, timeframe_map in grouped.items():
        candidates: List[Dict[str, Any]] = []
        for profile_name in PROFILE_ORDER:
            candidate = _build_profile_candidate(pair, profile_name, PROFILE_WEIGHTS[profile_name], timeframe_map)
            if candidate is not None:
                candidates.append(candidate)

        if not candidates:
            continue

        best = sorted(candidates, key=lambda row: (row.get("Score", 0), -PROFILE_ORDER.index(str(row.get("Profile")))), reverse=True)[0]
        ranking.append(best)

    ranking = sorted(ranking, key=lambda row: row.get("Score", 0), reverse=True)
    return ranking, ranking[:limit]