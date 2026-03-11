from __future__ import annotations

from typing import Dict, List, Tuple

import pandas as pd


BASE_PROB = {
    "INIT": 25,
    "EARLY": 60,
    "CONTINUATION": 65,
    "MATURE": 40,
    "EXHAUSTION": 30,
    "REVERSAL": 50,
    "UNDEFINED": 20,
}


def _window_df(df: pd.DataFrame, lookback: int = 48) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    return df.tail(lookback).copy()


def compute_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high_low = df["high"] - df["low"]
    high_prev_close = (df["high"] - df["close"].shift(1)).abs()
    low_prev_close = (df["low"] - df["close"].shift(1)).abs()

    tr = pd.concat([high_low, high_prev_close, low_prev_close], axis=1).max(axis=1)
    return tr.rolling(period, min_periods=period).mean()


def compute_drive_score(df: pd.DataFrame) -> int:
    atr = compute_atr(df)
    atr_last = atr.iloc[-1]
    if pd.isna(atr_last) or atr_last <= 0:
        return 0

    last_range = float(df["high"].iloc[-1] - df["low"].iloc[-1])
    ratio = last_range / float(atr_last)

    if ratio < 0.8:
        return 0
    if ratio < 1.2:
        return 1
    if ratio < 1.8:
        return 2
    return 3


def compute_cycle_age(df: pd.DataFrame, lookback: int = 48) -> int:
    if len(df) < lookback:
        lookback = len(df)
    if lookback <= 1:
        return 1

    recent = df.tail(lookback)
    recent_high = float(recent["high"].max())
    recent_low = float(recent["low"].min())
    last_close = float(df["close"].iloc[-1])

    total_range = abs(recent_high - recent_low)
    ratio = abs(last_close - recent_low) / total_range if total_range != 0 else 0

    if ratio < 0.33:
        return 1
    if ratio < 0.66:
        return 2
    return 3


def determine_state(age: int, drive: int) -> str:
    if age == 1 and drive == 0:
        return "INIT"
    if age == 1 and drive in [1, 2]:
        return "EARLY"
    if age == 2 and drive in [1, 2]:
        return "CONTINUATION"
    if age == 3 and drive == 1:
        return "MATURE"
    if age == 3 and drive == 3:
        return "EXHAUSTION"
    return "UNDEFINED"


def _trend_bool(df: pd.DataFrame, bars: int = 10) -> bool:
    if df is None or df.empty:
        return False
    if len(df) <= bars:
        bars = max(1, len(df) - 1)
    return float(df["close"].iloc[-1]) > float(df["close"].iloc[-1 - bars])


def compute_alignment(df_m5: pd.DataFrame, df_m30: pd.DataFrame, df_h4: pd.DataFrame, df_d1: pd.DataFrame, m5_m30_lookback: int = 48) -> int:
    trend_fast = _trend_bool(_window_df(df_m5, m5_m30_lookback))
    trend_stable = _trend_bool(_window_df(df_m30, m5_m30_lookback))
    trend_h4 = _trend_bool(df_h4)
    trend_d1 = _trend_bool(df_d1)

    alignment_score = 0
    if trend_fast == trend_h4:
        alignment_score += 10
    if trend_stable == trend_d1:
        alignment_score += 15
    if trend_fast == trend_stable == trend_h4:
        alignment_score += 25

    return alignment_score


def compute_opportunity_score(state: str, age: int, drive: int, alignment: int) -> int:
    base = BASE_PROB.get(state, 20)

    risk_penalty = 0
    if age == 3:
        risk_penalty += 15
    if age == 3 and drive == 3:
        risk_penalty += 25

    return int(base + alignment - risk_penalty)


def _split_symbol(symbol: str) -> Tuple[str, str]:
    clean = symbol.replace("_", "")
    if len(clean) == 6:
        return clean[:3], clean[3:]
    if clean.startswith("XAU"):
        return "XAU", clean[3:]
    return clean[:3], clean[3:6] if len(clean) >= 6 else ""


def _usd_direction(item: Dict[str, object]) -> str:
    base = str(item["base"])
    quote = str(item["quote"])
    trend_up = bool(item.get("trend_m30_up", False))

    if base == "USD":
        return "USD_STRONG" if trend_up else "USD_WEAK"
    if quote == "USD":
        return "USD_WEAK" if trend_up else "USD_STRONG"
    return "NO_USD"


def correlation_filter(ranked_list: List[Dict[str, object]], limit: int = 3) -> List[Dict[str, object]]:
    selected: List[Dict[str, object]] = []
    used_bases = set()
    usd_bias_count = {"USD_STRONG": 0, "USD_WEAK": 0}

    for item in ranked_list:
        base = item["base"]
        usd_bias = _usd_direction(item)

        if base in used_bases:
            continue

        if usd_bias in usd_bias_count and usd_bias_count[usd_bias] >= 2:
            continue

        selected.append(item)
        used_bases.add(base)

        if usd_bias in usd_bias_count:
            usd_bias_count[usd_bias] += 1

        if len(selected) >= limit:
            break

    return selected


def build_opportunity_row(
    symbol: str,
    df_m5: pd.DataFrame,
    df_m30: pd.DataFrame,
    df_h4: pd.DataFrame,
    df_d1: pd.DataFrame,
    m5_m30_lookback: int = 48,
) -> Dict[str, object]:
    m5_windowed = _window_df(df_m5, m5_m30_lookback)
    m30_windowed = _window_df(df_m30, m5_m30_lookback)

    age = compute_cycle_age(m30_windowed, lookback=m5_m30_lookback)
    drive = compute_drive_score(m30_windowed)
    state = determine_state(age, drive)
    alignment = compute_alignment(m5_windowed, m30_windowed, df_h4, df_d1, m5_m30_lookback=m5_m30_lookback)
    score = compute_opportunity_score(state, age, drive, alignment)

    base, quote = _split_symbol(symbol)
    trend_m30_up = _trend_bool(m30_windowed)

    return {
        "symbol": symbol,
        "displaySymbol": f"{base}_{quote}" if quote else symbol,
        "base": base,
        "quote": quote,
        "state": state,
        "age": age,
        "drive": drive,
        "alignment": alignment,
        "score": score,
        "trend_m30_up": trend_m30_up,
        "m5Lookback": m5_m30_lookback,
        "m30Lookback": m5_m30_lookback,
    }
