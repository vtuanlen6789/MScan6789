"""
Swing Engine – BOS / CHoCH detection for BizClaw
Detects proper swing points (pivot highs/lows) over a configurable lookback,
then derives Break-of-Structure (BOS) and Change-of-Character (CHoCH) signals.
"""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import pandas as pd


# ─────────────────────────────────────────────
# 1. ATR helper (local, avoids circular import)
# ─────────────────────────────────────────────

def _compute_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high_low = df["high"] - df["low"]
    high_pc = (df["high"] - df["close"].shift(1)).abs()
    low_pc = (df["low"] - df["close"].shift(1)).abs()
    tr = pd.concat([high_low, high_pc, low_pc], axis=1).max(axis=1)
    return tr.rolling(period, min_periods=period).mean()


# ─────────────────────────────────────────────
# 2. Adaptive Swing Detector
# ─────────────────────────────────────────────

def detect_swing_points(df: pd.DataFrame, base_window: int = 5) -> pd.DataFrame:
    """
    Detect pivot swing-high / swing-low with ATR-adaptive window.
    Adds boolean columns: swing_high, swing_low
    """
    df = df.copy()

    atr = _compute_atr(df, period=14)
    atr_mean = atr.mean()

    if atr_mean > 0:
        raw_window = (base_window * (atr / atr_mean)).clip(3, 20).round()
    else:
        raw_window = pd.Series([base_window] * len(df), index=df.index)

    df["_swing_window"] = raw_window.fillna(base_window).astype(int)
    df["swing_high"] = False
    df["swing_low"] = False

    margin = 20
    for i in range(margin, len(df) - margin):
        w = int(df["_swing_window"].iloc[i])
        slice_high = df["high"].iloc[max(0, i - w): i + w + 1]
        slice_low = df["low"].iloc[max(0, i - w): i + w + 1]

        if df["high"].iloc[i] == slice_high.max():
            df.at[df.index[i], "swing_high"] = True
        if df["low"].iloc[i] == slice_low.min():
            df.at[df.index[i], "swing_low"] = True

    df.drop(columns=["_swing_window"], inplace=True)
    return df


# ─────────────────────────────────────────────
# 3. BOS – Break of Structure
# Using CLOSE (not wick) as per SMC rules
# ─────────────────────────────────────────────

def detect_bos(df: pd.DataFrame) -> pd.DataFrame:
    """
    BOS_UP  : close breaks above previous swing high
    BOS_DOWN: close breaks below previous swing low
    """
    if "swing_high" not in df.columns:
        df = detect_swing_points(df)

    df = df.copy()
    df["bos_up"] = False
    df["bos_down"] = False

    last_sh: Optional[float] = None
    last_sl: Optional[float] = None

    for i in range(len(df)):
        close = float(df["close"].iloc[i])

        # Check break BEFORE updating reference levels
        if last_sh is not None and close > last_sh:
            df.at[df.index[i], "bos_up"] = True

        if last_sl is not None and close < last_sl:
            df.at[df.index[i], "bos_down"] = True

        # Update reference swing levels
        if df["swing_high"].iloc[i]:
            last_sh = float(df["high"].iloc[i])
        if df["swing_low"].iloc[i]:
            last_sl = float(df["low"].iloc[i])

    return df


# ─────────────────────────────────────────────
# 4. CHoCH – Change of Character
# Uptrend  : close < last HL → CHoCH bearish
# Downtrend: close > last LH → CHoCH bullish
# ─────────────────────────────────────────────

def detect_choch(df: pd.DataFrame) -> pd.DataFrame:
    """
    Tracks evolving HH/HL or LL/LH structure and flags CHoCH when
    the most recent counterpoint is broken by close price.
    """
    if "swing_high" not in df.columns:
        df = detect_bos(df)      # also runs detect_swing_points

    df = df.copy()
    df["choch_bull"] = False
    df["choch_bear"] = False

    # Track swing sequence with labels
    swing_seq: List[Tuple[str, float]] = []   # ("H"|"L", price)

    def _trend_from_seq(seq: List[Tuple[str, float]]) -> str:
        highs = [p for t, p in seq if t == "H"]
        lows = [p for t, p in seq if t == "L"]
        if len(highs) >= 2 and len(lows) >= 2:
            if highs[-1] > highs[-2] and lows[-1] > lows[-2]:
                return "bullish"
            if highs[-1] < highs[-2] and lows[-1] < lows[-2]:
                return "bearish"
        return "undefined"

    for i in range(len(df)):
        close = float(df["close"].iloc[i])

        if df["swing_high"].iloc[i]:
            swing_seq.append(("H", float(df["high"].iloc[i])))

        if df["swing_low"].iloc[i]:
            swing_seq.append(("L", float(df["low"].iloc[i])))

        # Keep only last 8 swing points for trend assessment
        if len(swing_seq) > 8:
            swing_seq = swing_seq[-8:]

        trend = _trend_from_seq(swing_seq)

        if trend == "bullish":
            # CHoCH bear: close breaks last swing low (HL)
            lows = [p for t, p in swing_seq if t == "L"]
            if lows and close < lows[-1]:
                df.at[df.index[i], "choch_bear"] = True

        elif trend == "bearish":
            # CHoCH bull: close breaks last swing high (LH)
            highs = [p for t, p in swing_seq if t == "H"]
            if highs and close > highs[-1]:
                df.at[df.index[i], "choch_bull"] = True

    return df


# ─────────────────────────────────────────────
# 5. Summary builder – returns latest state dict
# ─────────────────────────────────────────────

def build_swing_analysis(df: pd.DataFrame, base_window: int = 5) -> Dict:
    """
    Full pipeline: swing → BOS → CHoCH.
    Returns a dict with the latest signals only (compatible with scan row).
    """
    if df is None or len(df) < 60:
        return {
            "swing_bos_up": False,
            "swing_bos_down": False,
            "swing_choch_bull": False,
            "swing_choch_bear": False,
            "swing_trend": "UNDEFINED",
            "swing_last_sh": None,
            "swing_last_sl": None,
        }

    df = detect_swing_points(df, base_window=base_window)
    df = detect_bos(df)
    df = detect_choch(df)

    # Collect last known swing levels
    sh_rows = df[df["swing_high"]]
    sl_rows = df[df["swing_low"]]
    last_sh = float(sh_rows["high"].iloc[-1]) if not sh_rows.empty else None
    last_sl = float(sl_rows["low"].iloc[-1]) if not sl_rows.empty else None

    # Trend from swing sequence
    highs = sh_rows["high"].tolist()
    lows = sl_rows["low"].tolist()
    if len(highs) >= 2 and len(lows) >= 2:
        if highs[-1] > highs[-2] and lows[-1] > lows[-2]:
            swing_trend = "BULLISH"
        elif highs[-1] < highs[-2] and lows[-1] < lows[-2]:
            swing_trend = "BEARISH"
        else:
            swing_trend = "RANGING"
    else:
        swing_trend = "UNDEFINED"

    # Latest bar signals (look at last 5 bars for recency)
    tail = df.tail(5)

    return {
        "swing_bos_up": bool(tail["bos_up"].any()),
        "swing_bos_down": bool(tail["bos_down"].any()),
        "swing_choch_bull": bool(tail["choch_bull"].any()),
        "swing_choch_bear": bool(tail["choch_bear"].any()),
        "swing_trend": swing_trend,
        "swing_last_sh": last_sh,
        "swing_last_sl": last_sl,
    }
