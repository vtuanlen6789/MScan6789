"""
SMC Engine – Displacement / FVG / Order Block / Liquidity / POI Ranking
Implements the Smart Money Concepts analysis pipeline on a single DataFrame.
"""
from __future__ import annotations

from typing import Dict, List, Optional

import pandas as pd
import numpy as np


# ─────────────────────────────────────────────
# ATR (shared helper)
# ─────────────────────────────────────────────

def _atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    hl = df["high"] - df["low"]
    hc = (df["high"] - df["close"].shift(1)).abs()
    lc = (df["low"] - df["close"].shift(1)).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    return tr.rolling(period, min_periods=period).mean()


# ─────────────────────────────────────────────
# 1. Displacement Engine
# Strong move: |body| > ATR * multiplier
# ─────────────────────────────────────────────

def compute_displacement(df: pd.DataFrame, multiplier: float = 1.5) -> pd.DataFrame:
    df = df.copy()
    atr_series = _atr(df)
    df["body"] = (df["close"] - df["open"]).abs()
    df["atr"] = atr_series
    df["displacement"] = df["body"] > df["atr"] * multiplier
    df["disp_bull"] = df["displacement"] & (df["close"] > df["open"])
    df["disp_bear"] = df["displacement"] & (df["close"] < df["open"])
    return df


# ─────────────────────────────────────────────
# 2. FVG – Fair Value Gap
# Bullish FVG: candle[i-2].high < candle[i].low
# Bearish FVG: candle[i-2].low  > candle[i].high
# ─────────────────────────────────────────────

def detect_fvg(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["fvg_bull"] = False
    df["fvg_bear"] = False

    for i in range(2, len(df)):
        if df["high"].iloc[i - 2] < df["low"].iloc[i]:
            df.at[df.index[i], "fvg_bull"] = True
        if df["low"].iloc[i - 2] > df["high"].iloc[i]:
            df.at[df.index[i], "fvg_bear"] = True

    return df


# ─────────────────────────────────────────────
# 3. Order Block
# Last opposing candle before a displacement move
# Bullish OB : last bearish candle before bullish displacement
# Bearish OB : last bullish candle before bearish displacement
# ─────────────────────────────────────────────

def detect_order_block(df: pd.DataFrame) -> pd.DataFrame:
    if "displacement" not in df.columns:
        df = compute_displacement(df)

    df = df.copy()
    df["ob_bull"] = False
    df["ob_bear"] = False

    for i in range(2, len(df)):
        if df["disp_bull"].iloc[i]:
            # Bullish OB: previous candle is bearish
            prev = i - 1
            if df["close"].iloc[prev] < df["open"].iloc[prev]:
                df.at[df.index[prev], "ob_bull"] = True

        if df["disp_bear"].iloc[i]:
            # Bearish OB: previous candle is bullish
            prev = i - 1
            if df["close"].iloc[prev] > df["open"].iloc[prev]:
                df.at[df.index[prev], "ob_bear"] = True

    return df


# ─────────────────────────────────────────────
# 4. Liquidity Map – Equal Highs / Equal Lows
# ─────────────────────────────────────────────

def detect_liquidity(df: pd.DataFrame, lookback: int = 5) -> pd.DataFrame:
    if "atr" not in df.columns:
        df = compute_displacement(df)

    df = df.copy()
    df["equal_high"] = False
    df["equal_low"] = False

    tol = float(df["atr"].mean()) * 0.15

    for i in range(lookback, len(df)):
        for j in range(i - lookback, i):
            if abs(df["high"].iloc[i] - df["high"].iloc[j]) < tol:
                df.at[df.index[i], "equal_high"] = True
            if abs(df["low"].iloc[i] - df["low"].iloc[j]) < tol:
                df.at[df.index[i], "equal_low"] = True

    return df


# ─────────────────────────────────────────────
# 5. Liquidity Sweep
# Price wicks above equal high (or below equal low) then closes back
# ─────────────────────────────────────────────

def detect_liquidity_sweep(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["liq_sweep"] = False

    for i in range(2, len(df)):
        prev_high = df["high"].iloc[i - 1]
        prev_low = df["low"].iloc[i - 1]

        # Sweep high: wick above prev high but close below it
        if df["high"].iloc[i] > prev_high and df["close"].iloc[i] < prev_high:
            df.at[df.index[i], "liq_sweep"] = True

        # Sweep low: wick below prev low but close above it
        if df["low"].iloc[i] < prev_low and df["close"].iloc[i] > prev_low:
            df.at[df.index[i], "liq_sweep"] = True

    return df


# ─────────────────────────────────────────────
# 6. Liquidity Void
# Gap between consecutive closes > ATR * 2
# ─────────────────────────────────────────────

def detect_liquidity_void(df: pd.DataFrame) -> pd.DataFrame:
    if "atr" not in df.columns:
        df = compute_displacement(df)

    df = df.copy()
    df["liq_void"] = False

    for i in range(1, len(df)):
        gap = abs(df["close"].iloc[i] - df["close"].iloc[i - 1])
        if gap > df["atr"].iloc[i] * 2:
            df.at[df.index[i], "liq_void"] = True

    return df


# ─────────────────────────────────────────────
# 7. POI Ranking – Confluence score per bar
# OB=3, BOS=3, FVG=2, Liq Sweep=2, CHoCH=2
# ─────────────────────────────────────────────

def compute_poi_score(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    score = pd.Series(0.0, index=df.index)

    if "ob_bull" in df.columns:
        score += df["ob_bull"].astype(float) * 3
    if "ob_bear" in df.columns:
        score += df["ob_bear"].astype(float) * 3
    if "fvg_bull" in df.columns:
        score += df["fvg_bull"].astype(float) * 2
    if "fvg_bear" in df.columns:
        score += df["fvg_bear"].astype(float) * 2
    if "liq_sweep" in df.columns:
        score += df["liq_sweep"].astype(float) * 2
    if "liq_void" in df.columns:
        score += df["liq_void"].astype(float) * 1
    if "bos_up" in df.columns:
        score += df["bos_up"].astype(float) * 3
    if "bos_down" in df.columns:
        score += df["bos_down"].astype(float) * 3
    if "choch_bull" in df.columns:
        score += df["choch_bull"].astype(float) * 2
    if "choch_bear" in df.columns:
        score += df["choch_bear"].astype(float) * 2

    df["poi_score"] = score
    return df


# ─────────────────────────────────────────────
# 8. Master SMC analysis for a single DataFrame
# Returns a compact summary dict for scan output
# ─────────────────────────────────────────────

def run_smc_analysis(df: pd.DataFrame, swing_df: Optional[pd.DataFrame] = None) -> Dict:
    """
    Accepts OHLC DataFrame (already augmented with swing/BOS/CHoCH columns
    if available) and returns a summary dict ready for the scan row.
    """
    if df is None or len(df) < 30:
        return _empty_smc()

    try:
        # Build SMC layer
        df_smc = compute_displacement(df)
        df_smc = detect_fvg(df_smc)
        df_smc = detect_order_block(df_smc)
        df_smc = detect_liquidity(df_smc)
        df_smc = detect_liquidity_sweep(df_smc)
        df_smc = detect_liquidity_void(df_smc)

        # Merge swing signals if available
        if swing_df is not None:
            for col in ["bos_up", "bos_down", "choch_bull", "choch_bear"]:
                if col in swing_df.columns:
                    df_smc[col] = swing_df[col].values

        df_smc = compute_poi_score(df_smc)

        tail = df_smc.tail(5)
        top_poi = float(df_smc["poi_score"].max())
        recent_poi = float(tail["poi_score"].max())

        # Latest OB levels (for zone price reference)
        ob_bull_rows = df_smc[df_smc.get("ob_bull", pd.Series(False, index=df_smc.index))]
        ob_bear_rows = df_smc[df_smc.get("ob_bear", pd.Series(False, index=df_smc.index))]
        latest_ob_bull_price = float(ob_bull_rows["low"].iloc[-1]) if not ob_bull_rows.empty else None
        latest_ob_bear_price = float(ob_bear_rows["high"].iloc[-1]) if not ob_bear_rows.empty else None

        # Count FVGs and sweeps in last 20 bars
        recent20 = df_smc.tail(20)
        fvg_count = int(recent20.get("fvg_bull", pd.Series(False)).sum() +
                        recent20.get("fvg_bear", pd.Series(False)).sum())
        sweep_count = int(recent20.get("liq_sweep", pd.Series(False)).sum())
        void_count = int(recent20.get("liq_void", pd.Series(False)).sum())

        return {
            "smc_disp_bull": bool(tail["disp_bull"].any()),
            "smc_disp_bear": bool(tail["disp_bear"].any()),
            "smc_fvg_bull": bool(tail["fvg_bull"].any()),
            "smc_fvg_bear": bool(tail["fvg_bear"].any()),
            "smc_ob_bull": bool(tail.get("ob_bull", pd.Series(False)).any()),
            "smc_ob_bear": bool(tail.get("ob_bear", pd.Series(False)).any()),
            "smc_liq_sweep": bool(tail["liq_sweep"].any()),
            "smc_liq_void": bool(tail["liq_void"].any()),
            "smc_equal_high": bool(tail.get("equal_high", pd.Series(False)).any()),
            "smc_equal_low": bool(tail.get("equal_low", pd.Series(False)).any()),
            "smc_top_poi": round(top_poi, 1),
            "smc_recent_poi": round(recent_poi, 1),
            "smc_ob_bull_price": latest_ob_bull_price,
            "smc_ob_bear_price": latest_ob_bear_price,
            "smc_fvg_count_20": fvg_count,
            "smc_sweep_count_20": sweep_count,
            "smc_void_count_20": void_count,
        }

    except Exception:
        return _empty_smc()


def _empty_smc() -> Dict:
    return {
        "smc_disp_bull": False,
        "smc_disp_bear": False,
        "smc_fvg_bull": False,
        "smc_fvg_bear": False,
        "smc_ob_bull": False,
        "smc_ob_bear": False,
        "smc_liq_sweep": False,
        "smc_liq_void": False,
        "smc_equal_high": False,
        "smc_equal_low": False,
        "smc_top_poi": 0.0,
        "smc_recent_poi": 0.0,
        "smc_ob_bull_price": None,
        "smc_ob_bear_price": None,
        "smc_fvg_count_20": 0,
        "smc_sweep_count_20": 0,
        "smc_void_count_20": 0,
    }
