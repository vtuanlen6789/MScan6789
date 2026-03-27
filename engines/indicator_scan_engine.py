from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd
import numpy as np

from config import PAIRS
from data_layer import TF_D1, TF_H4, TF_M30, TF_M5, get_data


RSI_LENGTH = 8
ATR_LENGTH = 8
SMA_LENGTH = 13
WMA_LENGTH = 55

TIMEFRAME_MAP = [
    ("D1", TF_D1),
    ("H4", TF_H4),
    ("M30", TF_M30),
    ("M5", TF_M5),
]


def _rma(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()


def _sma(series: pd.Series, length: int) -> pd.Series:
    return series.rolling(length, min_periods=length).mean()


def _wma(series: pd.Series, length: int) -> pd.Series:
    weights = np.arange(1, length + 1, dtype="float64")
    weight_sum = float(weights.sum())
    return series.rolling(length, min_periods=length).apply(
        lambda values: float((values * weights).sum() / weight_sum),
        raw=True,
    )


def compute_rsi_with_ma(close: pd.Series, rsi_length: int = RSI_LENGTH) -> Dict[str, pd.Series]:
    change = close.diff()
    up = _rma(change.clip(lower=0), rsi_length)
    down = _rma((-change.clip(upper=0)), rsi_length)

    rs = up / down.replace(0, pd.NA)
    rsi = 100 - (100 / (1 + rs))
    rsi = rsi.where(down != 0, 100)
    rsi = rsi.where(up != 0, 0)

    return {
        "rsi": rsi,
        "rsiwSMA": _sma(rsi, SMA_LENGTH),
        "rsiwWMA": _wma(rsi, WMA_LENGTH),
    }


def compute_atr_with_ma(df: pd.DataFrame, atr_length: int = ATR_LENGTH) -> Dict[str, pd.Series]:
    high_low = df["high"] - df["low"]
    high_prev_close = (df["high"] - df["close"].shift(1)).abs()
    low_prev_close = (df["low"] - df["close"].shift(1)).abs()
    tr = pd.concat([high_low, high_prev_close, low_prev_close], axis=1).max(axis=1)

    atr = _rma(tr, atr_length)

    return {
        "atr": atr,
        "atrwSMA": _sma(atr, SMA_LENGTH),
        "atrwWMA": _wma(atr, WMA_LENGTH),
    }


def _safe_last(series: Optional[pd.Series], digits: int = 4) -> Optional[float]:
    if series is None or series.empty:
        return None

    valid = series.dropna()
    if valid.empty:
        return None

    return round(float(valid.iloc[-1]), digits)


def build_indicator_row(pair: str, timeframe: str, df: Optional[pd.DataFrame]) -> Dict[str, Any]:
    base_row: Dict[str, Any] = {
        "Pair": pair,
        "Timeframe": timeframe,
        "RSI": None,
        "RSIwSMA": None,
        "RSIwWMA": None,
        "ATR": None,
        "ATRwSMA": None,
        "ATRwWMA": None,
    }

    if df is None or df.empty:
        return base_row

    rsi_pack = compute_rsi_with_ma(df["close"])
    atr_pack = compute_atr_with_ma(df)

    base_row.update(
        {
            "RSI": _safe_last(rsi_pack["rsi"], digits=2),
            "RSIwSMA": _safe_last(rsi_pack["rsiwSMA"], digits=2),
            "RSIwWMA": _safe_last(rsi_pack["rsiwWMA"], digits=2),
            "ATR": _safe_last(atr_pack["atr"], digits=5),
            "ATRwSMA": _safe_last(atr_pack["atrwSMA"], digits=5),
            "ATRwWMA": _safe_last(atr_pack["atrwWMA"], digits=5),
        }
    )
    return base_row


def run_indicator_scan_table() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for pair in PAIRS:
        for timeframe_label, timeframe_code in TIMEFRAME_MAP:
            rows.append(build_indicator_row(pair, timeframe_label, get_data(pair, timeframe_code)))

    return rows