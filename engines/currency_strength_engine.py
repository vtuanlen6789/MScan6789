from __future__ import annotations

from typing import Dict, List, Tuple

import pandas as pd


REQUIRED_PAIRS = [
    "USDJPY",
    "EURUSD",
    "EURJPY",
    "GBPJPY",
    "GBPUSD",
    "EURGBP",
    "USDCHF",
    "EURCHF",
    "GBPCHF",
    "CHFJPY",
]

CURRENCIES = ["USD", "EUR", "JPY", "GBP", "CHF"]


def compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()

    rs = avg_gain / avg_loss.replace(0, pd.NA)
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(50)


def compute_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high_low = df["high"] - df["low"]
    high_prev_close = (df["high"] - df["close"].shift(1)).abs()
    low_prev_close = (df["low"] - df["close"].shift(1)).abs()

    tr = pd.concat([high_low, high_prev_close, low_prev_close], axis=1).max(axis=1)
    return tr.rolling(period, min_periods=period).mean()


def compute_pair_metrics(df: pd.DataFrame, rsi_period: int = 14, atr_period: int = 14) -> Dict[str, float]:
    close = df["close"]
    open_price = float(df["open"].iloc[-1])

    rsi_last = float(compute_rsi(close, rsi_period).iloc[-1])
    atr_last = float(compute_atr(df, atr_period).iloc[-1])

    if open_price == 0:
        price_change = 0.0
    else:
        price_change = float(((float(close.iloc[-1]) - open_price) / open_price) * 100.0)

    return {
        "rsi": rsi_last,
        "pc": price_change,
        "atr": atr_last,
    }


def _avg(values: List[float]) -> float:
    return sum(values) / len(values)


def compute_currency_strength(pair_metrics: Dict[str, Dict[str, float]]) -> Tuple[Dict[str, Dict[str, float]], List[str]]:
    missing_pairs = [pair for pair in REQUIRED_PAIRS if pair not in pair_metrics]

    if missing_pairs:
        empty = {ccy: {"rsi": None, "pc": None, "atr": None} for ccy in CURRENCIES}
        return empty, missing_pairs

    def v(pair: str, metric: str) -> float:
        return float(pair_metrics[pair][metric])

    usd_rsi = _avg([v("USDJPY", "rsi"), 100 - v("EURUSD", "rsi"), 100 - v("GBPUSD", "rsi"), v("USDCHF", "rsi")])
    jpy_rsi = _avg([100 - v("USDJPY", "rsi"), 100 - v("EURJPY", "rsi"), 100 - v("GBPJPY", "rsi"), 100 - v("CHFJPY", "rsi")])
    eur_rsi = _avg([v("EURUSD", "rsi"), v("EURJPY", "rsi"), v("EURGBP", "rsi"), v("EURCHF", "rsi")])
    gbp_rsi = _avg([v("GBPUSD", "rsi"), v("GBPJPY", "rsi"), 100 - v("EURGBP", "rsi"), v("GBPCHF", "rsi")])
    chf_rsi = _avg([v("USDCHF", "rsi"), v("EURCHF", "rsi"), v("GBPCHF", "rsi"), v("CHFJPY", "rsi")])

    usd_pc = _avg([v("USDJPY", "pc"), -v("EURUSD", "pc"), -v("GBPUSD", "pc"), v("USDCHF", "pc")])
    jpy_pc = _avg([-v("USDJPY", "pc"), -v("EURJPY", "pc"), -v("GBPJPY", "pc"), -v("CHFJPY", "pc")])
    eur_pc = _avg([v("EURUSD", "pc"), v("EURJPY", "pc"), v("EURGBP", "pc"), v("EURCHF", "pc")])
    gbp_pc = _avg([v("GBPUSD", "pc"), v("GBPJPY", "pc"), -v("EURGBP", "pc"), v("GBPCHF", "pc")])
    chf_pc = _avg([v("USDCHF", "pc"), v("EURCHF", "pc"), v("GBPCHF", "pc"), v("CHFJPY", "pc")])

    usd_atr = _avg([v("USDJPY", "atr"), v("EURUSD", "atr"), v("GBPUSD", "atr"), v("USDCHF", "atr")])
    jpy_atr = _avg([v("USDJPY", "atr"), v("EURJPY", "atr"), v("GBPJPY", "atr"), v("CHFJPY", "atr")])
    eur_atr = _avg([v("EURUSD", "atr"), v("EURJPY", "atr"), v("EURGBP", "atr"), v("EURCHF", "atr")])
    gbp_atr = _avg([v("GBPUSD", "atr"), v("GBPJPY", "atr"), v("EURGBP", "atr"), v("GBPCHF", "atr")])
    chf_atr = _avg([v("USDCHF", "atr"), v("EURCHF", "atr"), v("GBPCHF", "atr"), v("CHFJPY", "atr")])

    strength = {
        "USD": {"rsi": usd_rsi, "pc": usd_pc, "atr": usd_atr},
        "EUR": {"rsi": eur_rsi, "pc": eur_pc, "atr": eur_atr},
        "JPY": {"rsi": jpy_rsi, "pc": jpy_pc, "atr": jpy_atr},
        "GBP": {"rsi": gbp_rsi, "pc": gbp_pc, "atr": gbp_atr},
        "CHF": {"rsi": chf_rsi, "pc": chf_pc, "atr": chf_atr},
    }
    return strength, []
