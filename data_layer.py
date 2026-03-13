from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from config import CANDLE_COUNT, DATA_SOURCE, MT5_EXPORT_DIR, PAIRS, SUPPORTED_DATA_SOURCES

try:
    import MetaTrader5 as mt5
except Exception:
    mt5 = None

try:
    import yfinance as yf
except Exception:
    yf = None


TF_D1 = "D1"
TF_H4 = "H4"
TF_M30 = "M30"
TF_M5 = "M5"

YAHOO_SYMBOL_MAP = {
    "EURUSD": ["EURUSD=X"],
    "EURJPY": ["EURJPY=X"],
    "USDJPY": ["JPY=X", "USDJPY=X"],
    "CHFJPY": ["CHFJPY=X"],
    "GBPJPY": ["GBPJPY=X"],
    "GBPUSD": ["GBPUSD=X"],
    "EURGBP": ["EURGBP=X"],
    "EURCHF": ["EURCHF=X"],
    "GBPCHF": ["GBPCHF=X"],
    "USDCHF": ["CHF=X", "USDCHF=X"],
    "XAUUSD": ["GC=F", "XAUUSD=X"],
}

MT5_REQUIRED_TIMEFRAMES = [TF_M5, TF_M30, TF_H4, TF_D1]

_RUNTIME_SOURCE = DATA_SOURCE.lower()
_RUNTIME_MT5_EXPORT_DIR = os.path.abspath(os.path.expanduser(MT5_EXPORT_DIR))


def _normalize_source_name(source: Optional[str]) -> str:
    candidate = (source or DATA_SOURCE or "yahoo").strip().lower()
    if candidate not in SUPPORTED_DATA_SOURCES:
        raise ValueError(f"Unsupported DATA_SOURCE: {candidate}")
    return candidate


def _normalize_export_dir(export_dir: Optional[str]) -> str:
    candidate = (export_dir or MT5_EXPORT_DIR or "").strip()
    if not candidate:
        candidate = MT5_EXPORT_DIR
    return os.path.abspath(os.path.expanduser(candidate))


def set_runtime_data_source(source: Optional[str] = None, export_dir: Optional[str] = None) -> Dict[str, str]:
    global _RUNTIME_SOURCE, _RUNTIME_MT5_EXPORT_DIR

    if source is not None:
        _RUNTIME_SOURCE = _normalize_source_name(source)

    if export_dir is not None:
        _RUNTIME_MT5_EXPORT_DIR = _normalize_export_dir(export_dir)

    return get_runtime_data_source_context()


def get_runtime_data_source_context() -> Dict[str, str]:
    return {
        "source": _RUNTIME_SOURCE,
        "exportDir": _RUNTIME_MT5_EXPORT_DIR,
    }


def initialize_mt5():
    if mt5 is None:
        raise RuntimeError("MetaTrader5 package is not available in this environment")

    if not mt5.initialize():
        raise RuntimeError("MT5 init failed")


def initialize_data_source():
    if get_runtime_data_source_context()["source"] == "mt5":
        initialize_mt5()


def _parse_datetime_series(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_datetime(series, unit="s", errors="coerce")

    converted = pd.to_datetime(series, errors="coerce")
    if converted.notna().any():
        return converted

    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.notna().any():
        return pd.to_datetime(numeric, unit="s", errors="coerce")

    return converted


def _coerce_first_series(df: pd.DataFrame, column_name: str) -> Optional[pd.Series]:
    if column_name not in df.columns:
        return None

    selected = df.loc[:, column_name]
    if isinstance(selected, pd.DataFrame):
        if selected.shape[1] == 0:
            return None
        return selected.iloc[:, 0]
    return selected


def _canonicalize_ohlc_frame(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    required = ["open", "high", "low", "close"]
    canonical: Dict[str, pd.Series] = {}

    for col in required:
        series = _coerce_first_series(df, col)
        if series is None:
            return None
        canonical[col] = pd.to_numeric(series, errors="coerce")

    tick_volume = _coerce_first_series(df, "tick_volume")
    if tick_volume is not None:
        canonical["tick_volume"] = pd.to_numeric(tick_volume, errors="coerce")

    datetime_series = _coerce_first_series(df, "datetime")
    if datetime_series is not None:
        canonical["datetime"] = datetime_series

    out = pd.DataFrame(canonical)
    out = out.dropna(subset=required)

    if "datetime" in out.columns:
        out = out.dropna(subset=["datetime"]).sort_values("datetime")

    return out.reset_index(drop=True)


def _normalize_ohlc(df):
    if df is None or df.empty:
        return None

    if isinstance(df.columns, pd.MultiIndex):
        flattened = []
        for col in df.columns:
            if isinstance(col, tuple):
                flattened.append(col[0])
            else:
                flattened.append(col)
        df = df.copy()
        df.columns = flattened

    rename_map = {
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "tick_volume",
    }
    out = df.rename(columns=rename_map)
    return _canonicalize_ohlc_frame(out)


def _normalize_mt5_csv(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    if df is None or df.empty:
        return None

    out = df.copy()
    out.columns = [str(col).strip() for col in out.columns]

    normalized_name_map = {
        str(col).strip().lower().replace(" ", "").replace("<", "").replace(">", ""): col
        for col in out.columns
    }

    alias_groups = {
        "open": ["open"],
        "high": ["high"],
        "low": ["low"],
        "close": ["close"],
        "tick_volume": ["tick_volume", "tickvolume", "volume", "vol"],
        "date": ["date"],
        "time": ["time"],
        "datetime": ["datetime", "timestamp"],
    }

    rename_map: Dict[str, str] = {}
    for target, aliases in alias_groups.items():
        for alias in aliases:
            original = normalized_name_map.get(alias)
            if original is not None:
                rename_map[original] = target
                break

    out = out.rename(columns=rename_map)

    if "datetime" not in out.columns:
        if "date" in out.columns and "time" in out.columns:
            out["datetime"] = pd.to_datetime(
                out["date"].astype(str).str.strip() + " " + out["time"].astype(str).str.strip(),
                errors="coerce",
            )
        elif "time" in out.columns:
            out["datetime"] = _parse_datetime_series(out["time"])

    required = ["open", "high", "low", "close"]
    if any(col not in out.columns for col in required):
        return None

    for col in required + ["tick_volume"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    return _canonicalize_ohlc_frame(out)


def _candidate_mt5_paths(base_dir: Path, symbol: str, timeframe: str) -> List[Path]:
    symbol_upper = symbol.upper()
    timeframe_upper = timeframe.upper()

    direct_candidates = [
        base_dir / f"{symbol_upper}_{timeframe_upper}.csv",
        base_dir / f"{symbol_upper}-{timeframe_upper}.csv",
        base_dir / timeframe_upper / f"{symbol_upper}.csv",
        base_dir / symbol_upper / f"{timeframe_upper}.csv",
    ]

    recursive_patterns = [
        f"**/{symbol_upper}_{timeframe_upper}.csv",
        f"**/{symbol_upper}-{timeframe_upper}.csv",
        f"**/{symbol_upper}/{timeframe_upper}.csv",
        f"**/{timeframe_upper}/{symbol_upper}.csv",
        f"**/{symbol_upper.lower()}_{timeframe_upper.lower()}.csv",
        f"**/{symbol_upper.lower()}-{timeframe_upper.lower()}.csv",
    ]

    candidates: List[Path] = []
    for candidate in direct_candidates:
        if candidate.exists():
            candidates.append(candidate)

    for pattern in recursive_patterns:
        for candidate in sorted(base_dir.glob(pattern)):
            if candidate.is_file():
                candidates.append(candidate)

    unique_candidates: List[Path] = []
    seen = set()
    for candidate in candidates:
        resolved = str(candidate.resolve())
        if resolved in seen:
            continue
        seen.add(resolved)
        unique_candidates.append(candidate)

    return unique_candidates


def _resolve_mt5_csv_file(symbol: str, timeframe: str, export_dir: Optional[str] = None) -> Optional[Path]:
    base_dir = Path(_normalize_export_dir(export_dir) if export_dir is not None else get_runtime_data_source_context()["exportDir"])
    if not base_dir.exists() or not base_dir.is_dir():
        return None

    candidates = _candidate_mt5_paths(base_dir, symbol, timeframe)
    return candidates[0] if candidates else None


def summarize_mt5_export_dir(export_dir: Optional[str] = None) -> Dict[str, Any]:
    base_dir = Path(_normalize_export_dir(export_dir) if export_dir is not None else get_runtime_data_source_context()["exportDir"])
    exists = base_dir.exists() and base_dir.is_dir()

    matched_files: List[Dict[str, str]] = []
    missing_files: List[str] = []

    if exists:
        for symbol in PAIRS:
            for timeframe in MT5_REQUIRED_TIMEFRAMES:
                resolved = _resolve_mt5_csv_file(symbol, timeframe, str(base_dir))
                if resolved is None:
                    missing_files.append(f"{symbol}_{timeframe}.csv")
                else:
                    matched_files.append({
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "path": str(resolved),
                    })
    else:
        for symbol in PAIRS:
            for timeframe in MT5_REQUIRED_TIMEFRAMES:
                missing_files.append(f"{symbol}_{timeframe}.csv")

    available_symbols = sorted({item["symbol"] for item in matched_files})
    available_timeframes = sorted({item["timeframe"] for item in matched_files})
    file_count = len(list(base_dir.rglob("*.csv"))) if exists else 0

    return {
        "configured": True,
        "baseDir": str(base_dir),
        "exists": exists,
        "fileCount": file_count,
        "matchedFiles": matched_files,
        "missingFiles": missing_files,
        "availableSymbols": available_symbols,
        "availableTimeframes": available_timeframes,
    }


def _fetch_mt5_csv(symbol: str, timeframe: str, export_dir: Optional[str] = None):
    resolved = _resolve_mt5_csv_file(symbol, timeframe, export_dir)
    if resolved is None:
        return None

    raw = pd.read_csv(resolved)
    normalized = _normalize_mt5_csv(raw)
    if normalized is None or normalized.empty:
        return None

    if len(normalized) < CANDLE_COUNT:
        return None

    return normalized.tail(CANDLE_COUNT).reset_index(drop=True)


def _resample_h4(df_1h):
    if df_1h is None or df_1h.empty:
        return None

    out = df_1h.copy()
    if isinstance(out.columns, pd.MultiIndex):
        flattened = []
        for col in out.columns:
            if isinstance(col, tuple):
                flattened.append(col[0])
            else:
                flattened.append(col)
        out.columns = flattened

    rename_map = {
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "volume": "Volume",
    }
    out = out.rename(columns=rename_map)

    if "Volume" not in out.columns:
        out["Volume"] = 0

    out.index = pd.to_datetime(out.index)
    rs = out.resample("4h")

    h4 = pd.DataFrame({
        "Open": rs["Open"].first(),
        "High": rs["High"].max(),
        "Low": rs["Low"].min(),
        "Close": rs["Close"].last(),
        "Volume": rs["Volume"].sum(),
    }).dropna(subset=["Open", "High", "Low", "Close"])

    return h4


def _fetch_yahoo(symbol, timeframe):
    if yf is None:
        raise RuntimeError("yfinance is not installed")

    aliases = YAHOO_SYMBOL_MAP.get(symbol, [symbol])

    if timeframe == TF_D1:
        interval, period = "1d", "2y"
    elif timeframe == TF_H4:
        interval, period = "1h", "730d"
    elif timeframe == TF_M30:
        interval, period = "30m", "60d"
    elif timeframe == TF_M5:
        interval, period = "5m", "60d"
    else:
        raise ValueError(f"Unsupported timeframe: {timeframe}")

    for yahoo_symbol in aliases:
        raw = yf.download(
            tickers=yahoo_symbol,
            interval=interval,
            period=period,
            progress=False,
            auto_adjust=False,
            threads=False,
        )

        if raw is None or raw.empty:
            continue

        if timeframe == TF_H4:
            raw = _resample_h4(raw)

        normalized = _normalize_ohlc(raw)
        if normalized is None or normalized.empty:
            continue

        if len(normalized) < CANDLE_COUNT:
            continue

        return normalized.tail(CANDLE_COUNT).reset_index(drop=True)

    return None


def get_data(symbol, timeframe):
    context = get_runtime_data_source_context()
    source = context["source"]

    if source == "mt5":
        if mt5 is None:
            raise RuntimeError("MetaTrader5 package is not available in this environment")

        tf_map = {
            TF_D1: mt5.TIMEFRAME_D1,
            TF_H4: mt5.TIMEFRAME_H4,
            TF_M30: mt5.TIMEFRAME_M30,
            TF_M5: mt5.TIMEFRAME_M5,
        }
        mt5_timeframe = tf_map.get(timeframe)
        if mt5_timeframe is None:
            raise ValueError(f"Unsupported timeframe: {timeframe}")

        rates = mt5.copy_rates_from_pos(symbol, mt5_timeframe, 0, CANDLE_COUNT)
        if rates is None or len(rates) == 0:
            return None
        return pd.DataFrame(rates)

    if source == "mt5_csv":
        return _fetch_mt5_csv(symbol, timeframe, context["exportDir"])

    if source == "yahoo":
        return _fetch_yahoo(symbol, timeframe)

    raise ValueError(f"Unsupported DATA_SOURCE: {source}")
