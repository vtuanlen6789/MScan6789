import pandas as pd

from config import CANDLE_COUNT, DATA_SOURCE

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
    "GBPJPY": ["GBPJPY=X"],
    "GBPUSD": ["GBPUSD=X"],
    "EURGBP": ["EURGBP=X"],
    "EURCHF": ["EURCHF=X"],
    "GBPCHF": ["GBPCHF=X"],
    "USDCHF": ["CHF=X", "USDCHF=X"],
    "XAUUSD": ["GC=F", "XAUUSD=X"],
}


def initialize_mt5():
    if mt5 is None:
        raise RuntimeError("MetaTrader5 package is not available in this environment")

    if not mt5.initialize():
        raise RuntimeError("MT5 init failed")


def initialize_data_source():
    if DATA_SOURCE.lower() == "mt5":
        initialize_mt5()


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
    required = ["open", "high", "low", "close"]
    if any(col not in out.columns for col in required):
        return None

    return out.dropna(subset=required).reset_index(drop=True)


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
    source = DATA_SOURCE.lower()

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

    if source == "yahoo":
        return _fetch_yahoo(symbol, timeframe)

    raise ValueError(f"Unsupported DATA_SOURCE: {DATA_SOURCE}")
