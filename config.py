import os
from pathlib import Path


PAIRS = [
    "EURUSD", "EURJPY", "USDJPY", "GBPJPY", "GBPUSD",
    "EURGBP", "EURCHF", "GBPCHF", "USDCHF", "CHFJPY", "XAUUSD"
]

BASE_DIR = Path(__file__).resolve().parent

CANDLE_COUNT = 300
MIN_TRUST = 70

SUPPORTED_DATA_SOURCES = {"yahoo", "mt5", "mt5_csv"}

# Data source mode:
# - "mt5": MetaTrader5 local terminal (Windows)
# - "yahoo": Yahoo Finance API (macOS friendly)
# - "mt5_csv": local MT5 export folder (macOS/Windows friendly)
DATA_SOURCE = os.getenv("BIZCLAW_DATA_SOURCE", "yahoo").strip().lower()
if DATA_SOURCE not in SUPPORTED_DATA_SOURCES:
    DATA_SOURCE = "yahoo"

DEFAULT_MT5_EXPORT_DIR = str((BASE_DIR.parent / "market_data" / "mt5").resolve())
MT5_EXPORT_DIR = os.getenv("BIZCLAW_MT5_EXPORT_DIR", DEFAULT_MT5_EXPORT_DIR).strip() or DEFAULT_MT5_EXPORT_DIR

# Pine V_1_6 compatibility settings
TRADING_MODE = os.getenv("BIZCLAW_TRADING_MODE", "FAST").strip().upper()
if TRADING_MODE not in {"FAST", "STABLE"}:
    TRADING_MODE = "FAST"

FORMATION_BARS = int(os.getenv("BIZCLAW_FORMATION_BARS", "48"))
FORMATION_BARS = max(24, min(96, FORMATION_BARS))

M5_M30_LOOKBACK_BARS = int(os.getenv("BIZCLAW_M5_M30_LOOKBACK_BARS", "48"))
M5_M30_LOOKBACK_BARS = max(24, min(96, M5_M30_LOOKBACK_BARS))
