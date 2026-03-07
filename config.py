import os


PAIRS = [
    "EURUSD", "EURJPY", "USDJPY", "GBPJPY", "GBPUSD",
    "EURGBP", "EURCHF", "GBPCHF", "USDCHF", "CHFJPY", "XAUUSD"
]

CANDLE_COUNT = 300
MIN_TRUST = 70

# Data source mode:
# - "mt5": MetaTrader5 local terminal (Windows)
# - "yahoo": Yahoo Finance API (macOS friendly)
DATA_SOURCE = "yahoo"

# Pine V_1_6 compatibility settings
TRADING_MODE = os.getenv("BIZCLAW_TRADING_MODE", "FAST").strip().upper()
if TRADING_MODE not in {"FAST", "STABLE"}:
    TRADING_MODE = "FAST"

FORMATION_BARS = int(os.getenv("BIZCLAW_FORMATION_BARS", "48"))
FORMATION_BARS = max(24, min(96, FORMATION_BARS))
