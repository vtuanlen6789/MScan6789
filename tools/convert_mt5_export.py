from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

import pandas as pd

BIZCLAW_DIR = Path(__file__).resolve().parents[1]
if str(BIZCLAW_DIR) not in sys.path:
    sys.path.insert(0, str(BIZCLAW_DIR))

from config import PAIRS

DEFAULT_OUTPUT_DIR = (Path(__file__).resolve().parents[2] / "market_data" / "mt5").resolve()
SUPPORTED_TIMEFRAMES = ["M1", "M5", "M15", "M30", "H1", "H4", "D1", "W1", "MN1"]
SUPPORTED_EXTENSIONS = {".csv", ".txt", ".tsv"}


def _normalize_name(value: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", value.upper())


def _parse_datetime_series(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_datetime(series, unit="s", errors="coerce")

    converted = pd.to_datetime(series.astype(str).str.strip(), errors="coerce")
    if converted.notna().any():
        return converted

    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.notna().any():
        return pd.to_datetime(numeric, unit="s", errors="coerce")

    return converted


def _load_raw_csv(path: Path) -> pd.DataFrame:
    attempts = [
        {"sep": None, "engine": "python"},
        {"sep": "\t"},
        {"sep": ";"},
        {"sep": ","},
    ]

    last_error: Optional[Exception] = None
    for options in attempts:
        try:
            df = pd.read_csv(path, **options)
            if df is not None and not df.empty:
                return df
        except Exception as exc:  # pragma: no cover - defensive parsing fallback
            last_error = exc

    if last_error is not None:
        raise last_error

    return pd.DataFrame()


def _looks_headerless(df: pd.DataFrame) -> bool:
    columns = list(df.columns)
    if all(isinstance(col, int) for col in columns):
        return True

    normalized = {_normalize_name(str(col)) for col in columns}
    known = {
        "DATE", "TIME", "DATETIME", "TIMESTAMP",
        "OPEN", "HIGH", "LOW", "CLOSE",
        "TICKVOLUME", "VOLUME", "SPREAD",
    }
    return normalized.isdisjoint(known)


def _apply_headerless_columns(df: pd.DataFrame) -> pd.DataFrame:
    if not _looks_headerless(df):
        return df

    fallback_cols = [
        "date", "time", "open", "high", "low", "close",
        "tick_volume", "volume", "spread",
    ]
    renamed = df.copy()
    renamed.columns = fallback_cols[: len(renamed.columns)]
    return renamed


def normalize_mt5_export(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["datetime", "open", "high", "low", "close", "tick_volume"])

    out = _apply_headerless_columns(df.copy())
    out.columns = [str(col).strip() for col in out.columns]

    normalized_name_map = {
        _normalize_name(str(col)): col
        for col in out.columns
    }
    alias_groups = {
        "open": ["OPEN"],
        "high": ["HIGH"],
        "low": ["LOW"],
        "close": ["CLOSE"],
        "tick_volume": ["TICKVOLUME", "VOLUME", "VOL"],
        "date": ["DATE"],
        "time": ["TIME"],
        "datetime": ["DATETIME", "TIMESTAMP"],
    }

    rename_map = {}
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
        elif "date" in out.columns:
            out["datetime"] = _parse_datetime_series(out["date"])
        elif "time" in out.columns:
            out["datetime"] = _parse_datetime_series(out["time"])

    required = ["open", "high", "low", "close"]
    missing = [col for col in required if col not in out.columns]
    if missing:
        raise ValueError(f"Missing required OHLC columns: {', '.join(missing)}")

    for col in required + ["tick_volume"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    if "tick_volume" not in out.columns:
        out["tick_volume"] = 0

    out = out.dropna(subset=required)

    if "datetime" in out.columns:
        out = out.dropna(subset=["datetime"])
        out = out.sort_values("datetime").drop_duplicates(subset=["datetime"], keep="last")
    else:
        out["datetime"] = pd.NaT

    normalized = out[["datetime", "open", "high", "low", "close", "tick_volume"]].reset_index(drop=True)
    return normalized


def infer_symbol_timeframe(path: Path, symbol: Optional[str], timeframe: Optional[str]) -> Tuple[str, str]:
    if symbol and timeframe:
        return symbol.upper(), timeframe.upper()

    stem = _normalize_name(path.stem)

    detected_symbol = symbol.upper() if symbol else ""
    if not detected_symbol:
        for pair in sorted(PAIRS, key=len, reverse=True):
            if pair in stem:
                detected_symbol = pair
                break

    if not detected_symbol:
        generic_match = re.search(r"(XAUUSD|[A-Z]{6})", stem)
        if generic_match:
            detected_symbol = generic_match.group(1)

    detected_timeframe = timeframe.upper() if timeframe else ""
    if not detected_timeframe:
        for tf in sorted(SUPPORTED_TIMEFRAMES, key=len, reverse=True):
            if tf in stem:
                detected_timeframe = tf
                break

    if not detected_symbol or not detected_timeframe:
        raise ValueError(
            f"Cannot infer symbol/timeframe from file name '{path.name}'. "
            "Use --symbol and --timeframe for single-file conversion."
        )

    return detected_symbol, detected_timeframe


def collect_input_files(input_path: Path, recursive: bool) -> List[Path]:
    if input_path.is_file():
        return [input_path]

    if not input_path.exists():
        raise FileNotFoundError(f"Input path not found: {input_path}")

    iterator: Iterable[Path]
    if recursive:
        iterator = input_path.rglob("*")
    else:
        iterator = input_path.iterdir()

    files = [path for path in iterator if path.is_file() and path.suffix.lower() in SUPPORTED_EXTENSIONS]
    return sorted(files)


def convert_file(
    source_file: Path,
    output_dir: Path,
    symbol: Optional[str],
    timeframe: Optional[str],
    overwrite: bool,
) -> Tuple[Path, int]:
    detected_symbol, detected_timeframe = infer_symbol_timeframe(source_file, symbol, timeframe)
    raw = _load_raw_csv(source_file)
    normalized = normalize_mt5_export(raw)

    if normalized.empty:
        raise ValueError(f"No valid rows found in {source_file.name}")

    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"{detected_symbol}_{detected_timeframe}.csv"

    if output_file.exists() and not overwrite:
        raise FileExistsError(f"Output already exists: {output_file}")

    normalized.to_csv(output_file, index=False)
    return output_file, len(normalized)


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Convert MT5 export files into BizClaw local CSV format.",
    )
    parser.add_argument(
        "input",
        help="Input CSV file or directory exported from MT5.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Output directory for normalized BizClaw CSV files.",
    )
    parser.add_argument(
        "--symbol",
        help="Override symbol when converting a single file.",
    )
    parser.add_argument(
        "--timeframe",
        help="Override timeframe when converting a single file.",
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="Recursively scan subfolders for CSV/TXT export files.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing normalized output files.",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)

    input_path = Path(args.input).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    files = collect_input_files(input_path, recursive=args.recursive)

    if not files:
        parser.error("No CSV/TXT files found to convert.")

    converted = []
    failed = []

    for file_path in files:
        try:
            output_file, row_count = convert_file(
                source_file=file_path,
                output_dir=output_dir,
                symbol=args.symbol,
                timeframe=args.timeframe,
                overwrite=args.overwrite,
            )
            converted.append((file_path, output_file, row_count))
        except Exception as exc:
            failed.append((file_path, str(exc)))

    print(f"Converted: {len(converted)}")
    for source_file, output_file, row_count in converted:
        print(f"  OK  {source_file.name} -> {output_file.name} ({row_count} rows)")

    if failed:
        print(f"Failed: {len(failed)}")
        for file_path, message in failed:
            print(f"  ERR {file_path.name}: {message}")
        return 1

    print(f"Output directory: {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
