"""
Killzone Engine – London / New York session filter + SMC Entry Model.

Entry signal requires confluence of:
  ✓ CHoCH (trend reversal signal)
  ✓ Liquidity Sweep (stop hunt occurred)
  ✓ Order Block present
  ✓ Inside London or New York killzone
  ✓ Positive recent POI score (>= 4)
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Optional

import pandas as pd


# ─────────────────────────────────────────────
# 1. Killzone definition (UTC hours)
# London : 07:00 – 10:00 UTC
# New York: 13:00 – 16:00 UTC
# Tokyo  : 00:00 – 03:00 UTC  (optional)
# ─────────────────────────────────────────────

KILLZONES = {
    "LONDON": (7, 10),
    "NEW_YORK": (13, 16),
    "TOKYO": (0, 3),
}


def current_killzone(now: Optional[datetime] = None) -> Optional[str]:
    """Returns the active killzone name or None if outside all sessions."""
    if now is None:
        now = datetime.now(timezone.utc)

    hour = now.hour
    for name, (start, end) in KILLZONES.items():
        if start <= hour < end:
            return name
    return None


def is_in_killzone(now: Optional[datetime] = None) -> bool:
    return current_killzone(now) is not None


def annotate_killzone(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds 'killzone' bool column and 'killzone_name' string column.
    Requires DatetimeIndex (UTC). Falls back to False if index is not datetime.
    """
    df = df.copy()
    df["killzone"] = False
    df["killzone_name"] = ""

    if not isinstance(df.index, pd.DatetimeIndex):
        return df

    idx_utc = df.index.tz_localize("UTC") if df.index.tzinfo is None else df.index.tz_convert("UTC")
    hours = idx_utc.hour

    for name, (start, end) in KILLZONES.items():
        mask = (hours >= start) & (hours < end)
        df.loc[mask, "killzone"] = True
        df.loc[mask, "killzone_name"] = name

    return df


# ─────────────────────────────────────────────
# 2. Entry Model
# Confluence: CHoCH + Sweep + OB + Killzone + POI >= 4
# ─────────────────────────────────────────────

def build_entry_signal(smc_summary: Dict, swing_summary: Dict, now: Optional[datetime] = None) -> Dict:
    """
    Combines Swing analysis + SMC analysis + current time to produce
    an entry signal with direction and reason.

    Parameters
    ----------
    smc_summary  : output of smc_engine.run_smc_analysis()
    swing_summary: output of swing_engine.build_swing_analysis()
    now          : current UTC datetime (defaults to datetime.utcnow)

    Returns
    -------
    dict with keys:
        entry_signal   : bool
        entry_dir      : "LONG" | "SHORT" | None
        entry_reason   : str
        killzone_active: bool
        killzone_name  : str | None
        poi_score      : float
    """
    kz = current_killzone(now)
    kz_active = kz is not None
    poi = float(smc_summary.get("smc_recent_poi", 0))

    # --- Bull entry conditions ---
    bull_choch = bool(swing_summary.get("swing_choch_bull", False))
    bull_ob = bool(smc_summary.get("smc_ob_bull", False))
    sweep = bool(smc_summary.get("smc_liq_sweep", False))
    fvg_bull = bool(smc_summary.get("smc_fvg_bull", False))

    # --- Bear entry conditions ---
    bear_choch = bool(swing_summary.get("swing_choch_bear", False))
    bear_ob = bool(smc_summary.get("smc_ob_bear", False))
    fvg_bear = bool(smc_summary.get("smc_fvg_bear", False))

    reasons_bull = []
    reasons_bear = []

    if bull_choch:
        reasons_bull.append("CHoCH↑")
    if sweep:
        reasons_bull.append("LiqSweep")
        reasons_bear.append("LiqSweep")
    if bull_ob:
        reasons_bull.append("BullOB")
    if fvg_bull:
        reasons_bull.append("FVG↑")
    if kz_active:
        reasons_bull.append(f"KZ:{kz}")
        reasons_bear.append(f"KZ:{kz}")
    if bear_choch:
        reasons_bear.append("CHoCH↓")
    if bear_ob:
        reasons_bear.append("BearOB")
    if fvg_bear:
        reasons_bear.append("FVG↓")

    bull_score = (
        int(bull_choch) * 3
        + int(sweep) * 2
        + int(bull_ob) * 3
        + int(fvg_bull) * 2
        + int(kz_active) * 2
        + (1 if poi >= 4 else 0)
    )
    bear_score = (
        int(bear_choch) * 3
        + int(sweep) * 2
        + int(bear_ob) * 3
        + int(fvg_bear) * 2
        + int(kz_active) * 2
        + (1 if poi >= 4 else 0)
    )

    ENTRY_THRESHOLD = 7   # minimum confluence score to trigger signal

    if bull_score >= ENTRY_THRESHOLD and bull_score >= bear_score:
        return {
            "entry_signal": True,
            "entry_dir": "LONG",
            "entry_reason": " + ".join(reasons_bull) or "Confluence LONG",
            "entry_confluence_score": bull_score,
            "killzone_active": kz_active,
            "killzone_name": kz,
            "poi_score": poi,
        }

    if bear_score >= ENTRY_THRESHOLD:
        return {
            "entry_signal": True,
            "entry_dir": "SHORT",
            "entry_reason": " + ".join(reasons_bear) or "Confluence SHORT",
            "entry_confluence_score": bear_score,
            "killzone_active": kz_active,
            "killzone_name": kz,
            "poi_score": poi,
        }

    return {
        "entry_signal": False,
        "entry_dir": None,
        "entry_reason": "No confluence",
        "entry_confluence_score": max(bull_score, bear_score),
        "killzone_active": kz_active,
        "killzone_name": kz,
        "poi_score": poi,
    }
