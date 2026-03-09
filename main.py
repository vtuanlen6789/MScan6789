import pandas as pd
from datetime import datetime
from pathlib import Path

from config import PAIRS, TRADING_MODE, FORMATION_BARS
from data_layer import initialize_data_source, get_data, TF_D1, TF_H4, TF_M30, TF_M5

from engines.structure_engine import detect_direction, direction_label
from engines.momentum_engine import calculate_multi_tf_momentum
from engines.state_engine import calculate_history, calculate_driven, detect_state_from_history_driven
from engines.conflict_engine import detect_conflict
from engines.scoring_engine import compliance_score, trust_score, calculate_score_from_components
from engines.formation_engine import (
    build_formation_snapshot,
    get_anchor_risk_zone,
    get_entry_decision,
    formation_state_to_core,
)
from engines.analysis_engine import (
    structural_summary,
    analytical_focus,
    clarity_rating,
    calculate_momentum_memory,
    detect_momentum_driver,
    detect_cycle_state,
)
from engines.opportunity_engine import build_opportunity_row, correlation_filter
from engines.swing_engine import build_swing_analysis
from engines.smc_engine import run_smc_analysis
from engines.killzone_engine import build_entry_signal
from engines.currency_strength_engine import (
    REQUIRED_PAIRS as CURRENCY_STRENGTH_PAIRS,
    compute_pair_metrics,
    compute_currency_strength,
)


BASE_DIR = Path(__file__).resolve().parent
LOG_FILE = BASE_DIR / "logs" / "scan_log.csv"


def run_scanner(trading_mode=None):
    results = []

    mode = (trading_mode or TRADING_MODE).strip().upper()
    if mode not in {"FAST", "STABLE"}:
        mode = TRADING_MODE

    for pair in PAIRS:
        m5 = get_data(pair, TF_M5)
        d1 = get_data(pair, TF_D1)
        h4 = get_data(pair, TF_H4)
        m30 = get_data(pair, TF_M30)

        if any(x is None for x in [m5, d1, h4, m30]):
            continue

        d1_dir = detect_direction(d1)
        h4_dir = detect_direction(h4)
        m30_dir = detect_direction(m30)
        m5_dir = detect_direction(m5)

        momentum_pack = calculate_multi_tf_momentum(m5, m30, h4, d1)
        current_mom = momentum_pack["current"]
        history_mom = momentum_pack["history"]

        m5_history_level = calculate_history(current_mom["m5"], *history_mom["m5"])
        m30_history_level = calculate_history(current_mom["m30"], *history_mom["m30"])
        h4_history_level = calculate_history(current_mom["h4"], history_mom["h4"][0], history_mom["h4"][1], history_mom["h4"][2], 0)
        d1_history_level = calculate_history(current_mom["d1"], history_mom["d1"][0], history_mom["d1"][1], 0, 0)

        m5_driven = calculate_driven(m5)
        m30_driven = calculate_driven(m30)
        h4_driven = calculate_driven(h4)
        d1_driven = calculate_driven(d1)

        m5_state = detect_state_from_history_driven(m5_history_level, m5_driven, m5_dir)
        m30_state = detect_state_from_history_driven(m30_history_level, m30_driven, m30_dir)
        h4_state = detect_state_from_history_driven(h4_history_level, h4_driven, h4_dir)
        d1_state = detect_state_from_history_driven(d1_history_level, d1_driven, d1_dir)

        m5_score_display = calculate_score_from_components(m5_dir, m5_history_level, m5_driven)
        m30_score_display = calculate_score_from_components(m30_dir, m30_history_level, m30_driven)
        h4_score_display = calculate_score_from_components(h4_dir, h4_history_level, h4_driven)
        d1_score_display = calculate_score_from_components(d1_dir, d1_history_level, d1_driven)

        m5_transfer_avg = sum(history_mom["m5"]) / len(history_mom["m5"])
        m30_transfer_avg = sum(history_mom["m30"]) / len(history_mom["m30"])

        m30_memory = calculate_momentum_memory(
            current_mom["m30"],
            history_mom["m30"][0],
            history_mom["m30"][1],
            history_mom["m30"][2],
            m5_transfer_avg,
        )
        h4_memory = calculate_momentum_memory(
            current_mom["h4"],
            history_mom["h4"][0],
            history_mom["h4"][1],
            history_mom["h4"][2],
            m30_transfer_avg,
        )
        d1_memory = calculate_momentum_memory(
            current_mom["d1"],
            history_mom["d1"][0],
            history_mom["d1"][1],
            0,
            (current_mom["h4"] + history_mom["h4"][0]) / 2.0,
        )

        h4_driver = detect_momentum_driver(
            history_mom["m5"][0],
            current_mom["m5"],
            history_mom["m30"][0],
            current_mom["m30"],
            history_mom["h4"][0],
            current_mom["h4"],
            history_mom["d1"][0],
            current_mom["d1"],
        )

        cycle, cycle_state, legacy_entry, risk_info = detect_cycle_state(
            d1_score_display,
            h4_score_display,
            m30_score_display,
        )

        d1_dir_label = direction_label(d1_dir)
        h4_dir_label = direction_label(h4_dir)
        m30_dir_label = direction_label(m30_dir)
        m5_dir_label = direction_label(m5_dir)

        conflict = detect_conflict(current_mom["m5"], current_mom["m30"], current_mom["h4"], current_mom["d1"])
        compliance = compliance_score(d1_score_display, h4_score_display, m30_score_display, d1_dir, h4_dir)
        trust = trust_score(compliance, conflict)

        anchor_tf = "H4" if mode == "FAST" else "D1"
        formation_tf = "M5" if mode == "FAST" else "M30"

        anchor_direction = h4_dir if mode == "FAST" else d1_dir
        anchor_phase = h4_state if mode == "FAST" else d1_state
        anchor_history = h4_history_level if mode == "FAST" else d1_history_level
        anchor_driven = h4_driven if mode == "FAST" else d1_driven
        anchor_risk_zone = get_anchor_risk_zone(anchor_history, anchor_driven, anchor_phase)

        formation_snapshot = build_formation_snapshot(
            mode,
            FORMATION_BARS,
            m5,
            m30,
            h4,
            d1,
        )

        if formation_snapshot["formationReady"]:
            entry_status, entry_reason, size_factor = get_entry_decision(
                anchor_direction,
                formation_snapshot["formationState"],
                formation_snapshot["formationStatePrevious"],
                anchor_risk_zone,
            )
        else:
            entry_status = "⏳ LOADING..."
            entry_reason = (
                f"Collecting formation data ({formation_snapshot['formationArraySize']}/{FORMATION_BARS} bars)"
            )
            size_factor = 0.0

        core = formation_state_to_core(formation_snapshot["formationState"])

        if conflict == 0:
            conflict_level = "Perfect"
        elif conflict <= 2:
            conflict_level = "Minor"
        elif conflict <= 4:
            conflict_level = "Moderate"
        else:
            conflict_level = "High"

        summary = structural_summary(d1_dir_label, h4_dir_label, cycle_state, cycle=cycle, conflict=conflict)
        focus = analytical_focus(core, mode, current_state=cycle_state, driver=h4_driver)
        clarity = clarity_rating(trust, conflict)

        results.append({
            "Pair": pair,
            "Trust": trust,
            "Clarity": clarity,
            "D1": d1_dir_label,
            "H4": h4_dir_label,
            "State": anchor_phase,
            "Core": core,
            "Mode": mode,
            "Momentum": current_mom["m30"],
            "Cycle": cycle,
            "CycleState": cycle_state,
            "Entry": entry_status,
            "ConflictScore": conflict,
            "ConflictLevel": conflict_level,
            "M5_Dir": m5_dir_label,
            "M5_Hist": m5_history_level,
            "M5_Drv": m5_driven,
            "M5_State": m5_state,
            "M5_Score": m5_score_display,
            "M30_Dir": m30_dir_label,
            "M30_Hist": m30_history_level,
            "M30_Drv": m30_driven,
            "M30_State": m30_state,
            "M30_Score": m30_score_display,
            "H4_Dir": h4_dir_label,
            "H4_Hist": h4_history_level,
            "H4_Drv": h4_driven,
            "H4_State": h4_state,
            "H4_Score": h4_score_display,
            "D1_Dir": d1_dir_label,
            "D1_Hist": d1_history_level,
            "D1_Drv": d1_driven,
            "D1_State": d1_state,
            "D1_Score": d1_score_display,
            "M30_Memory": round(m30_memory, 3),
            "H4_Memory": round(h4_memory, 3),
            "D1_Memory": round(d1_memory, 3),
            "Driver": h4_driver,
            "Summary": f"{summary} | Entry: {entry_status}",
            "Focus": f"{focus} | Driver: {h4_driver} | Risk: {risk_info} | Mem(H4): {h4_memory:.2f}",
            "EntryReason": entry_reason,
            "SizeFactor": size_factor,
            "AnchorTF": anchor_tf,
            "AnchorDirection": direction_label(anchor_direction),
            "AnchorPhase": anchor_phase,
            "AnchorRiskZone": anchor_risk_zone,
            "FormationTF": formation_tf,
            "FormationReady": formation_snapshot["formationReady"],
            "FormationStatePrevious": formation_snapshot["formationStatePrevious"],
            "FormationState": formation_snapshot["formationState"],
            "FormationBias": formation_snapshot["formationBias"],
            "FormationDrive": formation_snapshot["formationDrive"],
            "SwingCount": formation_snapshot["swingCount"],
            "CompressionRatio": formation_snapshot["compressionRatio"],
            "FormationBars": FORMATION_BARS,
        })

    results = sorted(results, key=lambda x: x["Trust"], reverse=True)

    save_log(results)

    return results


def run_opportunity_scanner():
    results = []

    for pair in PAIRS:
        m5 = get_data(pair, TF_M5)
        d1 = get_data(pair, TF_D1)
        h4 = get_data(pair, TF_H4)
        m30 = get_data(pair, TF_M30)

        if any(x is None for x in [m5, d1, h4, m30]):
            continue

        row = build_opportunity_row(pair, m5, m30, h4, d1)
        results.append(row)

    ranked = sorted(results, key=lambda x: x["score"], reverse=True)
    top3 = correlation_filter(ranked, limit=3)

    return ranked, top3


def run_smc_scanner():
    """Run SMC analysis (BOS/CHoCH/FVG/OB/POI/Killzone) for all pairs on H4 + D1."""
    results = []

    for pair in PAIRS:
        h4 = get_data(pair, TF_H4)
        d1 = get_data(pair, TF_D1)
        m30 = get_data(pair, TF_M30)

        if any(x is None for x in [h4, d1]):
            continue

        # Swing analysis on H4 (anchor structure)
        swing_h4 = build_swing_analysis(h4)
        swing_d1 = build_swing_analysis(d1)

        # SMC analysis per timeframe
        smc_h4 = run_smc_analysis(h4)
        smc_d1 = run_smc_analysis(d1)
        smc_m30 = run_smc_analysis(m30) if m30 is not None else {}

        # Entry model based on H4 (primary execution frame)
        entry = build_entry_signal(smc_h4, swing_h4)

        results.append({
            "pair": pair,
            # Swing / structure
            "h4_swing_trend": swing_h4.get("swing_trend"),
            "h4_bos_up": swing_h4.get("swing_bos_up"),
            "h4_bos_down": swing_h4.get("swing_bos_down"),
            "h4_choch_bull": swing_h4.get("swing_choch_bull"),
            "h4_choch_bear": swing_h4.get("swing_choch_bear"),
            "h4_last_sh": swing_h4.get("swing_last_sh"),
            "h4_last_sl": swing_h4.get("swing_last_sl"),
            "d1_swing_trend": swing_d1.get("swing_trend"),
            "d1_bos_up": swing_d1.get("swing_bos_up"),
            "d1_bos_down": swing_d1.get("swing_bos_down"),
            "d1_choch_bull": swing_d1.get("swing_choch_bull"),
            "d1_choch_bear": swing_d1.get("swing_choch_bear"),
            # SMC H4
            "h4_disp_bull": smc_h4.get("smc_disp_bull"),
            "h4_disp_bear": smc_h4.get("smc_disp_bear"),
            "h4_fvg_bull": smc_h4.get("smc_fvg_bull"),
            "h4_fvg_bear": smc_h4.get("smc_fvg_bear"),
            "h4_ob_bull": smc_h4.get("smc_ob_bull"),
            "h4_ob_bear": smc_h4.get("smc_ob_bear"),
            "h4_liq_sweep": smc_h4.get("smc_liq_sweep"),
            "h4_equal_high": smc_h4.get("smc_equal_high"),
            "h4_equal_low": smc_h4.get("smc_equal_low"),
            "h4_poi_score": smc_h4.get("smc_recent_poi"),
            "h4_ob_bull_price": smc_h4.get("smc_ob_bull_price"),
            "h4_ob_bear_price": smc_h4.get("smc_ob_bear_price"),
            "h4_fvg_count": smc_h4.get("smc_fvg_count_20"),
            "h4_sweep_count": smc_h4.get("smc_sweep_count_20"),
            # SMC D1
            "d1_poi_score": smc_d1.get("smc_recent_poi"),
            "d1_ob_bull": smc_d1.get("smc_ob_bull"),
            "d1_ob_bear": smc_d1.get("smc_ob_bear"),
            "d1_fvg_bull": smc_d1.get("smc_fvg_bull"),
            "d1_fvg_bear": smc_d1.get("smc_fvg_bear"),
            # SMC M30
            "m30_poi_score": smc_m30.get("smc_recent_poi"),
            "m30_liq_sweep": smc_m30.get("smc_liq_sweep"),
            "m30_ob_bull": smc_m30.get("smc_ob_bull"),
            "m30_ob_bear": smc_m30.get("smc_ob_bear"),
            # Entry model
            "entry_signal": entry.get("entry_signal"),
            "entry_dir": entry.get("entry_dir"),
            "entry_reason": entry.get("entry_reason"),
            "entry_confluence": entry.get("entry_confluence_score"),
            "killzone_active": entry.get("killzone_active"),
            "killzone_name": entry.get("killzone_name"),
        })

    # Sort by H4 POI score descending
    results = sorted(results, key=lambda x: x.get("h4_poi_score") or 0, reverse=True)
    return results


def run_currency_strength_table(rsi_period: int = 14, atr_period: int = 14):
    rows = []
    timeframe_map = [
        ("M30", TF_M30),
        ("H4", TF_H4),
        ("D1", TF_D1),
    ]

    for timeframe_label, timeframe_code in timeframe_map:
        pair_metrics = {}

        for pair in CURRENCY_STRENGTH_PAIRS:
            df = get_data(pair, timeframe_code)
            if df is None or df.empty:
                continue

            pair_metrics[pair] = compute_pair_metrics(
                df,
                rsi_period=rsi_period,
                atr_period=atr_period,
            )

        strength, missing_pairs = compute_currency_strength(pair_metrics)
        rows.append({
            "timeframe": timeframe_label,
            "currencies": strength,
            "missingPairs": missing_pairs,
        })

    return rows


def save_log(results):
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(results)
    df["timestamp"] = datetime.now()

    try:
        df.to_csv(LOG_FILE, mode="a", header=False, index=False)
    except Exception:
        df.to_csv(LOG_FILE, index=False)


if __name__ == "__main__":
    initialize_data_source()
    output = run_scanner()
    print(pd.DataFrame(output))
