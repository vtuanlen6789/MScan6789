import pandas as pd
from datetime import datetime
from pathlib import Path

from config import PAIRS
from data_layer import initialize_data_source, get_data, TF_D1, TF_H4, TF_M30, TF_M5

from engines.structure_engine import detect_direction, direction_label
from engines.momentum_engine import calculate_multi_tf_momentum
from engines.state_engine import calculate_history, calculate_driven, detect_state_from_history_driven
from engines.core_engine import detect_core
from engines.conflict_engine import detect_conflict
from engines.mode_engine import detect_mode
from engines.scoring_engine import compliance_score, trust_score, calculate_score_from_components
from engines.analysis_engine import (
    structural_summary,
    analytical_focus,
    clarity_rating,
    calculate_momentum_memory,
    detect_momentum_driver,
    detect_cycle_state,
)


BASE_DIR = Path(__file__).resolve().parent
LOG_FILE = BASE_DIR / "logs" / "scan_log.csv"


def run_scanner():
    results = []

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

        cycle, cycle_state, entry_status, risk_info = detect_cycle_state(
            d1_score_display,
            h4_score_display,
            m30_score_display,
        )

        d1_dir_label = direction_label(d1_dir)
        h4_dir_label = direction_label(h4_dir)
        m30_dir_label = direction_label(m30_dir)
        m5_dir_label = direction_label(m5_dir)

        state = h4_state

        core = detect_core(d1_dir_label, h4_dir_label, abs(current_mom["m30"]) * 3)

        compliance = compliance_score(d1_score_display, h4_score_display, m30_score_display, d1_dir, h4_dir)
        conflict = detect_conflict(current_mom["m5"], current_mom["m30"], current_mom["h4"], current_mom["d1"])

        trust = trust_score(compliance, conflict)
        mode = detect_mode(state, conflict * 10)

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
            "State": state,
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
            "Focus": f"{focus} | Driver: {h4_driver} | Risk: {risk_info} | Mem(H4): {h4_memory:.2f}"
        })

    results = sorted(results, key=lambda x: x["Trust"], reverse=True)

    save_log(results)

    return results


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
