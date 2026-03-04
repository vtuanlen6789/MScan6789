from typing import Dict, List, Tuple

from engines.momentum_engine import calculate_hybrid_momentum_at_offset
from engines.structure_engine import detect_direction
from engines.state_engine import calculate_driven


def calculate_formation_metrics(
    direction_array: List[int],
    driven_array: List[int],
) -> Tuple[int, float, int, float]:
    formation_bias = 0
    formation_drive = 0.0
    swing_count = 0
    counter_swings = 0

    array_size = len(direction_array)
    if array_size == 0:
        return 0, 0.0, 0, 0.0

    for i in range(array_size):
        dir_val = direction_array[i]
        driven_val = driven_array[i]

        formation_bias += dir_val
        formation_drive += abs(driven_val)

        if i > 0:
            prev_dir = direction_array[i - 1]
            if dir_val != prev_dir and dir_val != 0 and prev_dir != 0:
                swing_count += 1
                if formation_bias > 0 and dir_val < 0:
                    counter_swings += 1
                elif formation_bias < 0 and dir_val > 0:
                    counter_swings += 1

    compression_ratio = counter_swings / float(array_size)
    return formation_bias, formation_drive, swing_count, compression_ratio


def detect_formation_state(bias: int, drive: float, swing_count: int, compression: float, bars: int) -> str:
    if bars <= 0:
        return "⚪ NEUTRAL"

    bias_strength = abs(bias) / float(bars)
    drive_avg = drive / float(bars)

    if bias_strength > 0.5 and drive_avg > 1.5:
        return "⬆️ EXPANSION" if bias > 0 else "⬇️ EXPANSION"

    if bias_strength > 0.3 and bias_strength <= 0.5 and drive_avg < 1.0 and compression < 0.4:
        return "🔄 PULLBACK (UP)" if bias > 0 else "🔄 PULLBACK (DOWN)"

    if compression >= 0.4:
        return "🔶 COMPRESSION"

    if swing_count > 12 and bias_strength < 0.3:
        return "⚠️ TRANSITION"

    if bias_strength > 0.2:
        return "UP-WEAK" if bias > 0 else "DOWN-WEAK"

    return "⚪ NEUTRAL"


def get_anchor_risk_zone(history: int, driven: int, phase: str) -> str:
    if history <= 2 and 1 <= driven <= 2:
        return "🟢 GREEN"
    if history == 2 and driven <= 2:
        return "🟡 YELLOW"
    if history >= 3 or "MATURE" in phase:
        return "🟠 ORANGE"
    if "EXHAUSTION" in phase or "WEAKENING" in phase:
        return "🔴 RED"
    return "🟡 YELLOW"


def get_entry_decision(
    anchor_dir: int,
    formation_state: str,
    formation_state_prev: str,
    risk_zone: str,
) -> Tuple[str, str, float]:
    entry_status = "❌ NO ENTRY"
    entry_reason = "Anchor NEUTRAL - no clear direction"
    size_factor = 0.0

    anchor_up = anchor_dir == 1
    anchor_down = anchor_dir == -1

    is_expansion = "EXPANSION" in formation_state
    was_pullback = "PULLBACK" in formation_state_prev
    was_compression = "COMPRESSION" in formation_state_prev

    if anchor_up:
        entry_status = "⏳ WAIT"
        entry_reason = "Waiting for EXPANSION signal"
        if is_expansion and "⬆️" in formation_state:
            if was_pullback or was_compression:
                entry_status = "✅ ENTRY READY"
                entry_reason = (
                    "Formation: PULLBACK → EXPANSION"
                    if was_pullback
                    else "Formation: COMPRESSION → EXPANSION"
                )
                if "GREEN" in risk_zone:
                    size_factor = 1.0
                elif "YELLOW" in risk_zone:
                    size_factor = 0.75
                elif "ORANGE" in risk_zone:
                    size_factor = 0.5
                else:
                    size_factor = 0.25
                    entry_status = "⚠️ CAUTION ENTRY"
            else:
                entry_status = "⏳ WAIT"
                entry_reason = "Already in EXPANSION - wait for pullback"

    elif anchor_down:
        entry_status = "⏳ WAIT"
        entry_reason = "Waiting for EXPANSION signal"
        if is_expansion and "⬇️" in formation_state:
            if was_pullback or was_compression:
                entry_status = "✅ ENTRY READY"
                entry_reason = (
                    "Formation: PULLBACK → EXPANSION"
                    if was_pullback
                    else "Formation: COMPRESSION → EXPANSION"
                )
                if "GREEN" in risk_zone:
                    size_factor = 1.0
                elif "YELLOW" in risk_zone:
                    size_factor = 0.75
                elif "ORANGE" in risk_zone:
                    size_factor = 0.5
                else:
                    size_factor = 0.25
                    entry_status = "⚠️ CAUTION ENTRY"
            else:
                entry_status = "⏳ WAIT"
                entry_reason = "Already in EXPANSION - wait for pullback"

    return entry_status, entry_reason, size_factor


def _shift_df(df, offset):
    if df is None:
        return None
    if offset <= 0:
        return df
    if len(df) <= offset:
        return None
    return df.iloc[:-offset]


def build_formation_snapshot(
    trading_mode: str,
    formation_bars: int,
    m5_df,
    m30_df,
    h4_df,
    d1_df,
) -> Dict[str, object]:
    use_fast = trading_mode == "FAST"
    direction_array: List[int] = []
    driven_array: List[int] = []

    for offset in range(formation_bars - 1, -1, -1):
        _ = calculate_hybrid_momentum_at_offset(m5_df, m30_df, h4_df, d1_df, offset)

        tf_df = _shift_df(m5_df if use_fast else m30_df, offset)
        if tf_df is None or len(tf_df) < 6:
            continue

        direction_val = detect_direction(tf_df)
        driven_val = calculate_driven(tf_df)

        direction_array.append(direction_val)
        driven_array.append(driven_val)

    current_size = len(direction_array)
    min_ready = min(formation_bars, 24)
    formation_ready = current_size >= min_ready

    if not formation_ready:
        return {
            "formationReady": False,
            "formationArraySize": current_size,
            "formationBias": 0,
            "formationDrive": 0.0,
            "swingCount": 0,
            "compressionRatio": 0.0,
            "formationState": "⏳ LOADING...",
            "formationStatePrevious": "⏳ LOADING...",
        }

    bias, drive, swings, compression = calculate_formation_metrics(direction_array, driven_array)
    formation_state = detect_formation_state(bias, drive, swings, compression, current_size)

    prev_state = "⏳ LOADING..."
    if current_size > 1:
        pbias, pdrive, pswings, pcompression = calculate_formation_metrics(
            direction_array[:-1], driven_array[:-1]
        )
        prev_state = detect_formation_state(
            pbias, pdrive, pswings, pcompression, current_size - 1
        )

    return {
        "formationReady": True,
        "formationArraySize": current_size,
        "formationBias": bias,
        "formationDrive": round(drive, 3),
        "swingCount": swings,
        "compressionRatio": round(compression, 4),
        "formationState": formation_state,
        "formationStatePrevious": prev_state,
    }


def formation_state_to_core(formation_state: str) -> str:
    if "EXPANSION" in formation_state:
        return "CORE_A"
    if "PULLBACK" in formation_state or "COMPRESSION" in formation_state:
        return "CORE_B"
    return "CORE_C"
