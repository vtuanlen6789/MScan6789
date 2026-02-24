def calculate_momentum_memory(current_mom, hist1, hist2, hist3, transfer_avg):
    base_memory = (current_mom + hist1 + hist2 + hist3) / 4.0
    memory = 0.7 * base_memory + 0.3 * transfer_avg
    return memory / 3.0


def detect_momentum_driver(m5_prev, m5_now, m30_prev, m30_now, h4_prev, h4_now, d1_prev, d1_now):
    if m5_prev >= 2 and m30_prev <= 0 and m30_now >= 1:
        return "M5_DRIVEN"
    if m5_now >= 1 and m30_prev <= 0 and m30_now > 0:
        return "M5_DRIVEN"
    if m30_prev >= 2 and h4_prev <= 0 and h4_now >= 1:
        return "M30_DRIVEN"
    if m30_now >= 1 and h4_prev <= 0 and h4_now > 0:
        return "M30_DRIVEN"
    if d1_now < d1_prev and h4_now > h4_prev and h4_now > 0:
        return "D1_PULLBACK"
    if (m5_now > 0 and m30_now > 0 and h4_now > 0 and d1_now > 0) or (m5_now < 0 and m30_now < 0 and h4_now < 0 and d1_now < 0):
        return "ALIGNED"
    if h4_now > 0 and m30_now <= 0:
        return "WEAK_BASE"
    return "MIXED"


def _in_range(value, min_value, max_value):
    return min_value <= value <= max_value


def detect_cycle_state(d1_score, h4_score, m30_score):
    if _in_range(d1_score, 0, 2) and _in_range(h4_score, 1, 3) and _in_range(m30_score, -1, 3):
        return "UP CYCLE", "UP-EARLY-TREND", "ENTRY OK", "Ưu tiên vốn"
    if _in_range(d1_score, -2, 0) and _in_range(h4_score, -3, -1) and _in_range(m30_score, -3, -1):
        return "DOWN CYCLE", "DOWN-EARLY-TREND", "ENTRY OK", "Ưu tiên vốn"
    if _in_range(d1_score, 2, 3) and _in_range(h4_score, 1, 3):
        return "UP CYCLE", "UP-MATURE", "Selective", "Rủi ro đảo pha"
    if _in_range(d1_score, -3, -2) and _in_range(h4_score, -3, -1):
        return "DOWN CYCLE", "DOWN-MATURE", "Selective", "Rủi ro đảo pha"
    if _in_range(d1_score, -1, 0) and _in_range(h4_score, -2, -1) and _in_range(m30_score, -3, -2):
        return "TRANSITION", "UP→DOWN TRANSITION", "NO ENTRY", "RESET CYCLE"
    if _in_range(d1_score, 0, 1) and _in_range(h4_score, 1, 2) and _in_range(m30_score, 2, 3):
        return "TRANSITION", "DOWN→UP TRANSITION", "NO ENTRY", "RESET CYCLE"
    return "NO CLEAR CYCLE", "UNDEFINED", "STANDBY", "Chờ tín hiệu"


def structural_summary(d1_dir, h4_dir, state, cycle=None, conflict=None):
    if d1_dir == "UP" and h4_dir == "UP":
        base = "Higher timeframe alignment – bullish structure intact"
    elif d1_dir == "DOWN" and h4_dir == "DOWN":
        base = "Bearish structure aligned across frames"
    elif d1_dir != h4_dir:
        base = "Timeframe conflict – structural disagreement"
    elif state == "MATURE":
        base = "Late phase structure – watch exhaustion"
    else:
        base = "Transitional market structure"

    if cycle and cycle != "NO CLEAR CYCLE":
        base = f"{base} | {cycle}"
    if conflict is not None:
        base = f"{base} | Conflict={conflict}/6"
    return base

def analytical_focus(core, mode, current_state=None, driver=None):
    if current_state in ["UP-EARLY-TREND", "DOWN-EARLY-TREND"]:
        return "Trend continuation setup – ưu tiên pullback entry"
    if current_state in ["UP-MATURE", "DOWN-MATURE"]:
        return "Mature phase – giảm size, theo dõi đảo pha"
    if current_state and "TRANSITION" in current_state:
        return "Transition phase – đứng ngoài chờ reset"
    if driver in ["M5_DRIVEN", "M30_DRIVEN"]:
        return "Bottom-up momentum detected – theo dõi xác nhận H4"

    if core == "CORE_B":
        return "Continuation bias – wait pullback resolution"

    if core == "CORE_C":
        return "Reversal watch – monitor structural break"

    if mode == "FAST":
        return "Tactical observation required"

    return "Monitor structure development"


def clarity_rating(trust, conflict):
    if trust > 85 and conflict <= 2:
        return "HIGH"

    if trust > 70 and conflict <= 4:
        return "MODERATE"

    return "LOW"
