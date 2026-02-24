def calculate_history(mom_current, mom_hist1, mom_hist2, mom_hist3, mom_hist4):
    same_direction_count = 0
    current_sign = 1 if mom_current > 0 else -1 if mom_current < 0 else 0

    if current_sign != 0:
        for hist in [mom_hist1, mom_hist2, mom_hist3, mom_hist4]:
            hist_sign = 1 if hist > 0 else -1 if hist < 0 else 0
            if hist_sign == current_sign:
                same_direction_count += 1

    abs_sum = abs(mom_current) + abs(mom_hist1) + abs(mom_hist2)

    if same_direction_count >= 4 or abs_sum >= 8:
        return 3
    if same_direction_count >= 3 or abs_sum >= 6:
        return 2
    if same_direction_count >= 2 or abs_sum >= 4:
        return 1
    return 0


def calculate_driven(df):
    if df is None or len(df) < 3:
        return 0

    recent = df.tail(3)
    highs = recent["high"].tolist()[::-1]
    lows = recent["low"].tolist()[::-1]
    opens = recent["open"].tolist()[::-1]
    closes = recent["close"].tolist()[::-1]

    range1 = highs[0] - lows[0]
    range2 = highs[1] - lows[1]
    range3 = highs[2] - lows[2]
    body1 = abs(closes[0] - opens[0])
    body2 = abs(closes[1] - opens[1])

    expanding = range1 > range2 and range2 > range3

    close_position1 = ((closes[0] - lows[0]) / range1) if range1 > 0 else 0.5
    close_near_high = close_position1 > 0.8
    close_near_low = close_position1 < 0.2

    body_ratio1 = (body1 / range1) if range1 > 0 else 0
    body_ratio2 = (body2 / range2) if range2 > 0 else 0
    strong_bodies = body_ratio1 > 0.6 and body_ratio2 > 0.5

    if expanding and (close_near_high or close_near_low) and strong_bodies:
        return 3
    if (expanding or strong_bodies) and (close_near_high or close_near_low):
        return 2
    if body_ratio1 > 0.4:
        return 1
    return 0


def detect_state_from_history_driven(history, driven, direction):
    if history <= 1 and driven <= 1:
        return "INIT"

    if history <= 1 and driven >= 2:
        if direction == 1:
            return "UP-REVERSAL"
        if direction == -1:
            return "DOWN-REVERSAL"
        return "REVERSAL"

    if history >= 1 and history <= 2 and driven >= 1 and driven <= 2:
        if direction == 1:
            return "UP-EARLY-TREND"
        if direction == -1:
            return "DOWN-EARLY-TREND"
        return "EARLY-TREND"

    if history == 2 and driven == 1:
        if direction == 1:
            return "UP-CONTINUATION"
        if direction == -1:
            return "DOWN-CONTINUATION"
        return "CONTINUATION"

    if history >= 3 and driven <= 1:
        if direction == 1:
            return "UP-MATURE"
        if direction == -1:
            return "DOWN-MATURE"
        return "MATURE"

    if history >= 3 and driven >= 3:
        if direction == 1:
            return "UP-EXHAUSTION"
        if direction == -1:
            return "DOWN-EXHAUSTION"
        return "EXHAUSTION"

    return "IMPULSE" if driven >= 2 else "CONSOLIDATION"
