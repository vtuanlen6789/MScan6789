def detect_direction(df):
    if df is None or len(df) < 6:
        return 0

    recent = df.tail(6)
    highs = recent["high"].tolist()[::-1]
    lows = recent["low"].tolist()[::-1]
    closes = recent["close"].tolist()[::-1]

    hh_pattern = highs[0] > highs[1] and highs[1] > highs[2]
    hl_pattern = lows[0] > lows[1] and lows[1] > lows[2]

    ll_pattern = lows[0] < lows[1] and lows[1] < lows[2]
    lh_pattern = highs[0] < highs[1] and highs[1] < highs[2]

    close_trend = 0
    if closes[0] > closes[1] and closes[1] > closes[2] and closes[2] > closes[3]:
        close_trend = 1
    elif closes[0] < closes[1] and closes[1] < closes[2] and closes[2] < closes[3]:
        close_trend = -1

    if (hh_pattern and hl_pattern) or close_trend == 1:
        return 1
    if (ll_pattern and lh_pattern) or close_trend == -1:
        return -1
    return 0


def direction_label(direction):
    if direction > 0:
        return "UP"
    if direction < 0:
        return "DOWN"
    return "RANGE"
