def _get_recent_ohlc(df, count, offset=0):
    if df is None:
        return None

    if offset > 0:
        if len(df) <= offset:
            return None
        base = df.iloc[:-offset]
    else:
        base = df

    if len(base) < count:
        return None

    recent = base.tail(count)
    highs = recent["high"].tolist()[::-1]
    lows = recent["low"].tolist()[::-1]
    opens = recent["open"].tolist()[::-1]
    closes = recent["close"].tolist()[::-1]
    return highs, lows, opens, closes


def _base_momentum_6(df, offset=0):
    values = _get_recent_ohlc(df, 6, offset)
    if values is None:
        return 0
    highs, lows, opens, closes = values

    bullish_count = sum(1 for i in range(6) if closes[i] > opens[i])
    bearish_count = sum(1 for i in range(6) if closes[i] < opens[i])

    high_increasing = 0
    low_increasing = 0
    high_decreasing = 0
    low_decreasing = 0

    high_increasing += 1 if highs[0] >= highs[1] and highs[1] >= highs[2] else 0
    high_increasing += 1 if highs[0] >= highs[1] and highs[1] >= highs[2] and highs[2] >= highs[3] else 0
    high_increasing += 1 if highs[0] >= highs[1] and highs[1] >= highs[2] and highs[2] >= highs[3] and highs[3] >= highs[4] else 0
    high_increasing += 2 if highs[0] >= highs[1] and highs[1] >= highs[2] and highs[2] >= highs[3] and highs[3] >= highs[4] and highs[4] >= highs[5] else 0

    low_increasing += 1 if lows[0] >= lows[1] and lows[1] >= lows[2] else 0
    low_increasing += 1 if lows[0] >= lows[1] and lows[1] >= lows[2] and lows[2] >= lows[3] else 0
    low_increasing += 1 if lows[0] >= lows[1] and lows[1] >= lows[2] and lows[2] >= lows[3] and lows[3] >= lows[4] else 0
    low_increasing += 2 if lows[0] >= lows[1] and lows[1] >= lows[2] and lows[2] >= lows[3] and lows[3] >= lows[4] and lows[4] >= lows[5] else 0

    high_decreasing += 1 if highs[0] <= highs[1] and highs[1] <= highs[2] else 0
    high_decreasing += 1 if highs[0] <= highs[1] and highs[1] <= highs[2] and highs[2] <= highs[3] else 0
    high_decreasing += 1 if highs[0] <= highs[1] and highs[1] <= highs[2] and highs[2] <= highs[3] and highs[3] <= highs[4] else 0
    high_decreasing += 2 if highs[0] <= highs[1] and highs[1] <= highs[2] and highs[2] <= highs[3] and highs[3] <= highs[4] and highs[4] <= highs[5] else 0

    low_decreasing += 1 if lows[0] <= lows[1] and lows[1] <= lows[2] else 0
    low_decreasing += 1 if lows[0] <= lows[1] and lows[1] <= lows[2] and lows[2] <= lows[3] else 0
    low_decreasing += 1 if lows[0] <= lows[1] and lows[1] <= lows[2] and lows[2] <= lows[3] and lows[3] <= lows[4] else 0
    low_decreasing += 2 if lows[0] <= lows[1] and lows[1] <= lows[2] and lows[2] <= lows[3] and lows[3] <= lows[4] and lows[4] <= lows[5] else 0

    momentum = 0
    if bullish_count >= 5:
        momentum = 3 if high_increasing >= 3 and low_increasing >= 3 else 2
    elif bullish_count >= 4:
        momentum = 2 if high_increasing >= 2 and low_increasing >= 2 else 1
    elif bullish_count >= 3 and high_increasing >= 2:
        momentum = 1
    elif bearish_count >= 5:
        momentum = -3 if high_decreasing >= 3 and low_decreasing >= 3 else -2
    elif bearish_count >= 4:
        momentum = -2 if high_decreasing >= 2 and low_decreasing >= 2 else -1
    elif bearish_count >= 3 and low_decreasing >= 2:
        momentum = -1

    return momentum


def _base_momentum_8(df, offset=0):
    values = _get_recent_ohlc(df, 8, offset)
    if values is None:
        return 0
    _, _, opens, closes = values

    bullish_count = sum(1 for i in range(8) if closes[i] > opens[i])
    bearish_count = sum(1 for i in range(8) if closes[i] < opens[i])

    if bullish_count >= 6:
        return 3
    if bullish_count >= 5:
        return 2
    if bullish_count >= 4:
        return 1
    if bearish_count >= 6:
        return -3
    if bearish_count >= 5:
        return -2
    if bearish_count >= 4:
        return -1
    return 0


def _transferred_momentum(df, count, offset=0):
    values = _get_recent_ohlc(df, count, offset)
    if values is None:
        return 0
    highs, lows, opens, closes = values

    impulse_sum = 0.0
    noise_count = 0

    for i in range(count):
        candle_range = highs[i] - lows[i]
        body = abs(closes[i] - opens[i])
        body_ratio = (body / candle_range) if candle_range > 0 else 0.0
        direction = 1 if closes[i] > opens[i] else -1 if closes[i] < opens[i] else 0

        impulse_sum += body_ratio * direction
        if body_ratio < 0.3:
            noise_count += 1

    noise_penalty = noise_count / count
    abs_impulse = abs(impulse_sum)
    direction_sign = 1 if impulse_sum > 0 else -1 if impulse_sum < 0 else 0

    strength = 0
    if abs_impulse >= 1.2 and noise_penalty <= 0.3:
        strength = 2
    elif abs_impulse >= 0.6:
        strength = 1

    return strength * direction_sign


def _interaction(base, transfer):
    if base == 0:
        result = round(transfer * 0.7)
    elif (base > 0 and transfer > 0) or (base < 0 and transfer < 0):
        result = round(base + 0.5 * transfer)
        result = max(-3, min(3, result))
    elif (base > 0 and transfer < 0) or (base < 0 and transfer > 0):
        result = round(base * 0.5)
    else:
        result = round(base * 0.8)

    return int(result)


def _hybrid_momentum(offset, m5_df, m30_df, h4_df, d1_df):
    m5 = _base_momentum_6(m5_df, offset)

    m30_base = _base_momentum_6(m30_df, offset)
    m30_transfer = _transferred_momentum(m5_df, 6, offset)
    m30 = _interaction(m30_base, m30_transfer)

    h4_base = _base_momentum_8(h4_df, offset)
    h4_transfer = _transferred_momentum(m30_df, 8, offset)
    h4 = _interaction(h4_base, h4_transfer)

    d1_base = _base_momentum_6(d1_df, offset)
    d1_transfer = _transferred_momentum(h4_df, 6, offset)
    d1 = _interaction(d1_base, d1_transfer)

    return {"m5": m5, "m30": m30, "h4": h4, "d1": d1}


def calculate_multi_tf_momentum(m5_df, m30_df, h4_df, d1_df):
    current = _hybrid_momentum(0, m5_df, m30_df, h4_df, d1_df)

    histories = {"m5": [], "m30": [], "h4": [], "d1": []}
    for offset in [1, 2, 3, 4]:
        hist = _hybrid_momentum(offset, m5_df, m30_df, h4_df, d1_df)
        histories["m5"].append(hist["m5"])
        histories["m30"].append(hist["m30"])
        histories["h4"].append(hist["h4"])
        histories["d1"].append(hist["d1"])

    return {
        "current": current,
        "history": histories,
    }
