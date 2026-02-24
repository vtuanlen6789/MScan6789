def _sign(value):
    if value > 0:
        return 1
    if value < 0:
        return -1
    return 0


def calculate_conflict_score(m5, m30, h4, d1):
    sign_m5 = _sign(m5)
    sign_m30 = _sign(m30)
    sign_h4 = _sign(h4)
    sign_d1 = _sign(d1)

    return abs(sign_m5 - sign_m30) + abs(sign_m30 - sign_h4) + abs(sign_h4 - sign_d1)


def detect_conflict(m5, m30, h4, d1):
    return calculate_conflict_score(m5, m30, h4, d1)
