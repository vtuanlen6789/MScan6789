def calculate_score_from_components(direction, history, driven):
    raw_power = history * 0.6 + driven * 0.4
    score = int(round(direction * raw_power))
    return max(-3, min(3, score))


def compliance_score(d1_score, h4_score, m30_score, d1_direction, h4_direction):
    score = 45

    score += min(abs(d1_score) * 8, 24)
    score += min(abs(h4_score) * 6, 18)
    score += min(abs(m30_score) * 4, 12)

    if d1_direction == h4_direction and d1_direction != 0:
        score += 16

    sign_d1 = 1 if d1_score > 0 else -1 if d1_score < 0 else 0
    sign_h4 = 1 if h4_score > 0 else -1 if h4_score < 0 else 0
    sign_m30 = 1 if m30_score > 0 else -1 if m30_score < 0 else 0
    if sign_d1 == sign_h4 == sign_m30 and sign_d1 != 0:
        score += 10

    return max(0, min(100, score))


def trust_score(compliance, conflict):
    trust = compliance - conflict * 8
    return max(0, min(100, trust))
