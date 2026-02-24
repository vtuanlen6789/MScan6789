def detect_mode(state, conflict):
    if conflict > 25:
        return "FAST"

    if state == "CONTINUE":
        return "STABLE"

    return "FAST"
