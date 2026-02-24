def detect_core(d1_dir, h4_dir, momentum):
    if d1_dir == h4_dir and momentum > 6:
        return "CORE_B"

    if d1_dir != h4_dir:
        return "CORE_C"

    return "CORE_A"
