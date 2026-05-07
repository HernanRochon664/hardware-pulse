"""
Deal detection signals for the Hardware Pulse dashboard.

Responsibilities:
- Classify price opportunities as deal/fair/expensive
- Format signals for UI display

Does NOT:
- Access the database directly (see queries.py)
- Handle UI rendering (see app.py)
"""

DEAL_THRESHOLD_PCT = 10.0


def detect_signal(current_price: float, median_price: float) -> str:
    if median_price == 0:
        return "fair"

    pct_diff = ((current_price - median_price) / median_price) * 100

    if pct_diff <= -DEAL_THRESHOLD_PCT:
        return "deal"
    elif pct_diff >= DEAL_THRESHOLD_PCT:
        return "expensive"
    else:
        return "fair"


def format_signal(signal: str, pct_diff: float) -> dict:
    colors = {
        "deal": "🟢",
        "fair": "🟡",
        "expensive": "🔴",
    }
    labels = {
        "deal": "DEAL",
        "fair": "FAIR",
        "expensive": "EXPENSIVE",
    }
    return {
        "emoji": colors.get(signal, "⚪"),
        "label": labels.get(signal, "UNKNOWN"),
        "pct_diff": pct_diff,
    }
