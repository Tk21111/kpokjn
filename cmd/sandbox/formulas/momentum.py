def run(bars, params):
    if not bars:
        raise ValueError("bars cannot be empty")

    lookback = int(params.get("lookback", 2))
    threshold = float(params.get("threshold", 0.0))
    if lookback < 1:
        raise ValueError("lookback must be >= 1")
    if len(bars) <= lookback:
        raise ValueError(f"need more than {lookback} bars")

    start = float(bars[-1 - lookback]["c"])
    end = float(bars[-1]["c"])
    change = (end - start) / start
    signal = 1 if change >= threshold else 0

    return {
        "signal": signal,
        "details": {
            "reason": "momentum threshold hit" if signal else "momentum below threshold",
            "lookback": lookback,
            "threshold": threshold,
            "start_close": start,
            "last_close": end,
            "change": change,
        },
    }
