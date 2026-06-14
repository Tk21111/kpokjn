def run(bars, params):
    if not bars:
        raise ValueError("bars cannot be empty")

    fast = int(params.get("fast", 2))
    slow = int(params.get("slow", 3))
    if fast < 1 or slow < 1:
        raise ValueError("fast and slow must be >= 1")
    if fast >= slow:
        raise ValueError("fast must be less than slow")
    if len(bars) < slow:
        raise ValueError(f"need at least {slow} bars")

    closes = [float(bar["c"]) for bar in bars]
    fast_sma = sum(closes[-fast:]) / fast
    slow_sma = sum(closes[-slow:]) / slow
    signal = 1 if fast_sma > slow_sma else 0

    return {
        "signal": signal,
        "details": {
            "reason": "fast SMA above slow SMA" if signal else "fast SMA not above slow SMA",
            "fast": fast,
            "slow": slow,
            "fast_sma": fast_sma,
            "slow_sma": slow_sma,
            "last_close": closes[-1],
        },
    }
