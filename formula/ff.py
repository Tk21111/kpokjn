# pd, np already injected into namespace

def evaluate(df, prev_state, params):
    fast_n = params.get("fast", 5)
    slow_n = params.get("slow", 20)

    fast = df["close"].tail(fast_n).mean()
    slow = df["close"].tail(slow_n).mean()

    signal = 0
    if fast > slow:
        signal = 1
    elif fast < slow:
        signal = -1

    return prev_state, signal