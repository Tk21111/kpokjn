import sys, json, traceback
import pandas as pd
import numpy as np

_formula_cache = {}  # formula_id -> evaluate_fn
_state_store   = {}  # ticker -> state

def cmd_load(job):
    formula_id   = job["formula_id"]
    formula_path = job["formula_path"]

    with open(formula_path, "r") as f:
        code_str = f.read()

    namespace = {"pd": pd, "np": np}
    compiled  = compile(code_str, formula_path, "exec")
    exec(compiled, namespace)

    fn = namespace.get("evaluate")
    if not fn:
        raise ValueError(f"missing evaluate() in {formula_path}")

    _formula_cache[formula_id] = fn
    return {"status": "ok", "formula_id": formula_id}

def cmd_eval(job):
    formula_id = job["formula_id"]
    ticker     = job["ticker"]

    fn = _formula_cache.get(formula_id)
    if not fn:
        raise ValueError(f"formula {formula_id} not loaded")

    df         = pd.DataFrame(job["data"])
    prev_state = _state_store.get(ticker, {})
    params     = job.get("params", {})

    new_state, signal = fn(df, prev_state, params)
    _state_store[ticker] = new_state

    return {"ticker": ticker, "signal": signal}

def cmd_unload(job):
    _formula_cache.pop(job["formula_id"], None)
    return {"status": "ok", "formula_id": job["formula_id"]}

def cmd_reset_state(job):
    _state_store.pop(job["ticker"], None)
    return {"status": "ok", "ticker": job["ticker"]}

def cmd_ping(_):
    return {"status": "pong"}

HANDLERS = {
    "load":        cmd_load,
    "eval":        cmd_eval,
    "unload":      cmd_unload,
    "reset_state": cmd_reset_state,
    "ping":        cmd_ping,
}

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        job     = json.loads(line)
        cmd     = job.get("cmd")
        handler = HANDLERS.get(cmd)
        if not handler:
            raise ValueError(f"unknown cmd: {cmd}")
        result = handler(job)
        sys.stdout.write(json.dumps(result) + "\n")
    except Exception as e:
        sys.stdout.write(json.dumps({
            "error":     str(e),
            "traceback": traceback.format_exc()
        }) + "\n")
    sys.stdout.flush()