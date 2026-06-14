import json
import os
import sys
import traceback

from formulas import REGISTRY

try:
    import pandas as pd

    PANDAS_LOADED = True
    IMPORT_ERROR = ""
except Exception as exc:
    pd = None
    PANDAS_LOADED = False
    IMPORT_ERROR = str(exc)


def write_response(payload):
    sys.stdout.write(json.dumps(payload, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def handle_request(req):
    if req.get("command") == "crash":
        print("intentional crash requested by Go sandbox", file=sys.stderr, flush=True)
        os._exit(42)

    formula_id = req.get("formula_id")
    if not formula_id:
        raise ValueError("formula_id is required")

    formula = REGISTRY.get(formula_id)
    if formula is None:
        raise ValueError(f"unknown formula_id: {formula_id}")

    result = formula(req.get("bars", []), req.get("params") or {})
    details = dict(result.get("details") or {})
    details["runtime_pid"] = os.getpid()
    details["pandas_loaded"] = PANDAS_LOADED

    return {
        "id": req.get("id"),
        "ok": True,
        "signal": int(result.get("signal", 0)),
        "formula_id": formula_id,
        "pid": os.getpid(),
        "pandas_loaded": PANDAS_LOADED,
        "import_error": IMPORT_ERROR,
        "details": details,
    }


def main():
    print("python worker booted; logs stay on stderr", file=sys.stderr, flush=True)
    write_response(
        {
            "ready": True,
            "pid": os.getpid(),
            "pandas_loaded": PANDAS_LOADED,
            "import_error": IMPORT_ERROR,
        }
    )

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        try:
            request = json.loads(line)
            write_response(handle_request(request))
        except Exception as exc:
            traceback.print_exc(file=sys.stderr)
            write_response(
                {
                    "id": request.get("id") if "request" in locals() else "",
                    "ok": False,
                    "formula_id": request.get("formula_id") if "request" in locals() else "",
                    "error": str(exc),
                    "pid": os.getpid(),
                    "pandas_loaded": PANDAS_LOADED,
                    "import_error": IMPORT_ERROR,
                }
            )


if __name__ == "__main__":
    main()
