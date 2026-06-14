from . import momentum, sma_cross

REGISTRY = {
    "momentum": momentum.run,
    "sma_cross": sma_cross.run,
}
