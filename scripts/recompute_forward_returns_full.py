"""Re-run compute_forward_returns() — picks up the new 2018-2019 prices automatically."""
from features.forward_returns import compute_forward_returns

if __name__ == "__main__":
    compute_forward_returns()
    print("DONE", flush=True)
