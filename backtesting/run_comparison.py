"""
Run strategy variants and print comparison table.

Usage:
    venv/bin/python -m backtesting.run_comparison
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backtesting.simulator import compare

VARIANTS = {
    # ── Baseline (must match production: ~195 trades, ~91.8% WR) ─────────────
    "00_baseline": {},

    # ── Signal override ───────────────────────────────────────────────────────
    "05_no_analyst_upside": {"no_analyst_upside": True},

    # ── Entry filters ─────────────────────────────────────────────────────────
    "01_min_3_analysts":   {"min_analysts": 3},
    "02_min_5_analysts":   {"min_analysts": 5},
    "03_health_gte_4":     {"health_score_min": 4},
    "04_health_gte_5":     {"health_score_min": 5},

    # ── Early exit rules ──────────────────────────────────────────────────────
    "10_profit_target_15": {"profit_target_pct": 0.15},
    "11_profit_target_20": {"profit_target_pct": 0.20},
    "12_profit_target_25": {"profit_target_pct": 0.25},
    "13_max_hold_30d":     {"max_hold_days": 30},
    "14_max_hold_45d":     {"max_hold_days": 45},
    "15_max_hold_60d":     {"max_hold_days": 60},

    # ── Combined ──────────────────────────────────────────────────────────────
    "20_min3a_profit20":   {"min_analysts": 3, "profit_target_pct": 0.20},
    "21_health5_profit20": {"health_score_min": 5, "profit_target_pct": 0.20},
    "22_min3a_max45d":     {"min_analysts": 3, "max_hold_days": 45},
    "23_health5_max45d":   {"health_score_min": 5, "max_hold_days": 45},
}


if __name__ == "__main__":
    print("=" * 70)
    print("STRATEGY COMPARISON BACKTEST")
    print("=" * 70)

    results = compare(VARIANTS)

    print("\n" + "=" * 70)
    print(results.to_string())
    print("=" * 70)

    print("\nTop 5 by win_rate:")
    print(results.nlargest(5, "win_rate")[["win_rate", "avg_return", "n_trades", "sharpe"]].to_string())

    print("\nTop 5 by avg_return:")
    print(results.nlargest(5, "avg_return")[["avg_return", "win_rate", "n_trades", "sharpe"]].to_string())
