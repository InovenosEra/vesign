"""Read-only: per-tier + year x tier trade-count / win-rate / avg-yield.
Compare against docs/superpowers/specs/2026-06-20-tiered-buy-signals-design.md.
    VESIGN_DB=vesign_tier_test.db venv/bin/python scripts/verify_tiers.py
"""
import os, sqlite3, pandas as pd

DB = os.environ.get("VESIGN_DB", "vesign.db")
con = sqlite3.connect(DB)
q = """
WITH lots AS (
  SELECT ticker, DATE(buy_date) bd, DATE(sell_date) sd,
         COUNT(*)*1.0/NULLIF(SUM(1.0/NULLIF(lot_price,0)),0) avg_cost
  FROM trade_lots GROUP BY ticker, DATE(buy_date), DATE(sell_date))
SELECT s.tier AS tier,
       CAST(strftime('%Y', tl.buy_date) AS INT) AS yr,
       CASE WHEN l.avg_cost > 0 THEN (tl.sell_price - l.avg_cost)/l.avg_cost
            ELSE tl.return_pct END AS y
FROM trade_log tl
JOIN signals s
  ON s.ticker = tl.ticker AND DATE(s.date) = DATE(tl.buy_date) AND s.signal='BUY'
LEFT JOIN lots l
  ON l.ticker = tl.ticker AND l.bd = DATE(tl.buy_date) AND l.sd = DATE(tl.sell_date)
WHERE tl.ticker NOT LIKE '%.TA' AND s.tier IS NOT NULL
"""
try:
    df = pd.read_sql(q, con)
except Exception as e:
    if "no such column" in str(e) and "tier" in str(e):
        print("No tiered closed trades found in", DB, "- has the rebuild run?")
        raise SystemExit(0)
    raise
if df.empty:
    print("No tiered closed trades found in", DB, "- has the rebuild run?")
    raise SystemExit(0)

print("=== PER-TIER TOTALS ===")
tot = df.groupby("tier")["y"].agg(
    trades="size",
    win_rate=lambda s: round((s > 0).mean() * 100, 1),
    avg_yield=lambda s: round(s.mean() * 100, 2),
)
print(tot.to_string())

for name, fn in [("TRADES", "size"),
                 ("WIN RATE %", lambda s: round((s > 0).mean() * 100, 1)),
                 ("AVG YIELD %", lambda s: round(s.mean() * 100, 2))]:
    print(f"\n=== {name} (year x tier) ===")
    print(df.pivot_table(index="yr", columns="tier", values="y",
                         aggfunc=fn, observed=True).to_string())
