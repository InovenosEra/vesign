"""Simulate hard-NaN (current) vs soft-NaN analyst rule on the rebuilt DB.

Walks forward per ticker, applies V1 OR V2 BUY logic with both rule variants,
applies the same SELL logic (10% stop / RSI>=70 with ml_negative / 365d), and
reports per-year WR + avg yield grouped by SELL year.

Note: this simulation has known ~12pp WR understatement vs build_trade_log,
but the *delta* between hard and soft NaN should be directionally accurate.
"""
import sqlite3
import pandas as pd

DB = "/opt/vesign/vesign.db"
START = "2020-01-02"
TRAILING_STOP_PCT = 0.10
MAX_DAYS = 365

conn = sqlite3.connect(DB)
etfs = set(pd.read_sql("SELECT ticker FROM companies WHERE sector='ETF'", conn)["ticker"])

q = f"""
SELECT date(date) AS d, ticker, close, rsi, prediction_score, target_mean_price,
       rsi_3day_flag, bb_condition, analyst_condition,
       volume_flag, week52_condition, health_condition, ml_condition, vqs
FROM signals
WHERE date >= '{START}'
ORDER BY ticker, d
"""
print("loading signals...")
df = pd.read_sql(q, conn, parse_dates=["d"])
df = df[~df["ticker"].isin(etfs)].copy()
print(f"rows: {len(df):,}  range: {df['d'].min().date()} -> {df['d'].max().date()}")

bb = df["bb_condition"].astype(bool)
vol = df["volume_flag"].astype(bool)
w52 = df["week52_condition"].astype(bool)
hth = df["health_condition"].astype(bool)
mlc = df["ml_condition"].astype(bool)
ana = df["analyst_condition"].astype(bool)
rsi3 = (df["rsi_3day_flag"] == 3)
vqs9 = df["vqs"].fillna(-1).astype(int) == 9
target_null = df["target_mean_price"].isna()

# HARD: V1 needs analyst_condition true
# SOFT: V1 passes if (analyst_condition true) OR (target IS NULL)
six = rsi3 & bb & vol & w52 & hth & mlc
df["buy_hard"] = (six & ana) | vqs9
df["buy_soft"] = (six & (ana | target_null)) | vqs9


def simulate(df, gate_col, label):
    trades = []
    for ticker, g in df.groupby("ticker", sort=False):
        g = g.sort_values("d").reset_index(drop=True)
        in_pos = False
        ep = ed = None
        for _, row in g.iterrows():
            if not in_pos:
                if bool(row[gate_col]):
                    in_pos = True
                    ep = row["close"]
                    ed = row["d"]
            else:
                close = row["close"]
                rsi = row["rsi"]
                pred = row["prediction_score"]
                ml_neg = pd.isna(pred) or (pred < 0)
                stop = close < ep * (1 - TRAILING_STOP_PCT)
                rsi_h = (not pd.isna(rsi)) and (rsi >= 70)
                days = (row["d"] - ed).days
                tx = days >= MAX_DAYS
                if ((stop or rsi_h) and ml_neg) or tx:
                    trades.append({"ticker": ticker, "buy_date": ed, "sell_date": row["d"],
                                   "return_pct": (close - ep) / ep, "days_held": days})
                    in_pos = False; ep = ed = None
        if in_pos:
            trades.append({"ticker": ticker, "buy_date": ed, "sell_date": None,
                           "return_pct": None, "days_held": (g["d"].iloc[-1] - ed).days})
    out = pd.DataFrame(trades)
    closed = out[out["sell_date"].notna()].copy()
    if len(closed):
        closed["sell_year"] = pd.to_datetime(closed["sell_date"]).dt.year
        per_year = closed.groupby("sell_year").agg(
            n=("return_pct", "size"),
            wr=("return_pct", lambda x: (x > 0).mean() * 100),
            avg=("return_pct", lambda x: x.mean() * 100),
        )
    else:
        per_year = pd.DataFrame()
    print(f"\n--- {label} ---")
    print(f"closed={len(closed)}  open={(out['sell_date'].isna()).sum()}")
    if len(closed):
        wins = (closed["return_pct"] > 0).sum()
        print(f"WR={wins/len(closed)*100:.1f}%  avg_yield={closed['return_pct'].mean()*100:+.2f}%")
        print(per_year.to_string())
    return per_year


h = simulate(df, "buy_hard", "HARD NaN (current)")
s = simulate(df, "buy_soft", "SOFT NaN (proposed)")

# Side-by-side
print("\n========== SIDE-BY-SIDE per sell year ==========")
print(f"{'year':>6s}  {'hard n':>6s}  {'hard WR':>8s}  {'hard avg':>9s}  | {'soft n':>6s}  {'soft WR':>8s}  {'soft avg':>9s}")
print("-" * 80)
years = sorted(set(h.index) | set(s.index))
for y in years:
    hn = h.loc[y, "n"] if y in h.index else 0
    hw = h.loc[y, "wr"] if y in h.index else 0
    ha = h.loc[y, "avg"] if y in h.index else 0
    sn = s.loc[y, "n"] if y in s.index else 0
    sw = s.loc[y, "wr"] if y in s.index else 0
    sa = s.loc[y, "avg"] if y in s.index else 0
    print(f"{y:>6}  {int(hn):>6d}  {hw:>+7.1f}%  {ha:>+8.2f}%  | {int(sn):>6d}  {sw:>+7.1f}%  {sa:>+8.2f}%")
