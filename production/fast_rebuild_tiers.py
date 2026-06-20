"""FAST historical rebuild of BUY/SELL/HOLD + tier + lot_seq.

Replicates signals.engine.run_scoring's per-day LABELING logic, but reuses the
already-stored indicator columns (vqs, the V1 condition flags, rsi,
prediction_score, close) instead of recomputing rolling features from raw
prices. That makes the rebuild minutes instead of ~8 hours.

Equivalence with the full per-date run_scoring path is PROVEN on a clean-start
window by scripts/prove_fast_equiv.py before this is trusted.

Honors VESIGN_DB via data.loaders.engine. Also builds trade_lots (which no
existing code maintains — build_trade_log only writes trade_log).
"""
import math
import pandas as pd
import yaml, os
from sqlalchemy import text
from data.loaders import engine
from signals.engine import _vqs_to_tier, _ensure_signals_columns

_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
with open(os.path.join(_BASE, "config", "settings.yaml")) as f:
    _CFG = yaml.safe_load(f)
STOP_PCT = _CFG.get("trailing_stop_pct", 0.25)


def _isnan(v):
    return v is None or (isinstance(v, float) and math.isnan(v))


def fast_signal_pass():
    """Recompute signal/tier/lot_seq for every row, threading open-position
    state per ticker. Writes the three columns back to `signals`. Does NOT
    touch indicator columns, news_block_reason, or any other column."""
    _ensure_signals_columns()  # make sure the `tier` column exists
    etfs = set(pd.read_sql(
        "SELECT ticker FROM companies WHERE sector = 'ETF'", engine)["ticker"])

    df = pd.read_sql(
        "SELECT rowid AS rid, DATE(date) AS d, ticker, vqs, rsi_3day_flag, "
        "bb_condition, analyst_condition, volume_flag, week52_condition, "
        "health_condition, ml_condition, rsi, prediction_score, close "
        "FROM signals ORDER BY ticker, date", engine)

    upd = []  # (signal, tier_or_None, lot_seq_or_None, rid)
    for ticker, g in df.groupby("ticker", sort=False):
        is_etf = ticker in etfs
        open_pos = False
        entry = buy_date = last_lot = None
        lot_count = 0
        for r in g.itertuples(index=False):
            close = r.close
            if is_etf or _isnan(close):
                upd.append(("HOLD", None, None, r.rid))
                continue

            # base BUY eligibility (V1 7-gate all true) OR (vqs >= 6)
            v1 = (r.rsi_3day_flag == 3 and r.bb_condition == 1 and
                  r.analyst_condition == 1 and r.volume_flag == 1 and
                  r.week52_condition == 1 and r.health_condition == 1 and
                  r.ml_condition == 1)
            v2 = (not _isnan(r.vqs)) and r.vqs >= 6
            base_buy = v1 or v2

            rsi_sell = (not _isnan(r.rsi)) and r.rsi >= 70
            ml_neg = _isnan(r.prediction_score) or r.prediction_score < 0

            if open_pos:
                stop_hit = close < entry * (1 - STOP_PCT)
                days_held = (pd.Timestamp(r.d) - pd.Timestamp(buy_date)).days
                time_exit = days_held >= 365
            else:
                stop_hit = False
                time_exit = False

            sell = ((stop_hit or rsi_sell) and ml_neg) or time_exit
            # add-on only when open AND price <= 90% of last lot; fresh when flat
            buy_ok = base_buy and (close <= last_lot * 0.90) if open_pos else base_buy

            # np.select precedence in run_scoring: SELL before BUY
            if sell:
                upd.append(("SELL", None, None, r.rid))
                open_pos = False
                entry = buy_date = last_lot = None
                lot_count = 0
            elif buy_ok:
                if open_pos:
                    last_lot = close
                    lot_count += 1
                else:
                    open_pos = True
                    entry = close
                    buy_date = r.d
                    last_lot = close
                    lot_count = 1
                tier = _vqs_to_tier(r.vqs)
                upd.append(("BUY", tier, lot_count, r.rid))
            else:
                upd.append(("HOLD", None, None, r.rid))

    # bulk write via temp table + UPDATE..FROM (single statement, fast)
    up = pd.DataFrame(upd, columns=["signal", "tier", "lot_seq", "rid"])
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS _fast_upd"))
        conn.execute(text(
            "CREATE TEMP TABLE _fast_upd (rid INTEGER PRIMARY KEY, "
            "signal TEXT, tier INTEGER, lot_seq INTEGER)"))
        up.to_sql("_fast_upd", conn, if_exists="append", index=False)
        conn.execute(text(
            "UPDATE signals SET "
            "signal = (SELECT signal FROM _fast_upd WHERE _fast_upd.rid = signals.rowid), "
            "tier   = (SELECT tier   FROM _fast_upd WHERE _fast_upd.rid = signals.rowid), "
            "lot_seq= (SELECT lot_seq FROM _fast_upd WHERE _fast_upd.rid = signals.rowid) "
            "WHERE rowid IN (SELECT rid FROM _fast_upd)"))
        conn.execute(text("DROP TABLE IF EXISTS _fast_upd"))
    return len(up)


def build_trade_lots():
    """(Re)build the trade_lots table from BUY signal rows paired to closed
    trades in trade_log. One row per lot: every BUY between a trade's buy_date
    and sell_date (inclusive) for that ticker. Mirrors the columns the app
    reads: ticker, buy_date, sell_date, lot_seq, lot_date, lot_price."""
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS trade_lots"))
        conn.execute(text(
            "CREATE TABLE trade_lots (ticker TEXT, buy_date TEXT, sell_date TEXT, "
            "lot_seq INTEGER, lot_date TEXT, lot_price REAL)"))
        # For each closed trade, gather its BUY lots from signals.
        conn.execute(text(
            "INSERT INTO trade_lots (ticker, buy_date, sell_date, lot_seq, lot_date, lot_price) "
            "SELECT tl.ticker, tl.buy_date, tl.sell_date, s.lot_seq, s.date, s.close "
            "FROM trade_log tl "
            "JOIN signals s ON s.ticker = tl.ticker AND s.signal = 'BUY' "
            "  AND DATE(s.date) >= DATE(tl.buy_date) AND DATE(s.date) <= DATE(tl.sell_date)"))


def main():
    from backtesting.engine import build_trade_log
    n = fast_signal_pass()
    print(f"Fast signal pass: {n:,} rows relabeled.")
    build_trade_log()
    build_trade_lots()
    print("Fast tier rebuild complete (signals + trade_log + trade_lots).")


if __name__ == "__main__":
    main()
