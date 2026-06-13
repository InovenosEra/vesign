"""Read-only universe-sizing analysis: how many ADDITIONAL US common stocks
could join the current ~1,800 universe without lowering its quality/liquidity/
analyst caliber.

READ-ONLY: SELECTs from vesign.db (current tickers + their stats), queries FMP
via the existing key, prints counts. Writes NOTHING to the live DB/engine.
Run with --analyst to do the per-ticker analyst refinement pass.
"""
import os, sys, time, argparse
import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sqlalchemy import create_engine, text          # noqa: E402
from data.fmp import _get                            # noqa: E402

DB = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "vesign.db")
ENG = create_engine(f"sqlite:///{DB}")
US_EXCH = {"NASDAQ", "NYSE", "AMEX"}


def norm(sym: str) -> str:
    return (sym or "").replace(".", "-").upper()


def is_common(sym: str) -> bool:
    s = sym or ""
    if "." in s or "-" in s:        # preferred / class shares like X-PA, foreign
        # keep dash? our universe uses '-' for class shares (BRK-B). Only drop
        # the preferred pattern '<base>-P<x>'. Treat plain dash class shares as OK.
        if "-P" in s.upper():
            return False
    if s.endswith("WW") or s.endswith("W"):
        return False
    if s.endswith("U"):
        return False
    if s.endswith("R"):
        return False
    return True


# ── STEP 1: profile current universe (pure DB) ────────────────────────────
def profile_current():
    with ENG.connect() as c:
        comp = pd.read_sql(text(
            "SELECT ticker FROM companies WHERE COALESCE(market,'US')='US'"), c)
        fund = pd.read_sql(text("SELECT ticker, market_cap FROM fundamentals"), c)
        # latest analyst count per ticker
        an = pd.read_sql(text("""
            SELECT s.ticker, s.number_of_analysts AS n_an
            FROM (SELECT ticker, MAX(date) md FROM signals GROUP BY ticker) t
            JOIN signals s ON s.ticker=t.ticker AND s.date=t.md
        """), c)
        # last close + avg daily volume (last 63 trading days)
        px = pd.read_sql(text("""
            WITH ranked AS (
                SELECT ticker, date, close, volume,
                       ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) rn
                FROM daily_prices
            )
            SELECT ticker,
                   MAX(CASE WHEN rn=1 THEN close END) AS last_close,
                   AVG(CASE WHEN rn<=63 THEN volume END) AS avg_vol
            FROM ranked WHERE rn<=63 GROUP BY ticker
        """), c)
    df = (comp.merge(fund, on="ticker", how="left")
              .merge(an, on="ticker", how="left")
              .merge(px, on="ticker", how="left"))
    df["dollar_vol"] = df["last_close"] * df["avg_vol"]
    return df


def pct_table(df):
    rows = {}
    for col, label in [("market_cap", "Market cap ($)"),
                       ("dollar_vol", "Dollar volume ($/day)"),
                       ("last_close", "Price ($)"),
                       ("n_an", "Analyst count")]:
        s = df[col].dropna()
        s = s[s > 0] if col != "n_an" else s.dropna()
        rows[label] = {"p10": s.quantile(0.10), "p25": s.quantile(0.25),
                       "p50": s.quantile(0.50), "n": len(s)}
    return pd.DataFrame(rows).T


# ── STEP 3: FMP screener candidate pull ───────────────────────────────────
def screener_candidates(cap_min, price_min=5):
    """Paginate the US common-stock screener above cap_min, banding by market
    cap to dodge the 10k row cap. Returns DataFrame of candidates."""
    bands = [(cap_min, 1e9), (1e9, 3e9), (3e9, 1e10), (1e10, 5e10),
             (5e10, 2e11), (2e11, 5e13)]
    bands = [(lo, hi) for lo, hi in bands if hi > cap_min]
    bands[0] = (cap_min, bands[0][1])
    out = []
    for lo, hi in bands:
        for exch in US_EXCH:
            rows = _get("company-screener", {
                "exchange": exch, "country": "US",
                "isEtf": "false", "isFund": "false", "isActivelyTrading": "true",
                "marketCapMoreThan": int(lo), "marketCapLowerThan": int(hi),
                "priceMoreThan": price_min, "limit": 10000,
            }) or []
            if len(rows) >= 10000:
                print(f"    WARN: band {lo:.0e}-{hi:.0e} {exch} hit 10k cap")
            out.extend(rows)
    df = pd.DataFrame(out)
    if df.empty:
        return df
    df = df.drop_duplicates("symbol")
    return df


def fetch_analyst_count(sym):
    """FMP numAnalystsEps (max across forward estimates) — closest match to the
    DB's yfinance number_of_analysts. Returns int or None."""
    d = _get("analyst-estimates", {"symbol": sym, "period": "annual", "limit": 6})
    if not isinstance(d, list) or not d:
        return None
    vals = [r.get("numAnalystsEps") or 0 for r in d]
    return max(vals) if vals else 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--analyst", action="store_true", help="run per-ticker analyst refinement")
    ap.add_argument("--analyst-sample", type=int, default=0, help="sample N candidates for analyst pass")
    args = ap.parse_args()

    cur = profile_current()
    cur_syms = set(norm(s) for s in cur["ticker"])
    print(f"Current universe: {len(cur)} US names "
          f"({cur['market_cap'].gt(0).sum()} w/ mcap, {cur['n_an'].gt(0).sum()} w/ analysts, "
          f"{cur['dollar_vol'].gt(0).sum()} w/ $vol)")

    print("\n=== STEP 1: CURRENT-UNIVERSE PROFILE (percentiles) ===")
    prof = pct_table(cur)
    pd.set_option("display.float_format", lambda x: f"{x:,.2f}")
    print(prof.to_string())

    TH = {
        "INCLUSIVE": {"cap": cur["market_cap"][cur.market_cap > 0].quantile(.10),
                      "dvol": cur["dollar_vol"][cur.dollar_vol > 0].quantile(.10),
                      "price": cur["last_close"][cur.last_close > 0].quantile(.10),
                      "an": cur["n_an"].dropna().quantile(.10)},
        "STRICT":    {"cap": cur["market_cap"][cur.market_cap > 0].quantile(.25),
                      "dvol": cur["dollar_vol"][cur.dollar_vol > 0].quantile(.25),
                      "price": cur["last_close"][cur.last_close > 0].quantile(.25),
                      "an": cur["n_an"].dropna().quantile(.25)},
    }
    print("\n=== STEP 2: THRESHOLD SETS ===")
    for name, t in TH.items():
        print(f"  {name:9s}: mcap>=${t['cap']:,.0f}  $vol>=${t['dvol']:,.0f}/day  "
              f"price>=${t['price']:.2f}  analysts>={t['an']:.0f}")

    print("\n=== STEP 3: ADDABLE-NAME COUNTS ===")
    for name, t in TH.items():
        cand = screener_candidates(t["cap"])
        if cand.empty:
            print(f"\n[{name}] screener returned nothing"); continue
        cand["nsym"] = cand["symbol"].map(norm)
        cand = cand[cand["symbol"].map(is_common)]
        cand["dollar_vol"] = cand["price"].astype(float) * cand["volume"].astype(float)
        cand = cand[cand["dollar_vol"] >= t["dvol"]]
        cand = cand[cand["price"].astype(float) >= t["price"]]
        new = cand[~cand["nsym"].isin(cur_syms)].copy()
        print(f"\n[{name}]  thresholds: mcap>=${t['cap']:,.0f}, $vol>=${t['dvol']:,.0f}, "
              f"price>=${t['price']:.2f}, analysts>={t['an']:.0f}")
        print(f"  candidates after cap/price/$vol/pattern filters: {len(cand)}")
        print(f"  NEW (not already tracked):                       {len(new)}")

        an_note = ""
        if args.analyst or args.analyst_sample:
            pool = new
            sampled = False
            if args.analyst_sample and len(new) > args.analyst_sample:
                pool = new.sample(args.analyst_sample, random_state=1); sampled = True
            counts = {}
            with ThreadPoolExecutor(max_workers=8) as ex:
                futs = {ex.submit(fetch_analyst_count, s): s for s in pool["symbol"]}
                for f in as_completed(futs):
                    counts[futs[f]] = f.result()
                    time.sleep(0.005)
            pool = pool.copy()
            pool["an_fmp"] = pool["symbol"].map(counts)
            passed = pool[(pool["an_fmp"].notna()) & (pool["an_fmp"] >= t["an"])]
            rate = len(passed) / len(pool) if len(pool) else 0
            n_final = int(round(rate * len(new))) if sampled else len(passed)
            an_note = (f" (analyst pass-rate {rate*100:.0f}% on "
                       f"{'sample of '+str(len(pool)) if sampled else 'full '+str(len(pool))})")
            print(f"  analyst coverage >= {t['an']:.0f}: {n_final} pass{an_note}")
            addable = n_final
        else:
            addable = len(new)
            print("  [analyst refinement skipped — run with --analyst]")

        print(f"  >>> ADDABLE: {addable}   ->  TOTAL universe: {len(cur)+addable}")
        # breakdowns on the pre-analyst NEW set (counts only)
        ex_b = new["exchangeShortName"].value_counts().to_dict() if "exchangeShortName" in new else {}
        print(f"      by exchange (pre-analyst NEW): {ex_b}")
        mc = new["marketCap"].astype(float)
        bands = {"$300M-1B": ((mc >= 3e8) & (mc < 1e9)).sum(),
                 "$1-3B": ((mc >= 1e9) & (mc < 3e9)).sum(),
                 "$3-10B": ((mc >= 3e9) & (mc < 1e10)).sum(),
                 "$10-50B": ((mc >= 1e10) & (mc < 5e10)).sum(),
                 ">$50B": (mc >= 5e10).sum()}
        print(f"      by market-cap band (pre-analyst NEW): {bands}")


if __name__ == "__main__":
    main()
