"""Onboard a single ticker into the Vesign database, scoped to that one ticker
(does NOT re-score the whole universe). Use for new IPOs / additions.

  cd /opt/vesign && venv/bin/python production/onboard_ticker.py SPCX
  venv/bin/python production/onboard_ticker.py NVDA --start 2018-01-02

The ticker is added as a custom (non-index) row in `companies`, which
utils.universe_loader.load_universe() preserves across daily rebuilds and
force-adds to the daily universe — so it stays in the pipeline forever.

Company name / sector / industry / domain are pulled from FMP company_profile.
Idempotent: safe to re-run (per-ticker delete+append everywhere).

A brand-new IPO (<~200 trading days) has NaN technical indicators → scores HOLD
until enough history accrues; real signals then appear automatically.

Rollback: DELETE the ticker from companies, daily_prices, features, fundamentals,
analyst_expectations, analyst_targets_history, market_cap_history,
company_health(_history), signals.
"""
import sys, os, argparse
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import date
from urllib.parse import urlparse
import pandas as pd
from sqlalchemy import text

from data.loaders import engine
from data import market_data, fmp
try:
    from data import fundamentals as fund   # redesign-only (pe_ttm/TTM); absent on prod `main`
except ImportError:
    fund = None
from data.analyst_targets import fetch_with_fallback
from features.technical import compute_features
from signals.engine import run_scoring
from production.backfill_historical_marketcap import fetch_enterprise_values
from utils.sectors import normalize_sector

BASELINE_START = "2018-01-02"   # universe baseline; a new IPO just returns its few rows


def step(msg):
    print(f"\n=== {msg} ===", flush=True)


def _domain_from_website(website):
    if not website:
        return None
    host = urlparse(website if "//" in website else "//" + website).netloc or website
    return host.lower().lstrip("www.") or None


_HEALTH_SYSTEM = (
    "You are a strict financial analyst rating company health on a FULL 1-5 scale. "
    "Use the ENTIRE range — do NOT cluster scores around 2-3.\n\n"
    "Scale definition (use each level freely):\n"
    "  1 = Weak:      Negative or near-zero margins, heavy debt load, negative/weak cash flow, "
    "shrinking revenue, or near-distress signals.\n"
    "  2 = Fair:      Below-average profitability, elevated leverage, modest or inconsistent cash flow, "
    "slow/flat growth. Survivable but uninspiring.\n"
    "  3 = Good:      Solid, average performance for the industry. Profitable, manageable debt, "
    "positive cash flow, stable growth.\n"
    "  4 = Great:     Above-average margins, strong free cash flow, low-to-moderate debt, "
    "healthy revenue/earnings growth. Financially sound.\n"
    "  5 = Excellent: Exceptional across ALL metrics — industry-leading margins, minimal debt, "
    "strong growing free cash flow, consistent double-digit growth.\n\n"
    "Rules:\n"
    "- If debtToEquity > 2.0 or profitMargins < 0, lean toward 1-2.\n"
    "- If freeCashFlow < 0 and revenueGrowth < 0, that is a 1 or 2.\n"
    "- If profitMargins > 0.20 and debtToEquity < 0.5 and revenueGrowth > 0.10, lean toward 4-5.\n"
    "- Score 5 requires excellence in ALL dimensions simultaneously.\n"
    "- If the company had a net loss in the prior year (one year ago), the score MUST be 3 or lower. No exceptions.\n"
    "- A single strong recovery year after a loss does NOT warrant a 4 or 5.\n"
    "- Context matters: benchmark within the company's industry.\n\n"
    "Respond with ONLY valid JSON: {\"score\": <integer 1-5>, \"reason\": \"<one concise sentence>\"}"
)


def score_health_one(ticker):
    """Per-ticker replica of market_data._score_us + _write_score. Avoids the
    universe-wide update_company_health() which mass-rescores every stale ticker.
    Writes company_health + company_health_history. No-op without ANTHROPIC_API_KEY."""
    import json as _json
    from datetime import datetime, timezone
    from dotenv import load_dotenv
    load_dotenv()
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("  ANTHROPIC_API_KEY not set — skipping health.")
        return
    import anthropic
    client = anthropic.Anthropic(api_key=api_key)
    now = datetime.now(timezone.utc)

    profile = fmp.company_profile(ticker) or {}
    ratios = fmp.ratios_ttm(ticker) or {}
    km = fmp.key_metrics_ttm(ticker) or {}
    growth = fmp.financial_growth(ticker) or {}
    cf = fmp.cash_flow(ticker) or {}
    stmts = fmp.income_statement(ticker, limit=2)
    headlines = fmp.stock_news(ticker, limit=5)

    metrics = {}
    for k, v in [
        ("profitMargins", ratios.get("netProfitMarginTTM")),
        ("operatingMargins", ratios.get("operatingProfitMarginTTM")),
        ("grossMargins", ratios.get("grossProfitMarginTTM")),
        ("returnOnEquity", km.get("returnOnEquityTTM")),
        ("returnOnAssets", km.get("returnOnAssetsTTM")),
        ("currentRatio", ratios.get("currentRatioTTM")),
        ("quickRatio", ratios.get("quickRatioTTM")),
        ("debtToEquity", ratios.get("debtToEquityRatioTTM")),
        ("revenueGrowth", growth.get("revenueGrowth")),
        ("netIncomeGrowth", growth.get("netIncomeGrowth")),
        ("freeCashFlow", cf.get("freeCashFlow")),
        ("operatingCashFlow", cf.get("operatingCashFlow")),
    ]:
        if v is not None:
            metrics[k] = v

    prompt = f"Company: {profile.get('companyName') or ticker} ({ticker})\nIndustry: {profile.get('industry') or 'Unknown'}\n\n"
    if metrics:
        prompt += "TTM financial metrics:\n"
        for k, v in metrics.items():
            prompt += f"  {k}: {round(v, 4) if isinstance(v, float) else f'{v:,}'}\n"
    if stmts:
        prompt += "\nAnnual income history (most recent first):\n"
        for s in stmts:
            rev = s.get("revenue"); ni = s.get("netIncome")
            yr = s.get("fiscalYear") or s.get("date", "")[:4]
            margin = round(ni / rev * 100, 1) if rev and ni is not None else None
            prompt += f"  {yr}: revenue={rev:,}, netIncome={(f'{ni:,}' if ni is not None else 'N/A')}, margin={(f'{margin}%' if margin is not None else 'N/A')}\n"
    if headlines:
        prompt += "\nRecent news:\n" + "".join(
            f"  - {h.get('title', '') if isinstance(h, dict) else h}\n" for h in headlines)

    msg = client.messages.create(model="claude-sonnet-4-6", max_tokens=120,
                                 system=_HEALTH_SYSTEM, messages=[{"role": "user", "content": prompt}])
    raw = msg.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    result = _json.loads(raw.strip())
    r = {"ticker": ticker, "score": max(1, min(5, int(result["score"]))),
         "reason": result["reason"], "last_update": now}
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT OR REPLACE INTO company_health (ticker, score, reason, last_update) "
            "VALUES (:ticker, :score, :reason, :last_update)"), r)
        conn.execute(text(
            "INSERT INTO company_health_history (ticker, score, reason, recorded_at) "
            "SELECT :ticker, :score, :reason, :last_update WHERE NOT EXISTS ("
            "SELECT 1 FROM company_health_history WHERE ticker=:ticker AND date(recorded_at)=date(:last_update))"), r)


def onboard(T, start=BASELINE_START):
    T = T.upper().strip()
    prof = fmp.company_profile(T) or {}
    company = prof.get("companyName") or T
    sector = normalize_sector(prof.get("sector")) or ""
    industry = prof.get("industry")
    domain = _domain_from_website(prof.get("website"))
    print(f"Onboarding {T} ({company}) — sector={sector!r} industry={industry!r} domain={domain!r}")
    if not prof:
        print("  WARNING: empty FMP profile — ticker may not exist. Continuing.")

    # 1) companies row (no UNIQUE constraint — guard against double-insert) ------
    step("1. companies row")
    desc = prof.get("description")
    with engine.begin() as c:
        exists = c.execute(text("SELECT 1 FROM companies WHERE ticker=:t"), {"t": T}).fetchone()
        if exists:
            c.execute(text(
                "UPDATE companies SET company=:co, sector=:s, market='US', industry=:ind, "
                "description=COALESCE(:d, description), domain=COALESCE(domain,:dom) WHERE ticker=:t"),
                {"t": T, "co": company, "s": sector, "ind": industry, "d": desc, "dom": domain})
            print("  updated existing companies row")
        else:
            c.execute(text(
                "INSERT INTO companies (ticker, company, sector, market, industry, description, domain) "
                "VALUES (:t,:co,:s,'US',:ind,:d,:dom)"),
                {"t": T, "co": company, "s": sector, "ind": industry, "d": desc, "dom": domain})
            print("  inserted new companies row")

    # 2) prices -> daily_prices -------------------------------------------------
    step("2. prices -> daily_prices")
    market_data._download_and_save([T], start, date.today())
    px = pd.read_sql(text("SELECT * FROM daily_prices WHERE ticker=:t ORDER BY date"),
                     engine, params={"t": T})
    print(f"  {len(px)} price rows: {px['date'].min() if len(px) else '—'} .. {px['date'].max() if len(px) else '—'}")
    if len(px) == 0:
        raise SystemExit("No price rows fetched — aborting (ticker has no FMP price data).")

    # 3) features for this ticker only (ticker-scoped delete; NOT save_features) -
    step("3. features")
    feat = compute_features(px)
    with engine.begin() as c:
        c.execute(text("DELETE FROM features WHERE ticker=:t"), {"t": T})
    feat.to_sql("features", engine, if_exists="append", index=False)
    print(f"  wrote {len(feat)} feature rows (indicators NaN at low history — expected)")

    # 4) predictions: SKIPPED (factors NaN -> NaN pred; scoring waives NULL ML)

    # 5) fundamentals (TTM if available) + market_cap from profile --------------
    step("5. fundamentals")
    if fund is not None:
        try:
            fund.store_fundamentals(T, fund.fetch_fundamentals(T), engine)
            print("  fundamentals (TTM) stored")
        except Exception as e:
            print(f"  fundamentals fetch/store failed (non-fatal): {e}")
    else:
        print("  data.fundamentals absent (prod main) — market_cap only below")
    mc = prof.get("marketCap")
    if mc:
        with engine.begin() as c:
            r = c.execute(text("UPDATE fundamentals SET market_cap=:m WHERE ticker=:t"), {"m": mc, "t": T})
            if r.rowcount == 0:
                c.execute(text("INSERT INTO fundamentals (ticker, market_cap) VALUES (:t,:m)"), {"t": T, "m": mc})
        print(f"  market_cap set: {mc:,}")

    # 6) market_cap_history (point-in-time shares) ------------------------------
    step("6. market_cap_history")
    try:
        rows = fetch_enterprise_values(T)
        if rows:
            with engine.begin() as c:
                c.execute(text("DELETE FROM market_cap_history WHERE ticker=:t"), {"t": T})
            pd.DataFrame(rows).to_sql("market_cap_history", engine, if_exists="append", index=False)
            print(f"  wrote {len(rows)} market_cap_history rows")
        else:
            print("  no enterprise-values rows from FMP — skipped")
    except Exception as e:
        print(f"  market_cap_history failed (non-fatal): {e}")

    # 7) analyst targets -> analyst_expectations + history ----------------------
    step("7. analyst targets")
    try:
        a = fetch_with_fallback([T]).get(T) or {}
        with engine.begin() as c:
            c.execute(text(
                "INSERT INTO analyst_expectations "
                "(ticker,target_mean_price,target_high_price,target_low_price,number_of_analysts,last_update,source) "
                "VALUES (:t,:m,:h,:l,:n,:u,:s) "
                "ON CONFLICT(ticker) DO UPDATE SET target_mean_price=excluded.target_mean_price, "
                "target_high_price=excluded.target_high_price, target_low_price=excluded.target_low_price, "
                "number_of_analysts=excluded.number_of_analysts, last_update=excluded.last_update, source=excluded.source"),
                {"t": T, "m": a.get("target_mean_price"), "h": a.get("target_high_price"),
                 "l": a.get("target_low_price"), "n": a.get("number_of_analysts"),
                 "u": date.today().isoformat(), "s": a.get("source")})
        market_data.snapshot_analyst_targets(date.today().isoformat())
        print(f"  analyst: mean={a.get('target_mean_price')} n={a.get('number_of_analysts')} src={a.get('source')}")
    except Exception as e:
        print(f"  analyst targets failed (non-fatal): {e}")

    # 8) company health — THIS ticker only (never the universe-wide function) ----
    step("8. company health")
    try:
        score_health_one(T)
        hs = pd.read_sql(text("SELECT score, reason FROM company_health WHERE ticker=:t"), engine, params={"t": T})
        if len(hs):
            print(f"  health score: {hs['score'].iloc[0]} — {hs['reason'].iloc[0]}")
        else:
            print("  health NOT SET — daily pipeline will fill it")
    except Exception as e:
        print(f"  company health failed (non-fatal): {e}")

    # 9) scoring -> signals (scoped) --------------------------------------------
    step("9. scoring -> signals")
    run_scoring(tickers=[T])
    sig = pd.read_sql(text("SELECT date, signal, vesign_score, vqs FROM signals WHERE ticker=:t ORDER BY date"),
                      engine, params={"t": T})
    print(f"  signal rows: {len(sig)}")
    if len(sig):
        print(sig.tail(3).to_string(index=False))

    # 10) logo ------------------------------------------------------------------
    step("10. logo")
    try:
        from production.download_logos import download_one, _sync_logo_urls_to_disk
        tk, src = download_one(T, domain)
        _sync_logo_urls_to_disk()
        lu = pd.read_sql(text("SELECT logo_url FROM companies WHERE ticker=:t"), engine, params={"t": T})
        print(f"  logo src={src} logo_url={lu['logo_url'].iloc[0] if len(lu) else None}")
        if src:
            print("  NOTE: if the logo is dark-on-transparent it is invisible on the dark UI — "
                  "bake a white bg and add a ?v=YYYYMMDD cache-bust to companies.logo_url.")
    except Exception as e:
        print(f"  logo download failed (non-fatal): {e}")

    print(f"\nDONE onboarding {T}.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Onboard a single ticker into the Vesign DB.")
    ap.add_argument("ticker", help="Ticker symbol, e.g. SPCX")
    ap.add_argument("--start", default=BASELINE_START, help=f"Price history start date (default {BASELINE_START})")
    args = ap.parse_args()
    onboard(args.ticker, args.start)
