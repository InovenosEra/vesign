"""FMP (Financial Modeling Prep) API client — stable endpoints.

All functions return None / empty on error so callers can handle gracefully.
Requires FMP_API_KEY in .env.
"""

import os
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env'))

_BASE = "https://financialmodelingprep.com/stable"


def _key() -> str:
    return os.environ.get("FMP_API_KEY", "")


def _get(endpoint: str, params: dict, _retries: int = 3) -> "list | dict | None":
    """Raw GET helper. Returns parsed JSON or None on error. Retries on 429."""
    import time
    params = {**params, "apikey": _key()}
    for attempt in range(_retries):
        try:
            r = requests.get(f"{_BASE}/{endpoint}", params=params, timeout=30)
            if r.status_code == 429:
                time.sleep(2 ** attempt)  # 1s, 2s, 4s
                continue
            r.raise_for_status()
            return r.json()
        except Exception:
            return None
    return None


def historical_prices(ticker: str, start, end) -> "pd.DataFrame | None":
    """OHLCV history for one US ticker. start/end can be date objects or 'YYYY-MM-DD' strings."""
    data = _get("historical-price-eod/full", {"symbol": ticker, "from": str(start), "to": str(end)})
    if not data:
        return None
    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"])
    df = df.rename(columns={"symbol": "ticker"}) if "symbol" in df.columns else df
    df["ticker"] = ticker
    keep = [c for c in ("date", "ticker", "open", "high", "low", "close", "volume") if c in df.columns]
    df = df[keep].dropna(subset=["close"])
    return df if not df.empty else None


def live_prices(tickers: list) -> dict:
    """Real-time regular-session prices via FMP's true batch endpoint.
    Chunks of 100 tickers per call, multiple chunks in parallel."""
    if not tickers:
        return {}

    CHUNK = 100
    chunks = [tickers[i:i + CHUNK] for i in range(0, len(tickers), CHUNK)]

    def _batch(chunk):
        # /stable/batch-quote returns a list of quote dicts when given symbols=A,B,C
        data = _get("batch-quote", {"symbols": ",".join(chunk)})
        if not isinstance(data, list):
            return {}
        return {row["symbol"]: row.get("price") for row in data if row.get("symbol")}

    out = {}
    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = [ex.submit(_batch, c) for c in chunks]
        for f in as_completed(futures):
            out.update(f.result())

    # Ensure every requested ticker has an entry (None if missing from response)
    for t in tickers:
        out.setdefault(t, None)
    return out


def aftermarket_trades(tickers: list) -> dict:
    """Latest extended-hours trade price for each ticker (pre OR post — FMP
    returns whichever is most recent). Tickers with no extended-hours
    activity return None."""
    if not tickers:
        return {}

    def _trade(t):
        data = _get("aftermarket-trade", {"symbol": t})
        if data and isinstance(data, list) and data:
            return t, data[0].get("price")
        return t, None

    out = {}
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(_trade, t): t for t in tickers}
        for f in as_completed(futures):
            t, price = f.result()
            out[t] = price
    return out


def company_profile(ticker: str) -> "dict | None":
    """Company profile: companyName, sector, industry, description, image, mktCap."""
    data = _get("profile", {"symbol": ticker})
    return data[0] if data else None


def ratios_ttm(ticker: str) -> "dict | None":
    """TTM financial ratios: margins, liquidity, leverage.
    Note: debtToEquityRatioTTM is a true ratio (e.g. 1.02), not ×100.
    ROE/ROA are in key_metrics_ttm, not here.
    """
    data = _get("ratios-ttm", {"symbol": ticker})
    return data[0] if data else None


def key_metrics_ttm(ticker: str) -> "dict | None":
    """TTM key metrics including returnOnAssetsTTM and returnOnEquityTTM."""
    data = _get("key-metrics-ttm", {"symbol": ticker})
    return data[0] if data else None


def financial_growth(ticker: str) -> "dict | None":
    """Latest annual revenue/earnings growth."""
    data = _get("financial-growth", {"symbol": ticker, "period": "annual", "limit": 1})
    return data[0] if data else None


def cash_flow(ticker: str) -> "dict | None":
    """Latest annual cash flow statement."""
    data = _get("cash-flow-statement", {"symbol": ticker, "period": "annual", "limit": 1})
    return data[0] if data else None


def price_target_consensus(ticker: str) -> "dict | None":
    """Analyst price target consensus with proper low/consensus/high from individual targets."""
    data = _get("price-target-consensus", {"symbol": ticker})
    return data[0] if data else None


def income_statement(ticker: str, limit: int = 2) -> list:
    """Last N annual income statements (for YoY comparison)."""
    data = _get("income-statement", {"symbol": ticker, "period": "annual", "limit": limit})
    return data if isinstance(data, list) else []


def stock_news(ticker: str, limit: int = 5) -> list:
    """Recent news articles with metadata. Returns empty list if not available on current plan."""
    data = _get("news/stock", {"symbols": ticker, "limit": limit})
    if not data or not isinstance(data, list):
        return []
    return [{"title": item.get("title", ""), "date": item.get("publishedDate", ""),
             "source": item.get("site", ""), "url": item.get("url", ""),
             "summary": item.get("text", ""), "image": item.get("image", "")}
            for item in data if item.get("title")]


def earnings_calendar(ticker: str) -> list:
    """Next upcoming earnings date with EPS/revenue estimates (deduplicated, future only)."""
    from datetime import date as _date
    data = _get("earnings-calendar", {"symbol": ticker})
    if not data or not isinstance(data, list):
        return []
    today = _date.today().isoformat()
    seen_dates = set()
    result = []
    for item in data:
        d = item.get("date")
        if not d or d < today or d in seen_dates:
            continue
        seen_dates.add(d)
        result.append({"date": d, "eps_est": item.get("epsEstimated"),
                        "revenue_est": item.get("revenueEstimated")})
        if len(result) == 2:
            break
    return result


def analyst_upgrades(ticker: str, limit: int = 8) -> list:
    """Recent analyst upgrades/downgrades."""
    data = _get("upgrades-downgrades", {"symbol": ticker})
    if not data or not isinstance(data, list):
        return []
    return [{"date": item.get("publishedDate", ""), "firm": item.get("gradingCompany", ""),
             "action": item.get("action", ""), "from_grade": item.get("previousGrade", ""),
             "to_grade": item.get("newGrade", ""), "price_target": item.get("priceTarget")}
            for item in data[:limit]]


# ─── Index constituents ───
# All four sources used by utils.universe_loader. Returns a list of dicts with
# at least {ticker, company, sector}. sector may be empty when the source is an
# ETF-holdings call (FMP exposes S&P 400/600 only via IJH/IJR holdings, which
# don't carry a sector field — universe_loader fills it from the existing
# companies table where possible).

def sp500_constituents() -> list:
    """S&P 500 index members. Returns [{ticker, company, sector}]."""
    data = _get("sp500-constituent", {})
    if not isinstance(data, list):
        return []
    return [{"ticker":  (r.get("symbol") or "").replace(".", "-"),
             "company": r.get("name") or r.get("symbol", ""),
             "sector":  r.get("sector") or ""}
            for r in data if r.get("symbol")]


def nasdaq100_constituents() -> list:
    """NASDAQ-100 index members. Returns [{ticker, company, sector}]."""
    data = _get("nasdaq-constituent", {})
    if not isinstance(data, list):
        return []
    return [{"ticker":  (r.get("symbol") or "").replace(".", "-"),
             "company": r.get("name") or r.get("symbol", ""),
             "sector":  r.get("sector") or ""}
            for r in data if r.get("symbol")]


def etf_holdings(symbol: str) -> list:
    """Raw ETF holdings — used to derive S&P 400 (IJH) and S&P 600 (IJR).

    Returns the raw FMP list with keys {symbol, asset, name, isin, securityCusip,
    sharesNumber, weightPercentage, marketValue, updatedAt}. Filtering out cash
    sleeves and non-stock entries is the caller's responsibility — see
    utils.universe_loader._etf_holdings_as_constituents.
    """
    data = _get("etf/holdings", {"symbol": symbol})
    return data if isinstance(data, list) else []
