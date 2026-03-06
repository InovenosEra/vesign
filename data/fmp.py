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
    """Real-time prices via parallel single-symbol quote calls (batch requires premium)."""
    if not tickers:
        return {}

    def _quote(t):
        data = _get("quote", {"symbol": t})
        if data and isinstance(data, list) and data:
            return t, data[0].get("price")
        return t, None

    prices = {}
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(_quote, t): t for t in tickers}
        for f in as_completed(futures):
            t, price = f.result()
            prices[t] = price
    return prices


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


def price_target_summary(ticker: str) -> "dict | None":
    """Analyst price target summary (last-month avg + count).
    Note: high/low targets not available in this endpoint.
    """
    data = _get("price-target-summary", {"symbol": ticker})
    return data[0] if data else None


def income_statement(ticker: str, limit: int = 2) -> list:
    """Last N annual income statements (for YoY comparison)."""
    data = _get("income-statement", {"symbol": ticker, "period": "annual", "limit": limit})
    return data if isinstance(data, list) else []


def stock_news(ticker: str, limit: int = 5) -> list:
    """Recent news headlines. Returns empty list if not available on current plan."""
    data = _get("news/stock", {"symbols": ticker, "limit": limit})
    if not data or not isinstance(data, list):
        return []
    return [item["title"] for item in data if item.get("title")]
