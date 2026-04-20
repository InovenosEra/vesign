"""Backfill per-year company health scores 2020-2025 via Claude Haiku Batches.

Fetches annual FMP financials (income/balance/cashflow/ratios/key-metrics) for
each US ticker, builds a year-indexed fundamentals snapshot, submits all prompts
in a single Claude batch, then writes results to company_health_history with
recorded_at = fiscal year end date.

Notes:
- News is omitted (FMP price-target-news only goes back ~2 years; historical
  news isn't reliably available).
- Annual-resolution: within-year signals all see the same health score.
- Runs only for US tickers (TASE uses yfinance which doesn't offer historical).
"""
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, UTC

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

load_dotenv(dotenv_path=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

from data import fmp
from data.loaders import engine

TARGET_YEARS = [2020, 2021, 2022, 2023, 2024, 2025]
MODEL = "claude-haiku-4-5-20251001"

# ---------------------------------------------------------------------------
# Prompt construction
# ---------------------------------------------------------------------------

_SYSTEM = (
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


def _fetch_annuals(ticker: str) -> dict:
    """Return {year: {metric: value, ...}, ...} for a single ticker."""
    by_year: dict[int, dict] = {}

    endpoints = [
        ("income-statement", [
            ("revenue", "revenue"),
            ("netIncome", "netIncome"),
            ("grossProfit", "grossProfit"),
            ("operatingIncome", "operatingIncome"),
            ("fiscalDate", "date"),
        ]),
        ("balance-sheet-statement", [
            ("totalDebt", "totalDebt"),
            ("totalEquity", "totalStockholdersEquity"),
        ]),
        ("cash-flow-statement", [
            ("freeCashFlow", "freeCashFlow"),
            ("operatingCashFlow", "netCashProvidedByOperatingActivities"),
        ]),
        ("ratios", [
            ("profitMargin", "netProfitMargin"),
            ("operatingMargin", "operatingProfitMargin"),
            ("grossMargin", "grossProfitMargin"),
            ("debtToEquity", "debtToEquityRatio"),
            ("currentRatio", "currentRatio"),
        ]),
        ("key-metrics", [
            ("returnOnEquity", "returnOnEquity"),
            ("returnOnAssets", "returnOnAssets"),
        ]),
    ]

    for endpoint, field_map in endpoints:
        data = fmp._get(endpoint, {"symbol": ticker, "period": "annual", "limit": 10})
        if not isinstance(data, list):
            continue
        for item in data:
            date_str = item.get("date") or item.get("fiscalDate")
            if not date_str:
                continue
            year = int(date_str[:4])
            if year < TARGET_YEARS[0] - 1:  # need 1 extra year for growth calcs
                continue
            yr_dict = by_year.setdefault(year, {})
            for out_k, src_k in field_map:
                if item.get(src_k) is not None:
                    yr_dict[out_k] = item[src_k]
    return by_year


def _build_prompt(ticker: str, year: int, data: dict, company_name: str, industry: str) -> str | None:
    yr = data.get(year, {})
    prior = data.get(year - 1, {})
    if not yr or "revenue" not in yr:
        return None

    # Derived: growth
    rev_growth = None
    ni_growth = None
    if "revenue" in yr and "revenue" in prior and prior["revenue"]:
        rev_growth = (yr["revenue"] - prior["revenue"]) / abs(prior["revenue"])
    if "netIncome" in yr and "netIncome" in prior and prior["netIncome"]:
        ni_growth = (yr["netIncome"] - prior["netIncome"]) / abs(prior["netIncome"])

    lines = [f"Company: {company_name} ({ticker})", f"Industry: {industry or 'Unknown'}",
             f"Fiscal year: {year}", ""]

    metric_order = [
        ("profitMargin", "profitMargin"), ("operatingMargin", "operatingMargin"),
        ("grossMargin", "grossMargin"), ("returnOnEquity", "returnOnEquity"),
        ("returnOnAssets", "returnOnAssets"), ("currentRatio", "currentRatio"),
        ("debtToEquity", "debtToEquity"),
        ("freeCashFlow", "freeCashFlow"), ("operatingCashFlow", "operatingCashFlow"),
    ]
    lines.append(f"Financial metrics for fiscal year {year}:")
    for label, key in metric_order:
        if key in yr:
            v = yr[key]
            lines.append(f"  {label}: {round(v, 4) if isinstance(v, float) else f'{v:,}'}")
    if rev_growth is not None:
        lines.append(f"  revenueGrowth: {round(rev_growth, 4)}")
    if ni_growth is not None:
        lines.append(f"  netIncomeGrowth: {round(ni_growth, 4)}")

    # Income history (prior 2 years relative to target year)
    lines.append("")
    lines.append(f"Annual income history (most recent first, relative to FY {year}):")
    for y in (year, year - 1, year - 2):
        h = data.get(y, {})
        if "revenue" in h and "netIncome" in h:
            rev, ni = h["revenue"], h["netIncome"]
            margin = round(ni / rev * 100, 1) if rev else None
            margin_str = f"{margin}%" if margin is not None else "N/A"
            lines.append(f"  {y}: revenue={rev:,}, netIncome={ni:,}, margin={margin_str}")

    return "\n".join(lines)


def _parse_claude(raw: str) -> dict | None:
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    try:
        return json.loads(raw.strip())
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ANTHROPIC_API_KEY not set")
        return

    import anthropic
    client = anthropic.Anthropic(api_key=api_key)

    # 1. Load tickers + company metadata
    companies = pd.read_sql(
        "SELECT ticker, company, industry FROM companies WHERE COALESCE(market,'US') = 'US'",
        engine,
    )
    tickers = companies["ticker"].tolist()
    company_map = {r["ticker"]: (r["company"] or r["ticker"], r["industry"] or "") for _, r in companies.iterrows()}
    print(f"Tickers: {len(tickers)}")

    # 2. Fetch annuals in parallel (10 workers; each ticker = 5 FMP calls)
    print(f"Fetching annuals for all tickers (up to {len(tickers) * 5:,} FMP calls)…")
    started = datetime.now()
    all_data: dict[str, dict] = {}
    done = 0
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(_fetch_annuals, t): t for t in tickers}
        for fut in as_completed(futures):
            t = futures[fut]
            try:
                all_data[t] = fut.result()
            except Exception:
                all_data[t] = {}
            done += 1
            if done % 200 == 0:
                print(f"  {done}/{len(tickers)} fetched — elapsed {datetime.now() - started}")
    print(f"  Annuals fetched for {sum(1 for d in all_data.values() if d)} tickers")

    # 3. Build prompts per (ticker, year)
    requests_list = []
    custom_to_info: dict[str, tuple[str, int]] = {}  # custom_id → (ticker, year)
    for ticker in tickers:
        data = all_data.get(ticker, {})
        if not data:
            continue
        company_name, industry = company_map.get(ticker, (ticker, ""))
        for year in TARGET_YEARS:
            prompt = _build_prompt(ticker, year, data, company_name, industry)
            if not prompt:
                continue
            cid = f"{ticker}__{year}"
            custom_to_info[cid] = (ticker, year)
            requests_list.append({
                "custom_id": cid,
                "params": {
                    "model": MODEL,
                    "max_tokens": 200,
                    "system": _SYSTEM,
                    "messages": [{"role": "user", "content": prompt}],
                },
            })
    print(f"Built {len(requests_list):,} prompts across {len(set(cid.split('__')[0] for cid in custom_to_info))} tickers")

    if not requests_list:
        print("No prompts — aborting.")
        return

    # 4. Submit in CHUNKS of 500 (large batches have been stuck)
    from anthropic.types.messages.batch_create_params import Request
    CHUNK = 500
    results: dict = {}
    for i in range(0, len(requests_list), CHUNK):
        chunk = requests_list[i:i + CHUNK]
        chunk_label = f"chunk {i // CHUNK + 1}/{(len(requests_list) + CHUNK - 1) // CHUNK}"
        print(f"\n--- Submitting {chunk_label} ({len(chunk)} prompts) ---", flush=True)
        batch_requests = [Request(custom_id=r["custom_id"], params=r["params"]) for r in chunk]
        batch = client.messages.batches.create(requests=batch_requests)
        print(f"  batch id: {batch.id}", flush=True)

        # Poll this chunk
        while True:
            b = client.messages.batches.retrieve(batch.id)
            st = b.processing_status
            c = b.request_counts
            print(f"  [{chunk_label}] status={st} proc={c.processing} ok={c.succeeded} err={c.errored}", flush=True)
            if st == "ended":
                break
            time.sleep(20)

        # Fetch results for this chunk
        chunk_ok = 0
        for line in client.messages.batches.results(batch.id):
            cid = line.custom_id
            if line.result.type != "succeeded":
                continue
            msg = line.result.message
            text_out = msg.content[0].text if msg.content else ""
            parsed = _parse_claude(text_out)
            if parsed and "score" in parsed:
                results[cid] = parsed
                chunk_ok += 1
        print(f"  [{chunk_label}] parsed {chunk_ok}/{len(chunk)}", flush=True)

    print(f"\nTotal parsed {len(results):,}/{len(requests_list):,} successful results")

    # 7. Write to company_health_history
    rows_to_write = []
    for cid, parsed in results.items():
        ticker, year = custom_to_info[cid]
        # Recorded at fiscal year end — use year-end date
        rows_to_write.append({
            "ticker": ticker,
            "score": int(parsed["score"]),
            "reason": parsed.get("reason", "")[:500],
            "recorded_at": f"{year}-12-31T00:00:00+00:00",
        })

    print(f"Writing {len(rows_to_write):,} rows to company_health_history…")
    with engine.begin() as conn:
        # Clear any prior-year rows we're about to overwrite (keep today's snapshot)
        conn.execute(text("""
            DELETE FROM company_health_history
            WHERE recorded_at < :cutoff
        """), {"cutoff": f"{TARGET_YEARS[-1] + 1}-01-01T00:00:00+00:00"})
        for r in rows_to_write:
            conn.execute(text("""
                INSERT OR REPLACE INTO company_health_history (ticker, score, reason, recorded_at)
                VALUES (:ticker, :score, :reason, :recorded_at)
            """), r)
    print("Done.")

    # 8. Summary
    import sqlite3
    c = sqlite3.connect("vesign.db")
    print("\nSnapshot distribution:")
    for row in c.execute("SELECT substr(recorded_at,1,4), COUNT(*) FROM company_health_history GROUP BY 1 ORDER BY 1").fetchall():
        print(f"  {row[0]}: {row[1]:,} rows")

    elapsed = datetime.now() - started
    print(f"\nCompleted in {elapsed}")


if __name__ == "__main__":
    main()
