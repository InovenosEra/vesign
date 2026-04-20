"""Quarterly per-ticker health score backfill via Claude Haiku Batches.

Fetches 10 years of QUARTERLY FMP financials for each US ticker, builds a TTM
(trailing-12-months) snapshot at every quarter end from 2020-Q1 onwards, and
generates a 1-5 financial health score for each snapshot via Claude Haiku.

Writes to company_health_history with recorded_at = quarter-end date.
"""
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

load_dotenv(dotenv_path=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

from data import fmp
from data.loaders import engine

TARGET_START = "2020-01-01"  # any quarter ending on/after this
MODEL = "claude-haiku-4-5-20251001"

_SYSTEM = (
    "You are a strict financial analyst rating company health on a FULL 1-5 scale. "
    "Use the ENTIRE range — do NOT cluster scores around 2-3.\n\n"
    "Scale:\n"
    "  1 = Weak, 2 = Fair, 3 = Good, 4 = Great, 5 = Excellent\n\n"
    "Rules:\n"
    "- If debtToEquity > 2.0 or profitMargin < 0, lean toward 1-2.\n"
    "- If freeCashFlow < 0 and revenueGrowth < 0, that is a 1 or 2.\n"
    "- If profitMargin > 0.20 and debtToEquity < 0.5 and revenueGrowth > 0.10, lean toward 4-5.\n"
    "- Score 5 requires excellence in ALL dimensions simultaneously.\n"
    "- If the company had a net loss in the prior year (TTM one year ago), the score MUST be 3 or lower.\n"
    "- Context matters: benchmark within the company's industry.\n\n"
    "Respond with ONLY valid JSON: {\"score\": <int 1-5>, \"reason\": \"<one concise sentence>\"}"
)


# ---------------------------------------------------------------------------
# FMP fetch
# ---------------------------------------------------------------------------

def _fetch_quarterly(ticker: str) -> dict:
    """Return {quarter_end_date: {metric: value, ...}} for one ticker, newest→oldest."""
    by_q: dict[str, dict] = {}
    endpoints = [
        ("income-statement", ["revenue", "netIncome", "grossProfit", "operatingIncome"]),
        ("balance-sheet-statement", [("totalDebt", "totalDebt"),
                                      ("totalEquity", "totalStockholdersEquity")]),
        ("cash-flow-statement", [("freeCashFlow", "freeCashFlow"),
                                  ("operatingCashFlow", "netCashProvidedByOperatingActivities")]),
    ]
    for endpoint, field_map in endpoints:
        data = fmp._get(endpoint, {"symbol": ticker, "period": "quarter", "limit": 40})
        if not isinstance(data, list):
            continue
        for item in data:
            date_str = item.get("date")
            if not date_str:
                continue
            qend = date_str[:10]
            yr_dict = by_q.setdefault(qend, {})
            for field in field_map:
                if isinstance(field, tuple):
                    out_k, src_k = field
                else:
                    out_k = src_k = field
                if item.get(src_k) is not None:
                    yr_dict[out_k] = item[src_k]
    return by_q


# ---------------------------------------------------------------------------
# TTM snapshot builder
# ---------------------------------------------------------------------------

def _build_ttm_snapshot(data: dict, quarter_end: str):
    """Given all quarterly data for a ticker, build a TTM snapshot as-of quarter_end.
    Returns dict with metrics, or None if insufficient history."""
    sorted_q = sorted(data.keys())  # ascending
    if quarter_end not in sorted_q:
        return None
    idx = sorted_q.index(quarter_end)
    if idx < 3:
        return None  # need 4 quarters for TTM

    last4 = [data[sorted_q[idx - i]] for i in range(4)]
    current = last4[0]

    def ttm_sum(field):
        vals = [q.get(field) for q in last4 if q.get(field) is not None]
        return sum(vals) if len(vals) == 4 else None

    rev = ttm_sum("revenue")
    ni = ttm_sum("netIncome")
    gp = ttm_sum("grossProfit")
    oi = ttm_sum("operatingIncome")
    fcf = ttm_sum("freeCashFlow")
    ocf = ttm_sum("operatingCashFlow")
    debt = current.get("totalDebt")
    equity = current.get("totalEquity")

    snap = {"revenue_ttm": rev, "netIncome_ttm": ni, "grossProfit_ttm": gp, "operatingIncome_ttm": oi,
            "freeCashFlow_ttm": fcf, "operatingCashFlow_ttm": ocf,
            "totalDebt": debt, "totalEquity": equity}

    # Derived
    if rev and ni is not None:
        snap["profitMargin"] = ni / rev
    if rev and gp is not None:
        snap["grossMargin"] = gp / rev
    if rev and oi is not None:
        snap["operatingMargin"] = oi / rev
    if debt and equity and equity > 0:
        snap["debtToEquity"] = debt / equity

    # YoY growth (compare to TTM 4 quarters prior)
    if idx >= 7:
        prior_last4 = [data[sorted_q[idx - i]] for i in range(4, 8)]
        def prior_sum(field):
            vals = [q.get(field) for q in prior_last4 if q.get(field) is not None]
            return sum(vals) if len(vals) == 4 else None
        prev_rev = prior_sum("revenue")
        prev_ni = prior_sum("netIncome")
        if rev and prev_rev:
            snap["revenueGrowth"] = (rev - prev_rev) / abs(prev_rev)
        if ni is not None and prev_ni:
            snap["netIncomeGrowth"] = (ni - prev_ni) / abs(prev_ni)

    # Prior-year net loss flag (Rules require score cap)
    # Get TTM netIncome one year back (idx-4)
    if idx >= 7:
        prior_last4 = [data[sorted_q[idx - i]] for i in range(4, 8)]
        prev_ni_ttm = sum((q.get("netIncome") or 0) for q in prior_last4) if all(q.get("netIncome") is not None for q in prior_last4) else None
        snap["priorYearNetIncome"] = prev_ni_ttm

    return snap


# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------

def _build_prompt(ticker: str, quarter_end: str, snap: dict, company: str, industry: str) -> str:
    lines = [f"Company: {company} ({ticker})", f"Industry: {industry or 'Unknown'}",
             f"As of quarter ending: {quarter_end}", "", "TTM financial metrics:"]
    for k in ("profitMargin", "grossMargin", "operatingMargin",
              "debtToEquity", "revenueGrowth", "netIncomeGrowth"):
        if k in snap and snap[k] is not None:
            lines.append(f"  {k}: {round(snap[k], 4)}")
    for k in ("freeCashFlow_ttm", "operatingCashFlow_ttm", "totalDebt", "totalEquity"):
        if k in snap and snap[k] is not None:
            lines.append(f"  {k}: {snap[k]:,.0f}")
    if "revenue_ttm" in snap and snap["revenue_ttm"] and "netIncome_ttm" in snap:
        lines.append("")
        lines.append(f"TTM revenue: {snap['revenue_ttm']:,.0f}, TTM netIncome: {snap['netIncome_ttm']:,.0f}")
    if "priorYearNetIncome" in snap and snap["priorYearNetIncome"] is not None:
        lines.append(f"TTM netIncome one year prior: {snap['priorYearNetIncome']:,.0f}")
    return "\n".join(lines)


def _parse_claude(raw: str):
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
    started = datetime.now()

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ANTHROPIC_API_KEY not set")
        return
    import anthropic
    client = anthropic.Anthropic(api_key=api_key)

    # 1. Tickers + metadata
    companies = pd.read_sql(
        "SELECT ticker, company, industry FROM companies WHERE COALESCE(market,'US')='US'",
        engine,
    )
    tickers = companies["ticker"].tolist()
    meta = {r["ticker"]: (r["company"] or r["ticker"], r["industry"] or "")
            for _, r in companies.iterrows()}
    print(f"Tickers: {len(tickers)}")

    # 2. Fetch quarterly (3 FMP calls per ticker = ~4,518 calls, ~6 min @ 750/min)
    print(f"Fetching quarterly financials…")
    all_data: dict[str, dict] = {}
    done = 0
    with ThreadPoolExecutor(max_workers=10) as ex:
        for fut in as_completed({ex.submit(_fetch_quarterly, t): t for t in tickers}):
            try:
                t = next(tt for f, tt in {fut: tt for _, tt in []}.items()) if False else None
            except Exception: pass
            try:
                result = fut.result()
            except Exception:
                result = {}
            # Figure out ticker from future→ticker mapping
            done += 1
            if done % 200 == 0:
                print(f"  {done}/{len(tickers)} fetched — elapsed {datetime.now() - started}", flush=True)
    # Redo — simpler:
    all_data = {}
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(_fetch_quarterly, t): t for t in tickers}
        done = 0
        for fut in as_completed(futures):
            t = futures[fut]
            try:
                all_data[t] = fut.result()
            except Exception:
                all_data[t] = {}
            done += 1
            if done % 300 == 0:
                print(f"  fetched {done}/{len(tickers)} — elapsed {datetime.now() - started}", flush=True)
    covered = sum(1 for d in all_data.values() if d)
    print(f"  quarterly data fetched for {covered}/{len(tickers)} tickers", flush=True)

    # 3. Build TTM snapshots → prompts
    from anthropic.types.messages.batch_create_params import Request
    requests_list = []
    custom_to_info: dict[str, tuple[str, str]] = {}
    for ticker in tickers:
        data = all_data.get(ticker, {})
        if not data:
            continue
        company, industry = meta.get(ticker, (ticker, ""))
        for qend in sorted(data.keys()):
            if qend < TARGET_START:
                continue
            snap = _build_ttm_snapshot(data, qend)
            if not snap or snap.get("revenue_ttm") is None:
                continue
            cid = f"{ticker}__{qend}"
            custom_to_info[cid] = (ticker, qend)
            prompt = _build_prompt(ticker, qend, snap, company, industry)
            requests_list.append({
                "custom_id": cid,
                "params": {
                    "model": MODEL,
                    "max_tokens": 200,
                    "system": _SYSTEM,
                    "messages": [{"role": "user", "content": prompt}],
                },
            })
    print(f"\nBuilt {len(requests_list):,} quarterly prompts", flush=True)
    if not requests_list:
        return

    # 4. Submit ONE big batch (the 8,865 batch worked; quarterly is larger but still fits)
    # If concerned, can chunk — but Anthropic ended the 8.8K batch successfully.
    print("Submitting Claude batch…", flush=True)
    batch_reqs = [Request(custom_id=r["custom_id"], params=r["params"]) for r in requests_list]
    batch = client.messages.batches.create(requests=batch_reqs)
    print(f"  batch id: {batch.id}", flush=True)

    # 5. Poll
    while True:
        b = client.messages.batches.retrieve(batch.id)
        rc = b.request_counts
        print(f"  status={b.processing_status} proc={rc.processing} ok={rc.succeeded} err={rc.errored}", flush=True)
        if b.processing_status == "ended":
            break
        time.sleep(30)

    # 6. Fetch + parse
    print("Fetching results…", flush=True)
    results = {}
    for line in client.messages.batches.results(batch.id):
        cid = line.custom_id
        if line.result.type != "succeeded":
            continue
        msg = line.result.message
        text_out = msg.content[0].text if msg.content else ""
        parsed = _parse_claude(text_out)
        if parsed and "score" in parsed:
            results[cid] = parsed
    print(f"  parsed {len(results):,}/{len(requests_list):,}", flush=True)

    # 7. Write to company_health_history
    rows = []
    for cid, parsed in results.items():
        ticker, qend = custom_to_info[cid]
        rows.append({
            "ticker": ticker,
            "score": int(parsed["score"]),
            "reason": parsed.get("reason", "")[:500],
            "recorded_at": f"{qend}T00:00:00+00:00",
        })
    print(f"Writing {len(rows):,} quarterly snapshots…", flush=True)
    with engine.begin() as conn:
        # Wipe previous historical snapshots (keep today's snapshot intact)
        conn.execute(text("""
            DELETE FROM company_health_history
            WHERE substr(recorded_at, 1, 4) IN ('2020','2021','2022','2023','2024','2025')
        """))
        for r in rows:
            conn.execute(text("""
                INSERT OR REPLACE INTO company_health_history (ticker, score, reason, recorded_at)
                VALUES (:ticker, :score, :reason, :recorded_at)
            """), r)
    print("Done.", flush=True)

    # 8. Summary
    import sqlite3
    c = sqlite3.connect("vesign.db")
    print("\nQuarter distribution:")
    for row in c.execute("SELECT substr(recorded_at,1,7), COUNT(*) FROM company_health_history GROUP BY 1 ORDER BY 1").fetchall():
        print(f"  {row[0]}: {row[1]:,}")
    print(f"\nElapsed: {datetime.now() - started}")


if __name__ == "__main__":
    main()
