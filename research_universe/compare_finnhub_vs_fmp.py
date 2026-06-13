#!/usr/bin/env python3
"""
compare_finnhub_vs_fmp.py — STANDALONE, READ-ONLY reliability probe.

Compares Finnhub (free API) field-by-field against the FMP/yfinance data already
sitting in vesign.db, to judge whether Finnhub could replace any of it BEFORE a
migration is attempted.

READ-ONLY GUARANTEES
  * Opens vesign.db with SELECT only (immutable, read-only URI). Writes nothing.
  * Touches no engine code, no pipeline, no live tables.
  * Network: only GETs to finnhub.io.
  * Outputs go ONLY to two new files under research_universe/.

WHAT IT CHECKS (per the brief)
  1. ANALYST TARGETS (priority)  Finnhub /stock/price-target  vs  analyst_expectations
  2. PRICE                       Finnhub /quote               vs  latest daily_prices close
  3. FUNDAMENTALS                Finnhub /stock/profile2 + /stock/metric  vs  fundamentals
  4. ENDPOINT ACCESS             which endpoints answer on the FREE key vs premium-locked
  Plus /stock/recommendation (free) as an analyst-coverage cross-check, since
  /stock/price-target is commonly premium-locked — that distinction is the whole point.

USAGE
    export FINNHUB_API_KEY=xxxx        # or put FINNHUB_API_KEY=xxxx in repo-root .env
    python3 research_universe/compare_finnhub_vs_fmp.py

Free-tier rate limit (~60 calls/min) is respected via a conservative pacer.
"""

import os
import sys
import csv
import json
import time
import sqlite3
import statistics
import urllib.request
import urllib.parse
import urllib.error
from pathlib import Path

# --------------------------------------------------------------------------- #
# Paths (resolve relative to this file so it runs from anywhere)
# --------------------------------------------------------------------------- #
HERE = Path(__file__).resolve().parent
REPO = HERE.parent
DB_PATH = REPO / "vesign.db"
OUT_DETAIL = HERE / "finnhub_vs_fmp.csv"
OUT_SUMMARY = HERE / "finnhub_vs_fmp_summary.csv"

FINNHUB_BASE = "https://finnhub.io/api/v1"

# Divergence thresholds from the brief
TH_PRICE_PCT = 5.0       # price % diff above this = material
TH_VALUE_PCT = 15.0      # targets / fundamentals % diff above this = material
TH_ANALYST_CNT = 2       # analyst-count diff above this = material


# --------------------------------------------------------------------------- #
# API key — env first (as requested), then a .env fallback for convenience
# --------------------------------------------------------------------------- #
def load_api_key():
    key = os.environ.get("FINNHUB_API_KEY", "").strip()
    if key:
        return key
    env_file = REPO / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line.startswith("FINNHUB_API_KEY"):
                _, _, v = line.partition("=")
                return v.strip().strip('"').strip("'")
    return ""


# --------------------------------------------------------------------------- #
# Conservative rate pacer: free tier is ~60/min. We hold ourselves to <= ~55/min
# by enforcing a minimum gap between calls, and we back off on HTTP 429.
# --------------------------------------------------------------------------- #
_MIN_GAP_S = 60.0 / 55.0
_last_call = [0.0]


def _pace():
    dt = time.monotonic() - _last_call[0]
    if dt < _MIN_GAP_S:
        time.sleep(_MIN_GAP_S - dt)
    _last_call[0] = time.monotonic()


def finnhub_get(path, params, key):
    """
    GET a Finnhub endpoint. Returns (data_or_None, access_flag).
      access_flag in {"ok", "premium", "empty", "error:<detail>"}
    Never raises; classifies failures so the report can describe free-tier reach.
    """
    params = dict(params)
    params["token"] = key
    url = f"{FINNHUB_BASE}{path}?{urllib.parse.urlencode(params)}"
    for attempt in range(4):
        _pace()
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "vesign-research/1.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = resp.read().decode("utf-8", "replace")
            data = json.loads(body) if body.strip() else None
            # Finnhub returns {} or {"s":"no_data"} or all-null dicts when it has nothing.
            if data in (None, {}, []):
                return None, "empty"
            return data, "ok"
        except urllib.error.HTTPError as e:
            detail = ""
            try:
                detail = e.read().decode("utf-8", "replace")[:200]
            except Exception:
                pass
            if e.code == 429:                       # rate limited — back off and retry
                time.sleep(2.0 * (attempt + 1))
                continue
            if e.code in (401, 403):                # premium / unauthorized endpoint
                return None, "premium"
            return None, f"error:HTTP{e.code} {detail[:80]}"
        except Exception as e:                      # noqa: BLE001 — classify, never crash
            time.sleep(1.0 * (attempt + 1))
            last = f"error:{type(e).__name__}"
    return None, last


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def pct_diff(finnhub_v, db_v):
    """Absolute % difference of finnhub vs the DB baseline. None if not comparable."""
    if finnhub_v is None or db_v is None:
        return None
    try:
        f = float(finnhub_v); d = float(db_v)
    except (TypeError, ValueError):
        return None
    if d == 0:
        return None
    return abs(f - d) / abs(d) * 100.0


def first_num(d, *keys):
    """Return the first present, finite numeric value among keys in dict d."""
    if not isinstance(d, dict):
        return None
    for k in keys:
        v = d.get(k)
        if v is None:
            continue
        try:
            fv = float(v)
        except (TypeError, ValueError):
            continue
        if fv == fv and fv not in (float("inf"), float("-inf")):  # not NaN/inf
            return fv
    return None


# --------------------------------------------------------------------------- #
# Sample selection — ~30 tickers spanning the cap/coverage range
# --------------------------------------------------------------------------- #
def pick_sample(con):
    rows = con.execute("""
        SELECT c.ticker,
               f.market_cap,
               ae.number_of_analysts,
               ae.target_mean_price
        FROM companies c
        JOIN fundamentals f ON f.ticker = c.ticker
        LEFT JOIN analyst_expectations ae ON ae.ticker = c.ticker
        WHERE f.market_cap IS NOT NULL AND f.market_cap > 0
          AND EXISTS (SELECT 1 FROM daily_prices dp
                      WHERE dp.ticker = c.ticker
                        AND dp.date = (SELECT MAX(date) FROM daily_prices))
        ORDER BY f.market_cap DESC
    """).fetchall()
    n = len(rows)
    large = [(r[0], "large") for r in rows[:10]]
    mid_start = max(0, n // 2 - 5)
    mid = [(r[0], "mid") for r in rows[mid_start:mid_start + 10]]
    # smallest / lowest-coverage: rank by analyst count (NULL -> 0), then smallest cap.
    by_cov = sorted(rows, key=lambda r: ((r[2] if r[2] is not None else 0), r[1]))
    small = [(r[0], "small/low-cov") for r in by_cov[:10]]
    # de-dupe while preserving bucket of first appearance
    seen, sample = set(), []
    for tk, bucket in large + mid + small:
        if tk not in seen:
            seen.add(tk); sample.append((tk, bucket))
    return sample, n


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main():
    key = load_api_key()
    if not key:
        sys.exit("ERROR: FINNHUB_API_KEY not set (export it, or add it to repo-root .env).")
    if not DB_PATH.exists():
        sys.exit(f"ERROR: {DB_PATH} not found.")

    # Read-only, immutable connection — physically cannot write.
    con = sqlite3.connect(f"file:{DB_PATH}?immutable=1", uri=True)

    sample, universe_n = pick_sample(con)
    latest_date = con.execute("SELECT MAX(date) FROM daily_prices").fetchone()[0]
    print(f"Universe size: {universe_n}   Sample: {len(sample)} tickers   "
          f"Price baseline date: {latest_date}\n")
    print(f"{'TICKER':<8}{'BUCKET':<14}{'finnhub calls':<14}status")

    detail_rows = []
    # accumulators for per-field summary
    acc = {
        "price_pct": [], "tgt_mean_pct": [], "tgt_high_pct": [], "tgt_low_pct": [],
        "analyst_cnt_diff": [], "mcap_pct": [], "pe_pct": [], "eps_pct": [],
        "roe_pct": [], "netmargin_pct": [],
    }
    coverage = {k: 0 for k in
                ["price", "target", "analyst_cnt", "mcap", "pe", "eps", "roe", "netmargin", "recommendation"]}
    endpoint_access = {  # endpoint -> Counter-ish dict of access flags
        "/quote": {}, "/stock/profile2": {}, "/stock/metric": {},
        "/stock/price-target": {}, "/stock/recommendation": {},
    }

    def bump(ep, flag):
        endpoint_access[ep][flag] = endpoint_access[ep].get(flag, 0) + 1

    for tk, bucket in sample:
        # ---- DB baselines ----
        db_close = con.execute(
            "SELECT close FROM daily_prices WHERE ticker=? ORDER BY date DESC LIMIT 1", (tk,)
        ).fetchone()
        db_close = db_close[0] if db_close else None

        db_ae = con.execute(
            "SELECT target_mean_price, target_high_price, target_low_price, number_of_analysts "
            "FROM analyst_expectations WHERE ticker=?", (tk,)
        ).fetchone() or (None, None, None, None)
        db_tmean, db_thigh, db_tlow, db_nan = db_ae

        db_f = con.execute(
            "SELECT market_cap, pe_ttm, eps_ttm, roe, net_margin FROM fundamentals WHERE ticker=?", (tk,)
        ).fetchone() or (None, None, None, None, None)
        db_mcap, db_pe, db_eps, db_roe, db_nm = db_f

        # ---- Finnhub pulls ----
        quote, f_quote = finnhub_get("/quote", {"symbol": tk}, key);            bump("/quote", f_quote)
        prof, f_prof = finnhub_get("/stock/profile2", {"symbol": tk}, key);     bump("/stock/profile2", f_prof)
        metric, f_metric = finnhub_get("/stock/metric", {"symbol": tk, "metric": "all"}, key)
        bump("/stock/metric", f_metric)
        ptgt, f_ptgt = finnhub_get("/stock/price-target", {"symbol": tk}, key); bump("/stock/price-target", f_ptgt)
        recs, f_recs = finnhub_get("/stock/recommendation", {"symbol": tk}, key)
        bump("/stock/recommendation", f_recs)

        calls = 5
        print(f"{tk:<8}{bucket:<14}{calls:<14}q={f_quote} p={f_prof} m={f_metric} "
              f"pt={f_ptgt} rec={f_recs}")

        # ---- PRICE ----
        fh_price = first_num(quote, "c") if quote else None
        # Finnhub returns c==0 for symbols it cannot quote — treat 0 as no data.
        if fh_price == 0:
            fh_price = None
        d_price = pct_diff(fh_price, db_close)
        if d_price is not None:
            acc["price_pct"].append(d_price); coverage["price"] += 1

        # ---- ANALYST TARGETS (priority) ----
        fh_tmean = first_num(ptgt, "targetMean") if ptgt else None
        fh_thigh = first_num(ptgt, "targetHigh") if ptgt else None
        fh_tlow = first_num(ptgt, "targetLow") if ptgt else None
        fh_ptgt_n = first_num(ptgt, "numberOfAnalysts", "numberAnalysts", "lastUpdatedAnalysts") if ptgt else None
        d_tmean = pct_diff(fh_tmean, db_tmean)
        d_thigh = pct_diff(fh_thigh, db_thigh)
        d_tlow = pct_diff(fh_tlow, db_tlow)
        if d_tmean is not None:
            acc["tgt_mean_pct"].append(d_tmean); coverage["target"] += 1
        if d_thigh is not None:
            acc["tgt_high_pct"].append(d_thigh)
        if d_tlow is not None:
            acc["tgt_low_pct"].append(d_tlow)

        # analyst-count cross-check via free recommendation-trends (sum of latest period)
        fh_rec_n = None
        if isinstance(recs, list) and recs:
            latest = recs[0]
            fh_rec_n = sum(first_num(latest, k) or 0 for k in
                           ("strongBuy", "buy", "hold", "sell", "strongSell")) or None
            coverage["recommendation"] += 1
        # prefer price-target's own count if present, else the recommendation sum
        fh_nan = fh_ptgt_n if fh_ptgt_n is not None else fh_rec_n
        d_nan = (abs(fh_nan - db_nan) if (fh_nan is not None and db_nan is not None) else None)
        if d_nan is not None:
            acc["analyst_cnt_diff"].append(d_nan); coverage["analyst_cnt"] += 1

        # ---- FUNDAMENTALS ----
        # profile2.marketCapitalization is in MILLIONS USD -> *1e6 to match DB absolute USD
        fh_mcap_m = first_num(prof, "marketCapitalization") if prof else None
        fh_mcap = fh_mcap_m * 1e6 if fh_mcap_m is not None else None
        d_mcap = pct_diff(fh_mcap, db_mcap)
        if d_mcap is not None:
            acc["mcap_pct"].append(d_mcap); coverage["mcap"] += 1

        m = metric.get("metric") if isinstance(metric, dict) else None
        fh_pe = first_num(m, "peTTM", "peBasicExclExtraTTM", "peInclExtraTTM") if m else None
        fh_eps = first_num(m, "epsTTM", "epsInclExtraItemsTTM", "epsBasicExclExtraItemsTTM") if m else None
        fh_roe_pct = first_num(m, "roeTTM", "roeRfy") if m else None          # Finnhub = percent
        fh_nm_pct = first_num(m, "netProfitMarginTTM", "netMarginTTM") if m else None  # percent
        # DB roe / net_margin are FRACTIONS -> convert to percent for apples-to-apples
        db_roe_pct = db_roe * 100 if db_roe is not None else None
        db_nm_pct = db_nm * 100 if db_nm is not None else None
        d_pe = pct_diff(fh_pe, db_pe)
        d_eps = pct_diff(fh_eps, db_eps)
        d_roe = pct_diff(fh_roe_pct, db_roe_pct)
        d_nm = pct_diff(fh_nm_pct, db_nm_pct)
        for val, key_acc, cov_key in [
            (d_pe, "pe_pct", "pe"), (d_eps, "eps_pct", "eps"),
            (d_roe, "roe_pct", "roe"), (d_nm, "netmargin_pct", "netmargin"),
        ]:
            if val is not None:
                acc[key_acc].append(val); coverage[cov_key] += 1

        detail_rows.append({
            "ticker": tk, "bucket": bucket,
            # price
            "db_close": db_close, "fh_price": fh_price, "price_pct_diff": d_price,
            # targets
            "db_target_mean": db_tmean, "fh_target_mean": fh_tmean, "target_mean_pct_diff": d_tmean,
            "db_target_high": db_thigh, "fh_target_high": fh_thigh, "target_high_pct_diff": d_thigh,
            "db_target_low": db_tlow, "fh_target_low": fh_tlow, "target_low_pct_diff": d_tlow,
            "db_num_analysts": db_nan, "fh_num_analysts": fh_nan,
            "fh_analyst_src": ("price-target" if fh_ptgt_n is not None
                               else "recommendation" if fh_rec_n is not None else ""),
            "analyst_cnt_diff": d_nan,
            # fundamentals
            "db_market_cap": db_mcap, "fh_market_cap": fh_mcap, "market_cap_pct_diff": d_mcap,
            "db_pe_ttm": db_pe, "fh_pe_ttm": fh_pe, "pe_pct_diff": d_pe,
            "db_eps_ttm": db_eps, "fh_eps_ttm": fh_eps, "eps_pct_diff": d_eps,
            "db_roe_pct": db_roe_pct, "fh_roe_pct": fh_roe_pct, "roe_pct_diff": d_roe,
            "db_net_margin_pct": db_nm_pct, "fh_net_margin_pct": fh_nm_pct, "net_margin_pct_diff": d_nm,
            # access flags
            "acc_quote": f_quote, "acc_profile2": f_prof, "acc_metric": f_metric,
            "acc_price_target": f_ptgt, "acc_recommendation": f_recs,
        })

    con.close()

    # ----------------------------------------------------------------------- #
    # Per-ticker CSV
    # ----------------------------------------------------------------------- #
    if detail_rows:
        with open(OUT_DETAIL, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(detail_rows[0].keys()))
            w.writeheader()
            w.writerows(detail_rows)

    # ----------------------------------------------------------------------- #
    # Per-field summary
    # ----------------------------------------------------------------------- #
    N = len(sample)

    def med(xs):
        return statistics.median(xs) if xs else None

    def p90(xs):
        if not xs:
            return None
        s = sorted(xs)
        i = min(len(s) - 1, int(round(0.9 * (len(s) - 1))))
        return s[i]

    FIELDS = [
        # (label, acc_key, coverage_key, threshold, kind)
        ("Analyst target mean", "tgt_mean_pct", "target", TH_VALUE_PCT, "pct"),
        ("Analyst target high", "tgt_high_pct", "target", TH_VALUE_PCT, "pct"),
        ("Analyst target low", "tgt_low_pct", "target", TH_VALUE_PCT, "pct"),
        ("Analyst count", "analyst_cnt_diff", "analyst_cnt", TH_ANALYST_CNT, "count"),
        ("Price (last close)", "price_pct", "price", TH_PRICE_PCT, "pct"),
        ("Market cap", "mcap_pct", "mcap", TH_VALUE_PCT, "pct"),
        ("P/E (TTM)", "pe_pct", "pe", TH_VALUE_PCT, "pct"),
        ("EPS (TTM)", "eps_pct", "eps", TH_VALUE_PCT, "pct"),
        ("ROE", "roe_pct", "roe", TH_VALUE_PCT, "pct"),
        ("Net margin", "netmargin_pct", "netmargin", TH_VALUE_PCT, "pct"),
    ]

    def verdict(label, acc_key, cov_key, thr, kind, ep_flag_for_field):
        cov = coverage.get(cov_key, 0)
        xs = acc[acc_key]
        # If the underlying endpoint was premium-locked across the board, say so.
        if ep_flag_for_field == "premium" and cov == 0:
            return "not accessible on free tier"
        if cov == 0:
            return "no overlapping data"
        m, p = med(xs), p90(xs)
        if kind == "count":
            if m is not None and m <= thr:
                return "matches closely"
            return "materially different" if (m or 0) > 2 * thr else "minor drift"
        # pct kind
        if m is None:
            return "no overlapping data"
        if m <= thr * 0.34:
            return "matches closely"
        if m <= thr:
            return "minor drift"
        return "materially different"

    # which endpoint backs each field (for the premium verdict)
    def ep_consensus(ep):
        flags = endpoint_access.get(ep, {})
        if not flags:
            return "n/a"
        # premium if it never returned ok
        if flags.get("ok", 0) == 0 and flags.get("premium", 0) > 0:
            return "premium"
        return "ok" if flags.get("ok", 0) else "empty"

    field_backing_ep = {
        "Analyst target mean": "/stock/price-target",
        "Analyst target high": "/stock/price-target",
        "Analyst target low": "/stock/price-target",
        "Analyst count": "/stock/price-target",
        "Price (last close)": "/quote",
        "Market cap": "/stock/profile2",
        "P/E (TTM)": "/stock/metric",
        "EPS (TTM)": "/stock/metric",
        "ROE": "/stock/metric",
        "Net margin": "/stock/metric",
    }

    print("\n" + "=" * 92)
    print("PER-FIELD SUMMARY  (diff = Finnhub vs vesign.db baseline)")
    print("=" * 92)
    hdr = f"{'FIELD':<22}{'coverage':>10}{'median%':>10}{'p90%':>10}{'material?':>12}  verdict"
    print(hdr)
    print("-" * 92)

    summary_rows = []
    for label, acc_key, cov_key, thr, kind in FIELDS:
        cov = coverage.get(cov_key, 0)
        xs = acc[acc_key]
        m, p = med(xs), p90(xs)
        ep_flag = ep_consensus(field_backing_ep[label])
        vd = verdict(label, acc_key, cov_key, thr, kind, ep_flag)
        # material flag count
        if kind == "count":
            material = sum(1 for x in xs if x > thr)
            mtxt = "—" if not xs else f"{material}/{len(xs)}"
            mfmt = "" if m is None else f"{m:.1f}"
            pfmt = "" if p is None else f"{p:.1f}"
        else:
            material = sum(1 for x in xs if x > thr)
            mtxt = "—" if not xs else f"{material}/{len(xs)}"
            mfmt = "" if m is None else f"{m:.1f}"
            pfmt = "" if p is None else f"{p:.1f}"
        cov_txt = f"{cov}/{N}"
        print(f"{label:<22}{cov_txt:>10}{mfmt:>10}{pfmt:>10}{mtxt:>12}  {vd}")
        summary_rows.append({
            "field": label, "coverage_n": cov, "coverage_pct": round(100 * cov / N, 1),
            "median_abs_diff": None if m is None else round(m, 3),
            "p90_abs_diff": None if p is None else round(p, 3),
            "unit": "count" if kind == "count" else "pct",
            "material_threshold": thr, "n_material": material, "n_compared": len(xs),
            "backing_endpoint": field_backing_ep[label],
            "endpoint_access": ep_flag, "verdict": vd,
        })

    # ----------------------------------------------------------------------- #
    # Endpoint access report
    # ----------------------------------------------------------------------- #
    print("\n" + "=" * 92)
    print("ENDPOINT ACCESS ON THE FREE KEY")
    print("=" * 92)
    for ep, flags in endpoint_access.items():
        flagstr = ", ".join(f"{k}={v}" for k, v in sorted(flags.items()))
        consensus = ep_consensus(ep)
        verdict_ep = {
            "ok": "ACCESSIBLE on free tier",
            "premium": "PREMIUM-LOCKED (not on free tier)",
            "empty": "reachable but returned no data",
            "n/a": "not called",
        }.get(consensus, consensus)
        print(f"  {ep:<26} {verdict_ep:<34} [{flagstr}]")

    # ----------------------------------------------------------------------- #
    # Headline call-out on ANALYST TARGETS — the core-signal risk
    # ----------------------------------------------------------------------- #
    print("\n" + "=" * 92)
    print("ANALYST-TARGET VERDICT  (core-signal risk)")
    print("=" * 92)
    pt_access = ep_consensus("/stock/price-target")
    rec_access = ep_consensus("/stock/recommendation")
    tgt_cov = coverage["target"]
    cnt_cov = coverage["analyst_cnt"]
    if pt_access == "premium" and tgt_cov == 0:
        print("  Finnhub /stock/price-target is PREMIUM-LOCKED on the free key — the actual")
        print("  price targets that drive the signal CANNOT be validated (or sourced) for free.")
        if rec_access == "ok":
            print(f"  However /stock/recommendation IS free and returned analyst coverage for")
            print(f"  {coverage['recommendation']}/{N} sampled tickers, so Finnhub DOES track")
            print("  analyst breadth — just not the numeric targets without the paid plan.")
        print("  => Migrating analyst TARGETS to Finnhub requires the PAID plan. Free tier")
        print("     can only cross-check analyst *count/coverage*, not target prices.")
    elif tgt_cov > 0:
        mm = med(acc["tgt_mean_pct"])
        print(f"  Finnhub returned price targets for {tgt_cov}/{N} sampled tickers; median mean-target")
        print(f"  diff vs DB = {mm:.1f}%. Analyst-count comparable for {cnt_cov}/{N}.")
        print("  => Targets ARE accessible — see the per-field verdict above for closeness.")
    else:
        print("  No analyst-target overlap could be established (endpoint empty or errored).")

    # ----------------------------------------------------------------------- #
    # Write summary CSV
    # ----------------------------------------------------------------------- #
    if summary_rows:
        with open(OUT_SUMMARY, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(summary_rows[0].keys()))
            w.writeheader()
            w.writerows(summary_rows)

    print(f"\nWrote per-ticker detail : {OUT_DETAIL}")
    print(f"Wrote per-field summary : {OUT_SUMMARY}")
    print("READ-ONLY run complete — vesign.db and live system untouched.")


if __name__ == "__main__":
    main()
