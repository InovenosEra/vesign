"""Pure, IO-free helpers for the live-price snapshot.

The stateful caches (TTL, single-flight, phase-flush) live in backend.main
alongside the other market caches; these helpers hold only the math so they
can be unit-tested without a DB or network."""


def mid_price(quote: dict) -> "float | None":
    """Mid of a batch-aftermarket-quote row's bid/ask. None if either side is
    missing or non-positive (the ticker then falls back to prev_close)."""
    bid = quote.get("bidPrice")
    ask = quote.get("askPrice")
    if not bid or not ask or bid <= 0 or ask <= 0:
        return None
    return round((bid + ask) / 2, 4)


def compute_universe_rows(snapshot: dict, baseline: dict) -> list:
    """One row per ticker in `baseline`, merging the live snapshot price.

    price      = snapshot[t] if present and truthy, else baseline prev_close
    change_pct = (price - prev_close) / prev_close * 100, or None if prev_close
                 is falsy. A ticker absent from the snapshot -> 0% move.
    All baseline metadata (sector, company, market_cap, hi52, lo52, ...) is
    spread onto the row."""
    rows = []
    for ticker, meta in baseline.items():
        prev = meta.get("prev_close")
        live = snapshot.get(ticker)
        price = live if live else prev
        if prev:
            change = round((price - prev) / prev * 100, 4)
        else:
            change = None
        rows.append({"ticker": ticker, "price": price, "change_pct": change, **meta})
    return rows


def last_session_rows(baseline: dict) -> list:
    """One row per ticker showing the LAST COMPLETED session's move — used when
    the market is idle (closed overnight) and there are no live prices.

    price      = prev_close (the latest completed-session close)
    change_pct = (prev_close - prior_close) / prior_close * 100, or None if
                 prior_close is falsy (a ticker with only one session of history).
    Same row shape as compute_universe_rows so every panel reads it identically."""
    rows = []
    for ticker, meta in baseline.items():
        prev = meta.get("prev_close")
        prior = meta.get("prior_close")
        if prior:
            change = round((prev - prior) / prior * 100, 4)
        else:
            change = None
        rows.append({"ticker": ticker, "price": prev, "change_pct": change, **meta})
    return rows


def day_change_pct(latest_close, prior_close, live_price=None) -> "float | None":
    """1-day % price move, anchored the same way as the market panels.

    With a live price it's the intraday move vs the latest completed-session
    close (NOT the prior close — anchoring on the session-before-last would span
    two days and could flip the sign vs everywhere else). Without a live price
    it's the latest close vs the prior close (the last completed session). None
    when the prices needed for the chosen path are missing or zero."""
    if live_price and latest_close:
        return round((live_price - latest_close) / latest_close * 100, 4)
    if latest_close and prior_close:
        return round((latest_close - prior_close) / prior_close * 100, 4)
    return None
