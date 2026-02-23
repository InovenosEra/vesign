import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine, inspect
from datetime import datetime, time as dt_time, UTC
import pytz
import time
import os

# ------------------------------
# SAFETY CHECK - DO NOT RUN PIPELINE HERE
# ------------------------------

DB_PATH = "vesign.db"

if not os.path.exists(DB_PATH):
    st.error(
        "Database not found.\n\n"
        "Please run production/run_daily.py first to generate signals."
    )
    st.stop()

# ------------------------------

if "signal_filter" not in st.session_state:
    st.session_state.signal_filter = "ALL"

st.set_page_config(layout="wide")

st.markdown(
    """
    <style>
        .block-container {
            padding-top: 5rem;
        }
        [data-testid="stToolbar"],
        [data-testid="stStatusWidget"],
        [data-testid="stDeployButton"],
        [data-testid="stAppDeployButton"],
        [data-testid="stHeader"] {
            display: none !important;
        }
    </style>
    """,
    unsafe_allow_html=True
)

engine = create_engine(f"sqlite:///{DB_PATH}")

# Wait until signals table exists (max ~30 seconds)
for _ in range(6):
    try:
        pd.read_sql("SELECT 1 FROM signals LIMIT 1", engine)
        break
    except Exception:
        time.sleep(5)
else:
    st.error("Signals table not found. Run production pipeline first.")
    st.stop()

if "last_refresh" not in st.session_state:
    et = pytz.timezone("US/Eastern")
    st.session_state.last_refresh = datetime.now(UTC).astimezone(et).strftime("%H:%M:%S ET")

st.markdown(f"""
<style>
  #sticky-header {{
    position: fixed;
    top: 0;
    left: 0;
    right: 0;
    z-index: 1000000;
    display: flex;
    justify-content: space-between;
    align-items: center;
    background-color: #0e1117;
    border-bottom: 1px solid #262730;
    padding: 10px 24px;
    font-family: sans-serif;
  }}
  #sticky-header h1 {{
    margin: 0;
    font-size: 2.6rem;
    color: #fafafa;
    font-weight: 700;
  }}
  #sticky-header .refresh-label {{
    color: #aaaaaa;
    font-size: 0.82rem;
    text-align: right;
  }}
  #sticky-header #market-status {{
    font-size: 1.1rem;
    font-weight: 700;
    text-align: right;
    margin-top: 2px;
  }}
</style>
<div id="sticky-header">
  <h1>Vesign Trading System</h1>
  <div>
    <div class="refresh-label">Last Update: <span id="last-update-time"></span></div>
    <div id="market-status"></div>
  </div>
</div>
""", unsafe_allow_html=True)

components.html("""
<script>
(function() {
  function getETDate() {
    const s = new Date().toLocaleString("en-US", {timeZone: "America/New_York"});
    return new Date(s);
  }
  function getNextOpen(et) {
    const h = et.getHours(), m = et.getMinutes(), day = et.getDay();
    const isWeekday = day >= 1 && day <= 5;
    const beforeOpen = h < 9 || (h === 9 && m < 30);
    let next = new Date(et);
    if (isWeekday && beforeOpen) {
      next.setHours(9, 30, 0, 0);
    } else {
      next.setDate(next.getDate() + 1);
      next.setHours(9, 30, 0, 0);
      while (next.getDay() === 0 || next.getDay() === 6)
        next.setDate(next.getDate() + 1);
    }
    return next;
  }
  function tick() {
    const et = getETDate();
    const h = et.getHours(), m = et.getMinutes(), day = et.getDay();
    const isWeekday = day >= 1 && day <= 5;
    const isOpen = isWeekday && (h > 9 || (h === 9 && m >= 30)) && h < 16;
    const el = window.parent.document.getElementById("market-status");
    if (!el) return;
    if (isOpen) {
      el.innerHTML = "🟢 Market Open";
      el.style.color = "#2ecc71";
    } else {
      const next = getNextOpen(et);
      const diff = Math.max(0, Math.floor((next - et) / 1000));
      const dh = Math.floor(diff / 3600);
      const dm = Math.floor((diff % 3600) / 60);
      const ds = diff % 60;
      const p = n => String(n).padStart(2, "0");
      el.innerHTML = "🔴 Opens in " + p(dh) + ":" + p(dm) + ":" + p(ds);
      el.style.color = "#e74c3c";
    }
  }
  tick();
  setInterval(tick, 1000);

  // Set last update time in user's local timezone
  const timeEl = window.parent.document.getElementById("last-update-time");
  if (timeEl) {
    timeEl.textContent = new Date().toLocaleTimeString([], {hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false});
  }

})();
</script>
""", height=0)

if "search_input" not in st.session_state:
    st.session_state.search_input = ""

def clear_search():
    st.session_state.search_input = ""

search_col, clear_col, _ = st.columns([2, 1, 8])

with search_col:
    search = st.text_input("Search Company or Ticker", key="search_input")

with clear_col:
    st.markdown("<div style='margin-top: 28px'>", unsafe_allow_html=True)
    st.button("Clear", on_click=clear_search)
    st.markdown("</div>", unsafe_allow_html=True)

# ---------- Styling ----------
def style_variance(val):
    if isinstance(val, str):
        if "▲" in val:
            return "color: green"
        elif "▼" in val:
            return "color: red"
    return ""

# ---------- Market helpers ----------
def market_is_open():
    et = pytz.timezone("US/Eastern")
    now = datetime.now(UTC).astimezone(et)
    return now.weekday() < 5 and dt_time(9, 30) <= now.time() <= dt_time(16, 0)


def add_live_price(df, allowed_tickers=None):

    # ---------- 1. empty dataframe ----------
    if df.empty:
        df["Live Price"] = "-"
        return df

    # ---------- 2. market closed ----------
    if not market_is_open():
        df["Live Price"] = "Market is closed"
        return df

    # ---------- 3. get tickers safely ----------
    tickers = df["ticker"].dropna().unique().tolist()
    if allowed_tickers is not None:
        tickers = [t for t in tickers if t in allowed_tickers]

    if len(tickers) == 0:
        df["Live Price"] = "-"
        return df

    # ---------- 4. download safely ----------
    prices = {}

    for t in tickers:
        try:
            hist = yf.Ticker(t).history(period="1d", interval="1m")
            if hist is None or hist.empty:
                prices[t] = None
            else:
                prices[t] = float(hist["Close"].dropna().iloc[-1])
        except Exception:
            prices[t] = None
    df["Live Price"] = df["ticker"].map(prices)
    return df


def add_live_variance(df):
    if df.empty or "Live Price" not in df.columns:
        df["Live Variance"] = "-"
        return df

    if isinstance(df["Live Price"].iloc[0], str):
        df["Live Variance"] = "-"
        return df

    df["Live Price"] = pd.to_numeric(df["Live Price"], errors="coerce")

    df["price_diff"] = df["Live Price"] - df["close"]
    df["pct_diff"] = (df["price_diff"] / df["close"]) * 100

    def format_var(row):
        if pd.isna(row["price_diff"]):
            return "-"
        arrow = "▲" if row["price_diff"] > 0 else "▼"
        return f"{arrow} {row['price_diff']:.2f} ({row['pct_diff']:.2f}%)"

    df["Live Variance"] = df.apply(format_var, axis=1)
    df.drop(columns=["price_diff", "pct_diff"], inplace=True)

    return df


@st.cache_data(ttl=3600)
def fetch_market_caps(tickers):
    ticker_objs = yf.Tickers(" ".join(tickers))
    caps = {}
    for t in tickers:
        try:
            caps[t] = ticker_objs.tickers[t].info.get("marketCap")
        except Exception:
            caps[t] = None
    return caps


def apply_signal_filter(df):
    if st.session_state.signal_filter != "ALL":
        df = df[df["signal"] == st.session_state.signal_filter]
    return df


def add_market_cap(df):
    caps = pd.read_sql("""
        SELECT ticker, MAX(market_cap) AS market_cap
        FROM fundamentals
        GROUP BY ticker
    """, engine)

    df = df.merge(caps, on="ticker", how="left")
    df["market_cap"] = df["market_cap"] / 1_000_000_000
    return df


def apply_search(df):
    if search:
        mask = (
            df["ticker"].str.contains(search, case=False, na=False) |
            df["company"].str.contains(search, case=False, na=False)
        )
        df = df[mask]
    return df


def format_dates(df):
    date_cols = [c for c in df.columns if "date" in c.lower()]
    for col in date_cols:
        try:
            df[col] = pd.to_datetime(df[col]).dt.strftime("%d/%m/%y")
        except Exception:
            pass
    return df


HIDDEN_COLUMNS = {"open", "high", "low", "Adj Close", "volume",
                  "bb_high", "bb_low", "macd", "rsi_factor", "macd_factor", "trend_factor", "bb_factor",
                  "prediction_score",
                  "number_of_analysts", "last_update", "analyst_condition",
                  "bb_condition", "rsi_below_30", "rsi_3day_flag",
                  "volume_sma_20", "week52_high",
                  "pct_from_52w_high", "bb_pct_b", "week52_condition", "volume_flag", "volume_ratio", "score"}

def reorder_columns(df):
    df = df.drop(columns=[c for c in HIDDEN_COLUMNS if c in df.columns])
    cols = list(df.columns)
    priority = []
    for col in ["date", "company", "logo_url", "ticker", "market_cap", "close"]:
        if col in cols:
            priority.append(col)
    remaining = [c for c in cols if c not in priority]
    return df[priority + remaining]


# ------------------------------
# DISPLAY FUNCTION
# ------------------------------

def display_section(title, query, show_live=True, allowed_tickers=None):

    df = pd.read_sql(query, engine)

    if "ticker" in df.columns and "date" in df.columns:
        df = (
            df.sort_values("date", ascending=False)
              .drop_duplicates(subset=["ticker", "date"], keep="first")
        )

    df = apply_search(df)

    if title == "Signals":
        header_col, spacer, control_col = st.columns([8, 1, 2])

        with header_col:
            df = apply_signal_filter(df)
            st.header(f"Last {len(df):,} {title}")

        with control_col:
            st.radio(
                "Signal filter",
                ["ALL", "BUY", "HOLD", "SELL"],
                horizontal=True,
                key="signal_filter",
                label_visibility="collapsed"
            )

        df = apply_signal_filter(df)
    else:
        st.header(f"{title} ({len(df):,})")

    df = add_market_cap(df)

    if "market_cap" in df.columns:
        df = df.sort_values("market_cap", ascending=False, na_position="last")

    if show_live and "close" in df.columns:
        df = add_live_price(df, allowed_tickers=allowed_tickers)
        df = add_live_variance(df)

    if "fair_value_upside" in df.columns:
        df["fair_value_upside"] = df["fair_value_upside"] * 100

    # Sorting by market_cap is already done; convert to formatted string now
    # so large numbers show with comma separators (e.g. 4,576.75).
    if "market_cap" in df.columns:
        df["market_cap"] = df["market_cap"].apply(
            lambda x: f"{x:,.2f}" if pd.notna(x) else "-"
        )

    df = format_dates(df)
    df = reorder_columns(df)

    LABEL_OVERRIDES = {"rsi": "RSI", "fair_value_upside": "Potential Upside", "target_mean_price": "Base Price", "target_high_price": "High Price", "target_low_price": "Low Price"}

    column_config = {}
    for col in df.columns:
        label = LABEL_OVERRIDES.get(col, col.replace("_", " ").title())
        if col == "logo_url":
            column_config[col] = st.column_config.ImageColumn(label="Logo", width="small")
        elif col == "fair_value_upside":
            column_config[col] = st.column_config.NumberColumn(label=label, format="%.2f%%")
        elif pd.api.types.is_numeric_dtype(df[col]):
            column_config[col] = st.column_config.NumberColumn(label=label, format="%.2f")
        else:
            column_config[col] = st.column_config.Column(label=label)

    if "Live Variance" in df.columns:
        styled = df.style.map(style_variance, subset=["Live Variance"])
        st.dataframe(styled, use_container_width=True, hide_index=True, column_config=column_config)
    else:
        st.dataframe(df, use_container_width=True, hide_index=True, column_config=column_config)


# ------------------------------
# SECTIONS
# ------------------------------

@st.fragment(run_every="2m")
def live_signals():
    components.html("""
    <script>
    const timeEl = window.parent.document.getElementById("last-update-time");
    if (timeEl) {
      timeEl.textContent = new Date().toLocaleTimeString([], {hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false});
    }
    </script>
    """, height=0)

    display_section(
        "Today's BUY signals",
        """
        SELECT s.*, c.company, c.logo_url
        FROM signals s
        LEFT JOIN companies c
        ON s.ticker = c.ticker
        WHERE DATE (s.date) = (
            SELECT DATE(MAX(date)) FROM signals
        )
        AND s.signal = 'BUY'
        """
    )

    display_section(
        "Today's SELL signals",
        """
        SELECT s.*, c.company, c.logo_url
        FROM signals s
        LEFT JOIN companies c
        ON s.ticker = c.ticker
        WHERE DATE(s.date) = (
            SELECT DATE(MAX(date)) FROM signals
        )
        AND s.signal = 'SELL'
        """
    )

live_signals()

display_section(
    "Signals",
    """
    SELECT s.*, c.company, c.logo_url
    FROM signals s
    LEFT JOIN companies c
    ON s.ticker = c.ticker
    WHERE DATE(s.date) >= DATE('now', '-12 months')
    ORDER BY s.date DESC
    """,
    show_live=False
)

inspector = inspect(engine)

if "signal_success_by_company" in inspector.get_table_names():
    display_section(
        "BUY→SELL Success Rate by Company (12M)",
        """
        SELECT s.*, c.company, c.logo_url
        FROM signal_success_by_company s
        LEFT JOIN companies c
        ON s.ticker = c.ticker
        ORDER BY success_rate DESC
        """
    )

# ------------------------------
# HISTORICAL TRANSACTIONS
# ------------------------------

st.header("Historical Transactions")

min_date = pd.read_sql("SELECT DATE(MIN(date)) as d FROM signals", engine)["d"][0]
max_date = pd.read_sql("SELECT DATE(MAX(date)) as d FROM signals", engine)["d"][0]

date_col1, date_col2, _ = st.columns([2, 2, 7])
with date_col1:
    start_filter = st.date_input("From", value=pd.Timestamp.today() - pd.DateOffset(years=1),
                                 min_value=pd.Timestamp(min_date), max_value=pd.Timestamp(max_date),
                                 key="tx_start")
with date_col2:
    end_filter = st.date_input("To", value=pd.Timestamp(max_date),
                               min_value=pd.Timestamp(min_date), max_value=pd.Timestamp(max_date),
                               key="tx_end")

signals_df = pd.read_sql(f"""
    SELECT s.date, s.ticker, s.signal, s.close, c.company, c.logo_url
    FROM signals s
    LEFT JOIN companies c ON s.ticker = c.ticker
    WHERE DATE(s.date) BETWEEN '{start_filter}' AND '{end_filter}'
    ORDER BY s.ticker, s.date
""", engine)

signals_df["date"] = pd.to_datetime(signals_df["date"])

trades = []
for ticker, grp in signals_df.groupby("ticker"):
    open_trade = None
    for _, row in grp.iterrows():
        if row["signal"] == "BUY" and open_trade is None:
            open_trade = row
        elif row["signal"] == "SELL" and open_trade is not None:
            ret = (row["close"] - open_trade["close"]) / open_trade["close"]
            trades.append({
                "company":    open_trade["company"],
                "logo_url":   open_trade["logo_url"],
                "ticker":     ticker,
                "buy_date":   open_trade["date"],
                "sell_date":  row["date"],
                "buy_price":  open_trade["close"],
                "sell_price": row["close"],
                "return_pct": ret * 100,
                "days_held":  (row["date"] - open_trade["date"]).days,
                "result":     "Win" if ret > 0 else "Loss",
            })
            open_trade = None

trades_df = pd.DataFrame(trades)

if trades_df.empty:
    st.info("No completed BUY→SELL trades in the selected period.")
else:
    wins  = (trades_df["return_pct"] > 0).sum()
    total = len(trades_df)
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Trades",        total)
    m2.metric("Win Rate",      f"{wins/total:.1%}")
    m3.metric("Avg Return",    f"{trades_df['return_pct'].mean():+.2f}%")
    m4.metric("Avg Days Held", f"{trades_df['days_held'].mean():.0f}")

    trades_df = trades_df.sort_values("return_pct", ascending=False)

    trades_df["buy_date"]  = trades_df["buy_date"].dt.strftime("%d/%m/%y")
    trades_df["sell_date"] = trades_df["sell_date"].dt.strftime("%d/%m/%y")
    trades_df["buy_price"]  = trades_df["buy_price"].apply(lambda x: f"{x:,.2f}" if pd.notna(x) else "-")
    trades_df["sell_price"] = trades_df["sell_price"].apply(lambda x: f"{x:,.2f}" if pd.notna(x) else "-")
    trades_df["return_pct"] = trades_df["return_pct"].apply(
        lambda x: f"▲ {x:.2f}%" if x > 0 else f"▼ {x:.2f}%"
    )

    tx_col_cfg = {}
    for col in trades_df.columns:
        label = col.replace("_", " ").title()
        if col == "logo_url":
            tx_col_cfg[col] = st.column_config.ImageColumn(label="Logo", width="small")
        elif col == "return_pct":
            tx_col_cfg[col] = st.column_config.TextColumn(label="% Yield")
        elif col in ("buy_price", "sell_price"):
            tx_col_cfg[col] = st.column_config.TextColumn(label=label)
        elif col == "rsi":
            tx_col_cfg[col] = st.column_config.NumberColumn(label="RSI", format="%.2f")
        else:
            tx_col_cfg[col] = st.column_config.Column(label=label)

    styled_trades = trades_df.style.map(style_variance, subset=["return_pct"])
    st.dataframe(styled_trades, use_container_width=True, hide_index=True, column_config=tx_col_cfg)
