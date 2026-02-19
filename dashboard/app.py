import streamlit as st
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
            padding-top: 1rem;
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

st.title("Vesign Trading System")

search_col, _ = st.columns([2, 9])

with search_col:
    search = st.text_input("Search Company or Ticker")

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


def add_live_price(df):

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

    if len(tickers) == 0:
        df["Live Price"] = "-"
        return df

    # ---------- 4. download safely ----------
    prices = {}

    for t in tickers:
        try:
            live_data = yf.download(
                t,
                period="1d",
                interval="1m",
                progress=False
            )

            # If yfinance returns empty dataframe
            if live_data is None or live_data.empty:
                prices[t] = None
            else:
                prices[t] = live_data["close"].dropna().iloc[-1]

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


# ------------------------------
# DISPLAY FUNCTION
# ------------------------------

def display_section(title, query):

    df = pd.read_sql(query, engine)

    if "ticker" in df.columns and "date" in df.columns:
        df = (
            df.sort_values("date", ascending=False)
              .drop_duplicates(subset=["ticker", "date"], keep="first")
        )

    df = apply_search(df)

    if title == "Today's BUY signals" and "rank" in df.columns:
        df = df.sort_values("rank", ascending=True)

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

    if "close" in df.columns:
        df = add_live_price(df)
        df = add_live_variance(df)

    st.dataframe(df, width="stretch", hide_index=True)


# ------------------------------
# SECTIONS
# ------------------------------

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
    "Signals",
    """
    SELECT s.*, c.company, c.logo_url
    FROM signals s
    LEFT JOIN companies c
    ON s.ticker = c.ticker
    ORDER BY s.date DESC
    LIMIT 500
    """
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
