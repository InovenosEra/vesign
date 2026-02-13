import streamlit as st
import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine
from datetime import datetime, time as dt_time, UTC
import pytz
import time
import os
from main import main   # your pipeline runner

if not os.path.exists("vesign.db"):
    main()   # build database automatically

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

# st.markdown(
#     """
#     <style>
#     div[data-testid="stDataFrame"] td {
#         text-align: left !important;
#     }
#     </style>
#     """,
#     unsafe_allow_html=True
# )

engine = create_engine("sqlite:///vesign.db")

# wait until tables exist
for _ in range(30):
    try:
        pd.read_sql("SELECT 1 FROM signals LIMIT 1", engine)
        break
    except:
        time.sleep(5)

st.title("Vesign Trading System")

search_col, _ = st.columns([2, 9])  # left small column, right empty space

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

    if not market_is_open():
        df["Live Price"] = "Market is closed"
        return df

    tickers = df["ticker"].unique().tolist()

    live_data = yf.download(
        tickers,
        period="1d",
        interval="1m",
        progress=False
    )

    prices = {}

    for t in tickers:
        try:
            prices[t] = live_data["Close"][t].dropna().iloc[-1]
        except:
            prices[t] = None

    df["Live Price"] = df["ticker"].map(prices)

    return df


def add_live_variance(df):

    if df.empty:
        df["Live Variance"] = "-"
        return df

    if "Live Price" not in df.columns:
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


@st.cache_data(ttl=3600)  # cache for 1 hour
def fetch_market_caps(tickers):

    ticker_objs = yf.Tickers(" ".join(tickers))

    caps = {}
    for t in tickers:
        try:
            caps[t] = ticker_objs.tickers[t].info.get("marketCap")
        except:
            caps[t] = None

    return caps


def apply_signal_filter(df):

    if "signal_filter" not in st.session_state:
        st.session_state.signal_filter = "ALL"

    if st.session_state.signal_filter != "ALL":
        df = df[df["signal"] == st.session_state.signal_filter]

    return df


# ---------- Market Cap ----------
def add_market_cap(df):

    caps = pd.read_sql(
        "SELECT ticker, market_cap FROM fundamentals",
        engine
    )

    df = df.merge(caps, on="ticker", how="left")

    # convert to billions
    df["market_cap"] = df["market_cap"] / 1_000_000_000

    return df



# ---------- Search ----------
def apply_search(df):
    if search:
        mask = (
            df["ticker"].str.contains(search, case=False, na=False) |
            df["company"].str.contains(search, case=False, na=False)
        )
        df = df[mask]
    return df


# ---------- Formatting ----------
def format_table(df):

    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%d/%m/%y")

    if "volume" in df.columns:
        df["volume"] = df["volume"] / 1_000_000

    drop_cols = [
        "open", "high", "low", "Adj Close",
        "bb_high", "bb_low", "macd",
        "rsi_below_30", "rsi_3day_flag",
        "bb_factor", "rsi_factor", "macd_factor",
        "sector", "pred_5d", "pred_20d", "regime_pass",
        "allocation_pct", "prediction_score", "score",
        "analyst_condition", "bb_condition", "recommendation_mean", "num_analysts", "volume",
        "rank", "trend_factor"
    ]

    for col in drop_cols:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)

    if "logo_url" in df.columns:
        df.rename(columns={"logo_url": "Logo"}, inplace=True)

    if "Logo" in df.columns:
        df["Logo"] = df["Logo"].fillna(
            "https://upload.wikimedia.org/wikipedia/commons/a/ac/No_image_available.svg"
        )

    base_cols = ["date", "Logo", "company", "ticker"]
    cols = base_cols + [c for c in df.columns if c not in base_cols]
    df = df[cols]

    df.columns = [c.capitalize() for c in df.columns]

    # place Market Cap after Ticker
    if "Market cap" in df.columns and "Ticker" in df.columns:
        cols = df.columns.tolist()
        cols.insert(cols.index("Ticker") + 1, cols.pop(cols.index("Market cap")))
        df = df[cols]

    # round numeric columns
    # numeric_cols = df.select_dtypes(include="number").columns
    # df[numeric_cols] = df[numeric_cols].round(2)

    if "Fair_value_upside" in df.columns:
        df["Fair_value_upside"] = df["Fair_value_upside"] * 100

    return df


column_config = {
    "Logo": st.column_config.ImageColumn("Logo", width="small"),
    "Date": st.column_config.Column(width="small"),
    "Company": st.column_config.Column(width="medium"),

    "Close": st.column_config.NumberColumn("Price",format="%.2f"),
    "Volume": st.column_config.NumberColumn(format="%.2f"),
    "Market cap": st.column_config.NumberColumn("Market Cap ($B)", format="%.1f"),
    "Trend_factor": st.column_config.NumberColumn("Trend Factor", format="%.2f"),
    "Rsi": st.column_config.NumberColumn("RSI", format="%.2f"),
    "Bb_ratio": st.column_config.NumberColumn("Boll Ratio", format="%.2f"),
    "Rank": st.column_config.NumberColumn(format="%.0f"),
    "Target_mean_price": st.column_config.NumberColumn("Target Mean", format="%.2f"),
    "Target_high_price": st.column_config.NumberColumn("Target High", format="%.0f"),
    "Target_low_price": st.column_config.NumberColumn("Target Low", format="%.0f"),
    "Fair_value_upside": st.column_config.NumberColumn("Fair Value", format="+%.1f%%"),
    "Live price": st.column_config.NumberColumn("Live Price", format="%.2f"),
}


def display_section(title, query):

    df = pd.read_sql(query, engine)

    df = apply_search(df)

    # Sort Today's BUY signals
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
    df = add_live_price(df)
    df = add_live_variance(df)

    if title == "Signals":
        df = apply_signal_filter(df)

    df = format_table(df)

    if title == "Signals" and "Rank" in df.columns:
        df.drop(columns=["Rank"], inplace=True)

    styled_df = df.style.set_properties(**{"text-align": "left"})

    if "Live variance" in df.columns:
        styled_df = styled_df.map(style_variance, subset=["Live variance"])

    st.dataframe(
        styled_df,
        width="stretch",
        hide_index=True,
        column_config=column_config
    )


display_section(
    "Today's BUY signals",
    """
    SELECT p.*, c.company, c.logo_url
    FROM daily_portfolio p
    LEFT JOIN companies c
    ON p.ticker = c.ticker
    WHERE p.date = (SELECT MAX(date) FROM daily_portfolio)
    """
)


# display_section(
#     "Ranked Signals",
#     """
#     SELECT r.*, c.company, c.logo_url
#     FROM daily_ranked r
#     LEFT JOIN companies c
#     ON r.ticker = c.ticker
#     WHERE r.date = (SELECT MAX(date) FROM daily_ranked)
#     """
# )


display_section(
    "Signals",
    """
    SELECT s.*, c.company, c.logo_url
    FROM signals s
    LEFT JOIN companies c
    ON s.ticker = c.ticker
    ORDER BY s.date DESC
    LIMIT 250
    """
)

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
