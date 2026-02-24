import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine, inspect, text, event as sa_event
from sqlalchemy.pool import NullPool
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

engine = create_engine(f"sqlite:///{DB_PATH}", poolclass=NullPool)

@sa_event.listens_for(engine, "connect")
def _set_sqlite_pragmas(dbapi_conn, _):
    cur = dbapi_conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL")   # concurrent reads + single writer
    cur.execute("PRAGMA busy_timeout=30000") # wait up to 30 s before raising
    cur.close()

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

# --- Watchlist schema setup / migration ---
_wl_insp = inspect(engine)
_wl_existing = set(_wl_insp.get_table_names())

with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS watchlist_lists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE
        )
    """))

if "watchlist" in _wl_existing:
    _wl_cols = {c["name"] for c in _wl_insp.get_columns("watchlist")}
    if "list_id" not in _wl_cols:
        # Migrate old single-table schema → new schema, preserving data under "Default"
        _wl_old = pd.read_sql("SELECT ticker, note FROM watchlist", engine)
        with engine.begin() as conn:
            conn.execute(text("INSERT OR IGNORE INTO watchlist_lists (name) VALUES ('Default')"))
        _wl_default_id = int(
            pd.read_sql("SELECT id FROM watchlist_lists WHERE name='Default'", engine)["id"].iloc[0]
        )
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE watchlist"))
            conn.execute(text("""
                CREATE TABLE watchlist (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    list_id INTEGER NOT NULL REFERENCES watchlist_lists(id),
                    ticker TEXT NOT NULL,
                    note TEXT DEFAULT '',
                    UNIQUE(list_id, ticker)
                )
            """))
            for _, _r in _wl_old.iterrows():
                conn.execute(
                    text("INSERT OR IGNORE INTO watchlist (list_id, ticker, note) VALUES (:lid, :t, :n)"),
                    {"lid": _wl_default_id, "t": _r["ticker"], "n": _r["note"]}
                )
else:
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS watchlist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                list_id INTEGER NOT NULL REFERENCES watchlist_lists(id),
                ticker TEXT NOT NULL,
                note TEXT DEFAULT '',
                UNIQUE(list_id, ticker)
            )
        """))

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

if "wl_selected_list" not in st.session_state:
    st.session_state.wl_selected_list = ""

if "wl_create_msg" not in st.session_state:
    st.session_state.wl_create_msg = None

if "wl_add_msg" not in st.session_state:
    st.session_state.wl_add_msg = None

if "wl_save_msg" not in st.session_state:
    st.session_state.wl_save_msg = None

if "wl_ticker" not in st.session_state:
    st.session_state.wl_ticker = ""

if "wl_note" not in st.session_state:
    st.session_state.wl_note = ""

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


@st.cache_data(ttl=60)
def _fetch_live_prices(tickers_tuple: tuple) -> dict:
    """Batch-fetch latest prices for all tickers in one yf.download call. Cached 60 s."""
    tickers = list(tickers_tuple)
    try:
        query = tickers[0] if len(tickers) == 1 else tickers
        raw = yf.download(query, period="1d", interval="1m",
                          progress=False, auto_adjust=True)
        close = raw.get("Close", pd.DataFrame() if len(tickers) > 1 else pd.Series(dtype=float))
        prices = {}
        if len(tickers) == 1:
            series = close.dropna() if isinstance(close, pd.Series) else pd.Series(dtype=float)
            prices[tickers[0]] = float(series.iloc[-1]) if not series.empty else None
        else:
            for t in tickers:
                try:
                    series = close[t].dropna()
                    prices[t] = float(series.iloc[-1]) if not series.empty else None
                except Exception:
                    prices[t] = None
    except Exception:
        prices = {t: None for t in tickers}
    return prices


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

    # ---------- 4. batch download (cached 60 s) ----------
    prices = _fetch_live_prices(tuple(sorted(tickers)))
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
# WATCHLIST HELPERS
# ------------------------------

def load_list_names():
    return pd.read_sql("SELECT name FROM watchlist_lists ORDER BY id", engine)["name"].tolist()


def _cb_delete_list():
    name = st.session_state.wl_selected_list
    if not name:
        return
    list_id = get_list_id(name)
    if list_id is not None:
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM watchlist WHERE list_id = :lid"), {"lid": list_id})
            conn.execute(text("DELETE FROM watchlist_lists WHERE id = :lid"), {"lid": list_id})
    st.session_state.wl_selected_list = ""


def _cb_add_ticker():
    list_id = get_list_id(st.session_state.wl_selected_list)
    if list_id is None:
        st.session_state.wl_add_msg = ("error", "List not found.")
        return
    tv = st.session_state.wl_ticker.strip().upper()
    nv = st.session_state.wl_note.strip()
    if not tv:
        st.session_state.wl_add_msg = ("warning", "Please enter a ticker symbol.")
        return
    try:
        with engine.begin() as conn:
            r = conn.execute(
                text("INSERT OR IGNORE INTO watchlist (list_id, ticker, note) VALUES (:lid, :ticker, :note)"),
                {"lid": list_id, "ticker": tv, "note": nv}
            )
        if r.rowcount > 0:
            st.session_state.wl_add_msg = ("success", f"Added {tv}.")
        else:
            st.session_state.wl_add_msg = ("warning", f"{tv} is already in this list.")
    except Exception as e:
        st.session_state.wl_add_msg = ("error", str(e))


def _cb_create_list():
    name = st.session_state.wl_new_list_name.strip()
    if not name:
        st.session_state.wl_create_msg = ("warning", "Please enter a list name.")
        return
    try:
        with engine.begin() as conn:
            r = conn.execute(
                text("INSERT OR IGNORE INTO watchlist_lists (name) VALUES (:name)"),
                {"name": name}
            )
        if r.rowcount > 0:
            st.session_state.wl_selected_list = name
            st.session_state.wl_create_msg = ("success", f"Created '{name}'.")
        else:
            st.session_state.wl_create_msg = ("warning", f"A list named '{name}' already exists.")
    except Exception as e:
        st.session_state.wl_create_msg = ("error", str(e))


def get_list_id(list_name):
    result = pd.read_sql(
        "SELECT id FROM watchlist_lists WHERE name = :name", engine, params={"name": list_name}
    )
    return int(result["id"].iloc[0]) if not result.empty else None


def load_watchlist(list_id):
    return pd.read_sql(
        "SELECT id, ticker, note FROM watchlist WHERE list_id = :lid ORDER BY id",
        engine, params={"lid": list_id}
    )


def watchlist_signal_data(tickers):
    if not tickers:
        return pd.DataFrame()
    ticker_list = "','".join(tickers)
    return pd.read_sql(f"""
        SELECT s.ticker, c.company, c.logo_url, s.close, s.signal, s.rsi,
               s.fair_value_upside, s.target_mean_price
        FROM signals s
        LEFT JOIN companies c ON s.ticker = c.ticker
        WHERE s.ticker IN ('{ticker_list}')
          AND DATE(s.date) = (
              SELECT DATE(MAX(s2.date)) FROM signals s2 WHERE s2.ticker = s.ticker
          )
    """, engine)


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
        st.dataframe(styled, width='stretch', hide_index=True, column_config=column_config)
    else:
        st.dataframe(df, width='stretch', hide_index=True, column_config=column_config)


# ------------------------------
# SECTIONS
# ------------------------------

@st.fragment(run_every="2m")
def live_signals():
    components.html(f"""
    <!-- ts:{int(time.time())} -->
    <script>
    const timeEl = window.parent.document.getElementById("last-update-time");
    if (timeEl) {{
      timeEl.textContent = new Date().toLocaleTimeString([], {{hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false}});
    }}
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

# ------------------------------
# WATCHLIST SECTION
# ------------------------------

st.header("My Watchlists")

# --- List selector + create / delete ---
sel_col, del_col, name_col, btn_col = st.columns([3, 1, 3, 1])

with sel_col:
    _wl_list_names = load_list_names()
    st.selectbox(
        "Load a list",
        options=[""] + _wl_list_names,
        key="wl_selected_list",
        format_func=lambda x: "— select a list —" if x == "" else x,
    )

with del_col:
    st.markdown("<div style='margin-top:28px'>", unsafe_allow_html=True)
    st.button(
        "Delete",
        key="wl_delete_list",
        disabled=not st.session_state.wl_selected_list,
        on_click=_cb_delete_list,
    )
    st.markdown("</div>", unsafe_allow_html=True)

with name_col:
    st.text_input("New list name", key="wl_new_list_name", placeholder="e.g. Tech Picks")

with btn_col:
    st.markdown("<div style='margin-top:28px'>", unsafe_allow_html=True)
    st.button("Create", key="wl_create_list", on_click=_cb_create_list)
    st.markdown("</div>", unsafe_allow_html=True)

_create_msg = st.session_state.pop("wl_create_msg", None)
if _create_msg:
    level, msg = _create_msg
    getattr(st, level)(msg)

# --- Ticker management (only when a list is selected) ---
_selected_list = st.session_state.wl_selected_list
if _selected_list:
    _list_id = get_list_id(_selected_list)

    if _list_id is not None:
        st.markdown(f"**{_selected_list}**")

        # Add ticker row
        tc1, tc2, tc3 = st.columns([2, 4, 1])
        with tc1:
            st.text_input("Ticker", key="wl_ticker")
        with tc2:
            st.text_input("Note (optional)", key="wl_note")
        with tc3:
            st.markdown("<div style='margin-top:28px'>", unsafe_allow_html=True)
            st.button("Add", key="wl_add", on_click=_cb_add_ticker)
            st.markdown("</div>", unsafe_allow_html=True)

        _add_msg = st.session_state.pop("wl_add_msg", None)
        if _add_msg:
            level, msg = _add_msg
            getattr(st, level)(msg)

        # Data editor
        wl_df = load_watchlist(_list_id)
        if wl_df.empty:
            st.info("This list is empty. Add a ticker above.")
        else:
            try:
                signal_data = watchlist_signal_data(wl_df["ticker"].tolist())
                display_df = wl_df.merge(signal_data, on="ticker", how="left")

                # Fill NaN for all string/image columns to avoid column config errors
                for _col in ["note", "company", "signal", "logo_url"]:
                    if _col in display_df.columns:
                        display_df[_col] = display_df[_col].fillna("").astype(str)

                display_df = add_live_price(display_df)
                display_df = add_live_variance(display_df)

                # Normalise Live Price → always a plain string (TextColumn requirement)
                if "Live Price" in display_df.columns:
                    display_df["Live Price"] = display_df["Live Price"].apply(
                        lambda x: f"{x:,.2f}" if isinstance(x, (int, float)) and not pd.isna(x)
                        else (str(x) if x is not None else "-")
                    )

                if "fair_value_upside" in display_df.columns:
                    display_df["fair_value_upside"] = pd.to_numeric(
                        display_df["fair_value_upside"], errors="coerce"
                    ) * 100

                desired_cols = ["logo_url", "ticker", "company", "signal", "close",
                                "rsi", "fair_value_upside", "target_mean_price",
                                "Live Price", "Live Variance", "note"]
                available_cols = [c for c in desired_cols if c in display_df.columns]
                editor_df = display_df[available_cols].copy().reset_index(drop=True)

                wl_col_config = {
                    "logo_url":          st.column_config.ImageColumn("Logo", width="small"),
                    "ticker":            st.column_config.TextColumn("Ticker", disabled=True),
                    "company":           st.column_config.TextColumn("Company", disabled=True),
                    "note":              st.column_config.TextColumn("Note"),
                    "signal":            st.column_config.TextColumn("Signal", disabled=True),
                    "close":             st.column_config.NumberColumn("Close", format="%.2f", disabled=True),
                    "Live Price":        st.column_config.TextColumn("Live Price", disabled=True),
                    "Live Variance":     st.column_config.TextColumn("Live Variance", disabled=True),
                    "rsi":               st.column_config.NumberColumn("RSI", format="%.2f", disabled=True),
                    "fair_value_upside": st.column_config.NumberColumn("Analyst Upside", format="%.2f%%", disabled=True),
                    "target_mean_price": st.column_config.NumberColumn("Target Price", format="%.2f", disabled=True),
                }

                edited = st.data_editor(
                    editor_df,
                    column_config=wl_col_config,
                    num_rows="dynamic",
                    hide_index=True,
                    key=f"wl_editor_{_selected_list}",
                )

                # Save uses `edited` directly — avoids the session-state delta-dict issue
                if st.button("Save changes", key="wl_save"):
                    original_tickers = set(wl_df["ticker"].tolist())
                    remaining_tickers = set(edited["ticker"].dropna().tolist())
                    try:
                        with engine.begin() as conn:
                            for removed in original_tickers - remaining_tickers:
                                conn.execute(
                                    text("DELETE FROM watchlist WHERE list_id = :lid AND ticker = :ticker"),
                                    {"lid": _list_id, "ticker": removed}
                                )
                            for _, row in edited.iterrows():
                                if row.get("ticker") in original_tickers:
                                    conn.execute(
                                        text("UPDATE watchlist SET note = :note WHERE list_id = :lid AND ticker = :ticker"),
                                        {"note": str(row["note"]) if pd.notna(row.get("note", "")) else "",
                                         "lid": _list_id, "ticker": row["ticker"]}
                                    )
                        st.success("Watchlist saved.")
                    except Exception as _e:
                        st.error(str(_e))

            except Exception as e:
                import traceback as _tb
                st.error(f"Watchlist error: {e}")
                st.code(_tb.format_exc())

# ------------------------------
# LIVE SIGNALS
# ------------------------------

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
    st.dataframe(styled_trades, width='stretch', hide_index=True, column_config=tx_col_cfg)
