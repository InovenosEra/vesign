# Vesign Trading System

An automated S&P 500 stock screening and portfolio allocation system that combines technical analysis, machine learning predictions, and analyst expectations to generate daily BUY/SELL trading signals — with a real-time Streamlit dashboard for monitoring.

---

## Features

- **Hybrid Signal Generation** — combines RSI, Bollinger Bands, MACD, volume filters, analyst targets, and 52-week high distance
- **Sector-Specific ML Models** — XGBoost models trained per sector, blending 5-day and 20-day return predictions
- **VIX-Adaptive Position Sizing** — position multipliers that respond to market volatility
- **Trailing Stop Loss** — 7% stop triggers automatic SELL signals on open positions
- **Live Dashboard** — real-time prices, live variance, market open countdown, and auto-refresh every 2 minutes
- **Historical Backtesting** — evaluates BUY→SELL trade history with win rate and return metrics

---

## Project Structure

```
vesign/
├── config/
│   └── settings.yaml          # Thresholds, VIX levels, scoring parameters
├── data/
│   ├── market_data.py         # Price, VIX, fundamentals fetching
│   └── loaders.py             # Database interface (SQLAlchemy)
├── features/
│   ├── technical_indicators.py # RSI, Bollinger Bands, MACD, volume, 52W high
│   ├── analyst_data.py        # Analyst price targets
│   └── forward_returns.py     # ML training labels (5d/20d returns)
├── models/
│   ├── train.py               # XGBoost training per sector
│   └── predict.py             # Prediction engine (blends 5d/20d)
├── signals/
│   └── engine.py              # Signal generation logic (BUY/HOLD/SELL)
├── portfolio/
│   ├── ranking.py             # Ranks signals by ML score
│   └── allocator.py           # Capital allocation with VIX adjustment
├── backtesting/
│   ├── engine.py              # Trade log builder & backtest runner
│   └── metrics.py             # Performance metrics
├── production/
│   └── run_daily.py           # Main daily pipeline orchestrator
├── dashboard/
│   └── app.py                 # Streamlit dashboard
├── utils/
│   ├── universe_loader.py     # S&P 500 universe (Wikipedia scraper)
│   └── update_guard.py        # Prevents duplicate pipeline runs
└── ml_models/                 # Trained XGBoost model files (.pkl)
```

---

## How It Works

### Daily Pipeline (`production/run_daily.py`)

```
1. update_prices()          → Incremental OHLCV data for all tickers
2. update_vix()             → VIX index for volatility sizing
3. update_fundamentals()    → Market cap data
4. update_analyst_data()    → Analyst price targets & consensus
5. compute_features()       → Technical indicators & factors
6. run_prediction_engine()  → ML prediction scores
7. run_scoring()            → BUY / HOLD / SELL signals
8. build_trade_log()        → Pair BUY→SELL for P&L history
9. run_ranking()            → Rank signals by score
10. run_allocator()         → Allocate capital with VIX adjustment
```

### Signal Logic

**BUY** requires all of:
- RSI < 30 for 3 consecutive days (oversold)
- Price in lower 20% of Bollinger Band
- Analyst upside ≥ 5%
- Volume spike: max 3-day ratio ≥ 1.5×
- Price ≤ −10% from 52-week high

**SELL** triggers on:
- Trailing stop: price drops 7% below entry
- RSI ≥ 70 (overbought)

### Position Sizing (VIX-based)

| VIX Level | Multiplier |
|-----------|------------|
| < 15      | 0.8×       |
| 15–25     | 1.0×       |
| 25–35     | 1.5×       |
| > 35      | 2.0×       |

Max position cap: **15%** of portfolio per ticker.

---

## Setup

```bash
# 1. Create virtual environment and install dependencies
bash setup.sh

# 2. Activate environment
source .venv/bin/activate

# 3. Run the daily pipeline (first time builds the database)
python production/run_daily.py

# 4. Launch the dashboard
streamlit run dashboard/app.py
```

---

## Dashboard

The Streamlit dashboard (`http://localhost:8501`) provides:

- **Today's BUY/SELL signals** with live prices and variance (auto-refreshes every 2 minutes)
- **12-month signal history** with signal filter (ALL / BUY / SELL / HOLD)
- **BUY→SELL success rate** by company
- **Historical transactions** with win rate, avg return, avg days held, and % Yield color coding
- **Sticky header** with market open/close countdown and last update time

> The dashboard is read-only — it never triggers the pipeline directly.

---

## Configuration (`config/settings.yaml`)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `buy_threshold` | 0.01 | Minimum score for BUY signal |
| `sell_threshold` | 0.01 | Minimum score for SELL signal |
| `trailing_stop_pct` | 0.07 | 7% trailing stop loss |
| `volume_ratio_threshold` | 1.5 | Volume spike minimum |
| `pct_from_52w_high_min` | 0.10 | Min distance from 52-week high |
| `position_cap` | 0.15 | Max 15% per position |

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Data | `yfinance`, `requests`, `beautifulsoup4` |
| Storage | SQLite via `SQLAlchemy` |
| Features | `pandas`, `ta` (technical analysis) |
| ML | `xgboost`, `scikit-learn` |
| Dashboard | `streamlit` |
| Config | `PyYAML` |
