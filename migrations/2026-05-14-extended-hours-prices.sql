CREATE TABLE IF NOT EXISTS extended_hours_prices (
  date TEXT NOT NULL,
  ticker TEXT NOT NULL,
  extended_close REAL NOT NULL,
  source TEXT DEFAULT 'fmp_aftermarket',
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (date, ticker)
);
CREATE INDEX IF NOT EXISTS idx_ext_date ON extended_hours_prices(date);
