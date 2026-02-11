import ta


def add_indicators(df):

    df["rsi"] = ta.momentum.RSIIndicator(df["close"]).rsi()

    bb = ta.volatility.BollingerBands(df["close"])
    df["bb_high"] = bb.bollinger_hband()
    df["bb_low"] = bb.bollinger_lband()

    macd = ta.trend.MACD(df["close"])
    df["macd"] = macd.macd()


    # ---------- FACTORS (needed for ML training) ----------
    df["rsi_factor"] = (50 - df["rsi"]) / 50
    df["bb_factor"] = (df["bb_low"] - df["close"]) / df["close"]
    df["macd_factor"] = df["macd"] / df["close"]

    df["trend_factor"] = df["close"].pct_change(20, fill_method=None)

    return df
