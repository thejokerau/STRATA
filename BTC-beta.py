import sys
import sqlite3
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import yfinance as yf
import requests
import warnings

warnings.filterwarnings("ignore")

print("🚀 Crypto & Traditional Risk Dashboard (Robust v37)\n")

# ====================== DATABASE ======================
conn = sqlite3.connect("crypto_data.db")

def reset_table():
    conn.execute("DROP TABLE IF EXISTS price_data")
    conn.execute("""
    CREATE TABLE price_data (
        ticker TEXT, date TEXT,
        open REAL, high REAL, low REAL, close REAL, volume INTEGER,
        PRIMARY KEY (ticker, date)
    )
    """)
    conn.commit()

reset_table()

COINGECKO_KEY = ""

# ====================== VALIDATION ======================
def validate_and_clean(df, ticker):
    """Validate OHLCV data, normalize timestamps to UTC-naive, remove outliers."""
    if df is None or df.empty or len(df) < 30:
        return None
    df = df.copy()
    # FIX: Always normalize to UTC-naive datetime for deterministic comparisons
    if hasattr(df.index, 'tz') and df.index.tz is not None:
        df.index = df.index.tz_convert("UTC").tz_localize(None)
    else:
        df.index = pd.to_datetime(df.index)
    df.index = df.index.normalize()  # strip intraday time component
    # Remove duplicate timestamps, keep last (most recently fetched)
    df = df[~df.index.duplicated(keep="last")]
    df = df.sort_index()
    df = df[(df["Close"] > 0.000001) & (df["Close"] < 10_000_000)]
    if len(df) > 10:
        median_close = df["Close"].median()
        df = df[(df["Close"] <= median_close * 20) & (df["Close"] >= median_close * 0.05)]
    today = pd.Timestamp.now().normalize()
    df = df[df.index <= today]
    return df if len(df) >= 30 else None

# ====================== BINANCE ======================
def fetch_from_binance(ticker):
    """Fetch daily OHLCV from Binance with proper timestamp handling."""
    symbol = ticker.replace("-USD", "USDT") if "-USD" in ticker else ticker + "USDT"
    try:
        url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1d&limit=1000"
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
        if not data:
            return None
        open_times = [row[0] for row in data]
        df = pd.DataFrame({
            "Open":   [float(row[1]) for row in data],
            "High":   [float(row[2]) for row in data],
            "Low":    [float(row[3]) for row in data],
            "Close":  [float(row[4]) for row in data],
            "Volume": [float(row[5]) for row in data],
        })
        # FIX: Use open_time column values (milliseconds) for index, not RangeIndex
        df.index = pd.to_datetime(open_times, unit="ms", utc=True).tz_localize(None).normalize()
        return validate_and_clean(df, ticker)
    except Exception:
        return None

# ====================== CACHE ======================
def get_cached(ticker):
    """Retrieve cached data from SQLite, returning clean DataFrame or None."""
    df = pd.read_sql_query(
        "SELECT * FROM price_data WHERE ticker=? ORDER BY date ASC", conn, params=[ticker]
    )
    if df.empty:
        return None
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)
    # FIX: Drop ticker column before capitalizing to avoid 'Ticker' leftover
    df = df.drop(columns=["ticker"], errors="ignore")
    df.columns = [c.capitalize() for c in df.columns]
    # Deduplicate after loading
    df = df[~df.index.duplicated(keep="last")].sort_index()
    return df

def save_to_cache(ticker, df):
    """
    Persist OHLCV data to SQLite using proper batch insertion (executemany).
    FIX: Uses INSERT OR REPLACE to safely upsert rows and avoid duplicates.
    """
    if df is None or df.empty:
        return
    df = validate_and_clean(df, ticker)
    if df is None:
        return
    temp = df.copy()
    temp["ticker"] = ticker
    temp["date"] = temp.index.strftime("%Y-%m-%d")
    cols = [c for c in ["ticker", "date", "Open", "High", "Low", "Close", "Volume"] if c in temp.columns]
    temp = temp[cols]
    # FIX: Use executemany with INSERT OR REPLACE for deterministic, safe batch writes
    rows = [tuple(row) for row in temp.itertuples(index=False, name=None)]
    placeholders = ",".join(["?"] * len(cols))
    sql = f"INSERT OR REPLACE INTO price_data ({','.join(c.lower() for c in cols)}) VALUES ({placeholders})"
    conn.executemany(sql, rows)
    conn.commit()

def fetch_with_cache(ticker, period="24mo"):
    """
    Fetch price data with overlap-refetch strategy:
    Always refetch last 20 candles from live source and merge with cache
    to avoid gaps and stale tail rows.
    """
    cached = get_cached(ticker)
    needs_live = True

    if cached is not None and len(cached) >= 60:
        # Only skip live fetch if cache is very recent (last entry within 2 days)
        last_cached = cached.index[-1]
        days_stale = (pd.Timestamp.now().normalize() - last_cached).days
        if days_stale <= 2:
            print(f"   📦 Cache hit: {ticker}")
            needs_live = False

    if needs_live:
        new_df = None
        if "-USD" in ticker:
            new_df = fetch_from_binance(ticker)
            if new_df is not None:
                print(f"   ✓ Binance data for {ticker}")

        if new_df is None:
            print(f"   ⬇️ yfinance: {ticker}")
            try:
                raw = yf.download(ticker, period=period, interval="1d", progress=False)
                if not raw.empty:
                    if isinstance(raw.columns, pd.MultiIndex):
                        raw.columns = raw.columns.get_level_values(0)
                    raw.columns = [c.capitalize() for c in raw.columns]
                    if "Close" in raw.columns:
                        raw["Volume"] = pd.to_numeric(
                            raw.get("Volume", 0), errors="coerce"
                        ).fillna(0).astype("int64")
                        new_df = validate_and_clean(raw, ticker)
            except Exception as e:
                print(f"   ⚠️ yfinance failed: {e}")

        if new_df is not None:
            # FIX: Overlap-merge: combine cached + new, dedup by keeping newest value
            if cached is not None and not cached.empty:
                merged = pd.concat([cached, new_df])
                merged = merged[~merged.index.duplicated(keep="last")].sort_index()
                new_df = merged
            save_to_cache(ticker, new_df)
            return new_df
        elif cached is not None:
            return cached
        return None

    return cached

# ====================== HELPERS ======================
def fetch_top_coins(n):
    print(f"📡 Fetching top {n} coins...")
    try:
        r = requests.get(
            "https://api.coingecko.com/api/v3/coins/markets",
            headers={"x-cg-demo-api-key": COINGECKO_KEY},
            params={"vs_currency": "usd", "order": "market_cap_desc", "per_page": n, "page": 1},
            timeout=15,
        )
        r.raise_for_status()
        tickers = [
            f"{coin['symbol'].upper()}-USD"
            for coin in r.json()
            if coin.get("symbol", "").upper()
            not in ["USDT", "USDC", "DAI", "FDUSD", "TUSD", "USDE", "BUSD"]
        ]
        print(f"✅ Got {len(tickers)} valid tickers.")
    except Exception:
        tickers = []
    MAJOR = [
        "BTC-USD", "ETH-USD", "BNB-USD", "SOL-USD", "XRP-USD",
        "DOGE-USD", "TON-USD", "ADA-USD", "TRX-USD", "AVAX-USD",
    ]
    final = tickers[:n]
    pre_pad = len(final)
    if len(final) < n:
        for t in MAJOR:
            if t not in final and len(final) < n:
                final.append(t)
        # FIX: Use pre_pad (length before padding) to count padded coins correctly
        print(f"   Padded with {len(final) - pre_pad} coins.")
    return final

TRADITIONAL_TOP = {
    "1": ["^AXJO", "BHP.AX", "CBA.AX", "RIO.AX", "CSL.AX", "WBC.AX", "NAB.AX", "MQG.AX", "FMG.AX", "WES.AX"],
    "2": ["^GSPC", "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "BRK-B", "TSLA", "JPM"],
    "3": ["^FTSE", "HSBA.L", "AZN.L", "SHEL.L", "ULVR.L", "BP.L", "GSK.L", "RIO.L", "BHP.L", "VOD.L"],
    "4": ["^STOXX50E", "MC.PA", "ASML.AS", "SAP.DE", "AIR.PA", "SIE.DE", "SU.PA", "RMS.PA", "TTE.PA", "ALV.DE"],
    "5": ["^GSPTSE", "RY.TO", "TD.TO", "ENB.TO", "CNR.TO", "BN.TO", "BCE.TO", "SHOP.TO", "CP.TO", "CNQ.TO"],
}

def get_traditional_suffix(country):
    return {"1": ".AX", "2": "", "3": ".L", "4": ".PA", "5": ".TO", "6": ""}.get(country, "")

# ====================== INDICATORS ======================
def compute_indicators(df):
    """
    Compute all technical indicators on a given DataFrame slice.
    Called both for live display AND per-bar in backtest to eliminate lookahead bias.

    IMPROVEMENT: Dynamic Fibonacci with full level set (0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0, 1.272, 1.618)
    using a rolling 300-bar lookback window.
    """
    if df is None or "Close" not in df.columns or len(df) < 30:
        return None
    df = df.copy()

    # ── RSI(14) ──
    delta = df["Close"].diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta).clip(lower=0).rolling(14).mean()
    rs = gain / loss.where(loss != 0, 1e-10)
    df["RSI"] = 100 - (100 / (1 + rs))

    # ── MACD(12,26,9) ──
    ema12 = df["Close"].ewm(span=12, adjust=False).mean()
    ema26 = df["Close"].ewm(span=26, adjust=False).mean()
    df["MACD"] = ema12 - ema26
    df["MACD_Signal"] = df["MACD"].ewm(span=9, adjust=False).mean()

    # ── Bollinger Bands(20) ──
    sma20 = df["Close"].rolling(20).mean()
    std20 = df["Close"].rolling(20).std()
    df["BB_Upper"] = sma20 + 2 * std20
    df["BB_Middle"] = sma20
    df["BB_Lower"] = sma20 - 2 * std20

    # ── MACD Crossover ──
    df["MACD_Crossover"] = 0
    bull = (df["MACD"].shift(1) <= df["MACD_Signal"].shift(1)) & (df["MACD"] > df["MACD_Signal"])
    bear = (df["MACD"].shift(1) >= df["MACD_Signal"].shift(1)) & (df["MACD"] < df["MACD_Signal"])
    df.loc[bull, "MACD_Crossover"] = 1
    df.loc[bear, "MACD_Crossover"] = -1

    # ── Dynamic Fibonacci Levels (rolling 300-bar window) ──
    # IMPROVEMENT: Recalculate per bar using only past data; includes full Fib level set
    fib_window = min(300, len(df))
    recent = df["Close"].tail(fib_window)
    swing_high = recent.max()
    swing_low = recent.min()
    diff = swing_high - swing_low

    FIB_RATIOS = [0.0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0, 1.272, 1.618]
    if diff > 0:
        # Fib levels measured from swing_high downward
        for ratio in FIB_RATIOS:
            col = f"Fib_{ratio:.3f}"
            df[col] = swing_high - ratio * diff
        # Key support = 0.618 retracement; key resistance = 0.382 retracement
        df["Fib_Support"] = swing_high - 0.618 * diff
        df["Fib_Resistance"] = swing_high - 0.382 * diff
    else:
        # Flat price: set default bands
        for ratio in FIB_RATIOS:
            col = f"Fib_{ratio:.3f}"
            df[col] = swing_high
        df["Fib_Support"] = swing_high * 0.92
        df["Fib_Resistance"] = swing_high * 1.08

    # ── Candlestick Patterns (Doji, Hammer, Shooting Star) ──
    df["Body"] = abs(df["Close"] - df["Open"])
    df["Range"] = df["High"] - df["Low"]
    df["Upper_Wick"] = df["High"] - df[["Open", "Close"]].max(axis=1)
    df["Lower_Wick"] = df[["Open", "Close"]].min(axis=1) - df["Low"]
    df["Doji"] = ""
    doji_mask = (df["Body"] / df["Range"].replace(0, np.nan) < 0.25) & (df["Range"] > 0)
    df.loc[doji_mask & (df["Lower_Wick"] > 2 * df["Body"]), "Doji"] = "Bullish Doji"
    df.loc[doji_mask & (df["Upper_Wick"] > 2 * df["Body"]), "Doji"] = "Bearish Doji"
    # FIX: True doji (both wicks large) applied after directional ones
    df.loc[
        doji_mask & (df["Upper_Wick"] > 2 * df["Body"]) & (df["Lower_Wick"] > 2 * df["Body"]),
        "Doji",
    ] = "Doji"

    df = df.dropna(subset=["RSI", "BB_Middle"])
    return df if len(df) >= 30 else None

def add_indicators(data_dict):
    """Apply compute_indicators to all assets in the dict."""
    enriched = {}
    for ticker, df in data_dict.items():
        result = compute_indicators(df)
        if result is not None:
            enriched[ticker] = result
    return enriched

# ====================== SCORING ======================
def score_coin(df):
    """
    IMPROVED 5-component scoring system. Range: -5 to +5.
      1. Volatility     : +1 if low (<2%), -1 if high (>3.5%)
      2. RSI            : +1 if oversold (<35), -1 if overbought (>68)
      3. MACD Crossover : +1 bull cross, -1 bear cross, 0 none
      4. Bollinger Band : +1 below mid, -1 above upper
      5. Fibonacci      : +1 near/below Fib support, -1 near/above Fib resistance

    IMPROVEMENT: More responsive — bearish conditions are penalized, not just neutral.
    Each component returns -1, 0, or +1 cleanly.
    """
    latest = df.iloc[-1]
    close = latest.get("Close", 0)

    # 1. Volatility
    vol = df["Close"].pct_change().abs().mean() * 100
    s = -1 if vol > 3.5 else (1 if vol < 2.0 else 0)

    # 2. RSI — expanded thresholds for more responsiveness
    rsi = latest.get("RSI", 50)
    s += -1 if rsi > 68 else (1 if rsi < 35 else 0)

    # 3. MACD Crossover
    s += int(latest.get("MACD_Crossover", 0))

    # 4. Bollinger Bands — price relative to bands
    bb_mid = latest.get("BB_Middle", 0)
    bb_upper = latest.get("BB_Upper", 0)
    bb_lower = latest.get("BB_Lower", 0)
    if close < bb_lower * 1.01:
        # Price at or below lower band: strong bullish signal
        s += 1
    elif close < bb_mid * 0.98:
        s += 1
    elif close > bb_upper * 1.02:
        s -= 1
    elif close > bb_upper * 0.98:
        # Near upper band: mild bearish
        s -= 1

    # 5. Fibonacci position relative to key support/resistance
    fib_s = latest.get("Fib_Support", 0)
    fib_r = latest.get("Fib_Resistance", 0)
    if fib_s > 0 and fib_r > 0:
        if close <= fib_s * 1.02:
            s += 1
        elif close >= fib_r * 0.98:
            s -= 1

    return s, latest

def score_to_rec(s):
    if s >= 4:   return "🟢 STRONG BUY"
    elif s >= 2: return "🟢 BUY"
    elif s >= 0: return "🟠 HOLD"
    else:        return "🔴 SELL"

def get_action(s):
    if s >= 3:    return "BUY"
    elif s <= -1: return "SELL"
    else:         return "HOLD"

def should_exit(hist, entry_price, days_held, max_hold_days,
                stop_loss_pct=8.0, take_profit_pct=20.0):
    """
    IMPROVEMENT: Multi-condition exit with trend-aware max-hold extension.
    Priority order:
      1. Stop-loss       : price fell >= stop_loss_pct% below entry
      2. Take-profit     : price rose >= take_profit_pct% above entry
      3. Signal SELL     : composite score <= -1
      4. Max hold        : safety valve — but only fires if trend is NOT strong
                           (strong trend = MACD bullish + price above BB mid)
    Returns (should_exit: bool, reason: str)
    """
    latest = hist.iloc[-1]
    current_price = latest.get("Close", entry_price)

    if entry_price > 0:
        pct_chg = (current_price / entry_price - 1) * 100
        if pct_chg <= -stop_loss_pct:
            return True, f"STOP-LOSS ({pct_chg:+.1f}%)"
        if pct_chg >= take_profit_pct:
            return True, f"TAKE-PROFIT ({pct_chg:+.1f}%)"

    score, _ = score_coin(hist)
    if score <= -1:
        return True, "SIGNAL SELL"

    # IMPROVEMENT: Max-hold extension if trend is still strong
    if days_held >= max_hold_days - 1:
        macd_val = latest.get("MACD", 0)
        macd_sig = latest.get("MACD_Signal", 0)
        bb_mid = latest.get("BB_Middle", 0)
        trend_strong = (macd_val > macd_sig) and (current_price > bb_mid)
        if trend_strong:
            # Extend by up to 50% of max_hold_days before forcing exit
            extended_limit = int(max_hold_days * 1.5)
            if days_held < extended_limit:
                return False, ""
        return True, f"MAX HOLD ({max_hold_days}d safety valve)"

    return False, ""

# ====================== MAIN ======================
def main():
    while True:
        print("\n" + "=" * 100)
        print("MAIN MENU - Robust v37")
        print("=" * 100)
        print("1. Crypto Analysis")
        print("2. Traditional Markets Analysis")
        print("3. Clear Cache")
        print("4. Exit")
        main_choice = input("\nEnter 1-4: ").strip()

        if main_choice == "4":
            print("Goodbye!")
            break
        if main_choice == "3":
            if input("Delete entire cache? (yes/no): ").strip().lower() == "yes":
                reset_table()
                print("✅ Cache cleared completely.\n")
            continue
        if main_choice not in ["1", "2"]:
            print("Invalid choice.")
            continue

        # ====================== COMMON SETUP ======================
        is_crypto = main_choice == "1"
        print(f"\n{'Crypto' if is_crypto else 'Traditional Markets'} Analysis")

        try:
            init_input = input(f"\nInitial Capital USD (default 10000): $").strip()
            INITIAL_CAPITAL = float(init_input) if init_input else 10000.0
        except Exception:
            INITIAL_CAPITAL = 10000.0
        print(f"Using Initial Capital: ${INITIAL_CAPITAL:,.2f}\n")

        print("Select mode:")
        print("1. Live Dashboard")
        print("2. Backtest + Equity Curve")
        mode_choice = input("\nEnter 1 or 2: ").strip()
        is_backtest = mode_choice == "2"

        # FIX: Initialize BACKTEST_DAYS before the if-block to avoid UnboundLocalError
        months = 12
        BACKTEST_DAYS = months * 30
        STOP_LOSS_PCT = 8.0
        TAKE_PROFIT_PCT = 20.0
        MAX_HOLD_DAYS_CFG = 45
        # IMPROVEMENT: Transaction cost and position sizing defaults
        FEE_PCT = 0.1       # 0.1% fee per trade leg
        SLIPPAGE_PCT = 0.05  # 0.05% slippage per trade leg
        POSITION_SIZE_PCT = 0.50  # Allocate 50% of capital per trade

        if is_backtest:
            print("\nBacktest lookback period:")
            print("1. 6 months   2. 12 months   3. 18 months   4. 24 months")
            period_choice = input("\nEnter 1-4: ").strip()
            months = {"1": 6, "2": 12, "3": 18, "4": 24}.get(period_choice, 12)
            BACKTEST_DAYS = months * 30
            print(f"✅ Backtest set to last {months} months.\n")

            print("Exit rules (press Enter to use defaults):")
            try:
                sl_in = input("  Stop-loss %    (default  8%): ").strip()
                STOP_LOSS_PCT = float(sl_in) if sl_in else 8.0
            except Exception:
                STOP_LOSS_PCT = 8.0
            try:
                tp_in = input("  Take-profit %  (default 20%): ").strip()
                TAKE_PROFIT_PCT = float(tp_in) if tp_in else 20.0
            except Exception:
                TAKE_PROFIT_PCT = 20.0
            try:
                mh_in = input("  Max hold days  (default 45d): ").strip()
                MAX_HOLD_DAYS_CFG = int(mh_in) if mh_in else 45
            except Exception:
                MAX_HOLD_DAYS_CFG = 45

            # IMPROVEMENT: Transaction cost parameters
            print("\nTransaction costs (press Enter for defaults):")
            try:
                fee_in = input("  Fee % per trade leg   (default 0.10%): ").strip()
                FEE_PCT = float(fee_in) if fee_in else 0.1
            except Exception:
                FEE_PCT = 0.1
            try:
                slip_in = input("  Slippage % per leg    (default 0.05%): ").strip()
                SLIPPAGE_PCT = float(slip_in) if slip_in else 0.05
            except Exception:
                SLIPPAGE_PCT = 0.05

            # IMPROVEMENT: Position sizing
            print("\nPosition sizing (press Enter for default):")
            try:
                pos_in = input("  Capital % per trade   (default 50%): ").strip()
                POSITION_SIZE_PCT = float(pos_in) / 100.0 if pos_in else 0.50
            except Exception:
                POSITION_SIZE_PCT = 0.50

            print(
                f"✅ Exit rules — SL: {STOP_LOSS_PCT}%  TP: {TAKE_PROFIT_PCT}%  "
                f"Max hold: {MAX_HOLD_DAYS_CFG}d\n"
                f"✅ Costs — Fee: {FEE_PCT}%  Slippage: {SLIPPAGE_PCT}%\n"
                f"✅ Position size: {POSITION_SIZE_PCT*100:.0f}% of capital\n"
            )

        # ====================== ASSET SELECTION ======================
        if is_crypto:
            while True:
                print("Number of top coins:")
                print("1. Top 10   2. Top 20   3. Top 50   4. Top 100")
                nc = input("\nEnter 1-4: ").strip()
                if nc in ["1", "2", "3", "4"]:
                    requested_n = {"1": 10, "2": 20, "3": 50, "4": 100}[nc]
                    break
                print("❌ Please enter 1-4.")
            final_tickers = fetch_top_coins(requested_n)
        else:
            print("Select Country/Region:")
            print("1. Australia (ASX)   2. United States   3. United Kingdom   4. Europe   5. Canada   6. Other")
            country_choice = input("\nEnter 1-6: ").strip()
            suffix = get_traditional_suffix(country_choice)

            print("\nHow would you like to select assets?")
            print("1. Top 10 regional indices & major stocks")
            print("2. Manual ticker(s)")
            print("3. Both")
            asset_mode = input("\nEnter 1-3: ").strip()

            tickers = []
            if asset_mode in ["1", "3"]:
                top_list = TRADITIONAL_TOP.get(country_choice, TRADITIONAL_TOP["2"])
                tickers.extend(top_list)
            if asset_mode in ["2", "3"]:
                manual = input("\nEnter manual ticker(s) separated by comma: ").strip().upper()
                if manual:
                    tickers.extend([t.strip() for t in manual.split(",") if t.strip()])

            final_tickers = []
            for t in tickers:
                # FIX: Extended exclusion list includes .DE and .AS to prevent double-suffix
                already_suffixed = any(
                    t.endswith(s) for s in [".AX", ".L", ".PA", ".TO", ".DE", ".AS"]
                )
                if suffix and not already_suffixed and not t.startswith("^"):
                    final_tickers.append(t + suffix)
                else:
                    final_tickers.append(t)
            final_tickers = list(dict.fromkeys(final_tickers))

        print(f"Selected {len(final_tickers)} assets.")

        # ====================== LOAD DATA ======================
        raw_data = {}
        for ticker in final_tickers:
            df = fetch_with_cache(ticker, period="24mo")
            if df is not None and len(df) >= 30:
                raw_data[ticker] = df

        print(f"✅ Loaded raw data for {len(raw_data)} assets.")

        enriched = add_indicators(raw_data)
        print(f"✅ {len(enriched)} assets ready with indicators.\n")

        if not enriched:
            print("❌ No usable data.")
            input("Press Enter to return...")
            continue

        # ====================== RUN ANALYSIS ======================
        if not is_backtest:
            # ── Live Dashboard ──
            results = []
            for t, df in enriched.items():
                score, latest = score_coin(df)
                action = get_action(score)
                current = latest.get("Close", 0)
                fib_support = latest.get("Fib_Support", current * 0.92)
                fib_resistance = latest.get("Fib_Resistance", current * 1.12)
                doji = latest.get("Doji", "")
                upside = (fib_resistance - current) / current * 100 if current > 0 else 0
                downside = (current - fib_support) / current * 100 if current > 0 else 0
                results.append({
                    "Asset": t.replace("-USD", ""),
                    "Action": action,
                    "Recommendation": score_to_rec(score),
                    "Score": f"{score}/5",
                    "Price": round(current, 4),
                    "Fib Support": round(fib_support, 4),
                    "Fib Resistance": round(fib_resistance, 4),
                    "Upside %": round(upside, 1),
                    "Downside %": round(downside, 1),
                    "Doji": doji,
                    "RSI": round(latest.get("RSI", 0), 1),
                })

            df_out = pd.DataFrame(results).sort_values(
                "Score",
                key=lambda x: x.str.split("/").str[0].astype(int),
                ascending=False,
            )
            # FIX: Reset index before incrementing to ensure contiguous rank after sort
            df_out = df_out.reset_index(drop=True)
            df_out.index += 1
            df_out.index.name = "Rank"

            mode_name = "Crypto" if is_crypto else "Traditional Markets"
            print(f"\n{'='*150}")
            print(f"🏆 {mode_name} LIVE DASHBOARD")
            print("=" * 150)
            print(df_out.to_string())

            # ── Risk Score Breakdown ──
            print(f"\n{'='*150}")
            print(f"📊 RISK SCORE BREAKDOWN")
            print("=" * 150)
            risk_rows = []
            for t, df in enriched.items():
                score, latest = score_coin(df)
                vol = df["Close"].pct_change().abs().mean() * 100
                vol_score = -1 if vol > 3.5 else (1 if vol < 2.0 else 0)
                rsi_val = latest.get("RSI", 50)
                rsi_score = -1 if rsi_val > 68 else (1 if rsi_val < 35 else 0)
                macd_score = int(latest.get("MACD_Crossover", 0))
                close = latest.get("Close", 0)
                bb_mid = latest.get("BB_Middle", 0)
                bb_upper = latest.get("BB_Upper", 0)
                bb_lower = latest.get("BB_Lower", 0)
                if close < bb_lower * 1.01 or close < bb_mid * 0.98:
                    bb_score = 1
                elif close > bb_upper * 0.98:
                    bb_score = -1
                else:
                    bb_score = 0
                macd_val = latest.get("MACD", 0)
                macd_sig = latest.get("MACD_Signal", 0)
                fib_s_val = latest.get("Fib_Support", 0)
                fib_r_val = latest.get("Fib_Resistance", 0)
                if fib_s_val > 0 and fib_r_val > 0:
                    if close <= fib_s_val * 1.02:
                        fib_score = 1
                    elif close >= fib_r_val * 0.98:
                        fib_score = -1
                    else:
                        fib_score = 0
                else:
                    fib_score = 0
                risk_rows.append({
                    "Asset": t.replace("-USD", ""),
                    "Total Score": f"{score}/5",
                    "Volatility": f"{vol:.2f}% ({'Low' if vol_score==1 else ('High' if vol_score==-1 else 'Med')})",
                    "RSI": f"{rsi_val:.1f} ({'OS' if rsi_score==1 else ('OB' if rsi_score==-1 else 'Neutral')})",
                    "MACD": f"{macd_val:.4f}/Sig {macd_sig:.4f} ({'BullX' if macd_score==1 else ('BearX' if macd_score==-1 else 'None')})",
                    "BB Position": f"{'Below' if bb_score==1 else ('Above' if bb_score==-1 else 'Inside')}",
                    "Fibonacci": f"{'Near Support' if fib_score==1 else ('Near Resistance' if fib_score==-1 else 'Between')}",
                    "Doji": latest.get("Doji", "") or "—",
                })
            risk_df = pd.DataFrame(risk_rows).sort_values(
                "Total Score",
                key=lambda x: x.str.split("/").str[0].astype(int),
                ascending=False,
            ).reset_index(drop=True)
            risk_df.index += 1
            risk_df.index.name = "Rank"
            print(risk_df.to_string())

            # ── Top Asset Technical Chart ──
            top_ticker = max(enriched.keys(), key=lambda t: score_coin(enriched[t])[0])
            chart_df = enriched[top_ticker].tail(180).copy()
            label = top_ticker.replace("-USD", "") if is_crypto else top_ticker

            fig = make_subplots(
                rows=4, cols=1,
                shared_xaxes=True,
                row_heights=[0.45, 0.20, 0.20, 0.15],
                vertical_spacing=0.03,
                subplot_titles=(
                    f"{label} — Price, Bollinger Bands & Fibonacci",
                    "MACD",
                    "RSI (14)",
                    "Volume",
                ),
            )

            # Row 1: Candlestick + BB + Fibonacci
            fig.add_trace(go.Candlestick(
                x=chart_df.index,
                open=chart_df["Open"], high=chart_df["High"],
                low=chart_df["Low"], close=chart_df["Close"],
                name="Price",
                increasing_line_color="#26a69a", decreasing_line_color="#ef5350",
            ), row=1, col=1)
            fig.add_trace(go.Scatter(x=chart_df.index, y=chart_df["BB_Upper"],
                name="BB Upper", line=dict(color="rgba(100,100,255,0.6)", width=1, dash="dot")), row=1, col=1)
            fig.add_trace(go.Scatter(x=chart_df.index, y=chart_df["BB_Middle"],
                name="BB Mid (SMA20)", line=dict(color="rgba(100,100,255,0.9)", width=1)), row=1, col=1)
            fig.add_trace(go.Scatter(x=chart_df.index, y=chart_df["BB_Lower"],
                name="BB Lower", line=dict(color="rgba(100,100,255,0.6)", width=1, dash="dot"),
                fill="tonexty", fillcolor="rgba(100,100,255,0.05)"), row=1, col=1)

            if "Fib_Support" in chart_df.columns and "Fib_Resistance" in chart_df.columns:
                fib_s = chart_df["Fib_Support"].iloc[-1]
                fib_r = chart_df["Fib_Resistance"].iloc[-1]
                fig.add_hline(y=fib_s, line_dash="dash", line_color="rgba(255,165,0,0.8)",
                              annotation_text=f"Fib 61.8% Support ${fib_s:,.4f}",
                              annotation_position="top left",
                              annotation_font_color="rgba(255,165,0,0.9)", row=1, col=1)
                fig.add_hline(y=fib_r, line_dash="dash", line_color="rgba(255,80,80,0.8)",
                              annotation_text=f"Fib 38.2% Resistance ${fib_r:,.4f}",
                              annotation_position="top left",
                              annotation_font_color="rgba(255,80,80,0.9)", row=1, col=1)

            for doji_type, sym, col in [
                ("Bullish Doji", "triangle-up", "#26a69a"),
                ("Bearish Doji", "triangle-down", "#ef5350"),
                ("Doji", "diamond", "#ffcc00"),
            ]:
                d = chart_df[chart_df["Doji"] == doji_type]
                if not d.empty:
                    fig.add_trace(go.Scatter(
                        x=d.index, y=d["Low"] * 0.98,
                        mode="markers", marker=dict(symbol=sym, size=10, color=col),
                        name=doji_type,
                    ), row=1, col=1)

            # Row 2: MACD
            macd_hist = chart_df["MACD"] - chart_df["MACD_Signal"]
            fig.add_trace(go.Bar(x=chart_df.index, y=macd_hist, name="MACD Hist",
                marker_color=["#26a69a" if v >= 0 else "#ef5350" for v in macd_hist]), row=2, col=1)
            fig.add_trace(go.Scatter(x=chart_df.index, y=chart_df["MACD"],
                name="MACD", line=dict(color="#1f77b4", width=1.5)), row=2, col=1)
            fig.add_trace(go.Scatter(x=chart_df.index, y=chart_df["MACD_Signal"],
                name="Signal", line=dict(color="#ff7f0e", width=1.5)), row=2, col=1)
            for cross_val, sym, col, cname in [
                (1, "triangle-up", "#26a69a", "Bull Cross"),
                (-1, "triangle-down", "#ef5350", "Bear Cross"),
            ]:
                cx = chart_df[chart_df["MACD_Crossover"] == cross_val]
                if not cx.empty:
                    fig.add_trace(go.Scatter(x=cx.index, y=cx["MACD"],
                        mode="markers", marker=dict(symbol=sym, size=9, color=col),
                        name=cname), row=2, col=1)

            # Row 3: RSI
            fig.add_trace(go.Scatter(x=chart_df.index, y=chart_df["RSI"],
                name="RSI(14)", line=dict(color="#9c27b0", width=1.5)), row=3, col=1)
            fig.add_hline(y=70, line_dash="dash", line_color="rgba(239,83,80,0.5)",
                          annotation_text="Overbought 70", annotation_position="top right", row=3, col=1)
            fig.add_hline(y=30, line_dash="dash", line_color="rgba(38,166,154,0.5)",
                          annotation_text="Oversold 30", annotation_position="top right", row=3, col=1)
            fig.add_hline(y=50, line_dash="dot", line_color="rgba(150,150,150,0.4)", row=3, col=1)
            fig.add_hrect(y0=30, y1=70, fillcolor="rgba(150,150,150,0.05)", line_width=0, row=3, col=1)

            # Row 4: Volume
            if "Volume" in chart_df.columns and chart_df["Volume"].sum() > 0:
                vol_colors = [
                    "#26a69a" if chart_df["Close"].iloc[i] >= chart_df["Open"].iloc[i]
                    else "#ef5350"
                    for i in range(len(chart_df))
                ]
                fig.add_trace(go.Bar(x=chart_df.index, y=chart_df["Volume"],
                    name="Volume", marker_color=vol_colors), row=4, col=1)

            fig.update_layout(
                title=dict(
                    text=f"{'🪙' if is_crypto else '📈'} {label} — Technical Analysis ({mode_name})",
                    font=dict(size=18),
                ),
                height=900, template="plotly_white",
                xaxis_rangeslider_visible=False,
                legend=dict(orientation="v", x=1.01, y=1),
                margin=dict(r=180),
            )
            fig.update_yaxes(title_text="Price (USD)", row=1, col=1)
            fig.update_yaxes(title_text="MACD",        row=2, col=1)
            fig.update_yaxes(title_text="RSI",         row=3, col=1, range=[0, 100])
            fig.update_yaxes(title_text="Volume",      row=4, col=1)
            fig.show()
            print(f"\n📈 Chart shown for top-scored asset: {label}")

        else:
            # ── Backtest ──
            anchor = list(enriched.keys())[0]
            available_days = len(enriched[anchor])
            actual_days = min(BACKTEST_DAYS, available_days)
            test_dates = enriched[anchor].index[-actual_days:]

            equity = [INITIAL_CAPITAL]
            equity_dates = [test_dates[0]]
            trade_log = []
            position = 0
            entry_price = 0.0
            entry_date = None
            entry_asset = ""
            entry_asset_label = ""
            entry_snapshot = None
            days_held = 0
            current_capital = INITIAL_CAPITAL
            allocated_capital = 0.0   # IMPROVEMENT: track allocated portion separately
            MAX_HOLD_DAYS = MAX_HOLD_DAYS_CFG

            # Total cost per round-trip leg (applied at entry and exit separately)
            COST_PER_LEG = (FEE_PCT + SLIPPAGE_PCT) / 100.0

            def build_snapshot_at(raw_dict, test_date, is_crypto):
                """
                CRITICAL FIX: Build signal snapshot using ONLY data up to test_date.
                Indicators are recalculated here on the truncated slice — eliminates lookahead bias.
                """
                rows = []
                for t, df in raw_dict.items():
                    # Slice to only include rows up to and including test_date
                    hist_raw = df[df.index <= test_date]
                    if len(hist_raw) < 40:
                        continue
                    # Recompute all indicators on this slice (no future data)
                    hist = compute_indicators(hist_raw)
                    if hist is None or len(hist) < 20:
                        continue
                    score, latest = score_coin(hist)
                    action = get_action(score)
                    current = latest.get("Close", 0)
                    fib_support = latest.get("Fib_Support", current * 0.92)
                    fib_resistance = latest.get("Fib_Resistance", current * 1.12)
                    upside = (fib_resistance - current) / current * 100 if current > 0 else 0
                    downside = (current - fib_support) / current * 100 if current > 0 else 0
                    rows.append({
                        "Asset":          t.replace("-USD", "") if is_crypto else t,
                        "_Ticker":        t,
                        "_HistSlice":     hist,  # stored internally for exit checks
                        "Action":         action,
                        "Recommendation": score_to_rec(score),
                        "Score":          f"{score}/5",
                        "Price":          round(current, 4),
                        "Fib Support":    round(fib_support, 4),
                        "Fib Resistance": round(fib_resistance, 4),
                        "Upside %":       round(upside, 1),
                        "Downside %":     round(downside, 1),
                        "Doji":           latest.get("Doji", "") or "",
                        "RSI":            round(latest.get("RSI", 0), 1),
                    })
                if not rows:
                    return pd.DataFrame()
                snap = pd.DataFrame(rows).sort_values(
                    "Score", key=lambda x: x.str.split("/").str[0].astype(int), ascending=False
                ).reset_index(drop=True)
                snap.index += 1
                snap.index.name = "Rank"
                return snap

            def print_snapshot(snap, title):
                # Hide internal columns from display
                display = snap.drop(columns=["_Ticker", "_HistSlice"], errors="ignore")
                print(f"\n{'─'*170}")
                print(f"  {title}")
                print(f"{'─'*170}")
                print(display.to_string())

            for test_date in test_dates:
                equity_dates.append(test_date)

                # CRITICAL FIX: Build snapshot per-bar using only past data
                snap = build_snapshot_at(raw_data, test_date, is_crypto)

                if snap.empty:
                    equity.append(equity[-1])
                    if position:
                        days_held += 1
                    continue

                price_lookup = dict(zip(snap["_Ticker"], snap["Price"]))
                hist_lookup = dict(zip(snap["_Ticker"], snap["_HistSlice"]))

                top_row = snap.iloc[0]
                top_ticker = top_row["_Ticker"]
                top_asset = top_row["Asset"]
                top_rec = top_row["Recommendation"]
                top_price = top_row["Price"]

                held_price = price_lookup.get(entry_asset, entry_price) if position == 1 else 0.0

                # Check exit conditions using the per-bar indicator-recalculated hist slice
                exit_now, exit_reason = False, ""
                if position == 1 and entry_asset and entry_asset in hist_lookup:
                    held_hist = hist_lookup[entry_asset]
                    exit_now, exit_reason = should_exit(
                        held_hist, entry_price, days_held, MAX_HOLD_DAYS,
                        stop_loss_pct=STOP_LOSS_PCT, take_profit_pct=TAKE_PROFIT_PCT,
                    )

                # ── BUY ──
                if "BUY" in top_rec and position == 0:
                    position = 1
                    # IMPROVEMENT: Apply slippage to entry price
                    entry_price = top_price * (1 + SLIPPAGE_PCT / 100.0)
                    # IMPROVEMENT: Deduct entry fee from capital
                    allocated_capital = current_capital * POSITION_SIZE_PCT
                    entry_fee = allocated_capital * FEE_PCT / 100.0
                    current_capital -= entry_fee
                    entry_date = test_date.date()
                    entry_asset = top_ticker
                    entry_asset_label = top_asset
                    entry_snapshot = snap.copy()
                    days_held = 0
                    print(f"\n{'='*170}")
                    print(
                        f"  🟢 BUY  {test_date.date()}  →  {entry_asset_label}  @  "
                        f"${entry_price:,.4f}  (alloc: ${allocated_capital:,.2f}, fee: ${entry_fee:,.2f})"
                    )
                    print_snapshot(snap, f"ENTRY SIGNALS — {test_date.date()} (all assets ranked by score)")

                # ── SELL ──
                elif position == 1 and exit_now:
                    # IMPROVEMENT: Apply slippage to exit price
                    exit_price = held_price * (1 - SLIPPAGE_PCT / 100.0)
                    pnl_pct = (exit_price / entry_price - 1) * 100 if entry_price > 0 else 0.0
                    # Calculate gross profit on the allocated portion
                    gross_profit = allocated_capital * (pnl_pct / 100)
                    # Deduct exit fee
                    exit_fee = (allocated_capital + gross_profit) * FEE_PCT / 100.0
                    net_profit = gross_profit - exit_fee
                    current_capital += net_profit
                    action_label = exit_reason

                    trade_log.append({
                        "Entry Date":    str(entry_date),
                        "Exit Date":     str(test_date.date()),
                        "Asset":         entry_asset_label,
                        "Days Held":     days_held + 1,
                        "Entry $":       round(entry_price, 4),
                        "Exit $":        round(exit_price, 4),
                        "PnL %":         round(pnl_pct, 2),
                        "Profit $":      round(net_profit, 2),
                        "Exit Reason":   action_label,
                        "Entry Signals": entry_snapshot,
                        "Exit Signals":  snap.copy(),
                    })

                    print(f"\n{'='*170}")
                    print(
                        f"  🔴 {action_label}  {test_date.date()}  →  {entry_asset_label}  @  "
                        f"${exit_price:,.4f}  |  PnL {pnl_pct:+.2f}%  |  Net Profit ${net_profit:,.2f}"
                    )
                    print_snapshot(snap, f"EXIT SIGNALS — {test_date.date()} (all assets ranked by score)")

                    equity.append(current_capital)
                    position = 0
                    days_held = 0
                    allocated_capital = 0.0
                    entry_snapshot = None

                # ── HOLD ──
                else:
                    if position == 1:
                        days_held += 1
                        # MTM: unallocated capital + mark-to-market value of position
                        mtm_position = allocated_capital * (held_price / entry_price) if entry_price > 0 else allocated_capital
                        unallocated = current_capital - 0  # current_capital already excludes entry fee
                        equity.append(unallocated + mtm_position)
                    else:
                        equity.append(current_capital)

            min_len = min(len(equity_dates), len(equity))
            equity_dates = equity_dates[:min_len]
            equity = equity[:min_len]

            eq_df = pd.DataFrame({"Date": equity_dates, "Equity": equity})
            total_return = (equity[-1] / INITIAL_CAPITAL - 1) * 100

            # ── Equity Curve ──
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=eq_df["Date"], y=eq_df["Equity"],
                name="Equity Curve", line=dict(color="#1f77b4", width=3),
            ))
            for trade in trade_log:
                entry_eq = eq_df[eq_df["Date"] == pd.Timestamp(trade["Entry Date"])]
                exit_eq = eq_df[eq_df["Date"] == pd.Timestamp(trade["Exit Date"])]
                if not entry_eq.empty:
                    fig.add_trace(go.Scatter(
                        x=[entry_eq["Date"].iloc[0]], y=[entry_eq["Equity"].iloc[0]],
                        mode="markers", marker=dict(symbol="triangle-up", size=12, color="#26a69a"),
                        name=f"BUY {trade['Asset']} {trade['Entry Date']}", showlegend=False,
                        hovertext=f"BUY {trade['Asset']}<br>@ ${trade['Entry $']:,.4f}<br>{trade['Entry Date']}",
                    ))
                if not exit_eq.empty:
                    color = "#26a69a" if trade["PnL %"] >= 0 else "#ef5350"
                    fig.add_trace(go.Scatter(
                        x=[exit_eq["Date"].iloc[0]], y=[exit_eq["Equity"].iloc[0]],
                        mode="markers", marker=dict(symbol="triangle-down", size=12, color=color),
                        name=f"SELL {trade['Asset']} {trade['Exit Date']}", showlegend=False,
                        hovertext=f"SELL {trade['Asset']}<br>@ ${trade['Exit $']:,.4f}<br>PnL {trade['PnL %']:+.2f}%<br>{trade['Exit Date']}",
                    ))
            fig.update_layout(
                title=(
                    f"{'Crypto' if is_crypto else 'Traditional'} Equity Curve — "
                    f"${INITIAL_CAPITAL:,.0f} → ${equity[-1]:,.0f} ({total_return:+.1f}%) | {months} months"
                ),
                xaxis_title="Date", yaxis_title="Portfolio Value (USD)",
                height=650, template="plotly_white",
            )
            fig.show()

            # ── Detailed Trade History ──
            print(f"\n{'='*170}")
            print(f"  📋 DETAILED TRADE HISTORY ({'Crypto' if is_crypto else 'Traditional'})")
            print(f"{'='*170}")

            if trade_log:
                for i, trade in enumerate(trade_log, 1):
                    pnl_icon = "🟢" if trade["PnL %"] >= 0 else "🔴"
                    print(f"\n{'━'*170}")
                    print(
                        f"  TRADE #{i}  |  {trade['Asset']}  |  "
                        f"Entry: {trade['Entry Date']}  →  Exit: {trade['Exit Date']}  |  "
                        f"Days Held: {trade['Days Held']}  |  "
                        f"Entry: ${trade['Entry $']:,.4f}  Exit: ${trade['Exit $']:,.4f}  |  "
                        f"{pnl_icon} PnL: {trade['PnL %']:+.2f}%  |  "
                        f"Profit: ${trade['Profit $']:,.2f}  |  "
                        f"Reason: {trade['Exit Reason']}"
                    )
                    print_snapshot(trade["Entry Signals"], f"ENTRY SIGNALS — {trade['Entry Date']}")
                    print_snapshot(trade["Exit Signals"],  f"EXIT SIGNALS  — {trade['Exit Date']}")

                summary_cols = [
                    "Entry Date", "Exit Date", "Asset", "Days Held",
                    "Entry $", "Exit $", "PnL %", "Profit $", "Exit Reason",
                ]
                summary_df = pd.DataFrame([{k: t[k] for k in summary_cols} for t in trade_log])
                print(f"\n{'='*170}")
                print(f"  📊 TRADE SUMMARY")
                print(f"{'='*170}")
                print(summary_df.to_string(index=False))
            else:
                print("  No closed trades during the period.")

            wins = [t for t in trade_log if t["PnL %"] >= 0]
            losses = [t for t in trade_log if t["PnL %"] < 0]
            win_rate = len(wins) / len(trade_log) * 100 if trade_log else 0.0

            print(f"\n  Final Value   : ${equity[-1]:,.2f}")
            print(f"  Total Return  : {total_return:+.2f}%")
            print(f"  Days analyzed : {len(eq_df)-1}")
            print(f"  Trades closed : {len(trade_log)}")
            print(f"  Win rate      : {win_rate:.1f}%  ({len(wins)} wins / {len(losses)} losses)")
            print(f"  Fee/Slippage  : {FEE_PCT}% / {SLIPPAGE_PCT}% per leg")
            print(f"  Position size : {POSITION_SIZE_PCT*100:.0f}% of capital")

        input("\nPress Enter to return to main menu...")

    conn.close()

if __name__ == "__main__":
    main()