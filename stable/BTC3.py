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

print("🚀 Crypto & Traditional Risk Dashboard (Consistent Flow v36)\n")

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
    if df is None or df.empty or len(df) < 30:
        return None
    df = df.copy()
    # BUG FIX 1: tz_localize(None) fails if index already has no tz;
    # use tz_convert + tz_localize, or just strip tz safely.
    if df.index.tz is not None:
        df.index = df.index.tz_convert(None)
    else:
        df.index = pd.to_datetime(df.index)
    df = df[(df["Close"] > 0.000001) & (df["Close"] < 10_000_000)]
    if len(df) > 10:
        median_close = df["Close"].median()
        df = df[(df["Close"] <= median_close * 20) & (df["Close"] >= median_close * 0.05)]
    today = pd.Timestamp.now().normalize()
    df = df[df.index <= today]
    return df if len(df) >= 30 else None

# ====================== BINANCE ======================
def fetch_from_binance(ticker):
    symbol = ticker.replace("-USD", "USDT") if "-USD" in ticker else ticker + "USDT"
    try:
        url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1d&limit=1000"
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
        if not data: return None
        df = pd.DataFrame(data, columns=['open_time','Open','High','Low','Close','Volume','close_time','qav','n','tav','tbv','ignore'])
        df = df[['Open','High','Low','Close','Volume']].astype(float)
        # BUG FIX 2: df.index is a RangeIndex (0,1,2...) here, not timestamps.
        # Must use the 'open_time' column values from the raw data instead.
        open_times = [row[0] for row in data]
        df.index = pd.to_datetime(open_times, unit='ms')
        return validate_and_clean(df, ticker)
    except:
        return None

# ====================== CACHE ======================
def get_cached(ticker):
    df = pd.read_sql_query("SELECT * FROM price_data WHERE ticker=?", conn, params=[ticker])
    if df.empty: return None
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)
    df = df.sort_index()
    # BUG FIX 3: After set_index("date"), "ticker" column is still present.
    # Drop it before capitalizing column names to avoid "Ticker" leftover column.
    df = df.drop(columns=["ticker"], errors="ignore")
    df.columns = [c.capitalize() for c in df.columns]
    return df

def save_to_cache(ticker, df):
    if df is None or df.empty: return
    df = validate_and_clean(df, ticker)
    if df is None: return
    conn.execute("DELETE FROM price_data WHERE ticker=?", (ticker,))
    conn.commit()
    temp = df.copy()
    temp["ticker"] = ticker
    temp["date"] = temp.index.strftime("%Y-%m-%d")
    # BUG FIX 4: Only select columns that exist; Volume may be missing for some assets.
    cols = [c for c in ["ticker", "date", "Open", "High", "Low", "Close", "Volume"] if c in temp.columns]
    temp = temp[cols]
    temp.to_sql("price_data", conn, if_exists="append", index=False)

def fetch_with_cache(ticker, period="24mo"):
    cached = get_cached(ticker)
    if cached is not None and len(cached) >= 60:
        print(f"   📦 Cache hit: {ticker}")
        return cached

    if "-USD" in ticker:
        df = fetch_from_binance(ticker)
        if df is not None and len(df) >= 30:
            print(f"   ✓ Binance data for {ticker}")
            save_to_cache(ticker, df)
            return df

    print(f"   ⬇️ yfinance: {ticker}")
    try:
        df = yf.download(ticker, period=period, interval="1d", progress=False)
        if df.empty: return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df.columns = [c.capitalize() for c in df.columns]
        if "Close" not in df.columns: return None
        df["Volume"] = pd.to_numeric(df.get("Volume", 0), errors="coerce").fillna(0).astype("int64")
        df = validate_and_clean(df, ticker)
        if df is not None:
            save_to_cache(ticker, df)
        return df
    except Exception as e:
        print(f"   ⚠️ yfinance failed: {e}")
        return None

# ====================== HELPERS ======================
def fetch_top_coins(n):
    print(f"📡 Fetching top {n} coins...")
    try:
        r = requests.get(
            "https://api.coingecko.com/api/v3/coins/markets",
            headers={"x-cg-demo-api-key": COINGECKO_KEY},
            params={"vs_currency":"usd", "order":"market_cap_desc", "per_page":n, "page":1},
            timeout=15
        )
        r.raise_for_status()
        tickers = [f"{coin['symbol'].upper()}-USD" for coin in r.json() 
                  if coin.get('symbol','').upper() not in ["USDT","USDC","DAI","FDUSD","TUSD","USDE","BUSD"]]
        print(f"✅ Got {len(tickers)} valid tickers.")
    except:
        tickers = []
    MAJOR = ["BTC-USD","ETH-USD","BNB-USD","SOL-USD","XRP-USD","DOGE-USD","TON-USD","ADA-USD","TRX-USD","AVAX-USD"]
    final = tickers[:n]
    if len(final) < n:
        for t in MAJOR:
            if t not in final and len(final) < n:
                final.append(t)
        # BUG FIX 5: Padding count was wrong — used len(tickers) instead of len(final before padding).
        print(f"   Padded with {len(final) - len(tickers[:n])} coins.")
    return final

TRADITIONAL_TOP = {
    "1": ["^AXJO", "BHP.AX", "CBA.AX", "RIO.AX", "CSL.AX", "WBC.AX", "NAB.AX", "MQG.AX", "FMG.AX", "WES.AX"],  # Australia
    "2": ["^GSPC", "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "BRK-B", "TSLA", "JPM"],                  # US
    "3": ["^FTSE", "HSBA.L", "AZN.L", "SHEL.L", "ULVR.L", "BP.L", "GSK.L", "RIO.L", "BHP.L", "VOD.L"],       # UK
    "4": ["^STOXX50E", "MC.PA", "ASML.AS", "SAP.DE", "AIR.PA", "SIE.DE", "SU.PA", "RMS.PA", "TTE.PA", "ALV.DE"], # Europe
    "5": ["^GSPTSE", "RY.TO", "TD.TO", "ENB.TO", "CNR.TO", "BN.TO", "BCE.TO", "SHOP.TO", "CP.TO", "CNQ.TO"], # Canada
}

def get_traditional_suffix(country):
    return {"1":".AX", "2":"", "3":".L", "4":".PA", "5":".TO", "6":""}.get(country, "")

# ====================== INDICATORS & SCORING ======================
def add_indicators(data_dict):
    enriched = {}
    for ticker, df in data_dict.items():
        if df is None or "Close" not in df.columns: continue
        df = df.copy()

        delta = df["Close"].diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta).clip(lower=0).rolling(14).mean()
        rs = gain / loss.where(loss != 0, 1e-10)
        df["RSI"] = 100 - (100 / (1 + rs))

        ema12 = df["Close"].ewm(span=12, adjust=False).mean()
        ema26 = df["Close"].ewm(span=26, adjust=False).mean()
        df["MACD"] = ema12 - ema26
        df["MACD_Signal"] = df["MACD"].ewm(span=9, adjust=False).mean()

        sma20 = df["Close"].rolling(20).mean()
        std20 = df["Close"].rolling(20).std()
        df["BB_Upper"] = sma20 + 2*std20
        df["BB_Middle"] = sma20
        df["BB_Lower"] = sma20 - 2*std20

        df["MACD_Crossover"] = 0
        bull = (df["MACD"].shift(1) <= df["MACD_Signal"].shift(1)) & (df["MACD"] > df["MACD_Signal"])
        bear = (df["MACD"].shift(1) >= df["MACD_Signal"].shift(1)) & (df["MACD"] < df["MACD_Signal"])
        df.loc[bull, "MACD_Crossover"] = 1
        df.loc[bear, "MACD_Crossover"] = -1

        recent = df.tail(180)
        if len(recent) >= 60:
            high = recent["Close"].max()
            low = recent["Close"].min()
            diff = high - low
            # BUG FIX 6: diff could be 0 if all prices are identical, causing NaN fib levels.
            if diff > 0:
                fib_support = high - 0.618 * diff
                fib_resistance = high - 0.382 * diff
                current = df["Close"].iloc[-1]
                if current < fib_support:
                    fib_support = low * 0.95
                df["Fib_Support"] = fib_support
                df["Fib_Resistance"] = fib_resistance
            else:
                df["Fib_Support"] = high * 0.92
                df["Fib_Resistance"] = high * 1.08

        df["Body"] = abs(df["Close"] - df["Open"])
        df["Range"] = df["High"] - df["Low"]
        df["Upper_Wick"] = df["High"] - df[["Open","Close"]].max(axis=1)
        df["Lower_Wick"] = df[["Open","Close"]].min(axis=1) - df["Low"]

        df["Doji"] = ""
        doji_mask = (df["Body"] / df["Range"].replace(0, np.nan) < 0.25) & (df["Range"] > 0)
        df.loc[doji_mask & (df["Lower_Wick"] > 2 * df["Body"]), "Doji"] = "Bullish Doji"
        df.loc[doji_mask & (df["Upper_Wick"] > 2 * df["Body"]), "Doji"] = "Bearish Doji"
        # BUG FIX 7: The combined "Doji" label must be applied AFTER the two directional ones,
        # and only when BOTH wicks are large (true Doji, not Hammer/Shooting Star).
        df.loc[doji_mask & (df["Upper_Wick"] > 2 * df["Body"]) & (df["Lower_Wick"] > 2 * df["Body"]), "Doji"] = "Doji"

        df = df.dropna(subset=["RSI", "BB_Middle"])
        if len(df) >= 30:
            enriched[ticker] = df
    return enriched

def score_coin(df):
    """
    5-component scoring system. Range: -5 to +5.
      1. Volatility     : +1 if low (<2%), -1 if high (>3.5%)
      2. RSI            : +1 if oversold (<35), -1 if overbought (>68)
      3. MACD Crossover : +1 bull cross, -1 bear cross, 0 none
      4. Bollinger Band : +1 below mid, -1 above upper
      5. Fibonacci      : +1 near/below Fib support, -1 near/above Fib resistance
    """
    latest = df.iloc[-1]
    close  = latest.get("Close", 0)

    # 1. Volatility
    vol = df["Close"].pct_change().abs().mean() * 100
    s = -1 if vol > 3.5 else (1 if vol < 2.0 else 0)

    # 2. RSI
    s += -1 if latest.get("RSI", 50) > 68 else (1 if latest.get("RSI", 50) < 35 else 0)

    # 3. MACD Crossover
    s += int(latest.get("MACD_Crossover", 0))

    # 4. Bollinger Bands
    s += 1 if close < latest.get("BB_Middle", 0) * 0.98 else (-1 if close > latest.get("BB_Upper", 0) * 1.02 else 0)

    # 5. Fibonacci position
    fib_s = latest.get("Fib_Support", 0)
    fib_r = latest.get("Fib_Resistance", 0)
    if fib_s > 0 and fib_r > 0:
        if close <= fib_s * 1.02:    # at or below Fib support — bullish
            s += 1
        elif close >= fib_r * 0.98:  # at or above Fib resistance — bearish
            s -= 1

    return s, latest

def score_to_rec(s):
    # Thresholds scaled for -5 to +5 range
    if s >= 4:   return "🟢 STRONG BUY"
    elif s >= 2: return "🟢 BUY"
    elif s >= 0: return "🟠 HOLD"
    else:        return "🔴 SELL"

def get_action(s):
    # Thresholds scaled for -5 to +5 range
    if s >= 3:   return "BUY"
    elif s <= -1: return "SELL"
    else:         return "HOLD"

def should_exit(hist, entry_price, days_held, max_hold_days,
                stop_loss_pct=8.0, take_profit_pct=20.0):
    """
    Multi-condition exit — checked in priority order:
      1. Stop-loss   : price fell more than stop_loss_pct % below entry
      2. Take-profit : price rose more than take_profit_pct % above entry
      3. Signal SELL : composite score <= -1 (MACD bear cross, RSI overbought, etc.)
      4. Max hold    : safety valve — exits after max_hold_days regardless
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
    if score <= -1:   # one or more bearish signals active
        return True, "SIGNAL SELL"
    if days_held >= max_hold_days - 1:
        return True, f"MAX HOLD ({max_hold_days}d safety valve — no exit signal fired)"
    return False, ""

# ====================== MAIN ======================
def main():
    while True:
        print("\n" + "="*100)
        print("MAIN MENU - Consistent Flow")
        print("="*100)
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

        # Initial Capital (same for both)
        try:
            init_input = input(f"\nInitial Capital USD (default 10000): $").strip()
            INITIAL_CAPITAL = float(init_input) if init_input else 10000.0
        except:
            INITIAL_CAPITAL = 10000.0
        print(f"Using Initial Capital: ${INITIAL_CAPITAL:,.2f}\n")

        # Mode: Live or Backtest (same for both)
        print("Select mode:")
        print("1. Live Dashboard")
        print("2. Backtest + Equity Curve")
        mode_choice = input("\nEnter 1 or 2: ").strip()
        is_backtest = mode_choice == "2"

        # Backtest lookback period (only if backtest)
        months = 12
        # BUG FIX 8: BACKTEST_DAYS was only defined inside the if-block but referenced
        # later outside it, causing UnboundLocalError when backtest is selected.
        BACKTEST_DAYS = months * 30
        if is_backtest:
            print("\nBacktest lookback period:")
            print("1. 6 months   2. 12 months   3. 18 months   4. 24 months")
            period_choice = input("\nEnter 1-4: ").strip()
            months = {"1":6, "2":12, "3":18, "4":24}.get(period_choice, 12)
            BACKTEST_DAYS = months * 30
            print(f"✅ Backtest set to last {months} months.\n")

            # ---- Exit rule parameters ----
            print("Exit rules (press Enter to use defaults):")
            try:
                sl_in = input("  Stop-loss %    (default  8%): ").strip()
                STOP_LOSS_PCT = float(sl_in) if sl_in else 8.0
            except: STOP_LOSS_PCT = 8.0
            try:
                tp_in = input("  Take-profit %  (default 20%): ").strip()
                TAKE_PROFIT_PCT = float(tp_in) if tp_in else 20.0
            except: TAKE_PROFIT_PCT = 20.0
            try:
                mh_in = input("  Max hold days  (default 45d): ").strip()
                MAX_HOLD_DAYS_CFG = int(mh_in) if mh_in else 45
            except: MAX_HOLD_DAYS_CFG = 45
            print(f"✅ Exit rules — SL: {STOP_LOSS_PCT}%  TP: {TAKE_PROFIT_PCT}%  Max hold: {MAX_HOLD_DAYS_CFG}d\n")

        if not is_backtest:
            STOP_LOSS_PCT     = 8.0
            TAKE_PROFIT_PCT   = 20.0
            MAX_HOLD_DAYS_CFG = 45

        # ====================== ASSET SELECTION ======================
        if is_crypto:
            # Crypto asset selection
            while True:
                print("Number of top coins:")
                print("1. Top 10   2. Top 20   3. Top 50   4. Top 100")
                nc = input("\nEnter 1-4: ").strip()
                if nc in ["1","2","3","4"]:
                    requested_n = {"1":10, "2":20, "3":50, "4":100}[nc]
                    break
                print("❌ Please enter 1-4.")
            final_tickers = fetch_top_coins(requested_n)
        else:
            # Traditional asset selection
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
                if suffix and not any(t.endswith(s) for s in [".AX",".L",".PA",".TO",".DE",".AS"]) and not t.startswith("^"):
                    # BUG FIX 9: European tickers (e.g. ASML.AS, SAP.DE) were getting
                    # the country suffix appended again (e.g. ASML.AS.PA). Added ".DE" and ".AS"
                    # to the exclusion list.
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
            # Live Dashboard (same for both crypto and traditional)
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
                    "RSI": round(latest.get("RSI", 0), 1)
                })

            df_out = pd.DataFrame(results).sort_values(
                "Score",
                key=lambda x: x.str.split('/').str[0].astype(int),
                ascending=False
            )
            # BUG FIX 10: `df_out.index += 1` on a RangeIndex doesn't work as intended
            # after sort_values (index is shuffled). Reset first, then add 1.
            df_out = df_out.reset_index(drop=True)
            df_out.index += 1
            df_out.index.name = "Rank"

            mode_name = "Crypto" if is_crypto else "Traditional Markets"
            print(f"\n{'='*150}")
            print(f"🏆 {mode_name} LIVE DASHBOARD")
            print("="*150)
            print(df_out.to_string())

            # ====================== RISK SCORE BREAKDOWN ======================
            print(f"\n{'='*150}")
            print(f"📊 RISK SCORE BREAKDOWN")
            print("="*150)
            risk_rows = []
            for t, df in enriched.items():
                score, latest = score_coin(df)
                vol = df["Close"].pct_change().abs().mean() * 100
                vol_score  = -1 if vol > 3.5 else (1 if vol < 2.0 else 0)
                rsi_val    = latest.get("RSI", 50)
                rsi_score  = -1 if rsi_val > 68 else (1 if rsi_val < 35 else 0)
                macd_score = int(latest.get("MACD_Crossover", 0))
                close      = latest.get("Close", 0)
                bb_mid     = latest.get("BB_Middle", 0)
                bb_upper   = latest.get("BB_Upper", 0)
                bb_score   = 1 if close < bb_mid * 0.98 else (-1 if close > bb_upper * 1.02 else 0)
                macd_val   = latest.get("MACD", 0)
                macd_sig   = latest.get("MACD_Signal", 0)
                fib_s_val  = latest.get("Fib_Support", 0)
                fib_r_val  = latest.get("Fib_Resistance", 0)
                close_val  = latest.get("Close", 0)
                if fib_s_val > 0 and fib_r_val > 0:
                    if close_val <= fib_s_val * 1.02:   fib_score = 1
                    elif close_val >= fib_r_val * 0.98: fib_score = -1
                    else:                                fib_score = 0
                else:
                    fib_score = 0
                risk_rows.append({
                    "Asset":       t.replace("-USD", ""),
                    "Total Score": f"{score}/5",
                    "Volatility":  f"{vol:.2f}% ({'Low' if vol_score==1 else ('High' if vol_score==-1 else 'Med')})",
                    "RSI":         f"{rsi_val:.1f} ({'OS' if rsi_score==1 else ('OB' if rsi_score==-1 else 'Neutral')})",
                    "MACD":        f"{macd_val:.4f}/Sig {macd_sig:.4f} ({'BullX' if macd_score==1 else ('BearX' if macd_score==-1 else 'None')})",
                    "BB Position": f"{'Below' if bb_score==1 else ('Above' if bb_score==-1 else 'Inside')}",
                    "Fibonacci":   f"{'Near Support' if fib_score==1 else ('Near Resistance' if fib_score==-1 else 'Between')}",
                    "Doji":        latest.get("Doji", "") or "—",
                })
            risk_df = pd.DataFrame(risk_rows).sort_values(
                "Total Score", key=lambda x: x.str.split('/').str[0].astype(int), ascending=False
            ).reset_index(drop=True)
            risk_df.index += 1
            risk_df.index.name = "Rank"
            print(risk_df.to_string())

            # ====================== TOP ASSET TECHNICAL CHART ======================
            top_ticker  = max(enriched.keys(), key=lambda t: score_coin(enriched[t])[0])
            chart_df    = enriched[top_ticker].tail(180).copy()
            label       = top_ticker.replace("-USD", "") if is_crypto else top_ticker

            fig = make_subplots(
                rows=4, cols=1,
                shared_xaxes=True,
                row_heights=[0.45, 0.20, 0.20, 0.15],
                vertical_spacing=0.03,
                subplot_titles=(
                    f"{label} — Price, Bollinger Bands & Fibonacci",
                    "MACD",
                    "RSI (14)",
                    "Volume"
                )
            )

            # ── Row 1: Candlestick + BB + Fibonacci ──
            fig.add_trace(go.Candlestick(
                x=chart_df.index,
                open=chart_df["Open"], high=chart_df["High"],
                low=chart_df["Low"],  close=chart_df["Close"],
                name="Price",
                increasing_line_color="#26a69a", decreasing_line_color="#ef5350"
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

            # Doji candlestick markers
            for doji_type, sym, col in [("Bullish Doji","triangle-up","#26a69a"),
                                         ("Bearish Doji","triangle-down","#ef5350"),
                                         ("Doji","diamond","#ffcc00")]:
                d = chart_df[chart_df["Doji"] == doji_type]
                if not d.empty:
                    fig.add_trace(go.Scatter(
                        x=d.index, y=d["Low"] * 0.98,
                        mode="markers", marker=dict(symbol=sym, size=10, color=col),
                        name=doji_type
                    ), row=1, col=1)

            # ── Row 2: MACD ──
            macd_hist = chart_df["MACD"] - chart_df["MACD_Signal"]
            fig.add_trace(go.Bar(x=chart_df.index, y=macd_hist, name="MACD Hist",
                marker_color=["#26a69a" if v >= 0 else "#ef5350" for v in macd_hist]), row=2, col=1)
            fig.add_trace(go.Scatter(x=chart_df.index, y=chart_df["MACD"],
                name="MACD", line=dict(color="#1f77b4", width=1.5)), row=2, col=1)
            fig.add_trace(go.Scatter(x=chart_df.index, y=chart_df["MACD_Signal"],
                name="Signal", line=dict(color="#ff7f0e", width=1.5)), row=2, col=1)

            for cross_val, sym, col, cname in [(1,"triangle-up","#26a69a","Bull Cross"),
                                                (-1,"triangle-down","#ef5350","Bear Cross")]:
                cx = chart_df[chart_df["MACD_Crossover"] == cross_val]
                if not cx.empty:
                    fig.add_trace(go.Scatter(x=cx.index, y=cx["MACD"],
                        mode="markers", marker=dict(symbol=sym, size=9, color=col),
                        name=cname), row=2, col=1)

            # ── Row 3: RSI ──
            fig.add_trace(go.Scatter(x=chart_df.index, y=chart_df["RSI"],
                name="RSI(14)", line=dict(color="#9c27b0", width=1.5)), row=3, col=1)
            fig.add_hline(y=70, line_dash="dash", line_color="rgba(239,83,80,0.5)",
                          annotation_text="Overbought 70", annotation_position="top right", row=3, col=1)
            fig.add_hline(y=30, line_dash="dash", line_color="rgba(38,166,154,0.5)",
                          annotation_text="Oversold 30", annotation_position="top right", row=3, col=1)
            fig.add_hline(y=50, line_dash="dot", line_color="rgba(150,150,150,0.4)", row=3, col=1)
            fig.add_hrect(y0=30, y1=70, fillcolor="rgba(150,150,150,0.05)", line_width=0, row=3, col=1)

            # ── Row 4: Volume ──
            if "Volume" in chart_df.columns and chart_df["Volume"].sum() > 0:
                vol_colors = ["#26a69a" if chart_df["Close"].iloc[i] >= chart_df["Open"].iloc[i]
                              else "#ef5350" for i in range(len(chart_df))]
                fig.add_trace(go.Bar(x=chart_df.index, y=chart_df["Volume"],
                    name="Volume", marker_color=vol_colors), row=4, col=1)

            fig.update_layout(
                title=dict(
                    text=f"{'🪙' if is_crypto else '📈'} {label} — Technical Analysis ({mode_name})",
                    font=dict(size=18)
                ),
                height=900,
                template="plotly_white",
                xaxis_rangeslider_visible=False,
                legend=dict(orientation="v", x=1.01, y=1),
                margin=dict(r=180)
            )
            fig.update_yaxes(title_text="Price (USD)", row=1, col=1)
            fig.update_yaxes(title_text="MACD",        row=2, col=1)
            fig.update_yaxes(title_text="RSI",         row=3, col=1, range=[0, 100])
            fig.update_yaxes(title_text="Volume",      row=4, col=1)
            fig.show()
            print(f"\n📈 Chart shown for top-scored asset: {label}")

        else:
            # Backtest (same for both)
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
            entry_snapshot = None   # full market snapshot DataFrame at entry
            days_held = 0
            current_capital = INITIAL_CAPITAL
            MAX_HOLD_DAYS = MAX_HOLD_DAYS_CFG

            def build_snapshot(enriched_dict, test_date, is_crypto):
                """Build a full signal snapshot for every asset at a given date."""
                rows = []
                for t, df in enriched_dict.items():
                    hist = df[df.index <= test_date]
                    if len(hist) < 20: continue
                    score, latest = score_coin(hist)
                    action = get_action(score)
                    current = latest.get("Close", 0)
                    fib_support    = latest.get("Fib_Support",    current * 0.92)
                    fib_resistance = latest.get("Fib_Resistance", current * 1.12)
                    upside   = (fib_resistance - current) / current * 100 if current > 0 else 0
                    downside = (current - fib_support)    / current * 100 if current > 0 else 0
                    rows.append({
                        "Asset":          t.replace("-USD", "") if is_crypto else t,
                        "_Ticker":        t,               # internal key, hidden in display
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
                    "Score", key=lambda x: x.str.split('/').str[0].astype(int), ascending=False
                ).reset_index(drop=True)
                snap.index += 1
                snap.index.name = "Rank"
                return snap

            def print_snapshot(snap, title):
                display = snap.drop(columns=["_Ticker"], errors="ignore")
                print(f"\n{'─'*170}")
                print(f"  {title}")
                print(f"{'─'*170}")
                print(display.to_string())

            for test_date in test_dates:
                equity_dates.append(test_date)

                # Build today's full snapshot (used for signals, price lookup, and logging)
                snap = build_snapshot(enriched, test_date, is_crypto)

                if snap.empty:
                    equity.append(equity[-1])
                    if position: days_held += 1
                    continue

                # Price lookup: full ticker → today's close
                price_lookup = dict(zip(snap["_Ticker"], snap["Price"]))

                # Top-ranked asset
                top_row    = snap.iloc[0]
                top_ticker = top_row["_Ticker"]
                top_asset  = top_row["Asset"]
                top_rec    = top_row["Recommendation"]
                top_price  = top_row["Price"]

                # Held asset current price
                held_price = price_lookup.get(entry_asset, entry_price) if position == 1 else 0.0

                # Check all exit conditions (SL, TP, signal, max-hold)
                exit_now, exit_reason = False, ""
                if position == 1 and entry_asset and entry_asset in enriched:
                    held_hist = enriched[entry_asset][enriched[entry_asset].index <= test_date]
                    exit_now, exit_reason = should_exit(
                        held_hist, entry_price, days_held, MAX_HOLD_DAYS,
                        stop_loss_pct=STOP_LOSS_PCT, take_profit_pct=TAKE_PROFIT_PCT
                    )

                # ── BUY ──
                if "BUY" in top_rec and position == 0:
                    position = 1
                    entry_price       = top_price
                    entry_date        = test_date.date()
                    entry_asset       = top_ticker
                    entry_asset_label = top_asset
                    entry_snapshot    = snap.copy()
                    days_held = 0
                    print(f"\n{'='*170}")
                    print(f"  🟢 BUY  {test_date.date()}  →  {entry_asset_label}  @  ${entry_price:,.4f}")
                    print_snapshot(snap, f"ENTRY SIGNALS — {test_date.date()} (all assets ranked by score)")

                # ── SELL ──
                elif position == 1 and exit_now:
                    exit_price = held_price
                    pnl_pct    = (exit_price / entry_price - 1) * 100 if entry_price > 0 else 0.0
                    profit     = current_capital * (pnl_pct / 100)
                    current_capital += profit
                    action_label = exit_reason

                    trade_log.append({
                        "Entry Date":    str(entry_date),
                        "Exit Date":     str(test_date.date()),
                        "Asset":         entry_asset_label,
                        "Days Held":     days_held + 1,
                        "Entry $":       round(entry_price, 4),
                        "Exit $":        round(exit_price, 4),
                        "PnL %":         round(pnl_pct, 2),
                        "Profit $":      round(profit, 2),
                        "Exit Reason":   action_label,
                        "Entry Signals": entry_snapshot,
                        "Exit Signals":  snap.copy(),
                    })

                    print(f"\n{'='*170}")
                    print(f"  🔴 {action_label}  {test_date.date()}  →  {entry_asset_label}  @  ${exit_price:,.4f}  |  PnL {pnl_pct:+.2f}%  |  Profit ${profit:,.2f}")
                    print_snapshot(snap, f"EXIT SIGNALS — {test_date.date()} (all assets ranked by score)")

                    equity.append(current_capital)
                    position  = 0
                    days_held = 0
                    entry_snapshot = None

                # ── HOLD ──
                else:
                    if position == 1:
                        days_held += 1
                        mtm = current_capital * (held_price / entry_price) if entry_price > 0 else current_capital
                        equity.append(mtm)
                    else:
                        equity.append(current_capital)

            min_len = min(len(equity_dates), len(equity))
            equity_dates = equity_dates[:min_len]
            equity       = equity[:min_len]

            eq_df        = pd.DataFrame({"Date": equity_dates, "Equity": equity})
            total_return = (equity[-1] / INITIAL_CAPITAL - 1) * 100

            # ── Equity Curve ──
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=eq_df["Date"], y=eq_df["Equity"],
                name="Equity Curve", line=dict(color="#1f77b4", width=3)
            ))
            # Mark trade entry/exit points on the curve
            for trade in trade_log:
                entry_eq = eq_df[eq_df["Date"] == pd.Timestamp(trade["Entry Date"])]
                exit_eq  = eq_df[eq_df["Date"] == pd.Timestamp(trade["Exit Date"])]
                if not entry_eq.empty:
                    fig.add_trace(go.Scatter(
                        x=[entry_eq["Date"].iloc[0]], y=[entry_eq["Equity"].iloc[0]],
                        mode="markers", marker=dict(symbol="triangle-up", size=12, color="#26a69a"),
                        name=f"BUY {trade['Asset']} {trade['Entry Date']}", showlegend=False,
                        hovertext=f"BUY {trade['Asset']}<br>@ ${trade['Entry $']:,.4f}<br>{trade['Entry Date']}"
                    ))
                if not exit_eq.empty:
                    color = "#26a69a" if trade["PnL %"] >= 0 else "#ef5350"
                    fig.add_trace(go.Scatter(
                        x=[exit_eq["Date"].iloc[0]], y=[exit_eq["Equity"].iloc[0]],
                        mode="markers", marker=dict(symbol="triangle-down", size=12, color=color),
                        name=f"SELL {trade['Asset']} {trade['Exit Date']}", showlegend=False,
                        hovertext=f"SELL {trade['Asset']}<br>@ ${trade['Exit $']:,.4f}<br>PnL {trade['PnL %']:+.2f}%<br>{trade['Exit Date']}"
                    ))
            fig.update_layout(
                title=f"{'Crypto' if is_crypto else 'Traditional'} Equity Curve — "
                      f"${INITIAL_CAPITAL:,.0f} → ${equity[-1]:,.0f} ({total_return:+.1f}%) | {months} months",
                xaxis_title="Date", yaxis_title="Portfolio Value (USD)",
                height=650, template="plotly_white"
            )
            fig.show()

            # ── Detailed Trade History with Signal Snapshots ──
            print(f"\n{'='*170}")
            print(f"  📋 DETAILED TRADE HISTORY ({'Crypto' if is_crypto else 'Traditional'})")
            print(f"{'='*170}")

            if trade_log:
                for i, trade in enumerate(trade_log, 1):
                    pnl_icon = "🟢" if trade["PnL %"] >= 0 else "🔴"
                    print(f"\n{'━'*170}")
                    print(f"  TRADE #{i}  |  {trade['Asset']}  |  "
                          f"Entry: {trade['Entry Date']}  →  Exit: {trade['Exit Date']}  |  "
                          f"Days Held: {trade['Days Held']}  |  "
                          f"Entry: ${trade['Entry $']:,.4f}  Exit: ${trade['Exit $']:,.4f}  |  "
                          f"{pnl_icon} PnL: {trade['PnL %']:+.2f}%  |  "
                          f"Profit: ${trade['Profit $']:,.2f}  |  "
                          f"Reason: {trade['Exit Reason']}")
                    print_snapshot(trade["Entry Signals"], f"ENTRY SIGNALS — {trade['Entry Date']}")
                    print_snapshot(trade["Exit Signals"],  f"EXIT SIGNALS  — {trade['Exit Date']}")

                # Summary table (no signal columns)
                summary_cols = ["Entry Date","Exit Date","Asset","Days Held","Entry $","Exit $","PnL %","Profit $","Exit Reason"]
                summary_df = pd.DataFrame([{k: t[k] for k in summary_cols} for t in trade_log])
                print(f"\n{'='*170}")
                print(f"  📊 TRADE SUMMARY")
                print(f"{'='*170}")
                print(summary_df.to_string(index=False))
            else:
                print("  No closed trades during the period.")

            print(f"\n  Final Value   : ${equity[-1]:,.2f}")
            print(f"  Total Return  : {total_return:+.2f}%")
            print(f"  Days analyzed : {len(eq_df)-1}")
            print(f"  Trades closed : {len(trade_log)}")

        input("\nPress Enter to return to main menu...")

    conn.close()

if __name__ == "__main__":
    main()