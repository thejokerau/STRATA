import sys
import time
import sqlite3
import warnings
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pandas_ta as ta
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
import yfinance as yf

try:
    import optuna
except Exception:
    optuna = None

warnings.filterwarnings("ignore")

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

print("Crypto & Traditional Risk Dashboard (Nightly Quant)")

DB_PATH = "crypto_data.db"
COINGECKO_KEY = ""

TIMEFRAME_MENU = {
    "1": ("1d", "1D"),
    "2": ("4h", "4H"),
    "3": ("8h", "8H"),
    "4": ("12h", "12H"),
}

TRADITIONAL_TOP = {
    "1": ["^AXJO", "BHP.AX", "CBA.AX", "RIO.AX", "CSL.AX", "WBC.AX", "NAB.AX", "MQG.AX", "FMG.AX", "WES.AX"],
    "2": ["^GSPC", "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "BRK-B", "TSLA", "JPM"],
    "3": ["^FTSE", "HSBA.L", "AZN.L", "SHEL.L", "ULVR.L", "BP.L", "GSK.L", "RIO.L", "BHP.L", "VOD.L"],
    "4": ["^STOXX50E", "MC.PA", "ASML.AS", "SAP.DE", "AIR.PA", "SIE.DE", "SU.PA", "RMS.PA", "TTE.PA", "ALV.DE"],
    "5": ["^GSPTSE", "RY.TO", "TD.TO", "ENB.TO", "CNR.TO", "BN.TO", "BCE.TO", "SHOP.TO", "CP.TO", "CNQ.TO"],
}

MAJOR_CRYPTO = [
    "BTC-USD", "ETH-USD", "BNB-USD", "SOL-USD", "XRP-USD",
    "DOGE-USD", "TON-USD", "ADA-USD", "TRX-USD", "AVAX-USD",
]


@dataclass
class TunedParams:
    position_size: float = 0.30
    atr_multiplier: float = 2.2
    adx_threshold: float = 25.0
    cmf_threshold: float = 0.02
    obv_slope_threshold: float = 0.0
    buy_threshold: int = 2
    sell_threshold: int = -2


@dataclass
class BacktestConfig:
    initial_capital: float = 10000.0
    stop_loss_pct: float = 8.0
    take_profit_pct: float = 20.0
    max_hold_days: int = 45
    fee_pct: float = 0.10
    slippage_pct: float = 0.05
    tuned: TunedParams = field(default_factory=TunedParams)


conn = sqlite3.connect(DB_PATH)


def ensure_table() -> None:
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='price_data'")
    exists = cur.fetchone() is not None
    if not exists:
        conn.execute(
            """
            CREATE TABLE price_data (
                ticker TEXT,
                timeframe TEXT,
                date TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                PRIMARY KEY (ticker, timeframe, date)
            )
            """
        )
        conn.commit()
        return

    cols = [r[1] for r in conn.execute("PRAGMA table_info(price_data)").fetchall()]
    if "timeframe" in cols:
        return

    conn.execute("ALTER TABLE price_data RENAME TO price_data_old")
    conn.execute(
        """
        CREATE TABLE price_data (
            ticker TEXT,
            timeframe TEXT,
            date TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            PRIMARY KEY (ticker, timeframe, date)
        )
        """
    )
    conn.execute(
        """
        INSERT OR REPLACE INTO price_data (ticker, timeframe, date, open, high, low, close, volume)
        SELECT ticker, '1d', date, open, high, low, close, volume
        FROM price_data_old
        """
    )
    conn.execute("DROP TABLE price_data_old")
    conn.commit()


def reset_table() -> None:
    conn.execute("DROP TABLE IF EXISTS price_data")
    conn.commit()
    ensure_table()


def tf_to_timedelta(tf: str) -> pd.Timedelta:
    if tf == "1d":
        return pd.Timedelta(days=1)
    if tf == "4h":
        return pd.Timedelta(hours=4)
    if tf == "8h":
        return pd.Timedelta(hours=8)
    if tf == "12h":
        return pd.Timedelta(hours=12)
    return pd.Timedelta(days=1)


def bars_per_day(tf: str) -> float:
    delta_hours = tf_to_timedelta(tf).total_seconds() / 3600.0
    return 24.0 / delta_hours if delta_hours > 0 else 1.0


def validate_and_clean(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    if df is None or df.empty:
        return None
    required = {"Open", "High", "Low", "Close"}
    if not required.issubset(set(df.columns)):
        return None
    out = df.copy()
    out.index = pd.to_datetime(out.index, utc=True).tz_convert(None)
    out = out[~out.index.duplicated(keep="last")].sort_index()
    out["Volume"] = pd.to_numeric(out.get("Volume", 0), errors="coerce").fillna(0.0)
    for c in ["Open", "High", "Low", "Close", "Volume"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    out = out.dropna(subset=["Open", "High", "Low", "Close"])
    out = out[(out["Close"] > 0.000001) & (out["Close"] < 10_000_000)]
    if len(out) < 30:
        return None
    return out


def get_cached(ticker: str, timeframe: str) -> Optional[pd.DataFrame]:
    q = (
        "SELECT date, open, high, low, close, volume "
        "FROM price_data WHERE ticker=? AND timeframe=? ORDER BY date ASC"
    )
    df = pd.read_sql_query(q, conn, params=[ticker, timeframe])
    if df.empty:
        return None
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date")
    df.columns = [c.capitalize() for c in df.columns]
    return validate_and_clean(df)


def save_to_cache(ticker: str, timeframe: str, df: pd.DataFrame) -> None:
    temp = validate_and_clean(df)
    if temp is None:
        return
    temp = temp.copy()
    temp["ticker"] = ticker
    temp["timeframe"] = timeframe
    temp["date"] = temp.index.strftime("%Y-%m-%d %H:%M:%S")
    cols = ["ticker", "timeframe", "date", "Open", "High", "Low", "Close", "Volume"]
    rows = [tuple(r) for r in temp[cols].itertuples(index=False, name=None)]
    sql = (
        "INSERT OR REPLACE INTO price_data "
        "(ticker, timeframe, date, open, high, low, close, volume) VALUES (?,?,?,?,?,?,?,?)"
    )
    conn.executemany(sql, rows)
    conn.commit()


def binance_symbol_from_ticker(ticker: str) -> str:
    return ticker.replace("-USD", "USDT") if "-USD" in ticker else f"{ticker}USDT"


def fetch_binance_paginated(symbol: str, interval: str, start: pd.Timestamp, end: pd.Timestamp) -> Optional[pd.DataFrame]:
    url = "https://api.binance.com/api/v3/klines"
    all_rows: List[list] = []
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)

    while True:
        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": 1000,
            "startTime": start_ms,
            "endTime": end_ms,
        }
        try:
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            rows = r.json()
        except Exception as e:
            print(f"   WARNING Binance failed for {symbol}: {e}")
            return None

        if not rows:
            break

        all_rows.extend(rows)
        last_open = int(rows[-1][0])
        if len(rows) < 1000 or last_open >= end_ms:
            break

        start_ms = last_open + 1
        time.sleep(0.12)

    if not all_rows:
        return None

    data = pd.DataFrame(
        {
            "Open": [float(r[1]) for r in all_rows],
            "High": [float(r[2]) for r in all_rows],
            "Low": [float(r[3]) for r in all_rows],
            "Close": [float(r[4]) for r in all_rows],
            "Volume": [float(r[5]) for r in all_rows],
        },
        index=pd.to_datetime([int(r[0]) for r in all_rows], unit="ms", utc=True).tz_convert(None),
    )
    return validate_and_clean(data)


def fetch_from_binance(ticker: str, timeframe: str, years: float = 2.2, warmup_bars: int = 350) -> Optional[pd.DataFrame]:
    interval = timeframe
    symbol = binance_symbol_from_ticker(ticker)

    end_ts = pd.Timestamp.utcnow().tz_localize(None)
    start_ts = end_ts - pd.Timedelta(days=(years * 365.25)) - warmup_bars * tf_to_timedelta(timeframe)
    return fetch_binance_paginated(symbol, interval, start_ts, end_ts)

def resample_ohlcv(df: pd.DataFrame, rule: str) -> Optional[pd.DataFrame]:
    if df is None or df.empty:
        return None
    agg = {
        "Open": "first",
        "High": "max",
        "Low": "min",
        "Close": "last",
        "Volume": "sum",
    }
    out = df.resample(rule).agg(agg).dropna(subset=["Open", "High", "Low", "Close"])
    return validate_and_clean(out)


def fetch_from_yfinance(ticker: str, timeframe: str, years: float = 2.2, warmup_bars: int = 350) -> Optional[pd.DataFrame]:
    end_ts = pd.Timestamp.utcnow().tz_localize(None)
    start_ts = end_ts - pd.Timedelta(days=(years * 365.25)) - warmup_bars * tf_to_timedelta(timeframe)

    try:
        if timeframe == "1d":
            raw = yf.download(ticker, start=start_ts, end=end_ts, interval="1d", auto_adjust=False, progress=False)
            if raw.empty:
                return None
            if isinstance(raw.columns, pd.MultiIndex):
                raw.columns = raw.columns.get_level_values(0)
            raw.columns = [c.capitalize() for c in raw.columns]
            return validate_and_clean(raw)

        intraday_start = max(start_ts, end_ts - pd.Timedelta(days=729))
        raw = yf.download(ticker, start=intraday_start, end=end_ts, interval="60m", auto_adjust=False, progress=False)
        if raw.empty:
            daily = yf.download(ticker, start=start_ts, end=end_ts, interval="1d", auto_adjust=False, progress=False)
            if daily.empty:
                return None
            if isinstance(daily.columns, pd.MultiIndex):
                daily.columns = daily.columns.get_level_values(0)
            daily.columns = [c.capitalize() for c in daily.columns]
            return validate_and_clean(daily)

        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = raw.columns.get_level_values(0)
        raw.columns = [c.capitalize() for c in raw.columns]
        base = validate_and_clean(raw)
        if base is None:
            return None

        rule = {"4h": "4H", "8h": "8H", "12h": "12H"}[timeframe]
        return resample_ohlcv(base, rule)
    except Exception as e:
        print(f"   WARNING yfinance failed for {ticker}: {e}")
        return None


def fetch_with_cache(ticker: str, timeframe: str, is_crypto: bool, years: float = 2.2) -> Optional[pd.DataFrame]:
    cached = get_cached(ticker, timeframe)

    live: Optional[pd.DataFrame] = None
    if is_crypto:
        live = fetch_from_binance(ticker, timeframe, years=years)
        if live is not None:
            print(f"   Binance loaded: {ticker}")

    if live is None:
        live = fetch_from_yfinance(ticker, timeframe, years=years)
        if live is not None:
            print(f"   yfinance loaded: {ticker}")

    if live is None and cached is None:
        return None
    if live is None:
        return cached
    if cached is None:
        save_to_cache(ticker, timeframe, live)
        return live

    merged = pd.concat([cached, live])
    merged = merged[~merged.index.duplicated(keep="last")].sort_index()
    merged = validate_and_clean(merged)
    if merged is not None:
        save_to_cache(ticker, timeframe, merged)
    return merged


def fetch_top_coins(n: int) -> List[str]:
    print(f"Fetching top {n} coins...")
    try:
        r = requests.get(
            "https://api.coingecko.com/api/v3/coins/markets",
            headers={"x-cg-demo-api-key": COINGECKO_KEY},
            params={"vs_currency": "usd", "order": "market_cap_desc", "per_page": n, "page": 1},
            timeout=20,
        )
        r.raise_for_status()
        tickers = [
            f"{coin['symbol'].upper()}-USD"
            for coin in r.json()
            if coin.get("symbol", "").upper() not in ["USDT", "USDC", "DAI", "FDUSD", "TUSD", "USDE", "BUSD"]
        ]
    except Exception as e:
        print(f"   WARNING CoinGecko failed: {e}")
        tickers = []

    final = tickers[:n]
    for t in MAJOR_CRYPTO:
        if len(final) >= n:
            break
        if t not in final:
            final.append(t)
    return final


def get_traditional_suffix(country: str) -> str:
    return {"1": ".AX", "2": "", "3": ".L", "4": ".PA", "5": ".TO", "6": ""}.get(country, "")


def robust_fib_levels(df: pd.DataFrame, timeframe: str) -> Dict[str, float]:
    if df is None or df.empty:
        return {"Fib_Support": np.nan, "Fib_Resistance": np.nan}

    close_now = float(df["Close"].iloc[-1])
    bpd = bars_per_day(timeframe)

    tail_1000 = df.tail(min(1000, len(df)))
    two_year_cutoff = df.index[-1] - pd.Timedelta(days=(2.0 * 365.25))
    by_time = df[df.index >= two_year_cutoff]

    look = by_time if len(by_time) >= 350 else tail_1000.tail(max(500, min(len(tail_1000), 1000)))
    if len(look) < 80:
        look = df.tail(min(len(df), 220))

    swing = max(3, min(10, int(len(look) / 80)))
    highs = look["High"]
    lows = look["Low"]

    peak_mask = highs == highs.rolling(window=2 * swing + 1, center=True).max()
    trough_mask = lows == lows.rolling(window=2 * swing + 1, center=True).min()

    peaks = look[peak_mask].dropna(subset=["High"])
    troughs = look[trough_mask].dropna(subset=["Low"])

    swing_high = np.nan
    swing_low = np.nan
    if not peaks.empty and not troughs.empty:
        recent_peaks = peaks.tail(max(5, min(25, len(peaks))))
        recent_troughs = troughs.tail(max(5, min(25, len(troughs))))
        swing_high = float(recent_peaks["High"].max())
        swing_low = float(recent_troughs["Low"].min())

    if not np.isfinite(swing_high) or not np.isfinite(swing_low) or swing_high <= swing_low:
        bars_52w = int(np.ceil(365.25 * bpd))
        recent_52w = df.tail(min(len(df), max(50, bars_52w)))
        recent_200 = df.tail(min(len(df), 200))
        hi_52 = float(recent_52w["High"].max())
        lo_52 = float(recent_52w["Low"].min())
        hi_200 = float(recent_200["High"].max())
        lo_200 = float(recent_200["Low"].min())
        swing_high = max(hi_52, hi_200)
        swing_low = min(lo_52, lo_200)

    diff = swing_high - swing_low
    if diff <= 0:
        return {
            "Fib_Support": min(close_now, swing_low),
            "Fib_Resistance": max(close_now, swing_high),
            "Fib_0.236": swing_low,
            "Fib_0.382": swing_low,
            "Fib_0.500": swing_low,
            "Fib_0.618": swing_low,
            "Fib_1.000": swing_high,
            "Fib_1.272": swing_high,
            "Fib_1.618": swing_high,
        }

    levels = {
        "Fib_0.236": swing_low + 0.236 * diff,
        "Fib_0.382": swing_low + 0.382 * diff,
        "Fib_0.500": swing_low + 0.500 * diff,
        "Fib_0.618": swing_low + 0.618 * diff,
        "Fib_1.000": swing_high,
        "Fib_1.272": swing_low + 1.272 * diff,
        "Fib_1.618": swing_low + 1.618 * diff,
    }

    support_candidates = [levels["Fib_0.618"], levels["Fib_0.500"], levels["Fib_0.382"], levels["Fib_0.236"], swing_low]
    below = [x for x in support_candidates if x <= close_now]
    fib_support = max(below) if below else min(close_now, swing_low)

    resistance_candidates = [levels["Fib_1.000"], levels["Fib_1.272"], levels["Fib_1.618"], levels["Fib_0.618"], levels["Fib_0.500"], levels["Fib_0.382"]]
    above = [x for x in resistance_candidates if x >= close_now]
    fib_resistance = min(above) if above else max(close_now, swing_high)

    fib_support = min(fib_support, close_now)
    fib_resistance = max(fib_resistance, close_now)

    out = {"Fib_Support": fib_support, "Fib_Resistance": fib_resistance}
    out.update(levels)
    return out

def compute_indicators(df: pd.DataFrame, timeframe: str) -> Optional[pd.DataFrame]:
    if df is None or len(df) < 60:
        return None

    out = df.copy()
    out["RSI"] = ta.rsi(out["Close"], length=14)
    macd = ta.macd(out["Close"], fast=12, slow=26, signal=9)
    out["MACD"] = macd.get("MACD_12_26_9")
    out["MACD_Signal"] = macd.get("MACDs_12_26_9")
    out["MACD_Hist"] = macd.get("MACDh_12_26_9")

    bb = ta.bbands(out["Close"], length=20, std=2)
    out["BB_Upper"] = bb.get("BBU_20_2.0")
    out["BB_Middle"] = bb.get("BBM_20_2.0")
    out["BB_Lower"] = bb.get("BBL_20_2.0")

    out["ATR"] = ta.atr(out["High"], out["Low"], out["Close"], length=14)
    adx = ta.adx(out["High"], out["Low"], out["Close"], length=14)
    out["ADX"] = adx.get("ADX_14")

    out["OBV"] = ta.obv(out["Close"], out["Volume"])
    out["CMF"] = ta.cmf(out["High"], out["Low"], out["Close"], out["Volume"], length=20)

    out["Body"] = (out["Close"] - out["Open"]).abs()
    out["Range"] = out["High"] - out["Low"]
    out["Upper_Wick"] = out["High"] - out[["Open", "Close"]].max(axis=1)
    out["Lower_Wick"] = out[["Open", "Close"]].min(axis=1) - out["Low"]
    out["Doji"] = ""
    doji_mask = (out["Body"] / out["Range"].replace(0, np.nan) < 0.25) & (out["Range"] > 0)
    out.loc[doji_mask & (out["Lower_Wick"] > 2 * out["Body"]), "Doji"] = "Bullish Doji"
    out.loc[doji_mask & (out["Upper_Wick"] > 2 * out["Body"]), "Doji"] = "Bearish Doji"
    out.loc[
        doji_mask & (out["Lower_Wick"] > 2 * out["Body"]) & (out["Upper_Wick"] > 2 * out["Body"]),
        "Doji",
    ] = "Doji"

    out["MACD_Crossover"] = 0
    bull_cross = (out["MACD"].shift(1) <= out["MACD_Signal"].shift(1)) & (out["MACD"] > out["MACD_Signal"])
    bear_cross = (out["MACD"].shift(1) >= out["MACD_Signal"].shift(1)) & (out["MACD"] < out["MACD_Signal"])
    out.loc[bull_cross, "MACD_Crossover"] = 1
    out.loc[bear_cross, "MACD_Crossover"] = -1

    fib = robust_fib_levels(out, timeframe)
    for k, v in fib.items():
        out[k] = float(v)

    out = out.dropna(subset=["RSI", "MACD", "MACD_Signal", "BB_Middle", "ATR", "ADX", "CMF"])
    return out if len(out) >= 30 else None


def score_asset(df: pd.DataFrame, tuned: TunedParams) -> Tuple[int, Dict[str, float], pd.Series]:
    latest = df.iloc[-1]
    close = float(latest["Close"])

    atr = float(latest.get("ATR", 0))
    atr_vol = (atr / close * 100) if close > 0 else 0.0
    adx = float(latest.get("ADX", 0))
    rsi = float(latest.get("RSI", 50))
    macd_cross = int(latest.get("MACD_Crossover", 0))
    cmf = float(latest.get("CMF", 0))

    obv = df["OBV"].tail(6)
    obv_slope = 0.0
    if len(obv) >= 3:
        x = np.arange(len(obv), dtype=float)
        y = obv.values.astype(float)
        if np.std(y) > 0:
            obv_slope = float(np.polyfit(x, y, 1)[0] / (np.mean(np.abs(y)) + 1e-9))

    bb_mid = float(latest.get("BB_Middle", close))
    bb_up = float(latest.get("BB_Upper", close))
    bb_lo = float(latest.get("BB_Lower", close))

    fib_support = float(latest.get("Fib_Support", close))
    fib_resistance = float(latest.get("Fib_Resistance", close))

    score = 0
    if atr_vol < 2.0:
        score += 1
    elif atr_vol > 5.0:
        score -= 1

    if rsi < 35:
        score += 1
    elif rsi > 68:
        score -= 1

    score += macd_cross

    if close <= bb_lo * 1.01:
        score += 1
    elif close >= bb_up * 0.99:
        score -= 1

    if adx > tuned.adx_threshold:
        score += 1
    elif adx < 20:
        score -= 1

    vol_confirm = int((cmf > tuned.cmf_threshold) and (obv_slope > tuned.obv_slope_threshold))
    vol_reject = int((cmf < -abs(tuned.cmf_threshold)) and (obv_slope < -abs(tuned.obv_slope_threshold)))
    if vol_confirm:
        score += 1
    elif vol_reject:
        score -= 1

    if close <= fib_support * 1.01:
        score += 1
    elif close >= fib_resistance * 0.99:
        score -= 1

    bullish_flags = sum([
        rsi < 45,
        macd_cross == 1,
        adx > tuned.adx_threshold,
        cmf > tuned.cmf_threshold,
        close > bb_mid,
    ])
    bearish_flags = sum([
        rsi > 60,
        macd_cross == -1,
        adx < 20,
        cmf < -abs(tuned.cmf_threshold),
        close < bb_mid,
    ])
    if bullish_flags >= 4:
        score += 1
    if bearish_flags >= 4:
        score -= 1

    score = int(np.clip(score, -5, 5))

    components = {
        "ATR Volatility": atr_vol,
        "ADX Trend Strength": adx,
        "CMF": cmf,
        "OBV Slope": obv_slope,
        "Fib Support": fib_support,
        "Fib Resistance": fib_resistance,
    }
    return score, components, latest


def score_to_rec(score: int, tuned: TunedParams) -> str:
    if score >= max(4, tuned.buy_threshold + 2):
        return "STRONG BUY"
    if score >= tuned.buy_threshold:
        return "BUY"
    if score <= tuned.sell_threshold:
        return "SELL"
    return "HOLD"


def action_emoji(rec: str) -> str:
    if rec in ["BUY", "STRONG BUY"]:
        return "?? BUY"
    if rec == "SELL":
        return "?? SELL"
    return "?? HOLD"


def build_live_tables(enriched: Dict[str, pd.DataFrame], is_crypto: bool, tuned: TunedParams) -> Tuple[pd.DataFrame, pd.DataFrame]:
    rows = []
    risk_rows = []

    for ticker, df in enriched.items():
        score, comp, latest = score_asset(df, tuned)
        rec = score_to_rec(score, tuned)
        close = float(latest.get("Close", 0))
        fib_s = float(latest.get("Fib_Support", close))
        fib_r = float(latest.get("Fib_Resistance", close))

        upside = (fib_r - close) / close * 100 if close > 0 else 0
        downside = (fib_s - close) / close * 100 if close > 0 else 0

        rows.append(
            {
                "_Ticker": ticker,
                "Asset": ticker.replace("-USD", "") if is_crypto else ticker,
                "Action": action_emoji(rec),
                "Score": f"{score}/5",
                "Price": round(close, 4),
                "Fib Support": round(fib_s, 4),
                "Fib Resistance": round(fib_r, 4),
                "Upside %": round(upside, 2),
                "Downside %": round(downside, 2),
                "Doji": latest.get("Doji", "") or "",
                "RSI": round(float(latest.get("RSI", 0)), 2),
            }
        )

        vol_confirm_txt = "Bullish" if (comp["CMF"] > tuned.cmf_threshold and comp["OBV Slope"] > tuned.obv_slope_threshold) else (
            "Bearish" if (comp["CMF"] < -abs(tuned.cmf_threshold) and comp["OBV Slope"] < -abs(tuned.obv_slope_threshold)) else "Neutral"
        )

        risk_rows.append(
            {
                "Asset": ticker.replace("-USD", "") if is_crypto else ticker,
                "Total Score": f"{score}/5",
                "ATR Volatility": f"{comp['ATR Volatility']:.2f}%",
                "ADX Trend Strength": f"{comp['ADX Trend Strength']:.2f}",
                "Volume Confirmation": f"{vol_confirm_txt} (CMF {comp['CMF']:.3f}, OBV slope {comp['OBV Slope']:.5f})",
                "Fib Zone": f"S {comp['Fib Support']:.4f} / R {comp['Fib Resistance']:.4f}",
            }
        )

    if not rows:
        return pd.DataFrame(), pd.DataFrame()

    table = pd.DataFrame(rows).sort_values("Score", key=lambda s: s.str.split("/").str[0].astype(int), ascending=False).reset_index(drop=True)
    table.index += 1
    table.index.name = "Rank"

    risk = pd.DataFrame(risk_rows).sort_values("Total Score", key=lambda s: s.str.split("/").str[0].astype(int), ascending=False).reset_index(drop=True)
    risk.index += 1
    risk.index.name = "Rank"

    return table, risk

def should_exit(hist: pd.DataFrame, entry_price: float, days_held: int, cfg: BacktestConfig) -> Tuple[bool, str]:
    latest = hist.iloc[-1]
    current = float(latest.get("Close", entry_price))

    if entry_price > 0:
        pct = (current / entry_price - 1.0) * 100.0
        if pct <= -cfg.stop_loss_pct:
            return True, f"STOP-LOSS ({pct:+.2f}%)"
        if pct >= cfg.take_profit_pct:
            return True, f"TAKE-PROFIT ({pct:+.2f}%)"

    atr = float(latest.get("ATR", np.nan))
    fib_s = float(latest.get("Fib_Support", np.nan))
    if np.isfinite(atr) and atr > 0:
        atr_stop = current - cfg.tuned.atr_multiplier * atr
        if current <= atr_stop:
            return True, "ATR STOP"
    elif np.isfinite(fib_s) and fib_s > 0 and current <= fib_s:
        return True, "FIB SUPPORT BREAK"

    score, _, _ = score_asset(hist, cfg.tuned)
    rec = score_to_rec(score, cfg.tuned)
    if rec == "SELL":
        return True, "SIGNAL SELL"

    if days_held >= cfg.max_hold_days:
        return True, f"MAX HOLD ({cfg.max_hold_days}d)"

    return False, ""


def simulate_backtest(
    raw_data: Dict[str, pd.DataFrame],
    timeframe: str,
    cfg: BacktestConfig,
    is_crypto: bool,
    start_date: Optional[pd.Timestamp] = None,
    end_date: Optional[pd.Timestamp] = None,
    verbose: bool = False,
) -> Dict[str, object]:
    if not raw_data:
        return {"equity": [], "dates": [], "trades": [], "final": cfg.initial_capital, "return_pct": 0.0}

    enriched = {}
    for t, df in raw_data.items():
        use = df.copy()
        if start_date is not None:
            use = use[use.index >= start_date]
        if end_date is not None:
            use = use[use.index <= end_date]
        use_ind = compute_indicators(use, timeframe)
        if use_ind is not None and len(use_ind) >= 30:
            enriched[t] = use_ind

    if not enriched:
        return {"equity": [], "dates": [], "trades": [], "final": cfg.initial_capital, "return_pct": 0.0}

    anchor = list(enriched.keys())[0]
    test_dates = enriched[anchor].index
    if len(test_dates) < 30:
        return {"equity": [], "dates": [], "trades": [], "final": cfg.initial_capital, "return_pct": 0.0}

    equity = [cfg.initial_capital]
    equity_dates = [test_dates[0]]
    trades = []

    position = 0
    entry_price = 0.0
    entry_date = None
    entry_asset = ""
    entry_asset_label = ""
    entry_capital = 0.0
    days_held = 0
    capital = cfg.initial_capital

    for d in test_dates:
        equity_dates.append(d)

        snap_rows = []
        hist_lookup: Dict[str, pd.DataFrame] = {}
        for t, full_df in raw_data.items():
            hist_raw = full_df[full_df.index <= d]
            hist = compute_indicators(hist_raw, timeframe)
            if hist is None or len(hist) < 30:
                continue
            s, _, latest = score_asset(hist, cfg.tuned)
            rec = score_to_rec(s, cfg.tuned)
            hist_lookup[t] = hist
            snap_rows.append(
                {
                    "_Ticker": t,
                    "_PriceRaw": float(latest["Close"]),
                    "Asset": t.replace("-USD", "") if is_crypto else t,
                    "Recommendation": rec,
                    "Score": s,
                }
            )

        if not snap_rows:
            equity.append(equity[-1])
            if position == 1:
                days_held += 1
            continue

        snap = pd.DataFrame(snap_rows).sort_values("Score", ascending=False).reset_index(drop=True)
        top = snap.iloc[0]
        top_ticker = top["_Ticker"]
        top_price = float(top["_PriceRaw"])
        top_rec = top["Recommendation"]
        held_price = float(snap.loc[snap["_Ticker"] == entry_asset, "_PriceRaw"].iloc[0]) if (position == 1 and entry_asset in set(snap["_Ticker"])) else entry_price

        if position == 1 and entry_asset in hist_lookup:
            exit_now, reason = should_exit(hist_lookup[entry_asset], entry_price, days_held, cfg)
        else:
            exit_now, reason = False, ""

        if position == 0 and top_rec in ["BUY", "STRONG BUY"]:
            position = 1
            entry_price = top_price * (1.0 + cfg.slippage_pct / 100.0)
            entry_date = d
            entry_asset = top_ticker
            entry_asset_label = top["Asset"]
            entry_capital = capital * cfg.tuned.position_size
            entry_fee = entry_capital * (cfg.fee_pct / 100.0)
            capital -= entry_fee
            days_held = 0
            if verbose:
                print(f"BUY {entry_asset_label} {d.date()} @ {entry_price:.4f}")

        elif position == 1 and exit_now:
            exit_price = held_price * (1.0 - cfg.slippage_pct / 100.0)
            pnl_pct = (exit_price / entry_price - 1.0) * 100 if entry_price > 0 else 0.0
            gross = entry_capital * (pnl_pct / 100.0)
            exit_fee = (entry_capital + gross) * (cfg.fee_pct / 100.0)
            net = gross - exit_fee
            capital += net

            trades.append(
                {
                    "Entry Date": str(entry_date.date()) if entry_date is not None else "",
                    "Exit Date": str(d.date()),
                    "Asset": entry_asset_label,
                    "Days Held": days_held + 1,
                    "Entry $": round(entry_price, 4),
                    "Exit $": round(exit_price, 4),
                    "PnL %": round(pnl_pct, 2),
                    "Profit $": round(net, 2),
                    "Exit Reason": reason,
                }
            )
            position = 0
            entry_price = 0.0
            entry_capital = 0.0
            entry_asset = ""
            days_held = 0
            equity.append(capital)
            if verbose:
                print(f"SELL {d.date()} reason={reason} pnl={pnl_pct:+.2f}%")
        else:
            if position == 1 and entry_price > 0:
                days_held += 1
                mtm_position = entry_capital * (held_price / entry_price)
                unallocated = capital
                equity.append(unallocated + mtm_position)
            else:
                equity.append(capital)

    n = min(len(equity), len(equity_dates))
    equity = equity[:n]
    equity_dates = equity_dates[:n]
    final = equity[-1] if equity else cfg.initial_capital
    ret = (final / cfg.initial_capital - 1.0) * 100 if cfg.initial_capital > 0 else 0.0

    return {
        "equity": equity,
        "dates": equity_dates,
        "trades": trades,
        "final": final,
        "return_pct": ret,
    }


def run_walk_forward_optuna(
    raw_data: Dict[str, pd.DataFrame],
    timeframe: str,
    is_crypto: bool,
    base_cfg: BacktestConfig,
    n_trials: int = 30,
) -> BacktestConfig:
    if optuna is None:
        print("Optuna not installed; skipping auto-tune.")
        return base_cfg

    if not raw_data:
        return base_cfg

    anchor = list(raw_data.keys())[0]
    all_dates = raw_data[anchor].index.sort_values()
    if len(all_dates) < 220:
        print("Not enough history for walk-forward tuning.")
        return base_cfg

    train_days = int(round(1.5 * 365.25))
    test_days = int(round(0.5 * 365.25))

    def objective(trial: "optuna.Trial") -> float:
        tuned = TunedParams(
            position_size=trial.suggest_float("position_size", 0.20, 0.40),
            atr_multiplier=trial.suggest_float("atr_multiplier", 2.0, 2.5),
            adx_threshold=trial.suggest_float("adx_threshold", 20.0, 30.0),
            cmf_threshold=trial.suggest_float("cmf_threshold", -0.02, 0.10),
            obv_slope_threshold=trial.suggest_float("obv_slope_threshold", -0.0005, 0.0050),
            buy_threshold=trial.suggest_int("buy_threshold", 1, 3),
            sell_threshold=trial.suggest_int("sell_threshold", -3, -1),
        )
        cfg = BacktestConfig(
            initial_capital=base_cfg.initial_capital,
            stop_loss_pct=base_cfg.stop_loss_pct,
            take_profit_pct=base_cfg.take_profit_pct,
            max_hold_days=base_cfg.max_hold_days,
            fee_pct=base_cfg.fee_pct,
            slippage_pct=base_cfg.slippage_pct,
            tuned=tuned,
        )

        fold_returns = []
        walk_start = all_dates[0] + pd.Timedelta(days=train_days)
        walk_end = all_dates[-1] - pd.Timedelta(days=test_days)
        cursor = walk_start

        while cursor <= walk_end:
            test_start = cursor
            test_end = cursor + pd.Timedelta(days=test_days)
            res = simulate_backtest(raw_data, timeframe, cfg, is_crypto, start_date=test_start, end_date=test_end, verbose=False)
            fold_returns.append(float(res["return_pct"]))
            cursor += pd.Timedelta(days=test_days)

        if not fold_returns:
            return -999.0

        mean_ret = float(np.mean(fold_returns))
        std_ret = float(np.std(fold_returns))
        return mean_ret - 0.4 * std_ret

    study = optuna.create_study(direction="maximize")
    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)

    bp = study.best_params
    tuned = TunedParams(
        position_size=float(bp["position_size"]),
        atr_multiplier=float(bp["atr_multiplier"]),
        adx_threshold=float(bp["adx_threshold"]),
        cmf_threshold=float(bp["cmf_threshold"]),
        obv_slope_threshold=float(bp["obv_slope_threshold"]),
        buy_threshold=int(bp["buy_threshold"]),
        sell_threshold=int(bp["sell_threshold"]),
    )

    out = BacktestConfig(
        initial_capital=base_cfg.initial_capital,
        stop_loss_pct=base_cfg.stop_loss_pct,
        take_profit_pct=base_cfg.take_profit_pct,
        max_hold_days=base_cfg.max_hold_days,
        fee_pct=base_cfg.fee_pct,
        slippage_pct=base_cfg.slippage_pct,
        tuned=tuned,
    )
    print("Auto-Tune complete. Best params:")
    print(bp)
    return out

def chart_top_asset(df: pd.DataFrame, label: str) -> None:
    chart = df.tail(220).copy()
    if chart.empty:
        return

    fig = make_subplots(
        rows=4,
        cols=1,
        shared_xaxes=True,
        row_heights=[0.46, 0.20, 0.18, 0.16],
        vertical_spacing=0.03,
        subplot_titles=(
            f"{label} Price + BB + Fibonacci",
            "MACD",
            "RSI",
            "Volume",
        ),
    )

    fig.add_trace(
        go.Candlestick(
            x=chart.index,
            open=chart["Open"],
            high=chart["High"],
            low=chart["Low"],
            close=chart["Close"],
            name="Price",
        ),
        row=1,
        col=1,
    )
    fig.add_trace(go.Scatter(x=chart.index, y=chart["BB_Upper"], name="BB Upper", line=dict(width=1)), row=1, col=1)
    fig.add_trace(go.Scatter(x=chart.index, y=chart["BB_Middle"], name="BB Mid", line=dict(width=1)), row=1, col=1)
    fig.add_trace(go.Scatter(x=chart.index, y=chart["BB_Lower"], name="BB Lower", line=dict(width=1)), row=1, col=1)

    fs = float(chart["Fib_Support"].iloc[-1])
    fr = float(chart["Fib_Resistance"].iloc[-1])
    fig.add_hline(y=fs, line_dash="dash", line_color="orange", annotation_text=f"Fib Support {fs:.4f}", row=1, col=1)
    fig.add_hline(y=fr, line_dash="dash", line_color="red", annotation_text=f"Fib Resistance {fr:.4f}", row=1, col=1)

    fig.add_trace(go.Bar(x=chart.index, y=chart["MACD_Hist"], name="MACD Hist"), row=2, col=1)
    fig.add_trace(go.Scatter(x=chart.index, y=chart["MACD"], name="MACD", line=dict(width=1.5)), row=2, col=1)
    fig.add_trace(go.Scatter(x=chart.index, y=chart["MACD_Signal"], name="Signal", line=dict(width=1.5)), row=2, col=1)

    fig.add_trace(go.Scatter(x=chart.index, y=chart["RSI"], name="RSI"), row=3, col=1)
    fig.add_hline(y=70, line_dash="dash", row=3, col=1)
    fig.add_hline(y=30, line_dash="dash", row=3, col=1)

    fig.add_trace(go.Bar(x=chart.index, y=chart["Volume"], name="Volume"), row=4, col=1)

    fig.update_layout(height=900, template="plotly_white", xaxis_rangeslider_visible=False)
    fig.show()


def choose_timeframe() -> str:
    print("Select timeframe:")
    print("1. Daily (1d)  2. 4h  3. 8h  4. 12h")
    t = input("Enter 1-4 (default 1): ").strip() or "1"
    return TIMEFRAME_MENU.get(t, TIMEFRAME_MENU["1"])[0]


def load_assets(is_crypto: bool) -> List[str]:
    if is_crypto:
        print("Number of top coins:")
        print("1. Top 10   2. Top 20   3. Top 50   4. Top 100")
        n_choice = input("Enter 1-4: ").strip()
        n = {"1": 10, "2": 20, "3": 50, "4": 100}.get(n_choice, 20)
        return fetch_top_coins(n)

    print("Select Country/Region:")
    print("1. Australia  2. United States  3. United Kingdom  4. Europe  5. Canada  6. Other")
    country = input("Enter 1-6: ").strip()
    suffix = get_traditional_suffix(country)

    print("Asset selection:")
    print("1. Top 10 regional  2. Manual  3. Both")
    mode = input("Enter 1-3: ").strip()

    tickers: List[str] = []
    if mode in ["1", "3"]:
        tickers.extend(TRADITIONAL_TOP.get(country, TRADITIONAL_TOP["2"]))
    if mode in ["2", "3"]:
        manual = input("Enter manual tickers (comma-separated): ").strip().upper()
        if manual:
            tickers.extend([x.strip() for x in manual.split(",") if x.strip()])

    out = []
    for t in tickers:
        already = any(t.endswith(s) for s in [".AX", ".L", ".PA", ".TO", ".DE", ".AS"])
        if suffix and not already and not t.startswith("^"):
            out.append(t + suffix)
        else:
            out.append(t)
    return list(dict.fromkeys(out))


def prompt_backtest_config() -> BacktestConfig:
    try:
        init = input("Initial Capital USD (default 10000): $").strip()
        initial = float(init) if init else 10000.0
    except ValueError:
        initial = 10000.0

    def read_float(msg: str, default: float) -> float:
        try:
            v = input(msg).strip()
            return float(v) if v else default
        except ValueError:
            return default

    def read_int(msg: str, default: int) -> int:
        try:
            v = input(msg).strip()
            return int(v) if v else default
        except ValueError:
            return default

    sl = read_float("Stop-loss % (default 8): ", 8.0)
    tp = read_float("Take-profit % (default 20): ", 20.0)
    mh = read_int("Max hold days (default 45): ", 45)
    fee = read_float("Fee % per trade leg (default 0.10): ", 0.10)
    slip = read_float("Slippage % per leg (default 0.05): ", 0.05)

    pos_pct = read_float("Position size % (default 30): ", 30.0) / 100.0
    atr_mult = read_float("ATR multiplier (default 2.2): ", 2.2)
    adx_thr = read_float("ADX threshold (default 25): ", 25.0)
    cmf_thr = read_float("CMF bullish threshold (default 0.02): ", 0.02)
    obv_thr = read_float("OBV slope threshold (default 0): ", 0.0)

    tuned = TunedParams(
        position_size=float(np.clip(pos_pct, 0.20, 0.40)),
        atr_multiplier=float(np.clip(atr_mult, 2.0, 2.5)),
        adx_threshold=adx_thr,
        cmf_threshold=cmf_thr,
        obv_slope_threshold=obv_thr,
        buy_threshold=2,
        sell_threshold=-2,
    )

    return BacktestConfig(
        initial_capital=initial,
        stop_loss_pct=sl,
        take_profit_pct=tp,
        max_hold_days=mh,
        fee_pct=fee,
        slippage_pct=slip,
        tuned=tuned,
    )


def main() -> None:
    ensure_table()

    while True:
        print("\n" + "=" * 100)
        print("MAIN MENU")
        print("=" * 100)
        print("1. Crypto Analysis")
        print("2. Traditional Markets Analysis")
        print("3. Clear Cache")
        print("4. Exit")

        main_choice = input("Enter 1-4: ").strip()
        if main_choice == "4":
            break
        if main_choice == "3":
            if input("Delete entire cache? (yes/no): ").strip().lower() == "yes":
                reset_table()
                print("Cache cleared.")
            continue
        if main_choice not in ["1", "2"]:
            print("Invalid choice.")
            continue

        is_crypto = main_choice == "1"
        timeframe = choose_timeframe()

        print("Select mode:")
        print("1. Live Dashboard")
        print("2. Backtest + Equity Curve")
        mode = input("Enter 1 or 2: ").strip()
        is_backtest = mode == "2"

        tickers = load_assets(is_crypto)
        if not tickers:
            print("No assets selected.")
            continue

        print(f"Selected {len(tickers)} assets. Loading {timeframe} data...")
        raw_data: Dict[str, pd.DataFrame] = {}
        for t in tickers:
            df = fetch_with_cache(t, timeframe, is_crypto=is_crypto, years=2.2)
            if df is not None and len(df) >= 60:
                raw_data[t] = df

        print(f"Loaded raw data for {len(raw_data)} assets.")
        if not raw_data:
            print("No usable data.")
            continue

        if not is_backtest:
            tuned = TunedParams()
            enriched = {}
            for t, df in raw_data.items():
                ind = compute_indicators(df, timeframe)
                if ind is not None:
                    enriched[t] = ind

            if not enriched:
                print("No indicator-ready data.")
                continue

            table, risk = build_live_tables(enriched, is_crypto, tuned)
            if table.empty:
                print("No scored assets.")
                continue

            print("\n" + "=" * 170)
            print(f"LIVE DASHBOARD ({'Crypto' if is_crypto else 'Traditional'}) [{timeframe}]")
            print("=" * 170)
            print(table.drop(columns=["_Ticker"], errors="ignore").to_string())

            print("\n" + "=" * 170)
            print("RISK SCORE BREAKDOWN")
            print("=" * 170)
            print(risk.to_string())

            top_ticker = table.iloc[0]["_Ticker"]
            chart_top_asset(enriched[top_ticker], top_ticker.replace("-USD", "") if is_crypto else top_ticker)

        else:
            cfg = prompt_backtest_config()
            if input("Enable Auto-Tune + Walk-Forward? (y/n): ").strip().lower() == "y":
                try:
                    n_trials_in = input("Optuna trials (default 30): ").strip()
                    n_trials = int(n_trials_in) if n_trials_in else 30
                except ValueError:
                    n_trials = 30
                cfg = run_walk_forward_optuna(raw_data, timeframe, is_crypto, cfg, n_trials=n_trials)

            result = simulate_backtest(raw_data, timeframe, cfg, is_crypto, verbose=True)
            eq = result["equity"]
            dts = result["dates"]
            trades = result["trades"]

            if not eq:
                print("Backtest returned no results.")
                continue

            eq_df = pd.DataFrame({"Date": dts, "Equity": eq})
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=eq_df["Date"], y=eq_df["Equity"], name="Equity Curve", line=dict(width=3)))
            fig.update_layout(
                title=f"Backtest Equity Curve ({timeframe}) | Final ${result['final']:,.2f} ({result['return_pct']:+.2f}%)",
                xaxis_title="Date",
                yaxis_title="Portfolio Value (USD)",
                height=620,
                template="plotly_white",
            )
            fig.show()

            print("\n" + "=" * 170)
            print("TRADE HISTORY")
            print("=" * 170)
            if trades:
                tr_df = pd.DataFrame(trades)
                print(tr_df.to_string(index=False))
                wins = (tr_df["PnL %"] >= 0).sum()
                total = len(tr_df)
                win_rate = wins / total * 100 if total else 0.0
            else:
                print("No closed trades.")
                win_rate = 0.0

            print(f"\nFinal Value   : ${result['final']:,.2f}")
            print(f"Total Return  : {result['return_pct']:+.2f}%")
            print(f"Trades closed : {len(trades)}")
            print(f"Win rate      : {win_rate:.2f}%")
            print(f"Fee/Slippage  : {cfg.fee_pct}% / {cfg.slippage_pct}% per leg")
            print(f"Position size : {cfg.tuned.position_size*100:.1f}%")

        input("\nPress Enter to return to main menu...")

    conn.close()


if __name__ == "__main__":
    main()
