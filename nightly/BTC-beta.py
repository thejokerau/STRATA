import sys
import time
import re
import os
import json
import getpass
import subprocess
import sqlite3
import warnings
import importlib.util
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
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

# Python runtime policy:
# - Enforce minimum Python 3.13
# - Advise that the primary tested target is Python 3.13.x
if sys.version_info < (3, 13):
    raise SystemExit(
        f"Python 3.13+ is required. Current: {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    )
if sys.version_info[:2] != (3, 13):
    print(
        f"Note: This script is tested primarily on Python 3.13.x. "
        f"Current runtime is {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}."
    )

print("STRATA - Crypto & Traditional Risk Dashboard (Nightly Quant)")

DB_PATH = "crypto_data.db"
COINGECKO_KEY = ""
CHAMPION_REGISTRY_PATH = Path("experiments") / "registry" / "champions.json"
GROK_OUTPUT_DIR = Path("experiments") / "grok"
LIVE_SNAPSHOT_DIR = Path("experiments") / "live_snapshots"
LATEST_LIVE_SNAPSHOT_PATH = LIVE_SNAPSHOT_DIR / "latest_live_dashboard.txt"
BACKTEST_SNAPSHOT_DIR = Path("experiments") / "backtest_snapshots"
LATEST_BACKTEST_SNAPSHOT_PATH = BACKTEST_SNAPSHOT_DIR / "latest_backtest.txt"
USER_PREFS_DIR = Path.home() / ".ctmt"
AI_PREFS_PATH = USER_PREFS_DIR / "ai_preferences.json"
AI_SECRETS_PATH = USER_PREFS_DIR / "ai_secrets.json"
REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "scripts"

DISPLAY_CCY_MENU = {
    "1": "USD",
    "2": "AUD",
    "3": "EUR",
    "4": "GBP",
    "5": "CAD",
    "6": "JPY",
    "7": "NZD",
    "8": "SGD",
    "9": "HKD",
    "10": "CHF",
}

TIMEFRAME_MENU = {
    "1": ("1d", "1d"),
    "2": ("4h", "4h"),
    "3": ("8h", "8h"),
    "4": ("12h", "12h"),
}

CRYPTO_QUOTE_MENU = {
    "1": "USD",
    "2": "USDT",
    "3": "BTC",
    "4": "ETH",
    "5": "BNB",
}

TRADITIONAL_TOP = {
    "1": [  # Australia
        "^AXJO", "BHP.AX", "CBA.AX", "RIO.AX", "CSL.AX", "WBC.AX", "NAB.AX", "MQG.AX", "FMG.AX", "WES.AX",
        "ANZ.AX", "TLS.AX", "WOW.AX", "GMG.AX", "TCL.AX", "QBE.AX", "SUN.AX", "STO.AX", "MIN.AX", "COL.AX",
        "APA.AX", "ALL.AX", "S32.AX", "WDS.AX", "ORG.AX", "IAG.AX", "JHX.AX", "RMD.AX", "ASX.AX", "REA.AX",
        "BXB.AX", "DMP.AX", "COH.AX", "XRO.AX", "A2M.AX", "ALU.AX", "BSL.AX", "GPT.AX", "MPL.AX", "AMP.AX",
    ],
    "2": [  # United States
        "^GSPC", "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "BRK-B", "TSLA", "JPM",
        "V", "MA", "UNH", "XOM", "LLY", "AVGO", "JNJ", "PG", "HD", "COST",
        "ABBV", "MRK", "PEP", "BAC", "KO", "ADBE", "NFLX", "CRM", "ORCL", "AMD",
        "CSCO", "TMO", "CVX", "WMT", "ACN", "DIS", "ABT", "MCD", "PFE", "INTC",
        "QCOM", "TXN", "AMAT", "CMCSA", "DHR", "NKE", "LIN", "UNP", "PM", "NEE",
        "LOW", "RTX", "SPGI", "MS", "GS", "BLK", "CAT", "GE", "IBM", "MU",
    ],
    "3": [  # United Kingdom
        "^FTSE", "HSBA.L", "AZN.L", "SHEL.L", "ULVR.L", "BP.L", "GSK.L", "RIO.L", "BHP.L", "VOD.L",
        "LSEG.L", "DGE.L", "GLEN.L", "NG.L", "REL.L", "BT-A.L", "BARC.L", "STAN.L", "PRU.L", "MNG.L",
        "LLOY.L", "NWG.L", "CPG.L", "AV.L", "AAL.L", "INF.L", "MKS.L", "SMIN.L", "SSE.L", "RR.L",
    ],
    "4": [  # Europe
        "^STOXX50E", "MC.PA", "ASML.AS", "SAP.DE", "AIR.PA", "SIE.DE", "SU.PA", "RMS.PA", "TTE.PA", "ALV.DE",
        "OR.PA", "BN.PA", "SAN.PA", "BNP.PA", "CS.PA", "ADS.DE", "BAS.DE", "BAYN.DE", "ENI.MI", "ISP.MI",
        "IFX.DE", "IBE.MC", "ITX.MC", "NOKIA.HE", "NOVN.SW", "ROG.SW", "NESN.SW", "SU.PA", "VOW3.DE", "MBG.DE",
    ],
    "5": [  # Canada
        "^GSPTSE", "RY.TO", "TD.TO", "ENB.TO", "CNR.TO", "BN.TO", "BCE.TO", "SHOP.TO", "CP.TO", "CNQ.TO",
        "BNS.TO", "CM.TO", "BMO.TO", "TRP.TO", "SU.TO", "MFC.TO", "NA.TO", "SLF.TO", "T.TO", "ATD.TO",
        "WCN.TO", "GIB-A.TO", "FTS.TO", "POW.TO", "CAR-UN.TO", "CSU.TO", "TOU.TO", "TRI.TO", "ABX.TO", "TECK-B.TO",
    ],
}

MAJOR_CRYPTO_BASES = [
    "BTC", "ETH", "BNB", "SOL", "XRP",
    "DOGE", "TON", "ADA", "TRX", "AVAX",
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
    min_hold_bars: int = 2
    cooldown_bars: int = 1
    same_asset_cooldown_bars: int = 3
    max_consecutive_same_asset_entries: int = 3
    max_drawdown_limit_pct: float = 35.0
    max_exposure_pct: float = 0.40
    tuned: TunedParams = field(default_factory=TunedParams)


conn = sqlite3.connect(DB_PATH, check_same_thread=False)

CUDA_AVAILABLE = False
CUDA_BACKEND = "cpu"
cp = None
_BINANCE_SPOT_SYMBOLS: Optional[set] = None
EMOJI_ENABLED = True
COLOR_ENABLED = False
FX_CACHE: Dict[str, Tuple[pd.Timestamp, float]] = {}

GROK_ANALYSIS_INSTRUCTION = """You are Grok, built by xAI — an expert quantitative crypto analyst with real-time tool access (web search, X/Twitter semantic & keyword search, price validation, etc.).

Your job is to deeply analyze the provided LIVE DASHBOARD output (Crypto or Traditional) and deliver maximum actionable insight.

INPUT:
- The full raw dashboard text (including loading logs, LIVE DASHBOARD table, RISK SCORE BREAKDOWN, and PORTFOLIO CONTEXT).
- Current date/time context: {datetime_context}.

TASKS — perform ALL of them in order:

1. Data Integrity & Accuracy Check
   - Verify all prices, Fib levels, RSI, and key metrics against real-time market data (use tools if available).
   - Flag any anomalies, delisted symbols, or inconsistencies.
   - Confirm whether the dashboard output is realistic and well-calibrated for the chosen timeframe (4h / 12h / Daily).

2. TLDR Insights
   - Overall market tone (bullish / cautious / bearish).
   - Top 2–3 standout assets and why.
   - Major red flags or caution zones.
   - How this snapshot compares to the previous one (if provided).

3. Analytical + Technical + Social Rating (1–10 scale)
   For the TOP 3 ranked assets AND the overall dashboard, give clear 1–10 scores with 1-sentence justification each:
   - Analytical Score (quant scores, Setup Exp%, Win%, Risk Cone, Total Score quality)
   - Technical Score (regime, Fib levels, RSI, Doji, trend strength, volume confirmation)
   - Social / Narrative Score (real-time sentiment, X chatter, news catalysts, Fear & Greed alignment, institutional vs retail flows)
   - Composite Conviction Score (average of the three, plus your final verdict)

4. Social & Narrative Overlay
   - Pull fresh X/Twitter sentiment, news, and on-chain narrative for the top assets (especially BUY/HOLD signals).
   - Explain how social signals support or contradict the dashboard.
   - Highlight any hidden catalysts or risks not visible in the numbers.

5. Actionable Bottom Line
   - Clear trading stance for the chosen timeframe (aggressive / opportunistic / defensive / wait).
   - Position-sizing / risk advice tied to Avg Post-Entry DD% and Risk Cone.
   - Suggested next steps (e.g., watch specific Fib levels, re-run on different timeframe, etc.).

6. Entry/Exit Guide (Simple)
   - Provide a practical simple guide for both:
     a) Short-term trade style (swing/tactical)
     b) Long-term trade style (position/core)
   - For each style include:
     - Entry trigger(s)
     - Exit trigger(s) (profit-taking + risk-off)
     - Invalidation/stop idea
     - "Do nothing" condition

7. Recommended API Snippet
   - At the very end, add a section titled exactly: "Recommended API Snippet"
   - Provide a concise Python example that maps your recommendations into executable API-style actions.
   - Include:
     - A duplicate-signal guard
     - Position sizing placeholder logic
     - Entry and exit handling branches
   - A minimal trade-ledger append step
   - Keep it practical and easy to adapt.

8. Structured Trade Plan (Machine-Readable Footer)
   - Keep human-readable interpretation and rationale at the top.
   - At the bottom, output a strict JSON block between these exact markers:
     BEGIN_STRATA_TRADE_PLAN_JSON
     { ...json... }
     END_STRATA_TRADE_PLAN_JSON
   - JSON schema:
     {
       "schema_version": "1.0",
       "generated_at": "<ISO8601 UTC>",
       "timeframe": "<1d|4h|8h|12h>",
       "risk_mode": "<defensive|opportunistic|aggressive|wait>",
       "trades": [
         {
           "symbol": "BTCUSDT",
           "asset": "BTC",
           "side": "BUY",
           "order_type": "MARKET",
           "quantity": 0.0,
           "confidence": "0-100",
           "timeframe": "<1d|4h|8h|12h>",
           "reason": "short explanation",
           "invalidation": "condition/level"
         }
       ]
     }
   - Do not include non-JSON text between the BEGIN/END markers.

OUTPUT FORMAT — strictly follow this structure with clean headings and bullet points. Use markdown tables only when helpful. Be concise yet insightful. Always end with:

"Would you like a deeper dive on any specific asset or a side-by-side comparison with the previous dashboard?"

Now analyze this dashboard:
{dashboard_text}
"""

ONLINE_SYSTEM_PROMPT = "You are Grok, built by xAI."

OFFLINE_SYSTEM_PROMPT = """You are a highly skilled quantitative crypto analyst with a direct, no-nonsense, and insightful style inspired by Grok from xAI.
You are pragmatic, clear, and focused on delivering maximum actionable value from the data provided.
You avoid corporate fluff, hedging language, and unnecessary politeness.
You are honest about limitations (especially that you have no real-time internet access or live market data).

When analyzing trading dashboards:
- Be concise but insightful.
- Prioritize numbers and logical interpretation.
- Use clear structure with headings and bullets.
- Flag what looks strong vs weak without exaggeration.

You are now analyzing crypto trading dashboard data. Think step-by-step and follow the analysis instructions precisely."""


def _enable_windows_vt_mode() -> bool:
    """
    Enable ANSI escape support on modern Windows terminals when possible.
    """
    if os.name != "nt":
        return True
    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32
        handle = kernel32.GetStdHandle(-11)  # STD_OUTPUT_HANDLE
        if handle == 0 or handle == -1:
            return False
        mode = ctypes.c_uint32()
        if kernel32.GetConsoleMode(handle, ctypes.byref(mode)) == 0:
            return False
        ENABLE_VIRTUAL_TERMINAL_PROCESSING = 0x0004
        new_mode = mode.value | ENABLE_VIRTUAL_TERMINAL_PROCESSING
        if kernel32.SetConsoleMode(handle, new_mode) == 0:
            return False
        return True
    except Exception:
        return False


def detect_terminal_display_capabilities() -> Tuple[bool, bool]:
    """
    Determine whether emoji and ANSI colors should be used in action labels.
    - Honors NO_COLOR and FORCE_COLOR env conventions.
    - Allows explicit emoji override via CTMT_DISABLE_EMOJI.
    """
    enc = (getattr(sys.stdout, "encoding", "") or "").lower()
    is_tty = bool(getattr(sys.stdout, "isatty", lambda: False)())
    no_color = os.getenv("NO_COLOR") is not None
    force_color = os.getenv("FORCE_COLOR") is not None
    disable_emoji = (
        os.getenv("STRATA_DISABLE_EMOJI", "").strip().lower() in {"1", "true", "yes", "y"}
        or os.getenv("CTMT_DISABLE_EMOJI", "").strip().lower() in {"1", "true", "yes", "y"}
    )

    emoji_ok = ("utf" in enc) and (not disable_emoji)

    ansi_ok = False
    if is_tty and not no_color:
        if force_color:
            ansi_ok = True
        else:
            term = (os.getenv("TERM", "") or "").lower()
            wt_session = os.getenv("WT_SESSION") is not None
            conemu = os.getenv("ConEmuANSI", "").upper() == "ON"
            ansicon = os.getenv("ANSICON") is not None
            ansi_ok = bool(term and term != "dumb") or wt_session or conemu or ansicon
        if ansi_ok:
            ansi_ok = _enable_windows_vt_mode()
    return emoji_ok, ansi_ok


def detect_cuda_backend() -> Tuple[bool, str, Optional[object]]:
    """
    Detect whether CUDA can be used through CuPy.
    Falls back cleanly to CPU if unavailable.
    """
    if importlib.util.find_spec("cupy") is None:
        return False, "cpu", None
    try:
        import cupy as _cp
        devs = _cp.cuda.runtime.getDeviceCount()
        if devs and devs > 0:
            return True, f"cuda (cupy, devices={devs})", _cp
        return False, "cpu", None
    except Exception:
        return False, "cpu", None


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
    # Cache may contain mixed legacy date formats (date-only and datetime).
    # Parse robustly to avoid hard failures during dashboard/backtest loads.
    df["date"] = pd.to_datetime(df["date"], format="mixed", errors="coerce")
    df = df.dropna(subset=["date"])
    if df.empty:
        return None
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


def split_crypto_ticker(ticker: str) -> Tuple[str, str]:
    t = str(ticker).strip().upper()
    if "-" in t:
        base, quote = t.split("-", 1)
        return base, quote
    return t, "USD"


def make_crypto_ticker(base: str, quote: str) -> str:
    return f"{base.upper()}-{quote.upper()}"


def binance_symbol_from_ticker(ticker: str) -> str:
    base, quote = split_crypto_ticker(ticker)
    # Binance commonly maps USD-style tracking to USDT spot pairs.
    bquote = "USDT" if quote == "USD" else quote
    return f"{base}{bquote}"


def get_binance_spot_symbols() -> set:
    """
    Load Binance tradable spot symbols once via exchangeInfo for pre-filtering.
    """
    global _BINANCE_SPOT_SYMBOLS
    if _BINANCE_SPOT_SYMBOLS is not None:
        return _BINANCE_SPOT_SYMBOLS
    try:
        r = requests.get("https://api.binance.com/api/v3/exchangeInfo", timeout=20)
        r.raise_for_status()
        data = r.json()
        symbols = set()
        for s in data.get("symbols", []):
            if s.get("status") == "TRADING":
                sym = str(s.get("symbol", "")).upper()
                if sym:
                    symbols.add(sym)
        _BINANCE_SPOT_SYMBOLS = symbols
    except Exception:
        _BINANCE_SPOT_SYMBOLS = set()
    return _BINANCE_SPOT_SYMBOLS


def filter_crypto_tickers_by_binance(tickers: List[str]) -> List[str]:
    symbols = get_binance_spot_symbols()
    if not symbols:
        return tickers
    kept = []
    dropped = []
    for t in tickers:
        if binance_symbol_from_ticker(t) in symbols:
            kept.append(t)
        else:
            dropped.append(t)
    if dropped:
        preview = ", ".join(dropped[:8])
        more = f" (+{len(dropped) - 8} more)" if len(dropped) > 8 else ""
        print(f"Filtered out non-Binance symbols: {preview}{more}")
    return kept if kept else tickers


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

        rule = {"4h": "4h", "8h": "8h", "12h": "12h"}[timeframe]
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

    # yfinance crypto support is reliable primarily for USD pairs.
    crypto_can_use_yf = (not is_crypto) or split_crypto_ticker(ticker)[1] == "USD"
    if live is None and crypto_can_use_yf:
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


def fetch_top_coins(n: int, quote_currency: str = "USD") -> List[str]:
    print(f"Fetching top {n} coins...")
    quote = quote_currency.upper().strip() or "USD"
    try:
        r = requests.get(
            "https://api.coingecko.com/api/v3/coins/markets",
            headers={"x-cg-demo-api-key": COINGECKO_KEY},
            params={"vs_currency": "usd", "order": "market_cap_desc", "per_page": n, "page": 1},
            timeout=20,
        )
        r.raise_for_status()
        tickers = []
        for coin in r.json():
            raw_sym = coin.get("symbol", "").upper()
            if raw_sym in ["USDT", "USDC", "DAI", "FDUSD", "TUSD", "USDE", "BUSD"]:
                continue
            # Keep only exchange-safe alphanumeric symbols to avoid malformed pairs/tickers.
            clean_sym = re.sub(r"[^A-Z0-9]", "", raw_sym)
            if not clean_sym:
                continue
            tickers.append(make_crypto_ticker(clean_sym, quote))
    except Exception as e:
        print(f"   WARNING CoinGecko failed: {e}")
        tickers = []

    final = tickers[:n]
    for b in MAJOR_CRYPTO_BASES:
        t = make_crypto_ticker(b, quote)
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
    def pick_col(frame: pd.DataFrame, starts_with: List[str]) -> Optional[str]:
        if frame is None or frame.empty:
            return None
        for sw in starts_with:
            for c in frame.columns:
                if str(c).upper().startswith(sw.upper()):
                    return c
        return None

    out["RSI"] = ta.rsi(out["Close"], length=14)
    macd = ta.macd(out["Close"], fast=12, slow=26, signal=9)
    if macd is None or macd.empty:
        return None
    macd_col = pick_col(macd, ["MACD_"])
    macd_sig_col = pick_col(macd, ["MACDS_", "MACDSIGNAL_", "SIGNAL_"])
    macd_hist_col = pick_col(macd, ["MACDH_", "MACDHIST_"])
    if not macd_col or not macd_sig_col:
        return None
    out["MACD"] = macd[macd_col]
    out["MACD_Signal"] = macd[macd_sig_col]
    out["MACD_Hist"] = macd[macd_hist_col] if macd_hist_col else (out["MACD"] - out["MACD_Signal"])

    bb = ta.bbands(out["Close"], length=20, std=2)
    if bb is None or bb.empty:
        return None
    bb_u = pick_col(bb, ["BBU_"])
    bb_m = pick_col(bb, ["BBM_"])
    bb_l = pick_col(bb, ["BBL_"])
    if not bb_u or not bb_m or not bb_l:
        return None
    out["BB_Upper"] = bb[bb_u]
    out["BB_Middle"] = bb[bb_m]
    out["BB_Lower"] = bb[bb_l]

    out["ATR"] = ta.atr(out["High"], out["Low"], out["Close"], length=14)
    adx = ta.adx(out["High"], out["Low"], out["Close"], length=14)
    if adx is None or adx.empty:
        return None
    adx_col = pick_col(adx, ["ADX_"])
    if not adx_col:
        return None
    out["ADX"] = adx[adx_col]

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

    # Traditional indices frequently have sparse/missing volume fields.
    # Keep CMF/OBV as neutral when not computable instead of dropping whole assets.
    out = out.replace([np.inf, -np.inf], np.nan)
    out["CMF"] = pd.to_numeric(out["CMF"], errors="coerce").fillna(0.0)
    out["OBV"] = pd.to_numeric(out["OBV"], errors="coerce").ffill().fillna(0.0)
    out["ADX"] = pd.to_numeric(out["ADX"], errors="coerce").ffill().fillna(0.0)

    out = out.dropna(subset=["RSI", "MACD", "MACD_Signal", "BB_Middle", "ATR"])
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
            # Optional CUDA acceleration for numeric fit when CuPy is available.
            if CUDA_AVAILABLE and cp is not None:
                try:
                    xg = cp.asarray(x)
                    yg = cp.asarray(y)
                    m = cp.polyfit(xg, yg, 1)[0]
                    denom = cp.mean(cp.abs(yg)) + 1e-9
                    obv_slope = float((m / denom).get())
                except Exception:
                    obv_slope = float(np.polyfit(x, y, 1)[0] / (np.mean(np.abs(y)) + 1e-9))
            else:
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


def pretty_asset_label(ticker: str, is_crypto: bool) -> str:
    if is_crypto:
        return split_crypto_ticker(ticker)[0]
    return ticker


def action_emoji(rec: str) -> str:
    if rec in ["BUY", "STRONG BUY"]:
        base = "BUY"
        emoji = "\U0001F7E2"
        color = "\x1b[92m"
    elif rec == "SELL":
        base = "SELL"
        emoji = "\U0001F534"
        color = "\x1b[91m"
    else:
        base = "HOLD"
        emoji = "\U0001F7E0"
        color = "\x1b[93m"

    label = f"{emoji} {base}" if EMOJI_ENABLED else base
    if COLOR_ENABLED:
        return f"{color}{label}\x1b[0m"
    return label


def build_live_tables(enriched: Dict[str, pd.DataFrame], is_crypto: bool, tuned: TunedParams, timeframe: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    def regime_tag_series(adx_s: pd.Series, atr_vol_s: pd.Series) -> pd.Series:
        conds = [
            (adx_s >= 25) & (atr_vol_s < 4.0),
            (adx_s < 18),
            (atr_vol_s >= 5.0),
        ]
        vals = ["Trending", "Choppy", "High-Vol"]
        return pd.Series(np.select(conds, vals, default="Transitional"), index=adx_s.index)

    def setup_stats_for_asset(df: pd.DataFrame, raw_score: int, timeframe: str) -> Dict[str, float]:
        n = len(df)
        if n < 80:
            return {
                "win_rate": 50.0,
                "expectancy": 0.0,
                "median_outcome": 0.0,
                "false_signal_rate": 50.0,
                "avg_post_entry_dd": -2.0,
                "avg_hold_days": 5.0,
                "risk_cone_10": -4.0,
                "risk_cone_90": 4.0,
                "reliability": 0.0,
                "regime": "Transitional",
            }

        close = df["Close"].astype(float)
        low = df["Low"].astype(float)
        atr_vol = (df["ATR"].astype(float) / close.replace(0, np.nan) * 100.0).fillna(0.0)
        reg_s = regime_tag_series(df["ADX"].astype(float), atr_vol)
        current_regime = str(reg_s.iloc[-1])

        obv_slope_proxy = df["OBV"].pct_change().rolling(5).mean().fillna(0.0)
        score_s = pd.Series(0, index=df.index, dtype="float64")
        score_s += np.where(atr_vol < 2.0, 1, np.where(atr_vol > 5.0, -1, 0))
        score_s += np.where(df["RSI"] < 35, 1, np.where(df["RSI"] > 68, -1, 0))
        score_s += df["MACD_Crossover"].fillna(0).astype(float)
        score_s += np.where(close <= df["BB_Lower"] * 1.01, 1, np.where(close >= df["BB_Upper"] * 0.99, -1, 0))
        score_s += np.where(df["ADX"] > tuned.adx_threshold, 1, np.where(df["ADX"] < 20, -1, 0))
        score_s += np.where((df["CMF"] > tuned.cmf_threshold) & (obv_slope_proxy > tuned.obv_slope_threshold), 1, 0)
        score_s += np.where((df["CMF"] < -abs(tuned.cmf_threshold)) & (obv_slope_proxy < -abs(tuned.obv_slope_threshold)), -1, 0)
        score_s += np.where(close <= df["Fib_Support"] * 1.01, 1, np.where(close >= df["Fib_Resistance"] * 0.99, -1, 0))
        score_s = score_s.clip(-5, 5)

        horizon = max(2, int(round(5.0 * bars_per_day(timeframe))))
        future_ret = (close.shift(-horizon) / close - 1.0) * 100.0

        lows_np = low.values
        close_np = close.values
        fut_dd = np.full(n, np.nan, dtype=float)
        for i in range(0, max(0, n - horizon)):
            mlow = np.min(lows_np[i + 1 : i + horizon + 1]) if (i + horizon + 1) <= n else np.min(lows_np[i + 1 :])
            if close_np[i] > 0:
                fut_dd[i] = (mlow / close_np[i] - 1.0) * 100.0

        mask = (score_s >= (raw_score - 1)) & (score_s <= (raw_score + 1)) & (reg_s == current_regime)
        mask = mask & future_ret.notna()
        sample = future_ret[mask]

        if sample.empty:
            win = 50.0
            exp = 0.0
            med = 0.0
            fsr = 50.0
            ddm = -2.0
            avg_hold_d = 5.0
            q10, q90 = -4.0, 4.0
        else:
            win = float((sample > 0).mean() * 100.0)
            exp = float(sample.mean())
            med = float(sample.median())
            buy_mask = mask & (score_s >= tuned.buy_threshold)
            buy_sample = future_ret[buy_mask]
            fsr = float((buy_sample <= 0).mean() * 100.0) if not buy_sample.empty else float((sample <= 0).mean() * 100.0)
            dd_vals = pd.Series(fut_dd, index=df.index)[mask].dropna()
            ddm = float(dd_vals.mean()) if not dd_vals.empty else -2.0
            q10 = float(sample.quantile(0.10))
            q90 = float(sample.quantile(0.90))

            # Approximate hold duration: bars until signal crosses below sell threshold.
            idxs = np.where(buy_mask.values)[0]
            hold_bars = []
            for j in idxs[:150]:
                end = min(n - 1, j + horizon)
                nxt = score_s.iloc[j + 1 : end + 1]
                hit = nxt[nxt <= tuned.sell_threshold]
                hb = int(hit.index[0] in nxt.index and (nxt.index.get_loc(hit.index[0]) + 1) if not hit.empty else max(1, end - j))
                hold_bars.append(hb)
            avg_hold_d = float(np.mean(hold_bars) / max(1e-9, bars_per_day(timeframe))) if hold_bars else (horizon / max(1e-9, bars_per_day(timeframe)))

        reliability = (
            np.clip((win - 50.0) / 25.0, -1.5, 1.5)
            + np.clip(exp / 6.0, -1.5, 1.5)
            - np.clip(fsr / 60.0, 0.0, 1.5)
            + np.clip(ddm / 15.0, -1.0, 0.5)
        )

        return {
            "win_rate": win,
            "expectancy": exp,
            "median_outcome": med,
            "false_signal_rate": fsr,
            "avg_post_entry_dd": ddm,
            "avg_hold_days": avg_hold_d,
            "risk_cone_10": q10,
            "risk_cone_90": q90,
            "reliability": float(reliability),
            "regime": current_regime,
        }

    rows = []
    risk_rows = []

    for ticker, df in enriched.items():
        score, comp, latest = score_asset(df, tuned)
        setup = setup_stats_for_asset(df, score, timeframe)
        rel_adj = float(np.clip(setup["reliability"], -2.0, 2.0))
        adj_score = int(np.clip(round(score + 0.8 * rel_adj), -5, 5))
        rec = score_to_rec(adj_score, tuned)
        close = float(latest.get("Close", 0))
        fib_s = float(latest.get("Fib_Support", close))
        fib_r = float(latest.get("Fib_Resistance", close))

        upside = (fib_r - close) / close * 100 if close > 0 else 0
        downside = (fib_s - close) / close * 100 if close > 0 else 0

        rows.append(
            {
                "_Ticker": ticker,
                "Asset": pretty_asset_label(ticker, is_crypto),
                "Action": action_emoji(rec),
                "Score": f"{adj_score}/5",
                "Raw Score": f"{score}/5",
                "Reliability": round(rel_adj, 2),
                "Regime": setup["regime"],
                "Price": round(close, 4),
                "Fib Support": round(fib_s, 4),
                "Fib Resistance": round(fib_r, 4),
                "Upside %": round(upside, 2),
                "Downside %": round(downside, 2),
                "Setup Exp %": round(setup["expectancy"], 2),
                "Setup Win %": round(setup["win_rate"], 1),
                "Median Out %": round(setup["median_outcome"], 2),
                "False Sig %": round(setup["false_signal_rate"], 1),
                "Avg Post-Entry DD %": round(setup["avg_post_entry_dd"], 2),
                "Avg Hold (d)": round(setup["avg_hold_days"], 1),
                "Risk Cone 10/90 %": f"{setup['risk_cone_10']:.2f}/{setup['risk_cone_90']:.2f}",
                "Doji": latest.get("Doji", "") or "",
                "RSI": round(float(latest.get("RSI", 0)), 2),
            }
        )

        vol_confirm_txt = "Bullish" if (comp["CMF"] > tuned.cmf_threshold and comp["OBV Slope"] > tuned.obv_slope_threshold) else (
            "Bearish" if (comp["CMF"] < -abs(tuned.cmf_threshold) and comp["OBV Slope"] < -abs(tuned.obv_slope_threshold)) else "Neutral"
        )

        risk_rows.append(
            {
                "Asset": pretty_asset_label(ticker, is_crypto),
                "Total Score": f"{score}/5",
                "ATR Volatility": f"{comp['ATR Volatility']:.2f}%",
                "ADX Trend Strength": f"{comp['ADX Trend Strength']:.2f}",
                "Volume Confirmation": f"{vol_confirm_txt} (CMF {comp['CMF']:.3f}, OBV slope {comp['OBV Slope']:.5f})",
                "Fib Zone": f"S {comp['Fib Support']:.4f} / R {comp['Fib Resistance']:.4f}",
                "Regime": setup["regime"],
                "Setup Quality": f"Exp {setup['expectancy']:.2f}% | Win {setup['win_rate']:.1f}% | False {setup['false_signal_rate']:.1f}%",
            }
        )

    if not rows:
        return pd.DataFrame(), pd.DataFrame()

    table = pd.DataFrame(rows).sort_values(
        ["Score", "Reliability", "Setup Exp %"],
        key=lambda s: s.str.split("/").str[0].astype(int) if s.dtype == object and s.str.contains("/").all() else s,
        ascending=False,
    ).reset_index(drop=True)
    table.index += 1
    table.index.name = "Rank"

    risk = pd.DataFrame(risk_rows).sort_values("Total Score", key=lambda s: s.str.split("/").str[0].astype(int), ascending=False).reset_index(drop=True)
    risk.index += 1
    risk.index.name = "Rank"

    return table, risk


def build_portfolio_insights(enriched: Dict[str, pd.DataFrame], table: pd.DataFrame) -> List[str]:
    """
    Portfolio-aware live suggestions:
    - concentration warnings
    - low-correlation alternatives versus top-ranked asset
    """
    notes: List[str] = []
    if table is None or table.empty or len(table) < 2:
        return notes

    top_ticker = str(table.iloc[0]["_Ticker"])
    top_asset = str(table.iloc[0]["Asset"])

    rets = {}
    for t, df in enriched.items():
        r = df["Close"].pct_change().dropna().tail(180)
        if len(r) >= 30:
            rets[t] = r
    if top_ticker not in rets:
        return notes

    ret_df = pd.DataFrame(rets).dropna(how="all")
    if ret_df.empty or top_ticker not in ret_df.columns:
        return notes

    corr = ret_df.corr()
    top_corr = corr[top_ticker].drop(index=top_ticker, errors="ignore").dropna()
    if top_corr.empty:
        return notes

    avg_corr = float(top_corr.mean())
    if avg_corr > 0.70:
        notes.append(f"Concentration warning: {top_asset} is highly correlated with this basket (avg corr {avg_corr:.2f}).")

    candidates = []
    for _, row in table.iloc[1:].iterrows():
        t = str(row["_Ticker"])
        if t in corr.columns and top_ticker in corr.index:
            c = float(corr.loc[t, top_ticker]) if pd.notna(corr.loc[t, top_ticker]) else 1.0
            if c < 0.45:
                candidates.append((str(row["Asset"]), c, float(row.get("Reliability", 0.0)), str(row.get("Score", ""))))
    candidates = sorted(candidates, key=lambda x: (-x[2], x[1]))[:3]
    if candidates:
        alts = ", ".join([f"{a} (corr {c:.2f}, rel {r:+.2f}, score {s})" for a, c, r, s in candidates])
        notes.append(f"Lower-correlation alternatives to {top_asset}: {alts}")

    return notes


def build_indicator_cache(
    raw_data: Dict[str, pd.DataFrame],
    timeframe: str,
    max_workers: int = 1,
    verbose: bool = False,
) -> Dict[str, pd.DataFrame]:
    """
    Build indicators once per asset and reuse in backtests/optimization.
    This removes repeated indicator recomputation from inner loops.
    """
    if not raw_data:
        return {}

    workers = max(1, int(max_workers))
    out: Dict[str, pd.DataFrame] = {}

    def _job(item: Tuple[str, pd.DataFrame]) -> Tuple[str, Optional[pd.DataFrame]]:
        t, df = item
        return t, compute_indicators(df, timeframe)

    if workers == 1:
        for t, df in raw_data.items():
            ind = compute_indicators(df, timeframe)
            if ind is not None and len(ind) >= 30:
                out[t] = ind
            elif verbose:
                print(f"Indicator cache dropped: {t}")
        return out

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(_job, item) for item in raw_data.items()]
        for fut in as_completed(futures):
            t, ind = fut.result()
            if ind is not None and len(ind) >= 30:
                out[t] = ind
            elif verbose:
                print(f"Indicator cache dropped: {t}")
    return out


def compute_performance_metrics(
    equity: List[float],
    dates: List[pd.Timestamp],
    trades: List[Dict[str, object]],
    initial_capital: float,
    timeframe: str,
) -> Dict[str, object]:
    if not equity or len(equity) < 2:
        return {
            "max_drawdown_pct": 0.0,
            "sharpe": 0.0,
            "sortino": 0.0,
            "turnover_per_year": 0.0,
            "avg_hold_days": 0.0,
            "total_years": 0.0,
            "asset_contrib": {},
        }

    eq = pd.Series(equity, dtype="float64")
    running_max = eq.cummax()
    dd = (eq / running_max - 1.0) * 100.0
    max_dd = float(abs(dd.min())) if not dd.empty else 0.0

    rets = eq.pct_change().dropna()
    bpd = bars_per_day(timeframe)
    bars_per_year = max(1.0, 365.25 * bpd)
    mu = float(rets.mean()) if not rets.empty else 0.0
    sigma = float(rets.std(ddof=0)) if not rets.empty else 0.0
    sharpe = (mu / sigma) * np.sqrt(bars_per_year) if sigma > 0 else 0.0

    downside = rets[rets < 0]
    downside_std = float(downside.std(ddof=0)) if not downside.empty else 0.0
    sortino = (mu / downside_std) * np.sqrt(bars_per_year) if downside_std > 0 else 0.0

    start = pd.Timestamp(dates[0])
    end = pd.Timestamp(dates[-1])
    years = max(1e-9, (end - start).total_seconds() / (365.25 * 24 * 3600))
    turnover = len(trades) / years

    avg_hold = float(np.mean([float(t.get("Days Held", 0)) for t in trades])) if trades else 0.0

    asset_contrib: Dict[str, float] = {}
    for t in trades:
        a = str(t.get("Asset", ""))
        p = float(t.get("Profit $", 0.0))
        asset_contrib[a] = asset_contrib.get(a, 0.0) + p
    asset_contrib = dict(sorted(asset_contrib.items(), key=lambda kv: kv[1], reverse=True))

    return {
        "max_drawdown_pct": max_dd,
        "sharpe": float(sharpe),
        "sortino": float(sortino),
        "turnover_per_year": float(turnover),
        "avg_hold_days": avg_hold,
        "total_years": float(years),
        "asset_contrib": asset_contrib,
    }

def should_exit(
    hist: pd.DataFrame,
    entry_price: float,
    holding_days: float,
    bars_held: int,
    max_hold_bars: int,
    cfg: BacktestConfig,
) -> Tuple[bool, str]:
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
    if rec == "SELL" and bars_held >= cfg.min_hold_bars:
        return True, "SIGNAL SELL"

    if holding_days >= cfg.max_hold_days or bars_held >= max_hold_bars:
        return True, f"MAX HOLD ({cfg.max_hold_days}d)"

    return False, ""


def simulate_backtest(
    raw_data: Dict[str, pd.DataFrame],
    timeframe: str,
    cfg: BacktestConfig,
    is_crypto: bool,
    start_date: Optional[pd.Timestamp] = None,
    end_date: Optional[pd.Timestamp] = None,
    indicator_cache: Optional[Dict[str, pd.DataFrame]] = None,
    verbose: bool = False,
) -> Dict[str, object]:
    if not raw_data:
        return {"equity": [], "dates": [], "trades": [], "final": cfg.initial_capital, "return_pct": 0.0}

    base = indicator_cache if indicator_cache is not None else build_indicator_cache(raw_data, timeframe, max_workers=1, verbose=False)
    if not base:
        return {"equity": [], "dates": [], "trades": [], "final": cfg.initial_capital, "return_pct": 0.0}

    enriched = {}
    dropped_assets = 0
    for t, df in base.items():
        use = df.copy()
        if start_date is not None:
            use = use[use.index >= start_date]
        if end_date is not None:
            use = use[use.index <= end_date]
        if use is not None and len(use) >= 30:
            enriched[t] = use
        else:
            dropped_assets += 1

    if not enriched:
        if verbose:
            print(
                f"No indicator-ready assets for backtest window "
                f"(kept 0/{len(raw_data)}, dropped {dropped_assets})."
            )
        # Return flat equity instead of empty result so caller can diagnose safely.
        flat_date = end_date if end_date is not None else pd.Timestamp.utcnow().tz_localize(None)
        return {
            "equity": [cfg.initial_capital],
            "dates": [flat_date],
            "trades": [],
            "final": cfg.initial_capital,
            "return_pct": 0.0,
        }

    anchor = list(enriched.keys())[0]
    test_dates = enriched[anchor].index
    if len(test_dates) < 30:
        if verbose:
            print(f"Not enough post-indicator bars in backtest window ({len(test_dates)} bars).")
        return {
            "equity": [cfg.initial_capital],
            "dates": [test_dates[-1] if len(test_dates) else pd.Timestamp.utcnow().tz_localize(None)],
            "trades": [],
            "final": cfg.initial_capital,
            "return_pct": 0.0,
        }

    equity = [cfg.initial_capital]
    equity_dates = [test_dates[0]]
    trades = []

    position = 0
    entry_price = 0.0
    entry_date = None
    entry_ts = None
    entry_asset = ""
    entry_asset_label = ""
    entry_capital = 0.0
    bars_held = 0
    cooldown_remaining = 0
    same_asset_cooldown_until: Dict[str, int] = {}
    last_entry_asset = ""
    consecutive_same_asset_entries = 0
    capital = cfg.initial_capital
    max_hold_bars = max(1, int(np.ceil(cfg.max_hold_days * bars_per_day(timeframe))))

    pos_lookup = {}
    test_vals = test_dates.values
    for t, df in enriched.items():
        pos_lookup[t] = np.searchsorted(df.index.values, test_vals, side="right")

    for i, d in enumerate(test_dates):
        equity_dates.append(d)

        snap_rows = []
        hist_lookup: Dict[str, pd.DataFrame] = {}
        for t, full_df in enriched.items():
            pos = int(pos_lookup[t][i])
            if pos < 30:
                continue
            hist = full_df.iloc[:pos]
            s, _, latest = score_asset(hist, cfg.tuned)
            rec = score_to_rec(s, cfg.tuned)
            hist_lookup[t] = hist
            snap_rows.append(
                {
                    "_Ticker": t,
                    "_PriceRaw": float(latest["Close"]),
                    "Asset": pretty_asset_label(t, is_crypto),
                    "Recommendation": rec,
                    "Score": s,
                }
            )

        if not snap_rows:
            equity.append(equity[-1])
            if position == 1:
                bars_held += 1
            continue

        snap = pd.DataFrame(snap_rows).sort_values("Score", ascending=False).reset_index(drop=True)
        top = snap.iloc[0]
        top_ticker = top["_Ticker"]
        top_price = float(top["_PriceRaw"])
        top_rec = top["Recommendation"]
        held_price = float(snap.loc[snap["_Ticker"] == entry_asset, "_PriceRaw"].iloc[0]) if (position == 1 and entry_asset in set(snap["_Ticker"])) else entry_price

        if position == 1 and entry_asset in hist_lookup and entry_ts is not None:
            holding_days = max(0.0, (pd.Timestamp(d) - pd.Timestamp(entry_ts)).total_seconds() / 86400.0)
            exit_now, reason = should_exit(
                hist_lookup[entry_asset],
                entry_price,
                holding_days,
                bars_held,
                max_hold_bars,
                cfg,
            )
        else:
            exit_now, reason = False, ""

        same_asset_ready = i >= int(same_asset_cooldown_until.get(top_ticker, -1))
        next_streak = (consecutive_same_asset_entries + 1) if top_ticker == last_entry_asset else 1
        streak_ok = next_streak <= max(1, int(cfg.max_consecutive_same_asset_entries))
        if position == 0 and cooldown_remaining == 0 and same_asset_ready and streak_ok and top_rec in ["BUY", "STRONG BUY"]:
            position = 1
            entry_price = top_price * (1.0 + cfg.slippage_pct / 100.0)
            entry_date = d
            entry_ts = pd.Timestamp(d)
            entry_asset = top_ticker
            last_entry_asset = top_ticker
            consecutive_same_asset_entries = next_streak
            entry_asset_label = top["Asset"]
            entry_capital = capital * cfg.tuned.position_size
            entry_fee = entry_capital * (cfg.fee_pct / 100.0)
            capital -= entry_fee
            bars_held = 0
            if verbose:
                print(f"BUY {entry_asset_label} {d.date()} @ {entry_price:.4f}")
        elif position == 0 and cooldown_remaining == 0 and same_asset_ready and (not streak_ok) and top_rec in ["BUY", "STRONG BUY"]:
            if verbose:
                print(
                    f"SKIP {top_ticker} on {d.date()} due to max consecutive same-asset entries "
                    f"({cfg.max_consecutive_same_asset_entries})."
                )

        elif position == 1 and exit_now:
            exited_ticker = entry_asset
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
                    "Days Held": int(np.floor(max(0.0, (pd.Timestamp(d) - pd.Timestamp(entry_ts)).total_seconds() / 86400.0))) + 1 if entry_ts is not None else 0,
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
            entry_ts = None
            entry_asset = ""
            bars_held = 0
            cooldown_remaining = max(0, int(cfg.cooldown_bars))
            if exited_ticker:
                same_asset_cooldown_until[exited_ticker] = i + max(0, int(cfg.same_asset_cooldown_bars))
            equity.append(capital)
            if verbose:
                print(f"SELL {d.date()} reason={reason} pnl={pnl_pct:+.2f}%")
        else:
            if position == 1 and entry_price > 0:
                bars_held += 1
                mtm_position = entry_capital * (held_price / entry_price)
                unallocated = capital
                equity.append(unallocated + mtm_position)
            else:
                equity.append(capital)
                if cooldown_remaining > 0:
                    cooldown_remaining -= 1

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
        "metrics": compute_performance_metrics(equity, equity_dates, trades, cfg.initial_capital, timeframe),
    }


def run_walk_forward_optuna(
    raw_data: Dict[str, pd.DataFrame],
    timeframe: str,
    is_crypto: bool,
    base_cfg: BacktestConfig,
    n_trials: int = 30,
    n_jobs: int = 1,
    indicator_cache: Optional[Dict[str, pd.DataFrame]] = None,
) -> Tuple[BacktestConfig, Optional[Dict[str, object]]]:
    if optuna is None:
        print("Optuna not installed; skipping auto-tune.")
        return base_cfg, None

    if not raw_data:
        return base_cfg, None

    anchor = list(raw_data.keys())[0]
    all_dates = raw_data[anchor].index.sort_values()
    if len(all_dates) < 220:
        print("Not enough history for walk-forward tuning.")
        return base_cfg, None

    # Explicit split:
    # - train/validate folds on earlier history
    # - final holdout test on most recent ~0.5y
    train_days = int(round(1.5 * 365.25))
    val_days = int(round(0.5 * 365.25))
    holdout_days = int(round(0.5 * 365.25))
    holdout_start = all_dates[-1] - pd.Timedelta(days=holdout_days)
    optimize_end = holdout_start - pd.Timedelta(seconds=1)

    def objective(trial: "optuna.Trial") -> float:
        max_pos = float(np.clip(base_cfg.max_exposure_pct, 0.20, 0.40))
        tuned = TunedParams(
            position_size=trial.suggest_float("position_size", 0.20, max_pos),
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
            min_hold_bars=base_cfg.min_hold_bars,
            cooldown_bars=base_cfg.cooldown_bars,
            same_asset_cooldown_bars=base_cfg.same_asset_cooldown_bars,
            max_consecutive_same_asset_entries=base_cfg.max_consecutive_same_asset_entries,
            max_drawdown_limit_pct=base_cfg.max_drawdown_limit_pct,
            max_exposure_pct=base_cfg.max_exposure_pct,
            fee_pct=base_cfg.fee_pct,
            slippage_pct=base_cfg.slippage_pct,
            tuned=tuned,
        )

        fold_returns: List[float] = []
        fold_dd: List[float] = []
        fold_turnover: List[float] = []
        walk_start = all_dates[0] + pd.Timedelta(days=train_days)
        walk_end = optimize_end - pd.Timedelta(days=val_days)
        cursor = walk_start

        if walk_end < walk_start:
            return -999.0

        while cursor <= walk_end:
            test_start = cursor
            test_end = cursor + pd.Timedelta(days=val_days)
            res = simulate_backtest(
                raw_data,
                timeframe,
                cfg,
                is_crypto,
                start_date=test_start,
                end_date=test_end,
                indicator_cache=indicator_cache,
                verbose=False,
            )
            # Penalize parameter sets that do not produce trades during walk-forward.
            if len(res.get("trades", [])) == 0:
                fold_returns.append(-0.25)
                fold_dd.append(100.0)
                fold_turnover.append(0.0)
            else:
                fold_returns.append(float(res["return_pct"]))
                m = res.get("metrics", {})
                this_dd = float(m.get("max_drawdown_pct", 100.0))
                # Hard risk constraint: reject any trial breaching drawdown limit.
                if this_dd > cfg.max_drawdown_limit_pct:
                    raise optuna.TrialPruned(f"Validation drawdown {this_dd:.2f}% > limit {cfg.max_drawdown_limit_pct:.2f}%")
                fold_dd.append(this_dd)
                fold_turnover.append(float(m.get("turnover_per_year", 0.0)))
            cursor += pd.Timedelta(days=val_days)

        if not fold_returns:
            return -999.0

        mean_ret = float(np.mean(fold_returns))
        std_ret = float(np.std(fold_returns))
        mean_dd = float(np.mean(fold_dd)) if fold_dd else 100.0
        mean_turnover = float(np.mean(fold_turnover)) if fold_turnover else 0.0

        # Objective: reward return stability and moderate churn.
        # Drawdown/exposure are enforced as hard constraints.
        score = mean_ret - 0.4 * std_ret
        turnover_penalty = max(0.0, mean_turnover - 40.0) * 0.05
        return score - turnover_penalty

    study = optuna.create_study(direction="maximize")
    study.optimize(objective, n_trials=n_trials, n_jobs=max(1, int(n_jobs)), show_progress_bar=False)

    complete_trials = [t for t in study.trials if t.state == optuna.trial.TrialState.COMPLETE]
    if not complete_trials:
        print("Auto-Tune produced no valid completed trials under current constraints; using base config.")
        return base_cfg, None
    best_trial = max(complete_trials, key=lambda t: float(t.value) if t.value is not None else -1e18)
    bp = best_trial.params
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
        min_hold_bars=base_cfg.min_hold_bars,
        cooldown_bars=base_cfg.cooldown_bars,
        same_asset_cooldown_bars=base_cfg.same_asset_cooldown_bars,
        max_consecutive_same_asset_entries=base_cfg.max_consecutive_same_asset_entries,
        max_drawdown_limit_pct=base_cfg.max_drawdown_limit_pct,
        max_exposure_pct=base_cfg.max_exposure_pct,
        fee_pct=base_cfg.fee_pct,
        slippage_pct=base_cfg.slippage_pct,
        tuned=tuned,
    )
    print("Auto-Tune complete. Best params:")
    print(bp)

    holdout = simulate_backtest(
        raw_data,
        timeframe,
        out,
        is_crypto,
        start_date=holdout_start,
        end_date=all_dates[-1],
        indicator_cache=indicator_cache,
        verbose=False,
    )
    hm = holdout.get("metrics", {})
    holdout_dd = float(hm.get("max_drawdown_pct", 0.0))
    if holdout_dd > out.max_drawdown_limit_pct:
        print(
            f"WARNING: Best tuned params failed holdout drawdown constraint "
            f"({holdout_dd:.2f}% > {out.max_drawdown_limit_pct:.2f}%)."
        )
    print(
        "Holdout test summary "
        f"({holdout_start.date()} to {all_dates[-1].date()}): "
        f"Return {holdout.get('return_pct', 0.0):+.2f}% | "
        f"MaxDD {hm.get('max_drawdown_pct', 0.0):.2f}% | "
        f"Sharpe {hm.get('sharpe', 0.0):.2f}"
    )
    return out, holdout

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


def choose_crypto_quote_currency() -> str:
    print("Select quote currency for crypto pairs:")
    print("1. USD  2. USDT  3. BTC  4. ETH  5. BNB")
    ch = input("Enter 1-5 (default 1): ").strip() or "1"
    return CRYPTO_QUOTE_MENU.get(ch, "USD")


def choose_traditional_top_n() -> int:
    print("Number of top traditional assets:")
    print("1. Top 10   2. Top 20   3. Top 50   4. Top 100")
    ch = input("Enter 1-4 (default 2): ").strip() or "2"
    return {"1": 10, "2": 20, "3": 50, "4": 100}.get(ch, 20)


def load_assets(is_crypto: bool, quote_currency: str = "USD") -> List[str]:
    if is_crypto:
        print("Number of top coins:")
        print("1. Top 10   2. Top 20   3. Top 50   4. Top 100")
        n_choice = input("Enter 1-4: ").strip()
        n = {"1": 10, "2": 20, "3": 50, "4": 100}.get(n_choice, 20)
        return filter_crypto_tickers_by_binance(fetch_top_coins(n, quote_currency=quote_currency))

    print("Select Country/Region:")
    print("1. Australia  2. United States  3. United Kingdom  4. Europe  5. Canada  6. Other")
    country = input("Enter 1-6: ").strip()
    suffix = get_traditional_suffix(country)

    print("Asset selection:")
    print("1. Top 10 regional  2. Manual  3. Both")
    mode = input("Enter 1-3: ").strip()

    tickers: List[str] = []
    if mode in ["1", "3"]:
        top_n = choose_traditional_top_n()
        universe = TRADITIONAL_TOP.get(country, TRADITIONAL_TOP["2"])
        tickers.extend(universe[:top_n])
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


def prompt_backtest_config(timeframe: str) -> BacktestConfig:
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
    default_min_hold = 4 if timeframe == "8h" else 2
    default_cooldown = 2 if timeframe == "8h" else 1
    default_same_asset_cd = 6 if timeframe == "8h" else 3
    min_hold_bars = read_int(f"Minimum hold bars before signal-exit (default {default_min_hold}): ", default_min_hold)
    cooldown_bars = read_int(f"Cooldown bars after exit (default {default_cooldown}): ", default_cooldown)
    same_asset_cd = read_int(f"Same-asset re-entry cooldown bars (default {default_same_asset_cd}): ", default_same_asset_cd)
    max_consecutive_same = read_int("Max consecutive same-asset entries (default 3): ", 3)
    max_dd_limit = read_float("Max drawdown target % for optimizer (default 35): ", 35.0)
    max_exposure = read_float("Max exposure % for optimizer (default 40): ", 40.0) / 100.0
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
        min_hold_bars=max(0, min_hold_bars),
        cooldown_bars=max(0, cooldown_bars),
        same_asset_cooldown_bars=max(0, same_asset_cd),
        max_consecutive_same_asset_entries=max(1, max_consecutive_same),
        max_drawdown_limit_pct=max(1.0, max_dd_limit),
        max_exposure_pct=float(np.clip(max_exposure, 0.20, 1.0)),
        fee_pct=fee,
        slippage_pct=slip,
        tuned=tuned,
    )


def prompt_backtest_months() -> int:
    print("Backtest lookback:")
    print("1. 1 month   2. 3 months   3. 6 months   4. 12 months   5. 18 months   6. 24 months")
    ch = input("Enter 1-6 (default 4): ").strip() or "4"
    months = {"1": 1, "2": 3, "3": 6, "4": 12, "5": 18, "6": 24}.get(ch, 12)
    approx_days = int(round(months * 365.25 / 12.0))
    print(f"Backtest window set: {months} month(s) (~{approx_days} days using 365.25-day year).")
    return months


def prompt_cpu_workers(label: str) -> int:
    default_workers = max(1, min(8, (os.cpu_count() or 1)))
    try:
        raw = input(f"{label} CPU workers (default {default_workers}): ").strip()
        val = int(raw) if raw else default_workers
        return max(1, val)
    except ValueError:
        return default_workers


def choose_display_currency(current: str = "USD") -> str:
    print("Select display currency for comparative reporting:")
    print("1. USD  2. AUD  3. EUR  4. GBP  5. CAD  6. JPY  7. NZD  8. SGD  9. HKD  10. CHF")
    default_key = next((k for k, v in DISPLAY_CCY_MENU.items() if v == current.upper()), "1")
    ch = input(f"Enter 1-10 (default {default_key}): ").strip() or default_key
    return DISPLAY_CCY_MENU.get(ch, current.upper())


def get_usd_to_currency_rate(currency: str) -> float:
    ccy = str(currency).upper().strip() or "USD"
    if ccy == "USD":
        return 1.0

    now = pd.Timestamp.utcnow()
    cached = FX_CACHE.get(ccy)
    if cached is not None:
        ts, val = cached
        if (now - ts) <= pd.Timedelta(minutes=30):
            return float(val)

    # Try direct USDXXX pair first (e.g., USDAUD=X).
    candidates = [(f"USD{ccy}=X", False), (f"{ccy}USD=X", True)]
    for sym, invert in candidates:
        try:
            df = yf.download(sym, period="7d", interval="1d", auto_adjust=False, progress=False)
            if df is None or df.empty:
                continue
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            close = pd.to_numeric(df.get("Close"), errors="coerce").dropna()
            if close.empty:
                continue
            rate = float(close.iloc[-1])
            if rate <= 0:
                continue
            val = (1.0 / rate) if invert else rate
            FX_CACHE[ccy] = (now, float(val))
            return float(val)
        except Exception:
            continue
    return 1.0


def run_subprocess_task(cmd: List[str], title: str) -> bool:
    print("\n" + "=" * 100)
    print(title)
    print("=" * 100)
    print("Command:", " ".join(cmd))
    try:
        proc = subprocess.run(cmd, cwd=str(REPO_ROOT), check=False)
        if proc.returncode == 0:
            print(f"{title}: completed successfully.")
            return True
        print(f"{title}: failed with exit code {proc.returncode}.")
        return False
    except Exception as e:
        print(f"{title}: failed to execute ({e}).")
        return False


def run_standard_auto_research_task() -> None:
    cmd = [sys.executable, str(SCRIPTS_DIR / "auto_research_cycle.py")]
    run_subprocess_task(cmd, "AUTO-RESEARCH (STANDARD)")


def build_comprehensive_scenarios() -> List[Dict[str, object]]:
    print("Comprehensive markets:")
    print("1. Crypto only")
    print("2. Traditional only")
    print("3. Both")
    market_choice = input("Enter 1-3 (default 3): ").strip() or "3"
    include_crypto = market_choice in ("1", "3")
    include_trad = market_choice in ("2", "3")

    crypto_quote = "USD"
    if include_crypto:
        crypto_quote = choose_crypto_quote_currency()

    trad_country = "2"
    if include_trad:
        print("Traditional region for comprehensive sweep:")
        print("1. Australia  2. United States  3. United Kingdom  4. Europe  5. Canada")
        trad_country = input("Enter 1-5 (default 2): ").strip() or "2"
        if trad_country not in {"1", "2", "3", "4", "5"}:
            trad_country = "2"

    months_set = [1, 3, 6, 12, 18, 24]
    tfs = ["1d", "4h", "8h", "12h"]
    scenarios: List[Dict[str, object]] = []

    if include_crypto:
        for tf in tfs:
            for m in months_set:
                for n in [10, 20, 50, 100]:
                    scenarios.append(
                        {
                            "id": f"crypto_{tf}_{m}m_top{n}_{crypto_quote}",
                            "market": "crypto",
                            "timeframe": tf,
                            "months": m,
                            "top_n": n,
                            "quote_currency": crypto_quote,
                        }
                    )

    if include_trad:
        for tf in tfs:
            for m in months_set:
                for n in [10, 20, 50, 100]:
                    scenarios.append(
                        {
                            "id": f"trad_c{trad_country}_{tf}_{m}m_top{n}",
                            "market": "traditional",
                            "country": trad_country,
                            "timeframe": tf,
                            "months": m,
                            "top_n": n,
                        }
                    )
    return scenarios


def run_comprehensive_auto_research_task() -> None:
    scenarios = build_comprehensive_scenarios()
    if not scenarios:
        print("No scenarios generated.")
        return

    try:
        default_trials = 10
        trials_in = input(f"Optuna trials per scenario (default {default_trials}): ").strip()
        optuna_trials = int(trials_in) if trials_in else default_trials
    except ValueError:
        optuna_trials = 10
    try:
        default_jobs = max(1, min(8, (os.cpu_count() or 1)))
        jobs_in = input(f"Optuna parallel jobs (default {default_jobs}): ").strip()
        optuna_jobs = int(jobs_in) if jobs_in else default_jobs
    except ValueError:
        optuna_jobs = max(1, min(8, (os.cpu_count() or 1)))

    runs_dir = REPO_ROOT / "experiments" / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    stamp = pd.Timestamp.utcnow().strftime("%Y%m%dT%H%M%SZ")
    scen_path = runs_dir / f"scenarios_comprehensive_{stamp}.json"
    scen_path.write_text(json.dumps(scenarios, indent=2), encoding="utf-8")
    print(f"Comprehensive scenarios written: {scen_path}")
    print(f"Total scenarios: {len(scenarios)}")

    ok1 = run_subprocess_task(
        [
            sys.executable,
            str(SCRIPTS_DIR / "run_experiments.py"),
            "--scenarios",
            str(scen_path),
            "--enable-optuna",
            "--optuna-trials",
            str(max(1, optuna_trials)),
            "--optuna-jobs",
            str(max(1, optuna_jobs)),
        ],
        "AUTO-RESEARCH (COMPREHENSIVE): RUN EXPERIMENTS",
    )
    if not ok1:
        return

    ok2 = run_subprocess_task(
        [sys.executable, str(SCRIPTS_DIR / "promote_champion.py")],
        "AUTO-RESEARCH (COMPREHENSIVE): PROMOTE CHAMPIONS",
    )
    if not ok2:
        return

    run_subprocess_task(
        [sys.executable, str(SCRIPTS_DIR / "update_handoff.py")],
        "AUTO-RESEARCH (COMPREHENSIVE): UPDATE HANDOFF",
    )


def load_champion_registry() -> Dict[str, dict]:
    if not CHAMPION_REGISTRY_PATH.exists():
        return {}
    try:
        data = json.loads(CHAMPION_REGISTRY_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def parse_scenario_id(sid: str) -> Dict[str, Optional[int]]:
    """
    Parse scenario ids like:
      crypto_8h_24m_top10
      traditional_1d_12m
    """
    out: Dict[str, Optional[int]] = {"market": None, "timeframe": None, "months": None, "top_n": None}
    m = re.match(r"^(crypto|traditional)_(1d|4h|8h|12h)_(\d+)m(?:_top(\d+))?$", str(sid).strip().lower())
    if not m:
        return out
    out["market"] = m.group(1)
    out["timeframe"] = m.group(2)
    out["months"] = int(m.group(3))
    out["top_n"] = int(m.group(4)) if m.group(4) else None
    return out


def find_best_champion_match(
    champions: Dict[str, dict],
    market: str,
    timeframe: str,
    months: Optional[int],
    requested_assets: Optional[int],
) -> Tuple[Optional[str], Optional[dict]]:
    if not champions:
        return None, None

    best_sid: Optional[str] = None
    best_entry: Optional[dict] = None
    best_score = -1e9

    for sid, entry in champions.items():
        if not isinstance(entry, dict):
            continue
        parsed = parse_scenario_id(sid)
        if parsed.get("market") is None or parsed.get("timeframe") is None:
            continue
        if str(parsed["market"]) != market:
            continue
        if str(parsed["timeframe"]) != timeframe:
            continue
        if not isinstance((entry.get("selected", {}) or {}).get("config", {}), dict):
            continue

        score = 0.0
        score += 10.0

        cm = parsed.get("months")
        if months is not None and cm is not None:
            score -= abs(int(months) - int(cm)) * 0.25
            if int(months) == int(cm):
                score += 4.0
        elif cm is not None:
            score += 1.0

        ct = parsed.get("top_n")
        if requested_assets is not None and ct is not None:
            if int(ct) == int(requested_assets):
                score += 2.5
            elif abs(int(ct) - int(requested_assets)) <= 5:
                score += 1.0

        metrics = entry.get("metrics", {}) or {}
        score += float(metrics.get("sharpe", 0.0)) * 0.2
        score += float(metrics.get("return_pct", 0.0)) * 0.02
        score -= max(0.0, float(metrics.get("max_drawdown_pct", 0.0)) - 35.0) * 0.03

        if score > best_score:
            best_score = score
            best_sid = str(sid)
            best_entry = entry

    return best_sid, best_entry


def champion_summary_line(sid: str, entry: dict) -> str:
    m = entry.get("metrics", {}) or {}
    return (
        f"Champion match `{sid}` | Return {float(m.get('return_pct', 0.0)):+.2f}% | "
        f"MaxDD {float(m.get('max_drawdown_pct', 0.0)):.2f}% | Sharpe {float(m.get('sharpe', 0.0)):.2f}"
    )


def apply_champion_tuned_params(base_tuned: TunedParams, champion_entry: dict) -> TunedParams:
    cfg = ((champion_entry.get("selected", {}) or {}).get("config", {}) or {})
    t = cfg.get("tuned", {}) or {}
    return TunedParams(
        position_size=float(np.clip(float(t.get("position_size", base_tuned.position_size)), 0.20, 0.40)),
        atr_multiplier=float(np.clip(float(t.get("atr_multiplier", base_tuned.atr_multiplier)), 2.0, 2.5)),
        adx_threshold=float(t.get("adx_threshold", base_tuned.adx_threshold)),
        cmf_threshold=float(t.get("cmf_threshold", base_tuned.cmf_threshold)),
        obv_slope_threshold=float(t.get("obv_slope_threshold", base_tuned.obv_slope_threshold)),
        buy_threshold=int(t.get("buy_threshold", base_tuned.buy_threshold)),
        sell_threshold=int(t.get("sell_threshold", base_tuned.sell_threshold)),
    )


def apply_champion_backtest_config(base_cfg: BacktestConfig, champion_entry: dict) -> BacktestConfig:
    cfg = ((champion_entry.get("selected", {}) or {}).get("config", {}) or {})
    tuned = apply_champion_tuned_params(base_cfg.tuned, champion_entry)
    return BacktestConfig(
        initial_capital=float(cfg.get("initial_capital", base_cfg.initial_capital)),
        stop_loss_pct=float(cfg.get("stop_loss_pct", base_cfg.stop_loss_pct)),
        take_profit_pct=float(cfg.get("take_profit_pct", base_cfg.take_profit_pct)),
        max_hold_days=int(cfg.get("max_hold_days", base_cfg.max_hold_days)),
        fee_pct=float(cfg.get("fee_pct", base_cfg.fee_pct)),
        slippage_pct=float(cfg.get("slippage_pct", base_cfg.slippage_pct)),
        min_hold_bars=max(0, int(cfg.get("min_hold_bars", base_cfg.min_hold_bars))),
        cooldown_bars=max(0, int(cfg.get("cooldown_bars", base_cfg.cooldown_bars))),
        same_asset_cooldown_bars=max(0, int(cfg.get("same_asset_cooldown_bars", base_cfg.same_asset_cooldown_bars))),
        max_consecutive_same_asset_entries=max(1, int(cfg.get("max_consecutive_same_asset_entries", base_cfg.max_consecutive_same_asset_entries))),
        max_drawdown_limit_pct=max(1.0, float(cfg.get("max_drawdown_limit_pct", base_cfg.max_drawdown_limit_pct))),
        max_exposure_pct=float(np.clip(float(cfg.get("max_exposure_pct", base_cfg.max_exposure_pct)), 0.20, 1.0)),
        tuned=tuned,
    )


def ensure_user_prefs_dir() -> None:
    USER_PREFS_DIR.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(USER_PREFS_DIR, 0o700)
    except Exception:
        pass


def default_ai_preferences() -> Dict[str, object]:
    return {
        "active_profile": "default_xai",
        "profiles": {
            "default_xai": {
                "provider": "xai",
                "model": "grok-3-mini",
                "endpoint": "https://api.x.ai/v1/chat/completions",
                "api_key_env": "XAI_API_KEY",
                "api_key_name": "default_xai",
                "temperature": 0.2,
            }
        },
    }


def load_json_file(path: Path, default: object) -> object:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def save_json_file(path: Path, data: object, file_mode: int = 0o600) -> None:
    ensure_user_prefs_dir()
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    try:
        os.chmod(path, file_mode)
    except Exception:
        pass


def load_ai_preferences() -> Dict[str, object]:
    raw = load_json_file(AI_PREFS_PATH, default_ai_preferences())
    if not isinstance(raw, dict):
        return default_ai_preferences()
    if "profiles" not in raw or not isinstance(raw["profiles"], dict):
        raw["profiles"] = default_ai_preferences()["profiles"]
    if "active_profile" not in raw or not isinstance(raw["active_profile"], str):
        raw["active_profile"] = next(iter(raw["profiles"].keys()), "default_xai")
    return raw


def save_ai_preferences(prefs: Dict[str, object]) -> None:
    save_json_file(AI_PREFS_PATH, prefs, file_mode=0o600)


def load_ai_secrets() -> Dict[str, str]:
    raw = load_json_file(AI_SECRETS_PATH, {})
    if not isinstance(raw, dict):
        return {}
    out: Dict[str, str] = {}
    for k, v in raw.items():
        if isinstance(k, str) and isinstance(v, str):
            out[k] = v
    return out


def save_ai_secrets(secrets: Dict[str, str]) -> None:
    save_json_file(AI_SECRETS_PATH, secrets, file_mode=0o600)


def provider_default_endpoint(provider: str) -> str:
    p = provider.lower().strip()
    if p == "xai":
        return "https://api.x.ai/v1/chat/completions"
    if p == "openai":
        return "https://api.openai.com/v1/chat/completions"
    if p == "anthropic":
        return "https://api.anthropic.com/v1/messages"
    if p == "ollama":
        return "http://localhost:11434/api/chat"
    if p in ("openai_compatible", "openclaw"):
        return "http://localhost:1234/v1/chat/completions"
    return ""


def provider_default_model(provider: str) -> str:
    p = provider.lower().strip()
    if p == "xai":
        return "grok-3-mini"
    if p == "openai":
        return "gpt-4o-mini"
    if p == "anthropic":
        return "claude-3-5-sonnet-latest"
    if p == "ollama":
        return "llama3.1"
    if p in ("openai_compatible", "openclaw"):
        return "openclaw"
    return "model"


def provider_default_envvar(provider: str) -> str:
    p = provider.lower().strip()
    if p == "xai":
        return "XAI_API_KEY"
    if p == "openai":
        return "OPENAI_API_KEY"
    if p == "anthropic":
        return "ANTHROPIC_API_KEY"
    if p in ("openai_compatible", "openclaw"):
        return "OPENCLAW_API_KEY"
    return ""


def provider_default_internet_access(provider: str) -> bool:
    p = provider.lower().strip()
    if p in ("xai", "openai", "anthropic"):
        return True
    return False


def provider_needs_api_key(provider: str) -> bool:
    return provider.lower().strip() in ("xai", "openai", "anthropic")


def get_active_ai_profile(prefs: Dict[str, object]) -> Tuple[str, Dict[str, object]]:
    profiles = prefs.get("profiles", {}) if isinstance(prefs, dict) else {}
    if not isinstance(profiles, dict) or not profiles:
        d = default_ai_preferences()
        return str(d["active_profile"]), d["profiles"][str(d["active_profile"])]
    active = str(prefs.get("active_profile", "")).strip()
    if active in profiles and isinstance(profiles[active], dict):
        return active, profiles[active]
    first = next(iter(profiles.keys()))
    return str(first), profiles[str(first)]


def resolve_profile_api_key(profile: Dict[str, object], secrets: Dict[str, str]) -> Tuple[str, str]:
    env_name = str(profile.get("api_key_env", "") or "").strip()
    if env_name:
        env_val = (os.getenv(env_name, "") or "").strip()
        if env_val:
            return env_val, f"env:{env_name}"
    key_name = str(profile.get("api_key_name", "") or "").strip()
    if key_name and key_name in secrets and secrets[key_name].strip():
        return secrets[key_name].strip(), f"prefs:{key_name}"
    return "", "missing"


def call_openai_style_chat(endpoint: str, model: str, prompt: str, system_prompt: str, temperature: float, api_key: str = "") -> Tuple[Optional[str], Optional[int], str]:
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ],
        "temperature": float(np.clip(temperature, 0.0, 1.0)),
    }
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    try:
        r = requests.post(endpoint, headers=headers, json=payload, timeout=180)
        if not (200 <= r.status_code < 300):
            return None, r.status_code, (r.text or "")[:1200]
        data = r.json()
        choices = data.get("choices", [])
        if not choices:
            return None, r.status_code, "Empty choices in response."
        msg = choices[0].get("message", {}) or {}
        content = msg.get("content")
        if isinstance(content, str):
            return content.strip(), r.status_code, ""
        return (str(content).strip() if content is not None else None), r.status_code, ""
    except Exception as e:
        return None, -1, str(e)


def call_anthropic_chat(endpoint: str, model: str, prompt: str, system_prompt: str, temperature: float, api_key: str) -> Tuple[Optional[str], Optional[int], str]:
    payload = {
        "model": model,
        "max_tokens": 1800,
        "temperature": float(np.clip(temperature, 0.0, 1.0)),
        "system": system_prompt,
        "messages": [{"role": "user", "content": prompt}],
    }
    headers = {
        "Content-Type": "application/json",
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
    }
    try:
        r = requests.post(endpoint, headers=headers, json=payload, timeout=180)
        if not (200 <= r.status_code < 300):
            return None, r.status_code, (r.text or "")[:1200]
        data = r.json()
        parts = data.get("content", [])
        text_parts = [p.get("text", "") for p in parts if isinstance(p, dict) and p.get("type") == "text"]
        content = "\n".join([t for t in text_parts if t]).strip()
        return (content if content else None), r.status_code, ""
    except Exception as e:
        return None, -1, str(e)


def call_ollama_chat(endpoint: str, model: str, prompt: str, system_prompt: str, temperature: float) -> Tuple[Optional[str], Optional[int], str]:
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ],
        "stream": False,
        "options": {"temperature": float(np.clip(temperature, 0.0, 1.0))},
    }
    headers = {"Content-Type": "application/json"}
    try:
        r = requests.post(endpoint, headers=headers, json=payload, timeout=180)
        if not (200 <= r.status_code < 300):
            return None, r.status_code, (r.text or "")[:1200]
        data = r.json()
        msg = data.get("message", {}) if isinstance(data, dict) else {}
        content = msg.get("content") if isinstance(msg, dict) else None
        if isinstance(content, str):
            return content.strip(), r.status_code, ""
        return None, r.status_code, "No message.content returned by Ollama."
    except Exception as e:
        return None, -1, str(e)


def try_start_ollama_server() -> bool:
    """
    Best-effort local Ollama startup for users who haven't started it yet.
    """
    try:
        kwargs = {"stdout": subprocess.DEVNULL, "stderr": subprocess.DEVNULL}
        if os.name == "nt":
            DETACHED_PROCESS = 0x00000008
            CREATE_NEW_PROCESS_GROUP = 0x00000200
            subprocess.Popen(["ollama", "serve"], creationflags=DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP, **kwargs)
        else:
            subprocess.Popen(["ollama", "serve"], start_new_session=True, **kwargs)
        time.sleep(2.0)
        return True
    except Exception:
        return False


def pull_ollama_model_if_needed(endpoint: str, model: str) -> bool:
    """
    Best-effort model pull when Ollama reports missing model.
    """
    base = endpoint
    if "/api/chat" in base:
        base = base.split("/api/chat")[0]
    pull_url = base.rstrip("/") + "/api/pull"
    payload = {"name": model, "stream": False}
    try:
        r = requests.post(pull_url, json=payload, timeout=900)
        return 200 <= r.status_code < 300
    except Exception:
        return False


def call_active_ai_provider(prompt: str, system_prompt: str = "You are Grok, built by xAI.") -> Optional[str]:
    prefs = load_ai_preferences()
    secrets = load_ai_secrets()
    profile_name, profile = get_active_ai_profile(prefs)
    provider = str(profile.get("provider", "xai")).strip().lower()
    model = str(profile.get("model", provider_default_model(provider))).strip()
    endpoint = str(profile.get("endpoint", provider_default_endpoint(provider))).strip() or provider_default_endpoint(provider)
    temperature = float(profile.get("temperature", 0.2))

    key, key_source = resolve_profile_api_key(profile, secrets)
    if provider_needs_api_key(provider) and not key:
        print(f"AI provider `{provider}` is missing API key (source: {key_source}).")
        print("Set env var or store key via AI Provider Settings.")
        return None

    print(f"Calling AI provider `{provider}` using profile `{profile_name}` (model `{model}`, key source: {key_source}).")

    if provider in ("xai", "openai", "openai_compatible", "openclaw"):
        # xAI frequently fails on dotted model aliases; try a few common fallbacks.
        model_candidates: List[str] = [model]
        if provider == "xai":
            for m in [model.replace(".", "-"), "grok-4-1-fast-reasoning", "grok-4-1-fast", "grok-3-mini"]:
                if m and m not in model_candidates:
                    model_candidates.append(m)
        last_status = None
        last_body = ""
        for m in model_candidates:
            text, status, err_body = call_openai_style_chat(endpoint, m, prompt, system_prompt, temperature, api_key=key)
            if text:
                return text
            last_status = status
            last_body = err_body
            if status not in (400, 404):
                break
            print(f"AI call failed for model `{m}` with status {status}. Trying fallback model...")
        if last_status is not None:
            print(f"Last AI error status: {last_status}")
        if last_body:
            print("Last AI error body (truncated):")
            print(last_body)
        return None

    if provider == "anthropic":
        text, status, err_body = call_anthropic_chat(endpoint, model, prompt, system_prompt, temperature, api_key=key)
        if text:
            return text
        print(f"Anthropic call failed. Status: {status}")
        if err_body:
            print("Error body (truncated):")
            print(err_body)
        return None

    if provider == "ollama":
        text, status, err_body = call_ollama_chat(endpoint, model, prompt, system_prompt, temperature)
        if text:
            return text
        # Try to recover when daemon/model is unavailable.
        if status == -1:
            print("Ollama server appears unreachable. Attempting to start `ollama serve`...")
            if try_start_ollama_server():
                text2, status2, err_body2 = call_ollama_chat(endpoint, model, prompt, system_prompt, temperature)
                if text2:
                    return text2
                status, err_body = status2, err_body2
        if status in (400, 404) and "model" in (err_body or "").lower() and "not found" in (err_body or "").lower():
            auto_pull = (os.getenv("OLLAMA_AUTO_PULL", "1").strip().lower() not in {"0", "false", "no", "n"})
            if auto_pull:
                print(f"Ollama model `{model}` not found. Attempting auto-pull...")
                if pull_ollama_model_if_needed(endpoint, model):
                    text3, status3, err_body3 = call_ollama_chat(endpoint, model, prompt, system_prompt, temperature)
                    if text3:
                        return text3
                    status, err_body = status3, err_body3
        print(f"Ollama call failed. Status: {status}")
        if err_body:
            print("Error body (truncated):")
            print(err_body)
        return None

    print(f"Unsupported AI provider: {provider}")
    return None


def print_ai_profiles(prefs: Dict[str, object]) -> None:
    active, _ = get_active_ai_profile(prefs)
    profiles = prefs.get("profiles", {}) if isinstance(prefs, dict) else {}
    if not isinstance(profiles, dict) or not profiles:
        print("No AI profiles configured.")
        return
    print("Available AI profiles:")
    for idx, (name, p) in enumerate(profiles.items(), start=1):
        if not isinstance(p, dict):
            continue
        mark = "*" if name == active else " "
        provider = str(p.get("provider", ""))
        model = str(p.get("model", ""))
        endpoint = str(p.get("endpoint", ""))
        internet_access = bool(p.get("internet_access", provider_default_internet_access(provider)))
        net_tag = "online" if internet_access else "offline"
        print(f"{mark} [{idx}] {name}  |  {provider}  |  {model}  |  {net_tag}")
        print(f"      endpoint: {endpoint}")


def choose_profile_name(prefs: Dict[str, object], prompt: str) -> Optional[str]:
    profiles = prefs.get("profiles", {}) if isinstance(prefs, dict) else {}
    if not isinstance(profiles, dict) or not profiles:
        print("No profiles available.")
        return None
    names = list(profiles.keys())
    print_ai_profiles(prefs)
    raw = input(prompt).strip()
    if not raw:
        return None
    if raw.isdigit():
        i = int(raw)
        if 1 <= i <= len(names):
            return names[i - 1]
    if raw in profiles:
        return raw
    print("Profile not found.")
    return None


def configure_ai_profile(prefs: Dict[str, object], secrets: Dict[str, str]) -> Tuple[Dict[str, object], Dict[str, str]]:
    print("Select provider:")
    print("1. xAI (Grok)")
    print("2. OpenAI")
    print("3. Anthropic")
    print("4. Ollama (local)")
    print("5. OpenAI-compatible")
    print("6. OpenClaw (OpenAI-compatible)")
    ch = (input("Enter 1-6: ").strip() or "1")
    provider_map = {
        "1": "xai",
        "2": "openai",
        "3": "anthropic",
        "4": "ollama",
        "5": "openai_compatible",
        "6": "openclaw",
    }
    provider = provider_map.get(ch, "xai")

    default_name = f"{provider}_profile"
    profile_name = (input(f"Profile name (default {default_name}): ").strip() or default_name)
    model_default = provider_default_model(provider)
    endpoint_default = provider_default_endpoint(provider)
    env_default = provider_default_envvar(provider)

    model = (input(f"Model (default {model_default}): ").strip() or model_default)
    endpoint = (input(f"Endpoint (default {endpoint_default}): ").strip() or endpoint_default)
    temp_in = (input("Temperature 0-1 (default 0.2): ").strip() or "0.2")
    try:
        temperature = float(np.clip(float(temp_in), 0.0, 1.0))
    except Exception:
        temperature = 0.2

    api_key_env = ""
    if provider != "ollama":
        api_key_env = (input(f"API key env var name (default {env_default or 'NONE'}): ").strip() or env_default)
    api_key_name = profile_name

    profiles = prefs.get("profiles", {}) if isinstance(prefs, dict) else {}
    if not isinstance(profiles, dict):
        profiles = {}
    profiles[profile_name] = {
        "provider": provider,
        "model": model,
        "endpoint": endpoint,
        "api_key_env": api_key_env,
        "api_key_name": api_key_name,
        "temperature": temperature,
        "internet_access": provider_default_internet_access(provider),
    }
    prefs["profiles"] = profiles
    if not str(prefs.get("active_profile", "")).strip():
        prefs["active_profile"] = profile_name

    if provider != "ollama":
        save_local_key = (input("Store API key securely in user prefs file? (y/n, default n): ").strip().lower() or "n") == "y"
        if save_local_key:
            key = getpass.getpass("Enter API key (input hidden): ").strip()
            if key:
                secrets[api_key_name] = key
                print(f"Stored API key for profile `{profile_name}` in user secrets file.")
    return prefs, secrets


def add_preset_profile(prefs: Dict[str, object], secrets: Dict[str, str]) -> Tuple[Dict[str, object], Dict[str, str]]:
    print("Quick-start presets:")
    print("1. xAI Cloud (Grok)")
    print("2. OpenAI Cloud")
    print("3. Anthropic Cloud")
    print("4. Ollama Local")
    print("5. OpenClaw Local (OpenAI-compatible)")
    print("6. OpenAI-compatible Custom")
    ch = (input("Enter 1-6: ").strip() or "4")
    provider_map = {
        "1": "xai",
        "2": "openai",
        "3": "anthropic",
        "4": "ollama",
        "5": "openclaw",
        "6": "openai_compatible",
    }
    provider = provider_map.get(ch, "ollama")
    profile_name_default = f"preset_{provider}"
    profile_name = (input(f"Profile name (default {profile_name_default}): ").strip() or profile_name_default)

    model = provider_default_model(provider)
    endpoint = provider_default_endpoint(provider)
    api_key_env = provider_default_envvar(provider)

    # Provider-tuned defaults.
    if provider == "ollama":
        model = "llama3.1:8b"
        endpoint = "http://localhost:11434/api/chat"
    elif provider == "openclaw":
        model = "openclaw"
        endpoint = "http://localhost:1234/v1/chat/completions"
        api_key_env = "OPENCLAW_API_KEY"

    override_model = input(f"Model (default {model}): ").strip()
    if override_model:
        model = override_model
    override_endpoint = input(f"Endpoint (default {endpoint}): ").strip()
    if override_endpoint:
        endpoint = override_endpoint

    profiles = prefs.get("profiles", {}) if isinstance(prefs, dict) else {}
    if not isinstance(profiles, dict):
        profiles = {}
    profiles[profile_name] = {
        "provider": provider,
        "model": model,
        "endpoint": endpoint,
        "api_key_env": api_key_env,
        "api_key_name": profile_name,
        "temperature": 0.2,
        "internet_access": provider_default_internet_access(provider),
    }
    prefs["profiles"] = profiles

    if provider != "ollama":
        store_now = (input("Store API key now in secure local prefs? (y/n, default n): ").strip().lower() or "n") == "y"
        if store_now:
            key = getpass.getpass("Enter API key (input hidden): ").strip()
            if key:
                secrets[profile_name] = key
                print(f"Stored API key for `{profile_name}`.")

    activate_now = (input("Set this profile as active now? (y/n, default y): ").strip().lower() or "y") == "y"
    if activate_now:
        prefs["active_profile"] = profile_name
    return prefs, secrets


def run_ai_provider_settings_menu() -> None:
    prefs = load_ai_preferences()
    secrets = load_ai_secrets()
    while True:
        print("\n" + "=" * 100)
        print("AI SETTINGS")
        print("=" * 100)
        active_name, active_profile = get_active_ai_profile(prefs)
        key_val, key_source = resolve_profile_api_key(active_profile, secrets)
        provider = str(active_profile.get("provider", "")).strip()
        model = str(active_profile.get("model", "")).strip()
        internet_access = bool(active_profile.get("internet_access", provider_default_internet_access(provider)))
        key_status = "configured" if key_val else "missing"
        if key_source.startswith("env:"):
            key_hint = f"from environment variable ({key_source.split(':', 1)[1]})"
        elif key_source.startswith("prefs:"):
            key_hint = "from secure local storage"
        else:
            key_hint = "not set"
        print(
            f"Current profile: {active_name}\n"
            f"Provider: {provider}\n"
            f"Model: {model}\n"
            f"Internet mode: {'online-aware' if internet_access else 'offline-aware'}\n"
            f"API key: {key_status} ({key_hint})"
        )
        print("1. View profiles")
        print("2. Add quick-start profile")
        print("3. Create or edit custom profile")
        print("4. Switch active profile")
        print("5. Delete a profile")
        print("6. Set or replace profile API key")
        print("7. Remove profile API key")
        print("8. Test active profile connection")
        print("9. Set model for a profile")
        print("10. Toggle internet-access flag for a profile")
        print("11. Back")

        ch = (input("Enter 1-11: ").strip() or "11")
        profiles = prefs.get("profiles", {}) if isinstance(prefs, dict) else {}
        if not isinstance(profiles, dict):
            profiles = {}
            prefs["profiles"] = profiles

        if ch == "11":
            save_ai_preferences(prefs)
            save_ai_secrets(secrets)
            return
        if ch == "1":
            print_ai_profiles(prefs)
            continue
        if ch == "2":
            prefs, secrets = add_preset_profile(prefs, secrets)
            save_ai_preferences(prefs)
            save_ai_secrets(secrets)
            print("Preset profile saved.")
            continue
        if ch == "3":
            prefs, secrets = configure_ai_profile(prefs, secrets)
            save_ai_preferences(prefs)
            save_ai_secrets(secrets)
            print("Profile saved.")
            continue
        if ch == "4":
            name = choose_profile_name(prefs, "Enter profile number or name to activate: ")
            if name and name in profiles:
                prefs["active_profile"] = name
                save_ai_preferences(prefs)
                print(f"Active profile set to `{name}`.")
            continue
        if ch == "5":
            name = choose_profile_name(prefs, "Enter profile number or name to delete: ")
            if name and name in profiles:
                del profiles[name]
                if str(prefs.get("active_profile", "")) == name:
                    prefs["active_profile"] = next(iter(profiles.keys()), "")
                save_ai_preferences(prefs)
                print(f"Removed profile `{name}`.")
            continue
        if ch == "6":
            name = choose_profile_name(prefs, "Enter profile number or name for API key update: ")
            if not name or name not in profiles or not isinstance(profiles[name], dict):
                continue
            key = getpass.getpass("Enter API key (input hidden): ").strip()
            if not key:
                print("No key entered.")
                continue
            key_name = str(profiles[name].get("api_key_name", name)).strip() or name
            profiles[name]["api_key_name"] = key_name
            secrets[key_name] = key
            save_ai_preferences(prefs)
            save_ai_secrets(secrets)
            print(f"Stored API key for profile `{name}`.")
            continue
        if ch == "7":
            name = choose_profile_name(prefs, "Enter profile number or name to remove stored API key: ")
            if not name or name not in profiles or not isinstance(profiles[name], dict):
                continue
            key_name = str(profiles[name].get("api_key_name", name)).strip() or name
            if key_name in secrets:
                del secrets[key_name]
                save_ai_secrets(secrets)
                print(f"Removed stored API key for profile `{name}`.")
            else:
                print("No stored key for that profile.")
            continue
        if ch == "8":
            resp = call_active_ai_provider("Reply with exactly: OK", system_prompt="You are a concise assistant.")
            if resp:
                print(f"AI test response: {resp[:200]}")
            else:
                print("AI test call failed.")
            continue
        if ch == "9":
            name = choose_profile_name(prefs, "Enter profile number or name to set model: ")
            if not name or name not in profiles or not isinstance(profiles[name], dict):
                continue
            current_model = str(profiles[name].get("model", "")).strip()
            new_model = input(f"Model for `{name}` (current `{current_model}`): ").strip()
            if not new_model:
                print("No change made.")
                continue
            profiles[name]["model"] = new_model
            save_ai_preferences(prefs)
            print(f"Updated model for `{name}` to `{new_model}`.")
            continue
        if ch == "10":
            name = choose_profile_name(prefs, "Enter profile number or name to toggle internet-access flag: ")
            if not name or name not in profiles or not isinstance(profiles[name], dict):
                continue
            cur = bool(profiles[name].get("internet_access", provider_default_internet_access(str(profiles[name].get("provider", "")))))
            profiles[name]["internet_access"] = (not cur)
            save_ai_preferences(prefs)
            print(f"Profile `{name}` internet_access set to {profiles[name]['internet_access']}.")
            continue
        print("Invalid choice.")


def read_multiline_input_until_end() -> str:
    print("Paste the FULL dashboard text. Type a new line with only END when finished:")
    lines: List[str] = []
    while True:
        try:
            line = input()
        except EOFError:
            break
        if line.strip() == "END":
            break
        lines.append(line)
    return "\n".join(lines).strip()


def compose_live_dashboard_snapshot_text(
    table: pd.DataFrame,
    risk: pd.DataFrame,
    portfolio_notes: List[str],
    is_crypto: bool,
    timeframe: str,
    requested_assets: int,
    loaded_assets: int,
) -> str:
    lines: List[str] = []
    lines.append(f"Snapshot Time: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"Market: {'Crypto' if is_crypto else 'Traditional'}")
    lines.append(f"Timeframe: {timeframe}")
    lines.append(f"Assets: requested={requested_assets}, loaded={loaded_assets}")
    lines.append("")
    lines.append("=" * 170)
    lines.append(f"LIVE DASHBOARD ({'Crypto' if is_crypto else 'Traditional'}) [{timeframe}]")
    lines.append("=" * 170)
    lines.append(table.drop(columns=["_Ticker"], errors="ignore").to_string())
    lines.append("")
    lines.append("=" * 170)
    lines.append("RISK SCORE BREAKDOWN")
    lines.append("=" * 170)
    lines.append(risk.to_string())
    if portfolio_notes:
        lines.append("")
        lines.append("=" * 170)
        lines.append("PORTFOLIO CONTEXT")
        lines.append("=" * 170)
        for n in portfolio_notes:
            lines.append(f"- {n}")
    return "\n".join(lines).strip() + "\n"


def save_live_dashboard_snapshot(snapshot_text: str) -> Path:
    LIVE_SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = pd.Timestamp.utcnow().strftime("%Y%m%dT%H%M%SZ")
    ts_path = LIVE_SNAPSHOT_DIR / f"live_dashboard_{stamp}.txt"
    ts_path.write_text(snapshot_text, encoding="utf-8")
    LATEST_LIVE_SNAPSHOT_PATH.write_text(snapshot_text, encoding="utf-8")
    return ts_path


def load_latest_live_dashboard_snapshot() -> Optional[str]:
    if not LATEST_LIVE_SNAPSHOT_PATH.exists():
        return None
    try:
        txt = LATEST_LIVE_SNAPSHOT_PATH.read_text(encoding="utf-8").strip()
        return txt if txt else None
    except Exception:
        return None


def compose_backtest_snapshot_text(
    result: Dict[str, object],
    trades: List[Dict[str, object]],
    metrics: Dict[str, object],
    cfg: BacktestConfig,
    timeframe: str,
    is_crypto: bool,
    backtest_months: int,
    loaded_assets: int,
    holdout_result: Optional[Dict[str, object]] = None,
) -> str:
    lines: List[str] = []
    lines.append(f"Snapshot Time: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"Market: {'Crypto' if is_crypto else 'Traditional'}")
    lines.append(f"Timeframe: {timeframe}")
    lines.append(f"Backtest Lookback Months: {backtest_months}")
    lines.append(f"Loaded Assets: {loaded_assets}")
    lines.append("")
    lines.append("=" * 170)
    lines.append("BACKTEST SUMMARY")
    lines.append("=" * 170)
    lines.append(f"Final Value   : ${float(result.get('final', 0.0)):,.2f}")
    lines.append(f"Total Return  : {float(result.get('return_pct', 0.0)):+.2f}%")
    lines.append(f"Trades closed : {len(trades)}")
    lines.append(f"Max Drawdown  : {float(metrics.get('max_drawdown_pct', 0.0)):.2f}%")
    lines.append(f"Sharpe/Sortino: {float(metrics.get('sharpe', 0.0)):.2f} / {float(metrics.get('sortino', 0.0)):.2f}")
    lines.append(f"Turnover/Year : {float(metrics.get('turnover_per_year', 0.0)):.2f} trades")
    lines.append(f"Avg Hold Days : {float(metrics.get('avg_hold_days', 0.0)):.2f}")
    lines.append(f"Fee/Slippage  : {cfg.fee_pct}% / {cfg.slippage_pct}% per leg")
    lines.append(f"Position size : {cfg.tuned.position_size*100:.1f}%")
    lines.append("")
    lines.append("=" * 170)
    lines.append("TRADE HISTORY")
    lines.append("=" * 170)
    if trades:
        tr_df = pd.DataFrame(trades)
        lines.append(tr_df.to_string(index=False))
    else:
        lines.append("No closed trades.")

    contrib = metrics.get("asset_contrib", {})
    if isinstance(contrib, dict) and contrib:
        lines.append("")
        lines.append("Top Asset Contribution:")
        for asset, pnl in list(contrib.items())[:5]:
            lines.append(f"  {asset:<8} ${float(pnl):,.2f}")

    if holdout_result is not None:
        hm = holdout_result.get("metrics", {}) if isinstance(holdout_result, dict) else {}
        lines.append("")
        lines.append(
            f"Holdout (out-of-sample) Return: {float(holdout_result.get('return_pct', 0.0)):+.2f}%  |  "
            f"MaxDD: {float(hm.get('max_drawdown_pct', 0.0)):.2f}%  |  Sharpe: {float(hm.get('sharpe', 0.0)):.2f}"
        )
    return "\n".join(lines).strip() + "\n"


def save_backtest_snapshot(snapshot_text: str) -> Path:
    BACKTEST_SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = pd.Timestamp.utcnow().strftime("%Y%m%dT%H%M%SZ")
    ts_path = BACKTEST_SNAPSHOT_DIR / f"backtest_{stamp}.txt"
    ts_path.write_text(snapshot_text, encoding="utf-8")
    LATEST_BACKTEST_SNAPSHOT_PATH.write_text(snapshot_text, encoding="utf-8")
    return ts_path


def load_latest_backtest_snapshot() -> Optional[str]:
    if not LATEST_BACKTEST_SNAPSHOT_PATH.exists():
        return None
    try:
        txt = LATEST_BACKTEST_SNAPSHOT_PATH.read_text(encoding="utf-8").strip()
        return txt if txt else None
    except Exception:
        return None


def build_grok_prompt(dashboard_text: str, datetime_context: str) -> str:
    return GROK_ANALYSIS_INSTRUCTION.format(
        datetime_context=datetime_context,
        dashboard_text=dashboard_text,
    )


def call_xai_grok(prompt: str) -> Optional[str]:
    # Backward-compatible wrapper: uses active AI provider settings.
    return call_active_ai_provider(prompt, system_prompt="You are Grok, built by xAI.")


def run_ai_analysis_mode() -> None:
    now_local = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    dt_ctx = input(f"Date/time context for prompt (default {now_local}): ").strip() or now_local
    live_exists = LATEST_LIVE_SNAPSHOT_PATH.exists()
    backtest_exists = LATEST_BACKTEST_SNAPSHOT_PATH.exists()
    print("Dashboard source:")
    print("1. Paste dashboard text")
    if live_exists:
        print("2. Use latest saved Live Dashboard snapshot")
    if backtest_exists:
        print("3. Use latest saved Backtest snapshot")
    default_src = "3" if backtest_exists else ("2" if live_exists else "1")
    src = (input(f"Enter source (default {default_src}): ").strip() or default_src)

    if src == "2" and live_exists:
        dashboard_text = load_latest_live_dashboard_snapshot() or ""
        if dashboard_text:
            print(f"Loaded latest snapshot: {LATEST_LIVE_SNAPSHOT_PATH}")
        else:
            print("No latest live snapshot content found. Please paste dashboard text instead.")
            dashboard_text = read_multiline_input_until_end()
    elif src == "3" and backtest_exists:
        dashboard_text = load_latest_backtest_snapshot() or ""
        if dashboard_text:
            print(f"Loaded latest snapshot: {LATEST_BACKTEST_SNAPSHOT_PATH}")
        else:
            print("No latest backtest snapshot content found. Please paste dashboard text instead.")
            dashboard_text = read_multiline_input_until_end()
    else:
        dashboard_text = read_multiline_input_until_end()
    if not dashboard_text:
        print("No dashboard text provided.")
        return

    prompt = build_grok_prompt(dashboard_text, dt_ctx)
    GROK_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = pd.Timestamp.utcnow().strftime("%Y%m%dT%H%M%SZ")
    prompt_path = GROK_OUTPUT_DIR / f"ai_prompt_{stamp}.txt"
    prompt_path.write_text(prompt, encoding="utf-8")
    print(f"AI analysis prompt saved: {prompt_path}")

    if (input("Send this prompt directly to active AI provider now? (y/n, default n): ").strip().lower() or "n") != "y":
        print("\nPrompt preview (first 40 lines):")
        preview = "\n".join(prompt.splitlines()[:40])
        print(preview)
        print("\n(Full prompt saved to file above.)")
        return

    ai_prefs = load_ai_preferences()
    active_name, active_profile = get_active_ai_profile(ai_prefs)
    internet_access = bool(active_profile.get("internet_access", provider_default_internet_access(str(active_profile.get("provider", "")))))
    system_prompt = ONLINE_SYSTEM_PROMPT if internet_access else OFFLINE_SYSTEM_PROMPT
    print(f"AI Analysis system prompt mode: {'online-aware' if internet_access else 'offline-aware'} (profile `{active_name}`).")

    response = call_active_ai_provider(prompt, system_prompt=system_prompt)
    if not response:
        print("No AI response returned.")
        return

    resp_path = GROK_OUTPUT_DIR / f"ai_response_{stamp}.md"
    resp_path.write_text(response, encoding="utf-8")
    print("\n" + "=" * 120)
    print("AI ANALYSIS")
    print("=" * 120)
    print(response)
    print(f"\nSaved response: {resp_path}")


def main() -> None:
    global CUDA_AVAILABLE, CUDA_BACKEND, cp, EMOJI_ENABLED, COLOR_ENABLED
    ensure_table()
    CUDA_AVAILABLE, CUDA_BACKEND, cp = detect_cuda_backend()
    EMOJI_ENABLED, COLOR_ENABLED = detect_terminal_display_capabilities()
    print(f"Runtime acceleration backend: {CUDA_BACKEND}")
    display_mode = "emoji+color" if (EMOJI_ENABLED and COLOR_ENABLED) else ("emoji" if EMOJI_ENABLED else ("color" if COLOR_ENABLED else "plain-text"))
    print(f"Action label display mode: {display_mode}")
    ai_prefs = load_ai_preferences()
    ai_secrets = load_ai_secrets()
    active_name, active_profile = get_active_ai_profile(ai_prefs)
    active_key, active_key_src = resolve_profile_api_key(active_profile, ai_secrets)
    active_provider = str(active_profile.get("provider", "")).strip()
    active_internet = bool(active_profile.get("internet_access", provider_default_internet_access(active_provider)))
    print(
        f"AI provider active: profile={active_name}, provider={active_provider}, "
        f"model={active_profile.get('model', '')}, mode={'online-aware' if active_internet else 'offline-aware'}, "
        f"key={'set' if active_key else 'missing'} ({active_key_src})"
    )
    display_currency = "USD"
    display_fx = 1.0

    while True:
        print("\n" + "=" * 100)
        print("MAIN MENU")
        print("=" * 100)
        print("1. Crypto Analysis")
        print("2. Traditional Markets Analysis")
        print("3. Clear Cache")
        print("4. AI Analysis Mode")
        print("5. AI Provider Settings")
        print("6. Display Currency Settings")
        print("7. Run Auto-Research (Standard)")
        print("8. Run Auto-Research (Comprehensive)")
        print("9. Exit")
        print(f"(Current display currency: {display_currency}, FX USD->{display_currency}: {display_fx:.4f})")

        main_choice = input("Enter 1-9: ").strip()
        if main_choice == "9":
            break
        if main_choice == "8":
            run_comprehensive_auto_research_task()
            input("\nPress Enter to return to main menu...")
            continue
        if main_choice == "7":
            run_standard_auto_research_task()
            input("\nPress Enter to return to main menu...")
            continue
        if main_choice == "6":
            display_currency = choose_display_currency(display_currency)
            display_fx = get_usd_to_currency_rate(display_currency)
            print(f"Display currency set to {display_currency}. FX USD->{display_currency} = {display_fx:.4f}")
            input("\nPress Enter to return to main menu...")
            continue
        if main_choice == "5":
            run_ai_provider_settings_menu()
            input("\nPress Enter to return to main menu...")
            continue
        if main_choice == "4":
            run_ai_analysis_mode()
            input("\nPress Enter to return to main menu...")
            continue
        if main_choice == "3":
            if input("Delete entire cache? (yes/no): ").strip().lower() == "yes":
                reset_table()
                print("Cache cleared.")
            continue
        if main_choice not in ["1", "2"]:
            print("Invalid choice.")
            continue

        is_crypto = main_choice == "1"
        quote_currency = choose_crypto_quote_currency() if is_crypto else "USD"
        timeframe = choose_timeframe()

        print("Select mode:")
        print("1. Live Dashboard")
        print("2. Backtest + Equity Curve")
        mode = input("Enter 1 or 2: ").strip()
        is_backtest = mode == "2"
        backtest_months = prompt_backtest_months() if is_backtest else None

        tickers = load_assets(is_crypto, quote_currency=quote_currency)
        if not tickers:
            print("No assets selected.")
            continue

        print(f"Selected {len(tickers)} assets. Loading {timeframe} data...")
        fetch_years = 2.2
        if is_backtest and backtest_months is not None:
            # Keep enough history for indicators/Fibonacci warm-up while still honoring chosen test window.
            fetch_years = max(2.2, (backtest_months / 12.0) + 0.25)
        raw_data: Dict[str, pd.DataFrame] = {}
        for t in tickers:
            df = fetch_with_cache(t, timeframe, is_crypto=is_crypto, years=fetch_years)
            if df is not None and len(df) >= 60:
                raw_data[t] = df

        print(f"Loaded raw data for {len(raw_data)} assets.")
        if not raw_data:
            print("No usable data.")
            continue

        champions = load_champion_registry()
        market_label = "crypto" if is_crypto else "traditional"

        if not is_backtest:
            tuned = TunedParams()
            sid, champion = find_best_champion_match(
                champions,
                market=market_label,
                timeframe=timeframe,
                months=None,
                requested_assets=len(tickers),
            )
            if champion is not None and sid is not None:
                print(champion_summary_line(sid, champion))
                use_ch = (input("Use champion tuned params for live scoring? (y/n, default y): ").strip().lower() or "y") == "y"
                if use_ch:
                    tuned = apply_champion_tuned_params(tuned, champion)
                    print("Applied champion tuned params for live dashboard scoring.")
            enriched = build_indicator_cache(raw_data, timeframe, max_workers=1, verbose=False)

            if not enriched:
                print("No indicator-ready data.")
                continue

            table, risk = build_live_tables(enriched, is_crypto, tuned, timeframe)
            if table.empty:
                print("No scored assets.")
                continue

            print("\n" + "=" * 170)
            print(f"LIVE DASHBOARD ({'Crypto' if is_crypto else 'Traditional'}) [{timeframe}]")
            print("=" * 170)
            table_print = table.drop(columns=["_Ticker"], errors="ignore").copy()
            fx = get_usd_to_currency_rate(display_currency)
            if display_currency != "USD":
                table_print[f"Price ({display_currency})"] = (pd.to_numeric(table_print["Price"], errors="coerce") * fx).round(4)
                table_print[f"Fib Support ({display_currency})"] = (pd.to_numeric(table_print["Fib Support"], errors="coerce") * fx).round(4)
                table_print[f"Fib Resistance ({display_currency})"] = (pd.to_numeric(table_print["Fib Resistance"], errors="coerce") * fx).round(4)
            print(table_print.to_string())

            print("\n" + "=" * 170)
            print("RISK SCORE BREAKDOWN")
            print("=" * 170)
            print(risk.to_string())

            portfolio_notes = build_portfolio_insights(enriched, table)
            if portfolio_notes:
                print("\n" + "=" * 170)
                print("PORTFOLIO CONTEXT")
                print("=" * 170)
                for n in portfolio_notes:
                    print(f"- {n}")

            snapshot_text = compose_live_dashboard_snapshot_text(
                table=table,
                risk=risk,
                portfolio_notes=portfolio_notes,
                is_crypto=is_crypto,
                timeframe=timeframe,
                requested_assets=len(tickers),
                loaded_assets=len(raw_data),
            )
            snap_path = save_live_dashboard_snapshot(snapshot_text)
            print(f"Saved latest Live Dashboard snapshot: {snap_path}")

            top_ticker = table.iloc[0]["_Ticker"]
            chart_top_asset(enriched[top_ticker], pretty_asset_label(top_ticker, is_crypto) if is_crypto else top_ticker)

        else:
            cfg = prompt_backtest_config(timeframe)
            sid, champion = find_best_champion_match(
                champions,
                market=market_label,
                timeframe=timeframe,
                months=backtest_months,
                requested_assets=len(tickers),
            )
            if champion is not None and sid is not None:
                print(champion_summary_line(sid, champion))
                use_ch = (input("Replace current backtest settings with champion config? (y/n, default y): ").strip().lower() or "y") == "y"
                if use_ch:
                    cfg = apply_champion_backtest_config(cfg, champion)
                    print("Applied champion backtest config.")
            holdout_result = None
            bt_workers = prompt_cpu_workers("Backtest indicator cache")
            indicator_cache = build_indicator_cache(raw_data, timeframe, max_workers=bt_workers, verbose=True)
            if not indicator_cache:
                print("No indicator-ready data for backtest.")
                continue
            auto_tune_used = False
            n_trials = 30
            n_jobs = max(1, min(8, (os.cpu_count() or 1)))
            if input("Enable Auto-Tune + Walk-Forward? (y/n): ").strip().lower() == "y":
                try:
                    n_trials_in = input("Optuna trials (default 30): ").strip()
                    n_trials = int(n_trials_in) if n_trials_in else n_trials
                except ValueError:
                    n_trials = n_trials
                try:
                    n_jobs_in = input(f"Optuna parallel jobs (default {n_jobs}): ").strip()
                    n_jobs = int(n_jobs_in) if n_jobs_in else n_jobs
                except ValueError:
                    n_jobs = n_jobs
                cfg, holdout_result = run_walk_forward_optuna(
                    raw_data,
                    timeframe,
                    is_crypto,
                    cfg,
                    n_trials=n_trials,
                    n_jobs=n_jobs,
                    indicator_cache=indicator_cache,
                )
                auto_tune_used = True

            if backtest_months is None:
                backtest_months = 12
            backtest_days = int(round(backtest_months * 365.25 / 12.0))
            anchor = list(raw_data.keys())[0]
            end_date = raw_data[anchor].index.max()
            start_date = end_date - pd.Timedelta(days=backtest_days)

            result = simulate_backtest(
                raw_data,
                timeframe,
                cfg,
                is_crypto,
                start_date=start_date,
                end_date=end_date,
                indicator_cache=indicator_cache,
                verbose=True,
            )
            metrics = result.get("metrics", {})
            final_dd = float(metrics.get("max_drawdown_pct", 0.0))
            if auto_tune_used and final_dd > cfg.max_drawdown_limit_pct:
                print(
                    f"Final backtest drawdown {final_dd:.2f}% breached target {cfg.max_drawdown_limit_pct:.2f}%; "
                    f"running one constrained re-tune pass."
                )
                constrained_cfg = BacktestConfig(
                    initial_capital=cfg.initial_capital,
                    stop_loss_pct=cfg.stop_loss_pct,
                    take_profit_pct=cfg.take_profit_pct,
                    max_hold_days=cfg.max_hold_days,
                    min_hold_bars=cfg.min_hold_bars,
                    cooldown_bars=cfg.cooldown_bars,
                    same_asset_cooldown_bars=cfg.same_asset_cooldown_bars,
                    max_consecutive_same_asset_entries=cfg.max_consecutive_same_asset_entries,
                    max_drawdown_limit_pct=cfg.max_drawdown_limit_pct,
                    max_exposure_pct=float(np.clip(cfg.max_exposure_pct * 0.90, 0.20, cfg.max_exposure_pct)),
                    fee_pct=cfg.fee_pct,
                    slippage_pct=cfg.slippage_pct,
                    tuned=TunedParams(
                        position_size=float(np.clip(cfg.tuned.position_size * 0.90, 0.20, 0.40)),
                        atr_multiplier=float(np.clip(cfg.tuned.atr_multiplier * 0.98, 2.0, 2.5)),
                        adx_threshold=float(np.clip(cfg.tuned.adx_threshold + 1.0, 20.0, 30.0)),
                        cmf_threshold=cfg.tuned.cmf_threshold,
                        obv_slope_threshold=cfg.tuned.obv_slope_threshold,
                        buy_threshold=cfg.tuned.buy_threshold,
                        sell_threshold=cfg.tuned.sell_threshold,
                    ),
                )
                cfg_retry, holdout_retry = run_walk_forward_optuna(
                    raw_data,
                    timeframe,
                    is_crypto,
                    constrained_cfg,
                    n_trials=max(10, int(max(10, n_trials // 2))),
                    n_jobs=n_jobs,
                    indicator_cache=indicator_cache,
                )
                retry_result = simulate_backtest(
                    raw_data,
                    timeframe,
                    cfg_retry,
                    is_crypto,
                    start_date=start_date,
                    end_date=end_date,
                    indicator_cache=indicator_cache,
                    verbose=True,
                )
                retry_metrics = retry_result.get("metrics", {})
                retry_dd = float(retry_metrics.get("max_drawdown_pct", 0.0))
                if retry_dd <= cfg.max_drawdown_limit_pct or (
                    retry_dd < final_dd and float(retry_result.get("return_pct", -1e9)) >= float(result.get("return_pct", -1e9)) - 5.0
                ):
                    print(
                        f"Using constrained re-tuned config (DD {retry_dd:.2f}% vs {final_dd:.2f}%)."
                    )
                    cfg = cfg_retry
                    holdout_result = holdout_retry
                    result = retry_result
                    metrics = retry_metrics
                    final_dd = retry_dd
                else:
                    print("Keeping original tuned config; constrained retry did not improve risk/return balance.")
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

            contrib = metrics.get("asset_contrib", {})
            top_contrib = list(contrib.items())[:5] if isinstance(contrib, dict) else []

            print(f"\nFinal Value   : ${result['final']:,.2f}")
            fx = get_usd_to_currency_rate(display_currency)
            if display_currency != "USD":
                print(f"Final Value   : {display_currency} {(result['final'] * fx):,.2f}  (FX USD->{display_currency} {fx:.4f})")
            print(f"Total Return  : {result['return_pct']:+.2f}%")
            print(f"Trades closed : {len(trades)}")
            print(f"Win rate      : {win_rate:.2f}%")
            print(f"Max Drawdown  : {metrics.get('max_drawdown_pct', 0.0):.2f}%")
            print(f"Sharpe/Sortino: {metrics.get('sharpe', 0.0):.2f} / {metrics.get('sortino', 0.0):.2f}")
            print(f"Turnover/Year : {metrics.get('turnover_per_year', 0.0):.2f} trades")
            print(f"Avg Hold Days : {metrics.get('avg_hold_days', 0.0):.2f}")
            print(f"Fee/Slippage  : {cfg.fee_pct}% / {cfg.slippage_pct}% per leg")
            print(f"Position size : {cfg.tuned.position_size*100:.1f}%")
            if top_contrib:
                print("Top Asset Contribution:")
                for asset, pnl in top_contrib:
                    print(f"  {asset:<8} ${pnl:,.2f}")
            if holdout_result is not None:
                hm = holdout_result.get("metrics", {})
                print(
                    f"Holdout (out-of-sample) Return: {holdout_result.get('return_pct', 0.0):+.2f}%  |  "
                    f"MaxDD: {hm.get('max_drawdown_pct', 0.0):.2f}%  |  Sharpe: {hm.get('sharpe', 0.0):.2f}"
                )

            bt_snapshot = compose_backtest_snapshot_text(
                result=result,
                trades=trades,
                metrics=metrics,
                cfg=cfg,
                timeframe=timeframe,
                is_crypto=is_crypto,
                backtest_months=backtest_months if backtest_months is not None else 12,
                loaded_assets=len(raw_data),
                holdout_result=holdout_result,
            )
            bt_snap_path = save_backtest_snapshot(bt_snapshot)
            print(f"Saved latest Backtest snapshot: {bt_snap_path}")

        input("\nPress Enter to return to main menu...")

    conn.close()


if __name__ == "__main__":
    main()
