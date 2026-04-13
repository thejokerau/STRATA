from __future__ import annotations

from datetime import datetime
import os
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import streamlit as st

from gui_app.engine_bridge import EngineBridge

# Streamlit + numba/pandas_ta safety for dynamic-loader contexts.
os.environ.setdefault("NUMBA_DISABLE_JIT", "1")
os.environ.setdefault("NUMBA_CACHE_DIR", str(Path.home() / ".numba_cache"))


st.set_page_config(page_title="STRATA Streamlit", layout="wide")
st.title("STRATA Streamlit (Variant)")
st.caption("Experimental Streamlit branch: `streamlit-nightly`")


@st.cache_resource
def get_bridge(repo_root: str) -> EngineBridge:
    return EngineBridge(Path(repo_root))


REPO_ROOT = str(Path(__file__).resolve().parents[1])
bridge = get_bridge(REPO_ROOT)


def _load_binance_profiles() -> List[str]:
    out = bridge.list_binance_profiles()
    if not out.get("ok"):
        return []
    profiles = out.get("profiles", []) or []
    names = [str(p.get("name", "")).strip() for p in profiles if isinstance(p, dict) and str(p.get("name", "")).strip()]
    return sorted(names)


def _confirmed_execution_rows(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for e in entries:
        if not isinstance(e, dict):
            continue
        if not bool(e.get("is_execution", False)):
            continue
        panel = str(e.get("panel", "") or "").strip().lower()
        ex_trade_id = str(e.get("exchange_trade_id", "") or "").strip()
        note = str(e.get("note", "") or "").strip().lower()
        is_confirmed_fill = bool(panel == "binance_reconcile" or ex_trade_id or ("reconciled binance fill" in note))
        if not is_confirmed_fill:
            continue
        action = str(e.get("action", "")).strip().upper()
        if action not in ("BUY", "SELL"):
            continue
        try:
            qty = float(e.get("qty", 0.0) or 0.0)
            px = float(e.get("price", 0.0) or 0.0)
        except Exception:
            qty, px = 0.0, 0.0
        if qty <= 0 or px <= 0:
            continue
        sym = str(e.get("exchange_symbol", "")).strip().upper()
        if not sym:
            asset = str(e.get("asset", "")).strip().upper()
            quote = str(e.get("quote_currency", "USDT")).strip().upper() or "USDT"
            if not asset:
                continue
            sym = f"{asset}{quote}"
        ts_raw = str(e.get("ts", "") or "")
        try:
            ord_val = float(pd.Timestamp(pd.to_datetime(ts_raw, utc=True, errors="coerce")).value)
        except Exception:
            ord_val = float(e.get("id", 0) or 0)
        rows.append({"symbol": sym, "action": action, "qty": qty, "price": px, "ord": ord_val})
    rows.sort(key=lambda r: (float(r.get("ord", 0.0) or 0.0), str(r.get("symbol", ""))))
    return rows


def _fifo_cost_basis_by_symbol(entries: List[Dict[str, Any]]) -> Dict[str, float]:
    rows = _confirmed_execution_rows(entries)
    lots: Dict[str, List[Dict[str, float]]] = {}
    for r in rows:
        sym = str(r.get("symbol", ""))
        action = str(r.get("action", ""))
        qty = float(r.get("qty", 0.0) or 0.0)
        px = float(r.get("price", 0.0) or 0.0)
        book = lots.setdefault(sym, [])
        if action == "BUY":
            book.append({"qty": qty, "price": px})
            continue
        rem = qty
        i = 0
        while rem > 1e-12 and i < len(book):
            q = float(book[i].get("qty", 0.0) or 0.0)
            if q <= rem + 1e-12:
                rem -= q
                book[i]["qty"] = 0.0
                i += 1
            else:
                book[i]["qty"] = max(0.0, q - rem)
                rem = 0.0
        lots[sym] = [x for x in book if float(x.get("qty", 0.0) or 0.0) > 1e-12]

    out: Dict[str, float] = {}
    for sym, book in lots.items():
        tq = sum(float(x.get("qty", 0.0) or 0.0) for x in book)
        if tq <= 1e-12:
            continue
        notional = sum(float(x.get("qty", 0.0) or 0.0) * float(x.get("price", 0.0) or 0.0) for x in book)
        if notional > 0:
            out[sym] = notional / tq
    return out


def _position_graph_rows(profile: str, quote_pref: str = "USDT") -> pd.DataFrame:
    l = bridge.get_trade_ledger()
    if not l.get("ok"):
        return pd.DataFrame()
    ledger = l.get("ledger", {}) if isinstance(l.get("ledger"), dict) else {}
    entries = ledger.get("entries", []) if isinstance(ledger.get("entries"), list) else []
    open_positions = ledger.get("open_positions", {}) if isinstance(ledger.get("open_positions"), dict) else {}
    cb = _fifo_cost_basis_by_symbol(entries)

    oo = bridge.list_open_binance_orders(profile_name=profile)
    open_orders = oo.get("orders", []) if isinstance(oo.get("orders"), list) else []
    by_symbol: Dict[str, List[Dict[str, Any]]] = {}
    for o in open_orders:
        if not isinstance(o, dict):
            continue
        sym = str(o.get("symbol", "")).strip().upper()
        if sym:
            by_symbol.setdefault(sym, []).append(o)

    merged: Dict[str, Dict[str, Any]] = {}
    for _, pos in open_positions.items():
        if not isinstance(pos, dict):
            continue
        asset = str(pos.get("asset", "")).strip().upper()
        if not asset:
            continue
        quote = str(pos.get("quote_currency", quote_pref) or quote_pref).strip().upper()
        sym = str(pos.get("symbol", "")).strip().upper() or f"{asset}{quote}"
        try:
            qty = float(pos.get("qty", 0.0) or 0.0)
            entry = float(pos.get("entry_price", 0.0) or 0.0)
        except Exception:
            qty, entry = 0.0, 0.0
        merged[sym] = {
            "symbol": sym,
            "tf": str(pos.get("timeframe", "spot") or "spot"),
            "qty": max(0.0, qty),
            "buy_price": max(0.0, entry) if entry > 0 else float(cb.get(sym, 0.0) or 0.0),
            "quote": quote,
        }

    for sym, olist in by_symbol.items():
        if sym in merged:
            continue
        qty_max = 0.0
        for o in olist:
            if str(o.get("side", "")).strip().upper() != "SELL":
                continue
            try:
                q = float(o.get("origQty", 0.0) or 0.0)
            except Exception:
                q = 0.0
            qty_max = max(qty_max, q)
        if qty_max > 0:
            merged[sym] = {
                "symbol": sym,
                "tf": "spot",
                "qty": qty_max,
                "buy_price": float(cb.get(sym, 0.0) or 0.0),
                "quote": quote_pref,
            }

    pf = bridge.fetch_binance_portfolio(profile_name=profile)
    balances = pf.get("balances", []) if isinstance(pf.get("balances"), list) else []
    stables = {"USDT", "USDC", "BUSD", "FDUSD", "TUSD", "USDP", "DAI", "USD"}
    for b in balances:
        if not isinstance(b, dict):
            continue
        asset = str(b.get("asset", "")).strip().upper()
        if not asset or asset in stables:
            continue
        try:
            total = float(b.get("total", 0.0) or 0.0)
        except Exception:
            total = 0.0
        if total <= 0:
            continue
        sym = f"{asset}{quote_pref}"
        if sym not in merged:
            merged[sym] = {"symbol": sym, "tf": "spot", "qty": total, "buy_price": float(cb.get(sym, 0.0) or 0.0), "quote": quote_pref}

    rows: List[Dict[str, Any]] = []
    for sym, r in merged.items():
        qty = float(r.get("qty", 0.0) or 0.0)
        if qty <= 0:
            continue
        lp = bridge.get_binance_last_price(symbol=sym, profile_name=profile)
        cur = float(lp.get("price", 0.0) or 0.0) if lp.get("ok") else 0.0
        stop, tp = 0.0, 0.0
        for o in by_symbol.get(sym, []):
            if str(o.get("side", "")).strip().upper() != "SELL":
                continue
            ot = str(o.get("type", "")).strip().upper()
            try:
                px = float(o.get("price", 0.0) or 0.0)
                sp = float(o.get("stopPrice", 0.0) or 0.0)
            except Exception:
                px, sp = 0.0, 0.0
            if "STOP" in ot:
                cand = sp if sp > 0 else px
                if cand > 0 and (stop <= 0 or cand > stop):
                    stop = cand
            elif ("TAKE_PROFIT" in ot) or (ot in ("LIMIT_MAKER", "LIMIT")):
                cand = px if px > 0 else sp
                if cand > 0 and (tp <= 0 or cand < tp):
                    tp = cand
        buy = float(r.get("buy_price", 0.0) or 0.0)
        pnl_pct = ((cur - buy) / buy * 100.0) if (buy > 0 and cur > 0) else 0.0
        pnl_quote = ((cur - buy) * qty) if (buy > 0 and cur > 0) else 0.0
        state = "PROTECTED" if (stop > 0 and tp > 0) else ("PARTIAL" if (stop > 0 or tp > 0) else "UNPROTECTED")
        rows.append(
            {
                "symbol": sym,
                "tf": str(r.get("tf", "spot")),
                "qty": qty,
                "buy_price": buy,
                "current": cur,
                "stop": stop,
                "tp": tp,
                "pnl_pct": pnl_pct,
                "pnl_quote": pnl_quote,
                "quote": str(r.get("quote", quote_pref)),
                "state": state,
            }
        )
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("symbol")
    return df


tab_live, tab_backtest, tab_ai, tab_portfolio, tab_graph = st.tabs(
    ["Live Dashboard", "Backtest", "AI Analysis", "Portfolio & Orders", "Position Graph"]
)

with tab_live:
    st.subheader("Live Dashboard")
    c1, c2, c3, c4, c5 = st.columns(5)
    market = c1.selectbox("Market", ["crypto", "traditional"], index=0)
    timeframe = c2.selectbox("Timeframe", ["1d", "4h", "8h", "12h"], index=0)
    top_n = c3.selectbox("Top N", [10, 20, 50, 100], index=1)
    quote = c4.selectbox("Quote", ["USDT", "USD", "BTC", "ETH", "BNB"], index=0)
    country = c5.selectbox("Country (traditional)", ["1", "2", "3", "4", "5", "6"], index=1)
    if st.button("Run Live Dashboard", type="primary"):
        cfg = {
            "name": f"{market}-{timeframe}",
            "market": market,
            "timeframe": timeframe,
            "quote_currency": quote,
            "top_n": int(top_n),
            "country": country,
            "display_currency": "USD",
        }
        with st.spinner("Running live panel..."):
            res = bridge.run_live_panel(cfg)
        if not res.get("ok"):
            st.error(res.get("error", "Live run failed"))
        else:
            st.success("Live run complete")
            st.text(res.get("table_text", ""))
            st.text(res.get("breakdown_text", ""))
            if res.get("context_text"):
                st.text(res.get("context_text", ""))

with tab_backtest:
    st.subheader("Backtest")
    c1, c2, c3, c4 = st.columns(4)
    market = c1.selectbox("Market", ["crypto", "traditional"], index=0, key="bt_market")
    timeframe = c2.selectbox("Timeframe", ["1d", "4h", "8h", "12h"], index=0, key="bt_tf")
    top_n = c3.selectbox("Top N", [10, 20, 50, 100], index=1, key="bt_topn")
    months = c4.selectbox("Lookback (months)", [1, 3, 6, 12, 18, 24], index=3, key="bt_months")
    if st.button("Run Backtest", type="primary"):
        cfg = {
            "market": market,
            "timeframe": timeframe,
            "top_n": int(top_n),
            "months": int(months),
            "country": "2",
            "quote_currency": "USDT",
            "display_currency": "USD",
            "initial_capital": 10000.0,
            "stop_loss_pct": 8.0,
            "take_profit_pct": 20.0,
            "max_hold_days": 45,
            "min_hold_bars": 2,
            "cooldown_bars": 1,
            "same_asset_cooldown_bars": 3,
            "max_consecutive_same_asset_entries": 3,
            "fee_pct": 0.1,
            "slippage_pct": 0.05,
            "position_size": 0.3,
            "atr_multiplier": 2.2,
            "adx_threshold": 25.0,
            "cmf_threshold": 0.02,
            "obv_slope_threshold": 0.0,
            "buy_threshold": 2,
            "sell_threshold": -2,
            "cache_workers": 4,
            "max_drawdown_limit_pct": 35.0,
            "max_exposure_pct": 0.4,
            "auto_tune": False,
            "optuna_trials": 10,
            "optuna_jobs": 2,
        }
        with st.spinner("Running backtest..."):
            res = bridge.run_backtest(cfg)
        if not res.get("ok"):
            st.error(res.get("error", "Backtest failed"))
        else:
            st.success("Backtest complete")
            st.text(res.get("summary_text", ""))
            st.text(res.get("trade_text", ""))

with tab_ai:
    st.subheader("AI Analysis")
    dt_context = st.text_input("Datetime context", value=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    src = st.text_area("Source text (dashboard/backtest/logs)", height=280, placeholder="Paste dashboard text here...")
    if st.button("Run AI Analysis", type="primary"):
        if not src.strip():
            st.warning("Provide source text first.")
        else:
            with st.spinner("Calling AI provider..."):
                res = bridge.run_ai_analysis(src, dt_context)
            if not res.get("ok"):
                st.error(res.get("error", "AI failed"))
            else:
                st.success("AI response received")
                st.text_area("AI Response", value=res.get("response", ""), height=420)

with tab_portfolio:
    st.subheader("Portfolio & Open Orders")
    names = _load_binance_profiles()
    profile = st.selectbox("Binance profile", names, index=0 if names else None)
    c1, c2, c3 = st.columns(3)
    do_pf = c1.button("Refresh Portfolio", type="primary", disabled=not bool(profile))
    do_oo = c2.button("Refresh Open Orders", disabled=not bool(profile))
    do_led = c3.button("Refresh Ledger", disabled=not bool(profile))

    if do_pf and profile:
        out = bridge.fetch_binance_portfolio(profile_name=profile)
        if not out.get("ok"):
            st.error(out.get("error", "Portfolio fetch failed"))
        else:
            st.success(f"Portfolio: {profile}")
            st.write(f"Estimated Total (USD): **${float(out.get('estimated_total_usd', 0.0) or 0.0):,.2f}**")
            df = pd.DataFrame(out.get("balances", []) or [])
            if not df.empty:
                st.dataframe(df, use_container_width=True, hide_index=True)

    if do_oo and profile:
        out = bridge.list_open_binance_orders(profile_name=profile)
        if not out.get("ok"):
            st.error(out.get("error", "Open orders fetch failed"))
        else:
            st.success("Open orders loaded")
            df = pd.DataFrame(out.get("orders", []) or [])
            if not df.empty:
                st.dataframe(df, use_container_width=True, hide_index=True)
            else:
                st.info("No open orders.")

    if do_led:
        out = bridge.get_trade_ledger()
        if not out.get("ok"):
            st.error(out.get("error", "Ledger fetch failed"))
        else:
            ledger = out.get("ledger", {}) if isinstance(out.get("ledger"), dict) else {}
            entries = ledger.get("entries", []) if isinstance(ledger.get("entries"), list) else []
            st.write(f"Entries: **{len(entries)}**")
            if entries:
                df = pd.DataFrame(entries)
                st.dataframe(df, use_container_width=True, hide_index=True)

with tab_graph:
    st.subheader("Position Graph Data")
    names = _load_binance_profiles()
    profile = st.selectbox("Binance profile (graph)", names, index=0 if names else None, key="graph_profile")
    quote_pref = st.selectbox("Quote", ["USDT", "USD", "USDC", "BUSD", "BTC", "ETH", "BNB"], index=0, key="graph_quote")
    if st.button("Refresh Position Graph Data", type="primary", disabled=not bool(profile)):
        with st.spinner("Building position graph data..."):
            df = _position_graph_rows(profile=profile, quote_pref=quote_pref)
        if df.empty:
            st.info("No open positions found.")
        else:
            st.success(f"Rows: {len(df)}")
            st.dataframe(df, use_container_width=True, hide_index=True)
            chart_df = df.copy()
            chart_df["pnl_pct"] = pd.to_numeric(chart_df["pnl_pct"], errors="coerce").fillna(0.0)
            st.bar_chart(chart_df.set_index("symbol")["pnl_pct"])
