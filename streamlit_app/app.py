from __future__ import annotations

from datetime import datetime
import os
import json
import re
import time
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

from gui_app.engine_bridge import EngineBridge

# Streamlit + numba/pandas_ta safety for dynamic-loader contexts.
os.environ.setdefault("NUMBA_DISABLE_JIT", "1")
os.environ.setdefault("NUMBA_CACHE_DIR", str(Path.home() / ".numba_cache"))
os.environ.setdefault("NO_PROXY", "api.binance.com,localhost,127.0.0.1")
os.environ.setdefault("no_proxy", os.environ.get("NO_PROXY", "api.binance.com,localhost,127.0.0.1"))


st.set_page_config(page_title="STRATA Streamlit", layout="wide")
st.title("STRATA Streamlit (Variant)")
st.caption("Experimental Streamlit branch: `streamlit-nightly`")


@st.cache_resource
def get_bridge(repo_root: str) -> EngineBridge:
    return EngineBridge(Path(repo_root))


REPO_ROOT = str(Path(__file__).resolve().parents[1])
bridge = get_bridge(REPO_ROOT)

COUNTRY_LABELS: Dict[str, str] = {
    "1": "Australia",
    "2": "United States",
    "3": "United Kingdom",
    "4": "Europe",
    "5": "Canada",
    "6": "Other",
}


def _country_display_values() -> List[str]:
    return [f"{v} ({k})" for k, v in COUNTRY_LABELS.items()]


def _country_display_to_code(display_or_code: str) -> str:
    raw = str(display_or_code or "").strip()
    if raw in COUNTRY_LABELS:
        return raw
    m = re.search(r"\((\d+)\)\s*$", raw)
    if m and m.group(1) in COUNTRY_LABELS:
        return m.group(1)
    for k, v in COUNTRY_LABELS.items():
        if raw.lower() == v.lower():
            return k
    return "2"


def _live_profiles_candidate_paths() -> List[Path]:
    cands: List[Path] = []
    home = Path.home()
    cands.append(home / ".ctmt" / "gui" / "streamlit_live_profiles.json")
    local_app = os.environ.get("LOCALAPPDATA", "").strip()
    if local_app:
        cands.append(Path(local_app) / "CTMT" / "gui" / "streamlit_live_profiles.json")
    repo_root = Path(__file__).resolve().parents[1]
    cands.append(repo_root / "experiments" / "streamlit" / "streamlit_live_profiles.json")
    cands.append(Path(tempfile.gettempdir()) / "ctmt" / "streamlit_live_profiles.json")
    uniq: List[Path] = []
    seen = set()
    for p in cands:
        k = str(p).lower()
        if k in seen:
            continue
        seen.add(k)
        uniq.append(p)
    return uniq


def _load_live_profiles() -> Dict[str, Any]:
    for p in _live_profiles_candidate_paths():
        if not p.exists():
            continue
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
            return obj if isinstance(obj, dict) else {}
        except Exception:
            continue
    return {}


def _save_live_profiles(profiles: Dict[str, Any]) -> Tuple[bool, str]:
    payload = json.dumps(profiles, indent=2)
    errs: List[str] = []
    for p in _live_profiles_candidate_paths():
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(payload, encoding="utf-8")
            return True, str(p)
        except Exception as exc:
            errs.append(f"{p}: {exc}")
            continue
    return False, " | ".join(errs[-3:])


def _init_state() -> None:
    defaults = {
        "task_logs": [],
        "task_history": [],
        "pending_recs": [],
        "pending_rec_seq": 0,
        "ai_source_text": "",
        "ai_response": "",
        "ai_prompt_preview": "",
        "last_live_text": "",
        "selected_pending_ids": [],
        "ai_source_text_area": "",
        "live_runs": [],
        "last_backtest_summary": "",
        "last_backtest_trades": "",
        "last_portfolio_df": pd.DataFrame(),
        "last_open_orders_df": pd.DataFrame(),
        "last_ledger_df": pd.DataFrame(),
        "last_review_df": pd.DataFrame(),
        "last_graph_df": pd.DataFrame(),
        "last_research_text": "",
        "live_profile_name": "default",
        "pending_live_profile_apply": None,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def _log(msg: str) -> None:
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    st.session_state.task_logs.append(line)
    st.session_state.task_logs = st.session_state.task_logs[-600:]


def _run_task(name: str, fn):
    t0 = time.time()
    _log(f"START {name}")
    try:
        out = fn()
        dt = time.time() - t0
        st.session_state.task_history.append({"task": name, "sec": round(dt, 3), "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "ok": True})
        st.session_state.task_history = st.session_state.task_history[-300:]
        _log(f"DONE {name} ({dt:.2f}s)")
        return out
    except Exception as exc:
        dt = time.time() - t0
        st.session_state.task_history.append({"task": name, "sec": round(dt, 3), "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "ok": False})
        st.session_state.task_history = st.session_state.task_history[-300:]
        _log(f"DONE {name} (error: {type(exc).__name__}: {exc})")
        raise


def _extract_json_block(text: str, begin: str, end: str) -> Optional[Dict[str, Any]]:
    src = str(text or "")
    pat = re.escape(begin) + r"\s*(\{[\s\S]*?\})\s*" + re.escape(end)
    m = re.search(pat, src, flags=re.IGNORECASE)
    blob = m.group(1).strip() if m else ""
    if not blob:
        m2 = re.search(r"(\{[\s\S]*\})", src)
        blob = m2.group(1).strip() if m2 else ""
    if not blob:
        return None
    try:
        obj = json.loads(blob)
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


def _extract_trade_recs(ai_text: str, default_tf: str = "4h") -> List[Dict[str, Any]]:
    obj = _extract_json_block(ai_text, "BEGIN_STRATA_TRADE_PLAN_JSON", "END_STRATA_TRADE_PLAN_JSON")
    recs: List[Dict[str, Any]] = []
    if isinstance(obj, dict):
        arr = obj.get("trades", [])
        if isinstance(arr, list):
            for t in arr:
                if not isinstance(t, dict):
                    continue
                side = str(t.get("side", "")).strip().upper()
                sym = str(t.get("symbol", "")).strip().upper()
                if side not in ("BUY", "SELL") or not sym:
                    continue
                recs.append(
                    {
                        "symbol": sym,
                        "asset": re.sub(r"(USDT|USDC|BUSD|FDUSD|USD|BTC|ETH|BNB)$", "", sym),
                        "side": side,
                        "order_type": str(t.get("order_type", "MARKET") or "MARKET").upper(),
                        "quantity": float(t.get("quantity", 0.0) or 0.0),
                        "quote_notional": float(t.get("quote_notional", 0.0) or 0.0),
                        "stop_loss_price": float(t.get("stop_loss_price", 0.0) or 0.0),
                        "take_profit_price": float(t.get("take_profit_price", 0.0) or 0.0),
                        "limit_price": float(t.get("limit_price", 0.0) or 0.0),
                        "take_profit_limit_price": float(t.get("take_profit_limit_price", 0.0) or 0.0),
                        "timeframe": str(t.get("timeframe", default_tf) or default_tf),
                        "confidence": int(float(t.get("confidence", 70) or 70)),
                        "status": "PENDING",
                        "reason": str(t.get("reason", "AI structured recommendation") or "AI structured recommendation"),
                    }
                )
    return recs


def _append_pending(recs: List[Dict[str, Any]], source: str = "ai_interpretation") -> int:
    n = 0
    for r in recs:
        if not isinstance(r, dict):
            continue
        st.session_state.pending_rec_seq += 1
        rr = dict(r)
        rr["id"] = int(st.session_state.pending_rec_seq)
        rr["pending_source"] = source
        st.session_state.pending_recs.append(rr)
        n += 1
    return n


def _pending_df() -> pd.DataFrame:
    rows = st.session_state.pending_recs if isinstance(st.session_state.pending_recs, list) else []
    if not rows:
        return pd.DataFrame(columns=["id", "symbol", "side", "order_type", "quantity", "quote_notional", "stop_loss_price", "take_profit_price", "timeframe", "confidence", "status", "reason"])
    return pd.DataFrame(rows)


def _submit_pending_rec(profile: str, rec: Dict[str, Any]) -> Dict[str, Any]:
    sym = str(rec.get("symbol", "")).strip().upper()
    side = str(rec.get("side", "BUY")).strip().upper()
    typ = str(rec.get("order_type", "MARKET")).strip().upper()
    qty = float(rec.get("quantity", 0.0) or 0.0)
    q_notional = float(rec.get("quote_notional", 0.0) or 0.0)
    sl = float(rec.get("stop_loss_price", 0.0) or 0.0)
    tp = float(rec.get("take_profit_price", 0.0) or 0.0)
    lp = float(rec.get("limit_price", 0.0) or 0.0)
    tp_lp = float(rec.get("take_profit_limit_price", 0.0) or 0.0)

    if typ == "OCO_BRACKET":
        if side != "SELL":
            return {"ok": False, "error": "OCO_BRACKET is SELL-only."}
        if qty <= 0 or sl <= 0 or tp <= 0:
            return {"ok": False, "error": "OCO_BRACKET requires qty, stop_loss and take_profit > 0."}
        return bridge.submit_binance_oco_sell(
            symbol=sym,
            quantity=qty,
            take_profit_price=tp,
            stop_price=sl,
            stop_limit_price=(lp if lp > 0 else sl * 0.999),
            take_profit_limit_price=(tp_lp if tp_lp > 0 else tp * 0.999),
            profile_name=profile,
        )
    return bridge.submit_binance_order(
        symbol=sym,
        side=side,
        order_type=typ,
        quantity=qty,
        quote_order_qty=q_notional,
        stop_loss_price=sl,
        limit_price=lp,
        profile_name=profile,
    )


def _load_binance_profiles() -> List[str]:
    out = bridge.list_binance_profiles()
    if not out.get("ok"):
        return []
    profiles = out.get("profiles", []) or []
    names = [str(p.get("name", "")).strip() for p in profiles if isinstance(p, dict) and str(p.get("name", "")).strip()]
    return sorted(names)


def _load_ai_profiles() -> Tuple[List[str], str]:
    out = bridge.list_ai_profiles()
    if not out.get("ok"):
        return [], ""
    profiles = out.get("profiles", []) or []
    names = [str(p.get("name", "")).strip() for p in profiles if isinstance(p, dict) and str(p.get("name", "")).strip()]
    return sorted(names), str(out.get("active_profile", "") or "")


def _portfolio_symbols_for_profile(profile: str, quote_pref: str = "USDT") -> List[str]:
    out = bridge.fetch_binance_portfolio(profile_name=profile)
    if not out.get("ok"):
        return []
    bals = out.get("balances", []) if isinstance(out.get("balances"), list) else []
    stable = {"USDT", "USDC", "BUSD", "FDUSD", "TUSD", "USDP", "DAI", "USD"}
    syms: List[str] = []
    seen = set()
    for b in bals:
        if not isinstance(b, dict):
            continue
        asset = str(b.get("asset", "")).strip().upper()
        if not asset or asset in stable:
            continue
        try:
            total = float(b.get("total", 0.0) or 0.0)
        except Exception:
            total = 0.0
        if total <= 0:
            continue
        sym = f"{asset}{quote_pref}".upper()
        if sym in seen:
            continue
        seen.add(sym)
        syms.append(sym)
    return syms


def _safe_select_option(label: str, options: List[str], preferred: str = "") -> Optional[str]:
    if not options:
        st.selectbox(label, ["(none)"], index=0, disabled=True)
        return None
    idx = options.index(preferred) if preferred in options else 0
    return st.selectbox(label, options, index=idx)


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


def _protect_open_positions_simple(profile: str, auto_submit: bool = False) -> Dict[str, Any]:
    out = bridge.analyze_open_positions_multi_tf(
        profile_name=profile,
        timeframes=["4h", "8h", "12h", "1d"],
        display_currency="USD",
    )
    if not out.get("ok"):
        return {"ok": False, "error": str(out.get("error", "Open position analysis failed"))}
    rows = out.get("rows", []) if isinstance(out.get("rows"), list) else []
    staged: List[Dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        symbol = str(r.get("symbol", "")).strip().upper()
        if not symbol:
            continue
        try:
            qty = float(r.get("qty", 0.0) or 0.0)
            price = float(r.get("last_price", 0.0) or 0.0)
        except Exception:
            qty, price = 0.0, 0.0
        if qty <= 0 or price <= 0:
            continue
        stop = price * 0.92
        tp = price * 1.12
        staged.append(
            {
                "symbol": symbol,
                "asset": re.sub(r"(USDT|USDC|BUSD|FDUSD|USD|BTC|ETH|BNB)$", "", symbol),
                "side": "SELL",
                "order_type": "OCO_BRACKET",
                "quantity": qty,
                "stop_loss_price": stop,
                "take_profit_price": tp,
                "limit_price": stop * 0.999,
                "take_profit_limit_price": tp * 0.999,
                "timeframe": str(r.get("timeframe", "spot") or "spot"),
                "confidence": int(float(r.get("confidence", 70) or 70)),
                "status": "PENDING",
                "reason": "Auto protection from open-position MTF review",
            }
        )
    # Replace older protection recommendations for same symbols.
    incoming = {str(x.get("symbol", "")).upper() for x in staged if isinstance(x, dict)}
    if incoming:
        kept = []
        for rec in st.session_state.pending_recs:
            sym = str(rec.get("symbol", "")).strip().upper() if isinstance(rec, dict) else ""
            src = str(rec.get("pending_source", "")).strip().lower() if isinstance(rec, dict) else ""
            if sym in incoming and src == "protect_open_positions_ai_bt":
                continue
            kept.append(rec)
        st.session_state.pending_recs = kept
    added = _append_pending(staged, source="protect_open_positions_ai_bt")
    submitted = 0
    if auto_submit and added > 0:
        for rec in st.session_state.pending_recs:
            if not isinstance(rec, dict):
                continue
            if str(rec.get("pending_source", "")).strip().lower() != "protect_open_positions_ai_bt":
                continue
            out_sub = _submit_pending_rec(profile, rec)
            rec["status"] = "SUBMITTED" if out_sub.get("ok") else "FAILED"
            rec["reason"] = str(rec.get("reason", "")) + (" | submitted" if out_sub.get("ok") else f" | {out_sub.get('error', 'submit failed')}")
            if out_sub.get("ok"):
                submitted += 1
    return {"ok": True, "staged": added, "submitted": submitted, "rows": len(rows)}


def _protect_open_positions_live_bt_ai(profile: str, auto_submit: bool = False) -> Dict[str, Any]:
    # Build open-position context first.
    review = bridge.analyze_open_positions_multi_tf(
        profile_name=profile,
        timeframes=["4h", "8h", "12h", "1d"],
        display_currency="USD",
    )
    if not review.get("ok"):
        return {"ok": False, "error": str(review.get("error", "Open position review failed"))}
    review_rows = review.get("rows", []) if isinstance(review.get("rows"), list) else []
    held_symbols = set(_portfolio_symbols_for_profile(profile=profile, quote_pref="USDT"))

    # Live context from current dashboard controls.
    live_cfg = {
        "name": f"{st.session_state.get('live_market', 'crypto')}-{st.session_state.get('live_tf', '1d')}",
        "market": str(st.session_state.get("live_market", "crypto")),
        "timeframe": str(st.session_state.get("live_tf", "1d")),
        "quote_currency": str(st.session_state.get("live_quote", "USDT")),
        "top_n": int(st.session_state.get("live_topn", 20)),
        "country": _country_display_to_code(str(st.session_state.get("live_country_display", "United States (2)"))),
        "display_currency": "USD",
    }
    live_res = bridge.run_live_panel(live_cfg)
    live_text = ""
    if live_res.get("ok"):
        live_text = "\n\n".join(
            [
                str(live_res.get("table_text", "") or ""),
                str(live_res.get("breakdown_text", "") or ""),
                str(live_res.get("context_text", "") or ""),
            ]
        ).strip()

    # Backtest context from current backtest controls.
    bt_cfg = {
        "market": str(st.session_state.get("bt_market", "crypto")),
        "timeframe": str(st.session_state.get("bt_tf", "1d")),
        "top_n": int(st.session_state.get("bt_topn", 20)),
        "months": int(st.session_state.get("bt_months", 12)),
        "country": "2",
        "quote_currency": str(st.session_state.get("live_quote", "USDT")),
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
        "position_size": 0.30,
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
    bt_res = bridge.run_backtest(bt_cfg)
    bt_text = ""
    if bt_res.get("ok"):
        bt_text = "\n\n".join([str(bt_res.get("summary_text", "") or ""), str(bt_res.get("trade_text", "") or "")]).strip()

    review_df = pd.DataFrame(review_rows) if review_rows else pd.DataFrame()
    review_text = review_df.to_csv(index=False) if not review_df.empty else str(review_rows)
    source = "\n\n".join(
        [
            "OPEN POSITIONS MTF REVIEW",
            str(review_text),
            "LIVE DASHBOARD CONTEXT",
            str(live_text),
            "BACKTEST CONTEXT",
            str(bt_text),
            "Task: return protective SELL orders for currently held symbols only.",
        ]
    ).strip()
    ai_out = bridge.run_ai_analysis(source, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    if not ai_out.get("ok"):
        # Fallback to deterministic simple protection
        return _protect_open_positions_simple(profile=profile, auto_submit=auto_submit)

    ai_resp = str(ai_out.get("response", "") or "")
    recs = _extract_trade_recs(ai_resp, default_tf=str(st.session_state.get("bt_tf", "4h")))
    filtered: List[Dict[str, Any]] = []
    for r in recs:
        sym = str(r.get("symbol", "")).strip().upper()
        side = str(r.get("side", "")).strip().upper()
        if side != "SELL":
            continue
        if held_symbols and sym not in held_symbols:
            continue
        filtered.append(r)
    if not filtered:
        return _protect_open_positions_simple(profile=profile, auto_submit=auto_submit)

    incoming = {str(x.get("symbol", "")).upper() for x in filtered if isinstance(x, dict)}
    if incoming:
        kept = []
        for rec in st.session_state.pending_recs:
            sym = str(rec.get("symbol", "")).strip().upper() if isinstance(rec, dict) else ""
            src = str(rec.get("pending_source", "")).strip().lower() if isinstance(rec, dict) else ""
            if sym in incoming and src in ("protect_open_positions_ai_bt", "protect_open_positions_live_bt_ai"):
                continue
            kept.append(rec)
        st.session_state.pending_recs = kept
    added = _append_pending(filtered, source="protect_open_positions_live_bt_ai")
    submitted = 0
    if auto_submit and added > 0:
        for rec in st.session_state.pending_recs:
            if not isinstance(rec, dict):
                continue
            if str(rec.get("pending_source", "")).strip().lower() != "protect_open_positions_live_bt_ai":
                continue
            out_sub = _submit_pending_rec(profile, rec)
            rec["status"] = "SUBMITTED" if out_sub.get("ok") else "FAILED"
            rec["reason"] = str(rec.get("reason", "")) + (" | submitted" if out_sub.get("ok") else f" | {out_sub.get('error', 'submit failed')}")
            if out_sub.get("ok"):
                submitted += 1
    return {"ok": True, "staged": added, "submitted": submitted, "rows": len(review_rows), "mode": "live_bt_ai"}


_init_state()

tab_live, tab_backtest, tab_ai, tab_portfolio, tab_graph, tab_research, tab_tasks, tab_settings = st.tabs(
    ["Live Dashboard", "Backtest", "AI Analysis", "Portfolio & Ledger", "Position Graph", "Auto-Research", "Task Monitor", "Settings"]
)

with tab_live:
    st.subheader("Live Dashboard")
    pending_apply = st.session_state.get("pending_live_profile_apply")
    if isinstance(pending_apply, dict):
        st.session_state["live_market"] = str(pending_apply.get("market", st.session_state.get("live_market", "crypto")))
        st.session_state["live_tf"] = str(pending_apply.get("timeframe", st.session_state.get("live_tf", "1d")))
        st.session_state["live_tf_multi"] = list(pending_apply.get("extra_timeframes", []))
        st.session_state["live_topn"] = int(pending_apply.get("top_n", st.session_state.get("live_topn", 20)))
        st.session_state["live_quote"] = str(pending_apply.get("quote", st.session_state.get("live_quote", "USDT")))
        code = str(pending_apply.get("country", "2"))
        st.session_state["live_country_display"] = f"{COUNTRY_LABELS.get(code, 'United States')} ({code})"
        st.session_state["pending_live_profile_apply"] = None

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    market = c1.selectbox("Market", ["crypto", "traditional"], index=0, key="live_market")
    timeframe = c2.selectbox("Timeframe", ["1d", "4h", "8h", "12h"], index=0, key="live_tf")
    tf_multi = c3.multiselect("Extra TFs", ["1d", "4h", "8h", "12h"], default=[], key="live_tf_multi")
    top_n = c4.selectbox("Top N", [10, 20, 50, 100], index=1, key="live_topn")
    quote = c5.selectbox("Quote", ["USDT", "USD", "BTC", "ETH", "BNB"], index=0, key="live_quote")
    country_display = c6.selectbox("Country (traditional)", _country_display_values(), index=1, key="live_country_display")
    country = _country_display_to_code(country_display)

    st.markdown("### Dashboard Profiles")
    lp = _load_live_profiles()
    lp_names = sorted(lp.keys())
    p1, p2, p3, p4 = st.columns([2, 2, 1, 1])
    profile_name = p1.text_input("Profile name", value=st.session_state.live_profile_name, key="live_profile_name_input")
    st.session_state.live_profile_name = profile_name.strip() or "default"
    selected_profile = p2.selectbox("Saved profiles", [""] + lp_names, index=0, key="live_profile_select")
    save_profile_btn = p3.button("Save/Update", key="live_profile_save_btn")
    load_profile_btn = p4.button("Load", key="live_profile_load_btn")
    p5, p6 = st.columns([1, 4])
    delete_profile_btn = p5.button("Delete", key="live_profile_delete_btn")

    if save_profile_btn:
        name = st.session_state.live_profile_name.strip() or "default"
        lp[name] = {
            "market": market,
            "timeframe": timeframe,
            "extra_timeframes": list(tf_multi),
            "top_n": int(top_n),
            "quote": quote,
            "country": str(country),
        }
        ok, where = _save_live_profiles(lp)
        if ok:
            st.success(f"Saved profile: {name}")
        else:
            st.error(f"Could not save profile. Tried: {where}")
        st.rerun()
    if load_profile_btn and selected_profile:
        prof = lp.get(selected_profile, {})
        if isinstance(prof, dict):
            st.session_state["pending_live_profile_apply"] = dict(prof)
            st.session_state.live_profile_name = selected_profile
            st.success(f"Loaded profile: {selected_profile}")
            st.rerun()
    if delete_profile_btn and selected_profile:
        if selected_profile in lp:
            del lp[selected_profile]
            ok, where = _save_live_profiles(lp)
            if ok:
                st.success(f"Deleted profile: {selected_profile}")
            else:
                st.error(f"Could not save profile deletion. Tried: {where}")
            st.rerun()
    if st.button("Run Live Dashboard", type="primary"):
        run_tfs = [timeframe] + [x for x in tf_multi if x != timeframe]
        combined: List[str] = []
        for tf in run_tfs:
            cfg = {
                "name": f"{market}-{tf}",
                "market": market,
                "timeframe": tf,
                "quote_currency": quote,
                "top_n": int(top_n),
                "country": country,
                "display_currency": "USD",
            }
            with st.spinner(f"Running live panel ({tf})..."):
                res = _run_task(f"Live Dashboard {tf}", lambda ccfg=cfg: bridge.run_live_panel(ccfg))
            if not res.get("ok"):
                st.error(f"{tf}: {res.get('error', 'Live run failed')}")
                continue
            t = res.get("table_text", "")
            b = res.get("breakdown_text", "")
            c = res.get("context_text", "")
            block = "\n\n".join([str(t), str(b), str(c)]).strip()
            st.session_state.live_runs.append({"name": f"{market}-{tf}", "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "text": block})
            st.session_state.live_runs = st.session_state.live_runs[-20:]
            combined.append(block)
        if combined:
            st.session_state.last_live_text = "\n\n".join(combined).strip()
            st.success(f"Live run complete ({len(combined)} panel(s)).")
    if st.session_state.live_runs:
        st.markdown("### Latest Live Outputs")
        for run in reversed(st.session_state.live_runs[-6:]):
            lbl = f"{run.get('ts', '')} | {run.get('name', '')}"
            with st.expander(lbl, expanded=False):
                st.text(str(run.get("text", "")))

with tab_backtest:
    st.subheader("Backtest")
    c1, c2, c3, c4 = st.columns(4)
    market = c1.selectbox("Market", ["crypto", "traditional"], index=0, key="bt_market")
    timeframe = c2.selectbox("Timeframe", ["1d", "4h", "8h", "12h"], index=0, key="bt_tf")
    top_n = c3.selectbox("Top N", [10, 20, 50, 100], index=1, key="bt_topn")
    months = c4.selectbox("Lookback (months)", [1, 3, 6, 12, 18, 24], index=3, key="bt_months")
    bt_cols = st.columns(6)
    stop_loss_pct = bt_cols[0].number_input("Stop-loss %", value=8.0, min_value=0.5, max_value=50.0, step=0.5)
    take_profit_pct = bt_cols[1].number_input("Take-profit %", value=20.0, min_value=1.0, max_value=200.0, step=1.0)
    max_hold_days = bt_cols[2].number_input("Max hold days", value=45, min_value=1, max_value=365, step=1)
    pos_size_pct = bt_cols[3].number_input("Position size %", value=30.0, min_value=1.0, max_value=100.0, step=1.0)
    auto_tune = bt_cols[4].checkbox("Auto-tune", value=False)
    optuna_trials = bt_cols[5].number_input("Optuna trials", value=10, min_value=1, max_value=300, step=1)
    bt2 = st.columns(5)
    fee_pct = bt2[0].number_input("Fee % (per leg)", value=0.10, min_value=0.0, max_value=5.0, step=0.01)
    slip_pct = bt2[1].number_input("Slippage % (per leg)", value=0.05, min_value=0.0, max_value=5.0, step=0.01)
    min_hold_bars = int(bt2[2].number_input("Min hold bars", value=2, min_value=0, max_value=100, step=1))
    cooldown_bars = int(bt2[3].number_input("Cooldown bars", value=1, min_value=0, max_value=100, step=1))
    cache_workers = int(bt2[4].number_input("Indicator cache workers", value=4, min_value=1, max_value=64, step=1))
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
            "stop_loss_pct": float(stop_loss_pct),
            "take_profit_pct": float(take_profit_pct),
            "max_hold_days": int(max_hold_days),
            "min_hold_bars": 2,
            "cooldown_bars": 1,
            "same_asset_cooldown_bars": 3,
            "max_consecutive_same_asset_entries": 3,
            "fee_pct": float(fee_pct),
            "slippage_pct": float(slip_pct),
            "position_size": max(0.01, min(1.0, float(pos_size_pct) / 100.0)),
            "atr_multiplier": 2.2,
            "adx_threshold": 25.0,
            "cmf_threshold": 0.02,
            "obv_slope_threshold": 0.0,
            "buy_threshold": 2,
            "sell_threshold": -2,
            "cache_workers": int(cache_workers),
            "max_drawdown_limit_pct": 35.0,
            "max_exposure_pct": 0.4,
            "auto_tune": bool(auto_tune),
            "optuna_trials": int(optuna_trials),
            "optuna_jobs": 2,
        }
        with st.spinner("Running backtest..."):
            res = _run_task("Backtest", lambda: bridge.run_backtest(cfg))
        if not res.get("ok"):
            st.error(res.get("error", "Backtest failed"))
        else:
            st.success("Backtest complete")
            st.session_state.last_backtest_summary = str(res.get("summary_text", "") or "")
            st.session_state.last_backtest_trades = str(res.get("trade_text", "") or "")
    if st.session_state.last_backtest_summary:
        st.text(st.session_state.last_backtest_summary)
    if st.session_state.last_backtest_trades:
        st.text(st.session_state.last_backtest_trades)

with tab_ai:
    st.subheader("AI Analysis")
    ai_names, ai_active = _load_ai_profiles()
    ai_cols = st.columns([2, 1, 1, 1])
    ai_profile = _safe_select_option("AI profile", ai_names, preferred=ai_active)
    if ai_cols[1].button("Set Active") and ai_profile:
        out_set = _run_task("AI Set Active", lambda: bridge.set_active_ai_profile(ai_profile))
        if out_set.get("ok"):
            st.success("AI profile set active.")
        else:
            st.error(out_set.get("error", "Failed to set active AI profile"))
    if ai_cols[2].button("Preview Prompt"):
        st.session_state.ai_prompt_preview = bridge.build_dashboard_prompt(st.session_state.ai_source_text or "", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    if ai_cols[3].button("Stage Recs"):
        recs = _extract_trade_recs(st.session_state.ai_response or "", default_tf="4h")
        if recs:
            n = _append_pending(recs, source="ai_interpretation")
            _log(f"AI staged {n} recommendation(s).")
            st.success(f"Staged {n} recommendation(s).")
        else:
            st.warning("No structured trade recommendations found.")
    dt_context = st.text_input("Datetime context", value=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), key="ai_dt_context")
    st.markdown("### Import Source")
    import_cols = st.columns([2, 1, 1, 1])
    source_mode = import_cols[0].selectbox(
        "Source",
        ["manual", "latest_live", "recent_live_combined", "last_backtest"],
        index=0,
        key="ai_source_mode",
    )
    recent_n = int(import_cols[1].number_input("Recent runs", min_value=1, max_value=20, value=3, step=1, key="ai_recent_runs"))
    do_import = import_cols[2].button("Import to AI Input", key="ai_import_btn")
    do_append = import_cols[3].button("Append Import", key="ai_append_btn")

    imported_text = ""
    if source_mode == "latest_live":
        if st.session_state.live_runs:
            imported_text = str(st.session_state.live_runs[-1].get("text", "") or "")
        elif st.session_state.last_live_text:
            imported_text = str(st.session_state.last_live_text or "")
    elif source_mode == "recent_live_combined":
        runs = st.session_state.live_runs[-recent_n:] if st.session_state.live_runs else []
        blocks = [str(r.get("text", "") or "") for r in runs if isinstance(r, dict)]
        imported_text = "\n\n".join([b for b in blocks if b]).strip()
        if not imported_text and st.session_state.last_live_text:
            imported_text = str(st.session_state.last_live_text or "")
    elif source_mode == "last_backtest":
        imported_text = "\n\n".join(
            [
                str(st.session_state.last_backtest_summary or ""),
                str(st.session_state.last_backtest_trades or ""),
            ]
        ).strip()

    if do_import:
        if imported_text.strip():
            st.session_state.ai_source_text = imported_text
            st.session_state.ai_source_text_area = imported_text
            st.success("Imported source into AI input.")
        else:
            st.warning("No source text available to import for this mode yet.")
    if do_append:
        if imported_text.strip():
            existing = str(st.session_state.ai_source_text or "").strip()
            merged = (existing + ("\n\n" if existing else "") + imported_text).strip()
            st.session_state.ai_source_text = merged
            st.session_state.ai_source_text_area = merged
            st.success("Imported source appended to AI input.")
        else:
            st.warning("No source text available to import for this mode yet.")

    st.markdown("### Full Pipeline")
    pipe_cols = st.columns([2, 1, 1])
    bn_names_for_pipe = _load_binance_profiles()
    pipe_profile = _safe_select_option("Binance profile for auto-submit", bn_names_for_pipe, preferred=(bn_names_for_pipe[0] if bn_names_for_pipe else ""))
    pipe_auto_submit = pipe_cols[1].checkbox("Auto-submit staged", value=False, key="ai_pipe_auto_submit")
    run_pipeline = pipe_cols[2].button("Run Live > BT > AI > Stage", key="ai_pipe_run_btn", type="primary")
    if run_pipeline:
        # 1) Live
        live_cfg = {
            "name": f"{st.session_state.get('live_market', 'crypto')}-{st.session_state.get('live_tf', '1d')}",
            "market": str(st.session_state.get("live_market", "crypto")),
            "timeframe": str(st.session_state.get("live_tf", "1d")),
            "quote_currency": str(st.session_state.get("live_quote", "USDT")),
            "top_n": int(st.session_state.get("live_topn", 20)),
            "country": _country_display_to_code(str(st.session_state.get("live_country_display", "United States (2)"))),
            "display_currency": "USD",
        }
        live_res = _run_task("Pipeline Live", lambda: bridge.run_live_panel(live_cfg))
        if not live_res.get("ok"):
            st.error(f"Pipeline failed at Live: {live_res.get('error', 'unknown error')}")
        else:
            live_block = "\n\n".join(
                [
                    str(live_res.get("table_text", "") or ""),
                    str(live_res.get("breakdown_text", "") or ""),
                    str(live_res.get("context_text", "") or ""),
                ]
            ).strip()
            st.session_state.live_runs.append({"name": live_cfg["name"], "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "text": live_block})
            st.session_state.live_runs = st.session_state.live_runs[-20:]
            st.session_state.last_live_text = live_block
            # 2) Backtest
            bt_cfg = {
                "market": str(st.session_state.get("bt_market", "crypto")),
                "timeframe": str(st.session_state.get("bt_tf", "1d")),
                "top_n": int(st.session_state.get("bt_topn", 20)),
                "months": int(st.session_state.get("bt_months", 12)),
                "country": "2",
                "quote_currency": str(st.session_state.get("live_quote", "USDT")),
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
                "position_size": 0.30,
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
            bt_res = _run_task("Pipeline Backtest", lambda: bridge.run_backtest(bt_cfg))
            if not bt_res.get("ok"):
                st.error(f"Pipeline failed at Backtest: {bt_res.get('error', 'unknown error')}")
            else:
                st.session_state.last_backtest_summary = str(bt_res.get("summary_text", "") or "")
                st.session_state.last_backtest_trades = str(bt_res.get("trade_text", "") or "")
                # 3) AI
                pipe_source = "\n\n".join([live_block, st.session_state.last_backtest_summary, st.session_state.last_backtest_trades]).strip()
                st.session_state.ai_source_text = pipe_source
                st.session_state.ai_source_text_area = pipe_source
                ai_res = _run_task("Pipeline AI", lambda: bridge.run_ai_analysis(pipe_source, dt_context))
                if not ai_res.get("ok"):
                    st.error(f"Pipeline failed at AI: {ai_res.get('error', 'unknown error')}")
                else:
                    st.session_state.ai_response = str(ai_res.get("response", "") or "")
                    recs = _extract_trade_recs(st.session_state.ai_response, default_tf=str(st.session_state.get("bt_tf", "4h")))
                    if recs:
                        seq0 = int(st.session_state.pending_rec_seq or 0)
                        n = _append_pending(recs, source="pipeline_live_bt_ai")
                        st.success(f"Pipeline complete. Staged {n} recommendation(s).")
                        if pipe_auto_submit:
                            if not pipe_profile:
                                st.warning("No Binance profile selected for auto-submit.")
                            else:
                                new_ids = set(range(seq0 + 1, seq0 + n + 1))
                                ok_n = 0
                                fail_n = 0
                                for rec in st.session_state.pending_recs:
                                    if int(rec.get("id", -1)) not in new_ids:
                                        continue
                                    out_sub = _submit_pending_rec(pipe_profile, rec)
                                    rec["status"] = "SUBMITTED" if out_sub.get("ok") else "FAILED"
                                    rec["reason"] = str(rec.get("reason", "")) + (
                                        " | submitted" if out_sub.get("ok") else f" | {out_sub.get('error', 'submit failed')}"
                                    )
                                    if out_sub.get("ok"):
                                        ok_n += 1
                                    else:
                                        fail_n += 1
                                st.info(f"Auto-submit results: submitted={ok_n}, failed={fail_n}")
                    else:
                        st.warning("Pipeline AI returned no structured trade recommendations to stage.")

    if not str(st.session_state.get("ai_source_text_area", "")).strip() and str(st.session_state.get("ai_source_text", "")).strip():
        st.session_state.ai_source_text_area = str(st.session_state.ai_source_text or "")

    src = st.text_area(
        "Source text (dashboard/backtest/logs)",
        height=320,
        placeholder="Paste dashboard text here or use Import Source above...",
        key="ai_source_text_area",
    )
    current_src = str(src or "").strip()
    st.session_state.ai_source_text = current_src
    if st.session_state.ai_prompt_preview:
        st.text_area("Prompt Preview", value=st.session_state.ai_prompt_preview, height=200)
    if st.button("Run AI Analysis", type="primary"):
        if not current_src:
            st.warning("Provide source text first.")
        else:
            with st.spinner("Calling AI provider..."):
                res = _run_task("AI Analysis", lambda: bridge.run_ai_analysis(current_src, dt_context))
            if not res.get("ok"):
                st.error(res.get("error", "AI failed"))
            else:
                st.session_state.ai_response = str(res.get("response", "") or "")
                st.success("AI response received")
    if st.session_state.ai_response:
        st.text_area("AI Response", value=st.session_state.ai_response, height=420)
    follow = st.text_input("Follow-up")
    if st.button("Run Follow-up") and follow.strip():
        compound = (st.session_state.ai_source_text or "") + "\n\nPrevious response:\n" + (st.session_state.ai_response or "") + "\n\nFollow-up:\n" + follow
        out_fu = _run_task("AI Follow-up", lambda: bridge.run_ai_analysis(compound, dt_context))
        if out_fu.get("ok"):
            st.session_state.ai_response = str(out_fu.get("response", "") or "")
        else:
            st.error(out_fu.get("error", "Follow-up failed"))

with tab_portfolio:
    st.subheader("Portfolio & Ledger")
    names = _load_binance_profiles()
    profile = _safe_select_option("Binance profile", names, preferred=(names[0] if names else ""))
    c1, c2, c3, c4, c5 = st.columns(5)
    do_pf = c1.button("Refresh Portfolio", type="primary", disabled=not bool(profile))
    do_oo = c2.button("Refresh Open Orders", disabled=not bool(profile))
    do_led = c3.button("Refresh Ledger")
    do_rec = c4.button("Reconcile Fills", disabled=not bool(profile))
    do_review = c5.button("Review Open Positions (MTF)", disabled=not bool(profile))
    pcols = st.columns(3)
    auto_send_protect = pcols[0].checkbox("Auto-send protect", value=False)
    protect_mode = pcols[1].selectbox("Protect mode", ["ai_bt", "live_bt_ai"], index=0, key="protect_mode")
    do_protect = pcols[2].button("Protect Open Positions (AI+BT)", disabled=not bool(profile))
    do_prune = st.button("Prune Signal History")

    if do_pf and profile:
        out = _run_task("Portfolio Refresh", lambda: bridge.fetch_binance_portfolio(profile_name=profile))
        if not out.get("ok"):
            st.error(out.get("error", "Portfolio fetch failed"))
        else:
            st.success(f"Portfolio: {profile}")
            st.write(f"Estimated Total (USD): **${float(out.get('estimated_total_usd', 0.0) or 0.0):,.2f}**")
            df = pd.DataFrame(out.get("balances", []) or [])
            st.session_state.last_portfolio_df = df
            if not df.empty:
                st.dataframe(df, use_container_width=True, hide_index=True)

    if do_oo and profile:
        out = _run_task("Open Orders Refresh", lambda: bridge.list_open_binance_orders(profile_name=profile))
        if not out.get("ok"):
            st.error(out.get("error", "Open orders fetch failed"))
        else:
            st.success("Open orders loaded")
            df = pd.DataFrame(out.get("orders", []) or [])
            st.session_state.last_open_orders_df = df
            if not df.empty:
                st.dataframe(df, use_container_width=True, hide_index=True)
            else:
                st.info("No open orders.")

    if do_led:
        out = _run_task("Ledger Refresh", lambda: bridge.get_trade_ledger())
        if not out.get("ok"):
            st.error(out.get("error", "Ledger fetch failed"))
        else:
            ledger = out.get("ledger", {}) if isinstance(out.get("ledger"), dict) else {}
            entries = ledger.get("entries", []) if isinstance(ledger.get("entries"), list) else []
            st.write(f"Entries: **{len(entries)}**")
            if entries:
                df = pd.DataFrame(entries)
                st.session_state.last_ledger_df = df
                st.dataframe(df, use_container_width=True, hide_index=True)
    if do_rec and profile:
        syms = _portfolio_symbols_for_profile(profile=profile, quote_pref=str(st.session_state.get("live_quote", "USDT")))
        if not syms:
            try:
                pos_out = bridge.analyze_open_positions_multi_tf(profile_name=profile, timeframes=["4h"], display_currency="USD")
                rows = pos_out.get("rows", []) if isinstance(pos_out.get("rows"), list) else []
                syms = sorted({str(r.get("symbol", "")).strip().upper() for r in rows if isinstance(r, dict) and str(r.get("symbol", "")).strip()})
            except Exception:
                syms = []
        if not syms:
            st.warning("No symbols detected from portfolio/open positions; reconcile skipped.")
            out = {"ok": False, "error": "No symbols available for reconciliation."}
        else:
            out = _run_task("Reconcile Fills", lambda: bridge.reconcile_binance_fills(profile_name=profile, symbols=syms))
        if out.get("ok"):
            st.success(f"Reconcile added={int(out.get('added', 0) or 0)}, dup={int(out.get('duplicates', 0) or 0)}")
        else:
            st.error(out.get("error", "Reconcile failed"))
    if do_review and profile:
        out = _run_task(
            "Open Position Review (MTF)",
            lambda: bridge.analyze_open_positions_multi_tf(profile_name=profile, timeframes=["4h", "8h", "12h", "1d"], display_currency="USD"),
        )
        if out.get("ok"):
            rdf = pd.DataFrame(out.get("rows", []) or [])
            st.session_state.last_review_df = rdf
            st.dataframe(rdf, use_container_width=True, hide_index=True)
        else:
            st.error(out.get("error", "Review failed"))
    if do_protect and profile:
        if protect_mode == "live_bt_ai":
            out = _run_task(
                "Protect Open Positions (Live->BT->AI)",
                lambda: _protect_open_positions_live_bt_ai(profile, auto_submit=bool(auto_send_protect)),
            )
        else:
            out = _run_task("Protect Open Positions (AI+BT)", lambda: _protect_open_positions_simple(profile, auto_submit=bool(auto_send_protect)))
        if out.get("ok"):
            mode_txt = str(out.get("mode", protect_mode))
            st.success(f"Protection ({mode_txt}) staged={int(out.get('staged', 0) or 0)}, submitted={int(out.get('submitted', 0) or 0)}")
        else:
            st.error(out.get("error", "Protect failed"))
    if do_prune:
        out = _run_task("Prune Signal History", lambda: bridge.prune_signal_only_history(keep_last_signals=0))
        if out.get("ok"):
            st.success("Signal history pruned.")
        else:
            st.error(out.get("error", "Prune failed"))

    st.markdown("### Pending Recommendations")
    pdf = _pending_df()
    if not pdf.empty:
        st.dataframe(pdf, use_container_width=True, hide_index=True)
        all_ids = [int(x) for x in pdf["id"].tolist()]
        selected = st.multiselect("Select pending IDs", options=all_ids, default=st.session_state.selected_pending_ids)
        st.session_state.selected_pending_ids = selected
        pc = st.columns(4)
        if pc[0].button("Submit Selected", disabled=not bool(profile)):
            ok_n = 0
            fail_n = 0
            for rid in selected:
                rec = next((r for r in st.session_state.pending_recs if int(r.get("id", -1)) == int(rid)), None)
                if not rec:
                    continue
                out_sub = _submit_pending_rec(profile, rec)
                rec["status"] = "SUBMITTED" if out_sub.get("ok") else "FAILED"
                rec["reason"] = str(rec.get("reason", "")) + (" | submitted" if out_sub.get("ok") else f" | {out_sub.get('error', 'submit failed')}")
                if out_sub.get("ok"):
                    ok_n += 1
                else:
                    fail_n += 1
            st.success(f"Submitted={ok_n}, failed={fail_n}")
        if pc[1].button("Remove Selected"):
            ids = set(int(x) for x in selected)
            st.session_state.pending_recs = [r for r in st.session_state.pending_recs if int(r.get("id", -1)) not in ids]
            st.success(f"Removed {len(ids)} selected.")
        if pc[2].button("Clear Pending"):
            st.session_state.pending_recs = []
            st.session_state.selected_pending_ids = []
            st.success("Pending cleared.")
        if pc[3].button("Download Pending CSV"):
            st.download_button("Save pending.csv", data=pdf.to_csv(index=False), file_name="pending_recommendations.csv", mime="text/csv")
    else:
        st.info("No pending recommendations.")
    st.markdown("### Cached Views")
    cv1, cv2, cv3 = st.columns(3)
    with cv1:
        st.caption("Portfolio (cached)")
        if isinstance(st.session_state.last_portfolio_df, pd.DataFrame) and not st.session_state.last_portfolio_df.empty:
            st.dataframe(st.session_state.last_portfolio_df, use_container_width=True, height=220, hide_index=True)
    with cv2:
        st.caption("Open Orders (cached)")
        if isinstance(st.session_state.last_open_orders_df, pd.DataFrame) and not st.session_state.last_open_orders_df.empty:
            st.dataframe(st.session_state.last_open_orders_df, use_container_width=True, height=220, hide_index=True)
    with cv3:
        st.caption("Open Position Review (cached)")
        if isinstance(st.session_state.last_review_df, pd.DataFrame) and not st.session_state.last_review_df.empty:
            st.dataframe(st.session_state.last_review_df, use_container_width=True, height=220, hide_index=True)
    st.caption("Ledger (cached)")
    if isinstance(st.session_state.last_ledger_df, pd.DataFrame) and not st.session_state.last_ledger_df.empty:
        st.dataframe(st.session_state.last_ledger_df, use_container_width=True, height=260, hide_index=True)

with tab_graph:
    st.subheader("Position Graph Data")
    names = _load_binance_profiles()
    profile = _safe_select_option("Binance profile (graph)", names, preferred=(names[0] if names else ""))
    quote_pref = st.selectbox("Quote", ["USDT", "USD", "USDC", "BUSD", "BTC", "ETH", "BNB"], index=0, key="graph_quote")
    if st.button("Refresh Position Graph Data", type="primary", disabled=not bool(profile)):
        with st.spinner("Building position graph data..."):
            df = _run_task("Position Graph Refresh", lambda: _position_graph_rows(profile=profile, quote_pref=quote_pref))
        if df.empty:
            st.info("No open positions found.")
        else:
            st.session_state.last_graph_df = df.copy()
            st.success(f"Rows: {len(df)}")
            st.dataframe(df, use_container_width=True, hide_index=True)
            chart_df = df.copy()
            chart_df["pnl_pct"] = pd.to_numeric(chart_df["pnl_pct"], errors="coerce").fillna(0.0)
            st.bar_chart(chart_df.set_index("symbol")["pnl_pct"])
    if isinstance(st.session_state.last_graph_df, pd.DataFrame) and not st.session_state.last_graph_df.empty:
        st.caption("Cached position graph data")
        st.dataframe(st.session_state.last_graph_df, use_container_width=True, hide_index=True)

with tab_research:
    st.subheader("Auto-Research")
    r1, r2, r3 = st.columns(3)
    std_btn = r1.button("Run Standard Research", type="primary")
    trials = int(r2.number_input("Comprehensive Optuna trials", value=10, min_value=1, max_value=200, step=1))
    jobs = int(r3.number_input("Comprehensive parallel jobs", value=4, min_value=1, max_value=64, step=1))
    scen_market = st.selectbox("Scenario market", ["crypto", "traditional"], index=0)
    scen_topn = st.selectbox("Scenario Top N", [10, 20, 50, 100], index=1)
    scen_tfs = st.multiselect("Scenario timeframes", ["1d", "4h", "8h", "12h"], default=["1d", "4h"])
    scen_months = st.selectbox("Scenario months", [1, 3, 6, 12, 18, 24], index=3)
    if std_btn:
        out = _run_task("Standard Research", lambda: bridge.run_standard_research())
        if out.get("ok"):
            st.success("Standard research completed.")
            st.session_state.last_research_text = str(out.get("output", "") or out.get("stdout", "") or "")
        else:
            st.error(out.get("error", "Standard research failed"))
    if st.button("Run Comprehensive Research"):
        scenarios = []
        for tf in (scen_tfs or ["1d"]):
            scenarios.append({"market": scen_market, "timeframe": tf, "top_n": int(scen_topn), "months": int(scen_months)})
        out = _run_task("Comprehensive Research", lambda: bridge.run_comprehensive_research(scenarios=scenarios, optuna_trials=trials, optuna_jobs=jobs))
        if out.get("ok"):
            st.success("Comprehensive research completed.")
            st.session_state.last_research_text = str(out.get("output", "") or out.get("stdout", "") or "")
        else:
            st.error(out.get("error", "Comprehensive research failed"))
    if st.session_state.last_research_text:
        st.text_area("Research Output", value=st.session_state.last_research_text, height=420)

with tab_tasks:
    st.subheader("Task Monitor")
    tc = st.columns(3)
    if tc[0].button("Clear Task Log"):
        st.session_state.task_logs = []
    if tc[1].button("Clear Task History"):
        st.session_state.task_history = []
    tc[2].write(f"Log lines: {len(st.session_state.task_logs)}")
    st.text_area("Task Log", value="\n".join(st.session_state.task_logs), height=260)
    hist_df = pd.DataFrame(st.session_state.task_history) if st.session_state.task_history else pd.DataFrame(columns=["task", "sec", "ts", "ok"])
    st.dataframe(hist_df, use_container_width=True, hide_index=True)

with tab_settings:
    st.subheader("Settings")
    st.caption("Profile management for AI and Binance.")

    st.markdown("### AI Profiles")
    ai_list_out = bridge.list_ai_profiles()
    ai_profiles = ai_list_out.get("profiles", []) if isinstance(ai_list_out.get("profiles"), list) else []
    ai_active = str(ai_list_out.get("active_profile", "") or "")
    ai_names = [str(p.get("name", "")).strip() for p in ai_profiles if isinstance(p, dict) and str(p.get("name", "")).strip()]
    ai_pick = _safe_select_option("Select AI profile", ai_names, preferred=ai_active)
    ai_obj = next((p for p in ai_profiles if isinstance(p, dict) and str(p.get("name", "")).strip() == ai_pick), {}) if ai_pick else {}
    a1, a2, a3 = st.columns(3)
    ai_name = a1.text_input("AI profile name", value=str(ai_pick or ""), key="set_ai_name")
    ai_provider = a2.selectbox("Provider", ["xai", "openai", "openai_compatible", "openclaw", "ollama", "anthropic"], index=0 if not ai_obj else max(0, ["xai", "openai", "openai_compatible", "openclaw", "ollama", "anthropic"].index(str(ai_obj.get("provider", "xai")).lower()) if str(ai_obj.get("provider", "xai")).lower() in ["xai", "openai", "openai_compatible", "openclaw", "ollama", "anthropic"] else 0), key="set_ai_provider")
    ai_model = a3.text_input("Model", value=str(ai_obj.get("model", "grok-3-mini") if isinstance(ai_obj, dict) else "grok-3-mini"), key="set_ai_model")
    a4, a5, a6 = st.columns([2, 1, 1])
    ai_endpoint = a4.text_input("Endpoint", value=str(ai_obj.get("endpoint", "") if isinstance(ai_obj, dict) else ""), key="set_ai_endpoint")
    ai_temp = a5.number_input("Temp", min_value=0.0, max_value=2.0, step=0.1, value=float(ai_obj.get("temperature", 0.2) if isinstance(ai_obj, dict) else 0.2), key="set_ai_temp")
    ai_internet = a6.checkbox("Internet-enabled", value=bool(ai_obj.get("internet_access", True) if isinstance(ai_obj, dict) else True), key="set_ai_internet")
    a7, a8, a9, a10 = st.columns(4)
    ai_save = a7.button("Save AI Profile", key="set_ai_save")
    ai_set = a8.button("Set Active", key="set_ai_active_btn", disabled=not bool(ai_pick or ai_name))
    ai_test = a9.button("Test", key="set_ai_test", disabled=not bool(ai_pick or ai_name))
    ai_delete = a10.button("Delete", key="set_ai_delete", disabled=not bool(ai_pick))
    ai_key = st.text_input("API key (optional; leave blank to keep)", type="password", key="set_ai_api_key")
    a11, a12 = st.columns(2)
    ai_set_key = a11.button("Store API Key", key="set_ai_store_key", disabled=not bool(ai_pick or ai_name))
    ai_remove_key = a12.button("Remove API Key", key="set_ai_remove_key", disabled=not bool(ai_pick))

    target_ai_name = (ai_pick or ai_name).strip()
    if ai_save:
        out = _run_task(
            "AI Save Profile",
            lambda: bridge.upsert_ai_profile(
                name=ai_name.strip(),
                provider=ai_provider,
                model=ai_model.strip(),
                endpoint=ai_endpoint.strip(),
                internet_access=bool(ai_internet),
                temperature=float(ai_temp),
                activate=False,
            ),
        )
        st.success("AI profile saved.") if out.get("ok") else st.error(out.get("error", "Save failed"))
        st.rerun()
    if ai_set and target_ai_name:
        out = _run_task("AI Set Active", lambda: bridge.set_active_ai_profile(target_ai_name))
        st.success("Active AI profile updated.") if out.get("ok") else st.error(out.get("error", "Set active failed"))
        st.rerun()
    if ai_test and target_ai_name:
        out = _run_task("AI Test", lambda: bridge.test_ai_profile(target_ai_name))
        st.success(str(out.get("response", "AI profile OK"))) if out.get("ok") else st.error(out.get("error", "AI profile test failed"))
    if ai_delete and ai_pick:
        out = _run_task("AI Delete Profile", lambda: bridge.delete_ai_profile(ai_pick))
        st.success("AI profile deleted.") if out.get("ok") else st.error(out.get("error", "Delete failed"))
        st.rerun()
    if ai_set_key and target_ai_name:
        if ai_key.strip():
            out = _run_task("AI Store Key", lambda: bridge.set_ai_profile_key(target_ai_name, ai_key.strip()))
            st.success("AI API key stored.") if out.get("ok") else st.error(out.get("error", "Store key failed"))
        else:
            st.warning("Enter an API key first.")
    if ai_remove_key and ai_pick:
        out = _run_task("AI Remove Key", lambda: bridge.remove_ai_profile_key(ai_pick))
        st.success("AI API key removed.") if out.get("ok") else st.error(out.get("error", "Remove key failed"))

    st.markdown("### Binance Profiles")
    bn_list_out = bridge.list_binance_profiles()
    bn_profiles = bn_list_out.get("profiles", []) if isinstance(bn_list_out.get("profiles"), list) else []
    bn_active = str(bn_list_out.get("active_profile", "") or "")
    bn_names = [str(p.get("name", "")).strip() for p in bn_profiles if isinstance(p, dict) and str(p.get("name", "")).strip()]
    bn_pick = _safe_select_option("Select Binance profile", bn_names, preferred=bn_active)
    bn_obj = next((p for p in bn_profiles if isinstance(p, dict) and str(p.get("name", "")).strip() == bn_pick), {}) if bn_pick else {}
    b1, b2, b3 = st.columns(3)
    bn_name = b1.text_input("Binance profile name", value=str(bn_pick or ""), key="set_bn_name")
    bn_endpoint = b2.text_input("Endpoint", value=str(bn_obj.get("endpoint", "https://api.binance.com") if isinstance(bn_obj, dict) else "https://api.binance.com"), key="set_bn_endpoint")
    b3.text_input("API key env", value=str(bn_obj.get("api_key_env", "BINANCE_API_KEY") if isinstance(bn_obj, dict) else "BINANCE_API_KEY"), key="set_bn_key_env")
    b4, b5, b6, b7 = st.columns(4)
    bn_save = b4.button("Save Binance Profile", key="set_bn_save")
    bn_set = b5.button("Set Active", key="set_bn_active", disabled=not bool(bn_pick or bn_name))
    bn_test = b6.button("Test", key="set_bn_test", disabled=not bool(bn_pick or bn_name))
    bn_delete = b7.button("Delete", key="set_bn_delete", disabled=not bool(bn_pick))
    bn_api_key = st.text_input("Binance API key (optional; leave blank to keep)", type="password", key="set_bn_api_key")
    bn_api_secret = st.text_input("Binance API secret (optional; leave blank to keep)", type="password", key="set_bn_api_secret")
    b8, b9 = st.columns(2)
    bn_store_keys = b8.button("Store Binance Keys", key="set_bn_store_keys", disabled=not bool(bn_pick or bn_name))
    bn_remove_keys = b9.button("Remove Binance Keys", key="set_bn_remove_keys", disabled=not bool(bn_pick))

    target_bn_name = (bn_pick or bn_name).strip()
    if bn_save:
        out = _run_task(
            "Binance Save Profile",
            lambda: bridge.upsert_binance_profile(
                name=bn_name.strip(),
                endpoint=bn_endpoint.strip() or "https://api.binance.com",
                api_key_env="BINANCE_API_KEY",
                api_secret_env="BINANCE_API_SECRET",
                activate=False,
            ),
        )
        st.success("Binance profile saved.") if out.get("ok") else st.error(out.get("error", "Save failed"))
        st.rerun()
    if bn_set and target_bn_name:
        out = _run_task("Binance Set Active", lambda: bridge.set_active_binance_profile(target_bn_name))
        st.success("Active Binance profile updated.") if out.get("ok") else st.error(out.get("error", "Set active failed"))
        st.rerun()
    if bn_test and target_bn_name:
        out = _run_task("Binance Test", lambda: bridge.test_binance_profile(target_bn_name))
        st.success(f"Profile OK. can_trade={out.get('can_trade', '')}") if out.get("ok") else st.error(out.get("error", "Binance profile test failed"))
    if bn_delete and bn_pick:
        out = _run_task("Binance Delete Profile", lambda: bridge.delete_binance_profile(bn_pick))
        st.success("Binance profile deleted.") if out.get("ok") else st.error(out.get("error", "Delete failed"))
        st.rerun()
    if bn_store_keys and target_bn_name:
        if bn_api_key.strip() and bn_api_secret.strip():
            out = _run_task("Binance Store Keys", lambda: bridge.set_binance_profile_keys(target_bn_name, bn_api_key.strip(), bn_api_secret.strip()))
            st.success("Binance API keys stored.") if out.get("ok") else st.error(out.get("error", "Store keys failed"))
        else:
            st.warning("Enter both API key and secret first.")
    if bn_remove_keys and bn_pick:
        out = _run_task("Binance Remove Keys", lambda: bridge.remove_binance_profile_keys(bn_pick))
        st.success("Binance API keys removed.") if out.get("ok") else st.error(out.get("error", "Remove keys failed"))
