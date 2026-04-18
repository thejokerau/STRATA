from __future__ import annotations

from datetime import datetime, timezone
import os
import json
import re
import time
import hashlib
import tempfile
import concurrent.futures
import uuid
import io
import queue
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from contextlib import redirect_stdout, redirect_stderr

import pandas as pd
import streamlit as st
import plotly.graph_objects as go

from gui_app.engine_bridge import EngineBridge
from gui_app.runtime_diag import RuntimeDiagLogger

# Streamlit + numba/pandas_ta safety for dynamic-loader contexts.
os.environ.setdefault("NUMBA_DISABLE_JIT", "1")
os.environ.setdefault("NUMBA_CACHE_DIR", str(Path.home() / ".numba_cache"))
os.environ.setdefault("NO_PROXY", "api.binance.com,localhost,127.0.0.1")
os.environ.setdefault("no_proxy", os.environ.get("NO_PROXY", "api.binance.com,localhost,127.0.0.1"))
os.environ.setdefault("CTMT_CACHE_TTL_LIVE_SEC", "600")


st.set_page_config(page_title="STRATA Streamlit", layout="wide")
st.title("STRATA Streamlit (Variant)")
st.caption("Experimental Streamlit branch: `streamlit-nightly`")


@st.cache_resource
def get_bridge(repo_root: str) -> EngineBridge:
    return EngineBridge(Path(repo_root))


REPO_ROOT = str(Path(__file__).resolve().parents[1])
bridge = get_bridge(REPO_ROOT)
diag = RuntimeDiagLogger(Path(REPO_ROOT))
_DEFAULT_BG_WORKERS = max(1, min(16, int(os.environ.get("CTMT_STREAMLIT_BG_WORKERS", "4") or "4")))
EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=_DEFAULT_BG_WORKERS)
EXECUTOR_WORKERS = _DEFAULT_BG_WORKERS

COUNTRY_LABELS: Dict[str, str] = {
    "1": "Australia",
    "2": "United States",
    "3": "United Kingdom",
    "4": "Europe",
    "5": "Canada",
    "6": "Other",
}

_OPEN_POS_REVIEW_CACHE: Dict[str, Dict[str, Any]] = {}
_OPEN_POS_REVIEW_LOCK = threading.Lock()
_PREFETCH_HEAVY_JOB_HINTS = (
    "Unified Cycle",
    "Protect Open Positions (Live->BT->AI)",
    "Live Dashboard (async)",
    "Backtest (async)",
    "Pipeline Live->BT->AI (async)",
)


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


def _run_live_panels_sync(configs: List[Dict[str, Any]], bridge_obj: Optional[EngineBridge] = None) -> Dict[str, Any]:
    br = bridge_obj or bridge
    combined: List[str] = []
    runs: List[Dict[str, str]] = []
    errors: List[str] = []
    perf_rows: List[Dict[str, Any]] = []
    total_sec = 0.0
    for cfg in configs:
        tf = str(cfg.get("timeframe", ""))
        res = br.run_live_panel(cfg)
        if not res.get("ok"):
            errors.append(f"{tf}: {res.get('error', 'Live run failed')}")
            continue
        pf = res.get("perf", {}) if isinstance(res.get("perf"), dict) else {}
        panel_name = str(cfg.get("name", f"panel-{tf}"))
        psec = float(pf.get("total_sec", 0.0) or 0.0)
        total_sec += psec
        perf_rows.append(
            {
                "stage": "LIVE",
                "panel": panel_name,
                "sec": round(psec, 3),
                "result_cache_hit": bool(pf.get("result_cache_hit", False)),
                "raw_cache_hit": bool(pf.get("raw_cache_hit", False)),
                "indicator_cache_hit": bool(pf.get("indicator_cache_hit", False)),
                "assets": int(res.get("loaded_assets", 0) or 0),
            }
        )
        t = res.get("table_text", "")
        b = res.get("breakdown_text", "")
        c = res.get("context_text", "")
        block = "\n\n".join([str(t), str(b), str(c)]).strip()
        runs.append({"name": str(cfg.get("name", f"panel-{tf}")), "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "text": block})
        combined.append(block)
    return {
        "ok": bool(combined),
        "runs": runs,
        "combined_text": "\n\n".join(combined).strip(),
        "errors": errors,
        "perf_rows": perf_rows,
        "perf_summary": {"live_total_sec": round(total_sec, 3), "live_panels": len(perf_rows)},
    }


def _expand_live_cfgs(live_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    base_tf = str(live_cfg.get("timeframe", "1d") or "1d")
    extras = [str(x).strip() for x in (live_cfg.get("extra_timeframes", []) or []) if str(x).strip()]
    run_tfs: List[str] = []
    for tf in [base_tf] + extras:
        if tf not in run_tfs:
            run_tfs.append(tf)
    out: List[Dict[str, Any]] = []
    for tf in run_tfs:
        c = dict(live_cfg)
        c["timeframe"] = tf
        c["name"] = str(live_cfg.get("name", f"{live_cfg.get('market', 'crypto')}-{base_tf}")).replace(base_tf, tf, 1)
        out.append(c)
    return out


def _run_live_cfg_bundle_sync(live_cfg: Dict[str, Any], bridge_obj: Optional[EngineBridge] = None) -> Dict[str, Any]:
    cfgs = _expand_live_cfgs(live_cfg)
    return _run_live_panels_sync(cfgs, bridge_obj=bridge_obj)


def _pipeline_bundle_cache_key(live_cfg: Dict[str, Any], bt_cfg: Dict[str, Any]) -> str:
    payload = {"live": live_cfg, "bt": bt_cfg}
    blob = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha1(blob.encode("utf-8")).hexdigest()


def _build_live_cfg_from_state_or_profile(profile_name: str = "") -> Dict[str, Any]:
    prof_name = str(profile_name or "").strip()
    if prof_name:
        lp = _load_live_profiles()
        prof = lp.get(prof_name, {}) if isinstance(lp, dict) else {}
        if isinstance(prof, dict) and prof:
            tf = str(prof.get("timeframe", "1d") or "1d")
            market = str(prof.get("market", "crypto") or "crypto")
            quote = str(prof.get("quote", "USDT") or "USDT")
            top_n = int(prof.get("top_n", 20) or 20)
            country = str(prof.get("country", "2") or "2")
            extra_tfs = [str(x).strip() for x in (prof.get("extra_timeframes", []) or []) if str(x).strip()]
            window_mode = str(prof.get("window_mode", "full") or "full")
            window_days = int(prof.get("window_days", 30) or 30)
            window_bars = int(prof.get("window_bars", 120) or 120)
            return {
                "name": f"{market}-{tf}",
                "market": market,
                "timeframe": tf,
                "extra_timeframes": extra_tfs,
                "quote_currency": quote,
                "top_n": top_n,
                "country": country,
                "display_currency": "USD",
                "analysis_window_mode": window_mode,
                "analysis_window_days": window_days,
                "analysis_window_bars": window_bars,
            }
    return {
        "name": f"{st.session_state.get('live_market', 'crypto')}-{st.session_state.get('live_tf', '1d')}",
        "market": str(st.session_state.get("live_market", "crypto")),
        "timeframe": str(st.session_state.get("live_tf", "1d")),
        "extra_timeframes": list(st.session_state.get("live_tf_multi", []) or []),
        "quote_currency": str(st.session_state.get("live_quote", "USDT")),
        "top_n": int(st.session_state.get("live_topn", 20)),
        "country": _country_display_to_code(str(st.session_state.get("live_country_display", "United States (2)"))),
        "display_currency": "USD",
        "analysis_window_mode": str(st.session_state.get("live_window_mode", "full")),
        "analysis_window_days": int(st.session_state.get("live_window_days", 30)),
        "analysis_window_bars": int(st.session_state.get("live_window_bars", 120)),
    }


def _build_backtest_cfg_from_state() -> Dict[str, Any]:
    return {
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


def _build_signal_controls_from_state(
    default_profile: str,
    signal_order_type: str = "as_ai",
    post_buy_protection_mode: str = "none",
    ai_only_oco_values: bool = False,
) -> Dict[str, Any]:
    return {
        "profile": str(st.session_state.get("sig_exec_profile", default_profile) or default_profile),
        "quote_asset": str(st.session_state.get("sig_quote", "USDT") or "USDT"),
        "order_type_policy": str(signal_order_type or "as_ai"),
        "autosize_mode": str(st.session_state.get("sig_autosize", "none") or "none"),
        "fixed_quote_notional": float(st.session_state.get("sig_fixed_notional", 0.0) or 0.0),
        "pct_quote_balance": float(st.session_state.get("sig_pct_balance", 0.0) or 0.0),
        "apply_protection": bool(st.session_state.get("sig_add_protect", False)),
        "stop_loss_pct": float(st.session_state.get("sig_sl_pct", 0.0) or 0.0),
        "take_profit_pct": float(st.session_state.get("sig_tp_pct", 0.0) or 0.0),
        "min_notional_buffer_pct": float(st.session_state.get("sig_min_notional_buffer_pct", 0.0) or 0.0),
        "min_notional_buffer_abs": float(st.session_state.get("sig_min_notional_buffer_abs", 0.0) or 0.0),
        "post_buy_protection_mode": str(post_buy_protection_mode or "none"),
        "ai_only_oco_values": bool(ai_only_oco_values),
    }


def _worker_stage(stage: str, status: str, note: str = "", sec: float = 0.0) -> None:
    stg = str(stage or "").upper() or "UNKNOWN"
    sta = str(status or "").lower() or "done"
    if sta == "start":
        print(f"[STAGE] {stg} start", flush=True)
        return
    suffix = f" ({float(sec or 0.0):.2f}s)"
    if note:
        print(f"[STAGE] {stg} {sta}{suffix}: {note}", flush=True)
    else:
        print(f"[STAGE] {stg} {sta}{suffix}", flush=True)


def _analyze_open_positions_cached(
    profile: str,
    timeframes: List[str],
    display_currency: str = "USD",
    ttl_sec: float = 0.0,
    force_refresh: bool = False,
) -> Dict[str, Any]:
    tf_key = ",".join(str(x).strip().lower() for x in timeframes if str(x).strip())
    cache_key = f"{str(profile).strip().lower()}::{tf_key}::{str(display_currency).strip().upper()}"
    now = time.time()
    max_age = max(0.0, float(ttl_sec or 0.0))
    if not force_refresh and max_age > 0:
        with _OPEN_POS_REVIEW_LOCK:
            rec = _OPEN_POS_REVIEW_CACHE.get(cache_key)
        if isinstance(rec, dict):
            ts = float(rec.get("ts", 0.0) or 0.0)
            payload = rec.get("payload", {}) if isinstance(rec.get("payload"), dict) else {}
            age = now - ts if ts > 0 else 10e9
            if ts > 0 and age <= max_age and payload:
                out = dict(payload)
                out["cache_hit"] = True
                out["cache_age_sec"] = round(age, 3)
                return out
    out = bridge.analyze_open_positions_multi_tf(
        profile_name=profile,
        timeframes=timeframes,
        display_currency=display_currency,
    )
    out = out if isinstance(out, dict) else {"ok": False, "error": "Open position analysis returned invalid payload"}
    out["cache_hit"] = False
    out["cache_age_sec"] = 0.0
    if out.get("ok"):
        with _OPEN_POS_REVIEW_LOCK:
            _OPEN_POS_REVIEW_CACHE[cache_key] = {"ts": now, "payload": dict(out)}
    return out


def _apply_pending_payload(profile: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    recs = payload.get("staged_recs", []) if isinstance(payload.get("staged_recs"), list) else []
    source = str(payload.get("staged_source", "ai_interpretation") or "ai_interpretation")
    replace_sources = [str(x).strip().lower() for x in (payload.get("replace_sources", []) or []) if str(x).strip()]
    incoming_symbols = {str(x.get("symbol", "")).strip().upper() for x in recs if isinstance(x, dict) and str(x.get("symbol", "")).strip()}
    if incoming_symbols and replace_sources:
        keep: List[Dict[str, Any]] = []
        rows = st.session_state.pending_recs if isinstance(st.session_state.pending_recs, list) else []
        for rec in rows:
            if not isinstance(rec, dict):
                keep.append(rec)
                continue
            sym = str(rec.get("symbol", "")).strip().upper()
            src = str(rec.get("pending_source", "")).strip().lower()
            if sym in incoming_symbols and src in replace_sources:
                continue
            keep.append(rec)
        st.session_state.pending_recs = keep
    seq0 = int(st.session_state.pending_rec_seq or 0)
    staged = _append_pending(recs, source=source)
    submitted = 0
    failed = 0
    if bool(payload.get("auto_submit_requested", False)) and staged > 0:
        auto_retry = bool(payload.get("auto_retry_locked_sell", True))
        new_ids = set(range(seq0 + 1, seq0 + staged + 1))
        for rec in st.session_state.pending_recs:
            if not isinstance(rec, dict):
                continue
            if int(rec.get("id", -1)) not in new_ids:
                continue
            out_sub = _submit_with_recovery(profile, rec, enable_recovery=auto_retry)
            if _apply_submit_result_to_rec(rec, out_sub):
                submitted += 1
            else:
                failed += 1
    return {"staged": int(staged), "submitted": int(submitted), "failed": int(failed)}


def _apply_unified_payload(profile: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict) or not payload.get("ok"):
        return payload if isinstance(payload, dict) else {"ok": False, "error": "Unified payload missing"}
    protect_payload = payload.get("protect_payload", {}) if isinstance(payload.get("protect_payload"), dict) else {}
    protect_apply = _apply_pending_payload(profile, protect_payload) if protect_payload.get("ok") else {"staged": 0, "submitted": 0, "failed": 0}
    live_block = str(payload.get("live_block", "") or "")
    bt_summary = str(payload.get("bt_summary", "") or "")
    bt_trades = str(payload.get("bt_trades", "") or "")
    ai_response = str(payload.get("ai_response", "") or "")
    if live_block:
        live_name = str(payload.get("live_name", "unified-live") or "unified-live")
        st.session_state.live_runs.append({"name": live_name, "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "text": live_block})
        st.session_state.live_runs = st.session_state.live_runs[-20:]
    st.session_state.last_live_text = live_block
    st.session_state.last_backtest_summary = bt_summary
    st.session_state.last_backtest_trades = bt_trades
    st.session_state.pending_ai_source_text = "\n\n".join([live_block, bt_summary, bt_trades]).strip()
    st.session_state.ai_response = ai_response
    new_apply = _apply_pending_payload(
        profile,
        {
            "staged_recs": payload.get("pending_new_recs", []) if isinstance(payload.get("pending_new_recs"), list) else [],
            "staged_source": "unified_new_opportunity",
            "replace_sources": [],
            "auto_submit_requested": bool(payload.get("auto_submit_new_requested", False)),
            "auto_retry_locked_sell": bool(payload.get("auto_retry_locked_sell", True)),
        },
    )
    parsed = int(payload.get("new_parsed", 0) or 0)
    eligible = int(payload.get("new_eligible", 0) or 0)
    note = str(payload.get("note", "") or "")
    summary_text = (
        f"protect(staged={int(protect_apply.get('staged', 0) or 0)}, submitted={int(protect_apply.get('submitted', 0) or 0)}), "
        f"new(parsed={parsed}, eligible={eligible}, staged={int(new_apply.get('staged', 0) or 0)}, submitted={int(new_apply.get('submitted', 0) or 0)}, failed={int(new_apply.get('failed', 0) or 0)})"
    )
    if note:
        summary_text += f" | note: {note}"
    out = dict(payload)
    out.update(
        {
            "protect_staged": int(protect_apply.get("staged", 0) or 0),
            "protect_submitted": int(protect_apply.get("submitted", 0) or 0),
            "new_staged": int(new_apply.get("staged", 0) or 0),
            "new_submitted": int(new_apply.get("submitted", 0) or 0),
            "new_failed": int(new_apply.get("failed", 0) or 0),
            "summary_text": summary_text,
        }
    )
    return out


def _run_full_pipeline_sync(
    live_cfg: Dict[str, Any],
    bt_cfg: Dict[str, Any],
    dt_context: str,
    bridge_obj: Optional[EngineBridge] = None,
) -> Dict[str, Any]:
    br = bridge_obj or bridge
    stage_events: List[Dict[str, Any]] = []
    perf_rows: List[Dict[str, Any]] = []
    bundle_ttl = float(os.environ.get("CTMT_PIPELINE_BUNDLE_TTL_SEC", "300") or "300")
    bundle_key = _pipeline_bundle_cache_key(live_cfg, bt_cfg)
    live_block = ""
    bt_summary = ""
    bt_trades = ""

    cached_bundle = br.get_pipeline_bundle_cache(bundle_key, max_age_sec=bundle_ttl)
    if isinstance(cached_bundle, dict):
        live_block = str(cached_bundle.get("live_block", "") or "")
        bt_summary = str(cached_bundle.get("bt_summary", "") or "")
        bt_trades = str(cached_bundle.get("bt_trades", "") or "")
        cached_perf = cached_bundle.get("perf_rows", []) if isinstance(cached_bundle.get("perf_rows"), list) else []
        perf_rows.extend(cached_perf)
        stage_events.append({"stage": "LIVE", "status": "done", "sec": 0.0, "note": "bundle-cache-hit"})
        stage_events.append({"stage": "BACKTEST", "status": "done", "sec": 0.0, "note": "bundle-cache-hit"})
        print("[STAGE] LIVE done (0.00s): bundle-cache-hit", flush=True)
        print("[STAGE] BACKTEST done (0.00s): bundle-cache-hit", flush=True)
    else:
        t_live0 = time.time()
        print("[STAGE] LIVE start", flush=True)
        live_res = _run_live_cfg_bundle_sync(live_cfg, bridge_obj=br)
        live_sec = round(time.time() - t_live0, 3)
        if not live_res.get("ok"):
            stage_events.append({"stage": "LIVE", "status": "failed", "sec": live_sec, "note": str(live_res.get("error", "Live failed"))})
            print(f"[STAGE] LIVE failed ({live_sec:.2f}s): {live_res.get('error', 'Live failed')}", flush=True)
            return {"ok": False, "stage": "live", "error": str(live_res.get("error", "Live failed")), "stage_events": stage_events}
        stage_events.append({"stage": "LIVE", "status": "done", "sec": live_sec, "note": ""})
        print(f"[STAGE] LIVE done ({live_sec:.2f}s)", flush=True)
        live_block = str(live_res.get("combined_text", "") or "").strip()
        perf_rows.extend(live_res.get("perf_rows", []) if isinstance(live_res.get("perf_rows"), list) else [])

        t_bt0 = time.time()
        print("[STAGE] BACKTEST start", flush=True)
        bt_res = br.run_backtest(bt_cfg)
        bt_sec = round(time.time() - t_bt0, 3)
        if not bt_res.get("ok"):
            stage_events.append({"stage": "BACKTEST", "status": "failed", "sec": bt_sec, "note": str(bt_res.get("error", "Backtest failed"))})
            print(f"[STAGE] BACKTEST failed ({bt_sec:.2f}s): {bt_res.get('error', 'Backtest failed')}", flush=True)
            return {
                "ok": False,
                "stage": "backtest",
                "error": str(bt_res.get("error", "Backtest failed")),
                "live_block": live_block,
                "stage_events": stage_events,
                "perf_rows": perf_rows,
            }
        stage_events.append({"stage": "BACKTEST", "status": "done", "sec": bt_sec, "note": ""})
        print(f"[STAGE] BACKTEST done ({bt_sec:.2f}s)", flush=True)
        bt_summary = str(bt_res.get("summary_text", "") or "")
        bt_trades = str(bt_res.get("trades_text", bt_res.get("trade_text", "")) or "")
        bt_perf = bt_res.get("perf", {}) if isinstance(bt_res.get("perf"), dict) else {}
        perf_rows.append(
            {
                "stage": "BACKTEST",
                "panel": str(bt_cfg.get("timeframe", "1d")),
                "sec": round(float(bt_perf.get("total_sec", bt_perf.get("simulate_sec", bt_sec)) or bt_sec), 3),
                "result_cache_hit": bool(bt_perf.get("result_cache_hit", False)),
                "raw_cache_hit": bool(bt_perf.get("raw_cache_hit", False)),
                "indicator_cache_hit": bool(bt_perf.get("indicator_cache_hit", False)),
                "incremental_backtest_hit": bool(bt_perf.get("incremental_backtest_hit", False)),
                "assets": int(bt_perf.get("loaded_assets", 0) or 0),
            }
        )
        br.put_pipeline_bundle_cache(
            bundle_key,
            {
                "live_block": live_block,
                "bt_summary": bt_summary,
                "bt_trades": bt_trades,
                "perf_rows": perf_rows,
            },
        )

    src = "\n\n".join([live_block, bt_summary, bt_trades]).strip()
    t_ai0 = time.time()
    print("[STAGE] AI start", flush=True)
    ai_res = br.run_ai_analysis(src, dt_context)
    ai_sec = round(time.time() - t_ai0, 3)
    if not ai_res.get("ok"):
        stage_events.append({"stage": "AI", "status": "failed", "sec": ai_sec, "note": str(ai_res.get("error", "AI failed"))})
        print(f"[STAGE] AI failed ({ai_sec:.2f}s): {ai_res.get('error', 'AI failed')}", flush=True)
        return {
            "ok": False,
            "stage": "ai",
            "error": str(ai_res.get("error", "AI failed")),
            "live_block": live_block,
            "bt_summary": bt_summary,
            "bt_trades": bt_trades,
            "stage_events": stage_events,
            "perf_rows": perf_rows,
        }
    stage_events.append({"stage": "AI", "status": "done", "sec": ai_sec, "note": ""})
    print(f"[STAGE] AI done ({ai_sec:.2f}s)", flush=True)

    return {
        "ok": True,
        "live_block": live_block,
        "bt_summary": bt_summary,
        "bt_trades": bt_trades,
        "ai_response": str(ai_res.get("response", "") or ""),
        "stage_events": stage_events,
        "perf_rows": perf_rows,
    }


def _run_unified_cycle_sync(
    profile: str,
    live_profile_name: str = "",
    auto_send_protect: bool = False,
    auto_submit_new: bool = False,
    dt_context: str = "",
    signal_order_type: str = "as_ai",
    post_buy_protection_mode: str = "none",
    ai_only_oco_values: bool = False,
    live_cfg: Optional[Dict[str, Any]] = None,
    bt_cfg: Optional[Dict[str, Any]] = None,
    signal_controls: Optional[Dict[str, Any]] = None,
    default_tf: str = "4h",
    quote_pref: str = "USDT",
    auto_retry_locked_sell: bool = True,
    review_ttl_sec: float = 0.0,
    force_review_refresh: bool = False,
) -> Dict[str, Any]:
    # Preflight: refresh exchange state so protection/discovery runs on latest fills/orders.
    try:
        qpref = str(quote_pref or "USDT").strip().upper() or "USDT"
        syms = _portfolio_symbols_for_profile(profile=profile, quote_pref=qpref)
        if syms:
            bridge.reconcile_binance_fills(profile_name=profile, symbols=syms)
        bridge.list_open_binance_orders(profile_name=profile)
    except Exception as ex:
        print(f"[STAGE] PREFLIGHT warning (0.00s): {ex}", flush=True)

    # 1) Manage existing positions.
    protect_out = _protect_open_positions_live_bt_ai(
        profile=profile,
        auto_submit=bool(auto_send_protect),
        live_profile_name=live_profile_name,
        live_cfg=(dict(live_cfg) if isinstance(live_cfg, dict) else None),
        bt_cfg=(dict(bt_cfg) if isinstance(bt_cfg, dict) else None),
        default_tf=str(default_tf or "4h"),
        auto_retry_locked_sell=bool(auto_retry_locked_sell),
        review_ttl_sec=float(review_ttl_sec or 0.0),
        force_review_refresh=bool(force_review_refresh),
    )
    if not protect_out.get("ok"):
        return {"ok": False, "stage": "protect", "error": str(protect_out.get("error", "Protection failed"))}

    # 2) Discover new opportunities (Live -> BT -> AI -> Stage).
    live_cfg_use = dict(live_cfg) if isinstance(live_cfg, dict) else _build_live_cfg_from_state_or_profile(profile_name=live_profile_name)
    bt_cfg_use = dict(bt_cfg) if isinstance(bt_cfg, dict) else _build_backtest_cfg_from_state()
    pipe_out = _run_full_pipeline_sync(live_cfg=live_cfg_use, bt_cfg=bt_cfg_use, dt_context=(dt_context or datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    if not pipe_out.get("ok"):
        return {
            "ok": False,
            "stage": "pipeline",
            "error": str(pipe_out.get("error", "Pipeline failed")),
            "protect": protect_out,
        }

    live_block = str(pipe_out.get("live_block", "") or "")
    bt_summary = str(pipe_out.get("bt_summary", "") or "")
    bt_trades = str(pipe_out.get("bt_trades", "") or "")
    ai_response = str(pipe_out.get("ai_response", "") or "")

    # Stage recommendations as "new opportunities".
    default_tf_use = str(default_tf or "4h")
    recs_raw = _extract_trade_recs(ai_response, default_tf=default_tf_use)
    recs_raw_n = len(recs_raw) if isinstance(recs_raw, list) else 0
    signal_ctx = dict(signal_controls) if isinstance(signal_controls, dict) else {
        "profile": str(profile or ""),
        "quote_asset": str(quote_pref or "USDT"),
        "order_type_policy": str(signal_order_type or "as_ai"),
        "autosize_mode": "none",
        "fixed_quote_notional": 0.0,
        "pct_quote_balance": 0.0,
        "apply_protection": False,
        "stop_loss_pct": 0.0,
        "take_profit_pct": 0.0,
        "min_notional_buffer_pct": 0.0,
        "min_notional_buffer_abs": 0.0,
        "post_buy_protection_mode": str(post_buy_protection_mode or "none"),
        "ai_only_oco_values": bool(ai_only_oco_values),
    }
    recs = _apply_signal_controls(
        recs=(recs_raw if isinstance(recs_raw, list) else []),
        profile=str(signal_ctx.get("profile", profile) or profile),
        quote_asset=str(signal_ctx.get("quote_asset", "USDT") or "USDT"),
        order_type_policy=str(signal_ctx.get("order_type_policy", signal_order_type) or "as_ai"),
        autosize_mode=str(signal_ctx.get("autosize_mode", "none") or "none"),
        fixed_quote_notional=float(signal_ctx.get("fixed_quote_notional", 0.0) or 0.0),
        pct_quote_balance=float(signal_ctx.get("pct_quote_balance", 0.0) or 0.0),
        apply_protection=bool(signal_ctx.get("apply_protection", False)),
        stop_loss_pct=float(signal_ctx.get("stop_loss_pct", 0.0) or 0.0),
        take_profit_pct=float(signal_ctx.get("take_profit_pct", 0.0) or 0.0),
        min_notional_buffer_pct=float(signal_ctx.get("min_notional_buffer_pct", 0.0) or 0.0),
        min_notional_buffer_abs=float(signal_ctx.get("min_notional_buffer_abs", 0.0) or 0.0),
        post_buy_protection_mode=str(signal_ctx.get("post_buy_protection_mode", "none") or "none"),
        ai_only_oco_values=bool(signal_ctx.get("ai_only_oco_values", False)),
        log_fn=lambda m: print(f"[SIGNAL] {m}", flush=True),
    )
    recs_after_controls = len(recs) if isinstance(recs, list) else 0
    no_new_reason = ""
    if not recs:
        if recs_raw_n <= 0:
            no_new_reason = "AI returned no structured recommendations."
        else:
            no_new_reason = "Recommendations were filtered by controls/portfolio constraints."

    return {
        "ok": True,
        "mode": "unified_manage_and_discover",
        "protect_payload": protect_out,
        "live_name": str(live_cfg_use.get("name", "unified-live") or "unified-live"),
        "live_block": live_block,
        "bt_summary": bt_summary,
        "bt_trades": bt_trades,
        "ai_response": ai_response,
        "pending_new_recs": recs if isinstance(recs, list) else [],
        "auto_submit_new_requested": bool(auto_submit_new),
        "auto_retry_locked_sell": bool(auto_retry_locked_sell),
        "new_parsed": int(recs_raw_n),
        "new_eligible": int(recs_after_controls),
        "note": no_new_reason,
    }


def _init_state() -> None:
    defaults = {
        "task_logs": [],
        "task_history": [],
        "pending_recs": [],
        "pending_history": [],
        "pending_rec_seq": 0,
        "ai_source_text": "",
        "pending_ai_source_text": "",
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
        "research_history": [],
        "live_profile_name": "default",
        "pending_live_profile_apply": None,
        "bg_jobs": {},
        "bg_live_running_id": "",
        "bg_pipeline_running_id": "",
        "bg_unified_running_id": "",
        "unified_monitor_enabled": False,
        "unified_monitor_interval_min": 30,
        "unified_monitor_next_epoch": 0.0,
        "unified_monitor_last_epoch": 0.0,
        "unified_monitor_busy": False,
        "unified_monitor_started_epoch": 0.0,
        "unified_monitor_live_profile": "",
        "unified_monitor_auto_send_protect": False,
        "unified_monitor_auto_submit_new": False,
        "unified_monitor_signal_order_type": "as_ai",
        "unified_monitor_post_buy_mode": "none",
        "unified_monitor_ai_only_oco": False,
        "unified_monitor_24x7": True,
        "unified_monitor_last_summary": "",
        "unified_monitor_history": [],
        "unified_signal_order_type": "as_ai",
        "unified_post_buy_mode": "none",
        "pf_last_loaded_epoch": 0.0,
        "pf_last_review_epoch": 0.0,
        "global_prefetch_enabled": True,
        "global_prefetch_sec": 60,
        "global_prefetch_last_defer_reason": "",
        "global_prefetch_last_defer_epoch": 0.0,
        "global_prefetch_last_defer_log_epoch": 0.0,
        "open_pos_review_ttl_sec": int(max(30, float(os.environ.get("CTMT_OPEN_POS_REVIEW_TTL_SEC", "300") or "300"))),
        "live_panel_cache_ttl_sec": int(max(30, float(getattr(bridge, "_cache_ttl_live_sec", 180.0) or 180.0))),
        "sig_min_notional_buffer_pct": float(max(0.0, float(os.environ.get("CTMT_MIN_NOTIONAL_BUFFER_PCT", "0.0") or "0.0"))),
        "sig_min_notional_buffer_abs": float(max(0.0, float(os.environ.get("CTMT_MIN_NOTIONAL_BUFFER_ABS", "0.0") or "0.0"))),
        "auto_retry_locked_sell": True,
        "graph_auto_refresh": False,
        "task_monitor_auto_refresh": False,
        "bg_max_workers": int(EXECUTOR_WORKERS),
        "bg_prefetch_running_id": "",
        "last_pipeline_stage_events": [],
        "last_pipeline_perf_rows": [],
        "perf_history": [],
        "perf_hud_enabled": True,
        "session_cache": {},
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def _session_cache_get(cache_key: str, max_age_sec: float) -> Any:
    cache = st.session_state.get("session_cache", {})
    if not isinstance(cache, dict):
        return None
    rec = cache.get(str(cache_key))
    if not isinstance(rec, dict):
        return None
    ts = float(rec.get("ts", 0.0) or 0.0)
    if ts <= 0 or (time.time() - ts) > float(max_age_sec):
        return None
    return rec.get("value")


def _session_cache_put(cache_key: str, value: Any) -> None:
    cache = st.session_state.get("session_cache", {})
    if not isinstance(cache, dict):
        cache = {}
    cache[str(cache_key)] = {"ts": time.time(), "value": value}
    # keep cache bounded
    if len(cache) > 128:
        try:
            keys = sorted(cache.keys(), key=lambda k: float(cache[k].get("ts", 0.0) or 0.0))
            for k in keys[:-96]:
                cache.pop(k, None)
        except Exception:
            pass
    st.session_state.session_cache = cache


def _is_main_streamlit_thread() -> bool:
    try:
        return threading.current_thread() is threading.main_thread()
    except Exception:
        return False


def _portfolio_cache_key(profile: str) -> str:
    return f"portfolio_only::{str(profile or '').strip().lower()}"


def _get_portfolio_payload_cached(profile: str, ttl_sec: float = 0.0) -> Dict[str, Any]:
    prof = str(profile or "").strip()
    if not prof:
        return {"ok": False, "error": "No profile selected."}
    max_age = float(ttl_sec or os.environ.get("CTMT_PORTFOLIO_ONLY_TTL_SEC", "8") or "8")
    key = _portfolio_cache_key(prof)
    if _is_main_streamlit_thread():
        cached = _session_cache_get(key, max_age_sec=max(1.0, max_age))
        if isinstance(cached, dict):
            out = dict(cached)
            out["cache_hit"] = True
            return out
    out = bridge.fetch_binance_portfolio(profile_name=prof)
    out = out if isinstance(out, dict) else {"ok": False, "error": "Invalid portfolio payload"}
    out["cache_hit"] = False
    if _is_main_streamlit_thread() and out.get("ok"):
        _session_cache_put(key, dict(out))
    return out


def _record_perf_event(task: str, sec: float, ok: bool, meta: Optional[Dict[str, Any]] = None) -> None:
    hist = st.session_state.get("perf_history", [])
    if not isinstance(hist, list):
        hist = []
    row = {
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "task": str(task),
        "sec": round(float(sec or 0.0), 3),
        "ok": bool(ok),
    }
    if isinstance(meta, dict):
        row.update(meta)
    hist.append(row)
    st.session_state.perf_history = hist[-1000:]


def _log(msg: str) -> None:
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    st.session_state.task_logs.append(line)
    st.session_state.task_logs = st.session_state.task_logs[-600:]


def _set_bg_executor_workers(max_workers: int) -> None:
    global EXECUTOR, EXECUTOR_WORKERS
    new_n = max(1, min(16, int(max_workers or 1)))
    if new_n == int(EXECUTOR_WORKERS):
        return
    old = EXECUTOR
    EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=new_n)
    EXECUTOR_WORKERS = new_n
    try:
        old.shutdown(wait=False, cancel_futures=False)
    except Exception:
        pass
    _log(f"Background worker pool resized to {new_n}")


def _fetch_portfolio_bundle_payload(profile: str, include_review: bool = False) -> Dict[str, Any]:
    ttl = float(os.environ.get("CTMT_PORTFOLIO_BUNDLE_TTL_SEC", "6") or "6")
    stage_soft_timeout = float(os.environ.get("CTMT_PORTFOLIO_STAGE_SOFT_TIMEOUT_SEC", "20") or "20")
    key = f"pf_bundle::{str(profile)}::{int(bool(include_review))}"
    if _is_main_streamlit_thread():
        cached = _session_cache_get(key, max_age_sec=max(1.0, ttl))
        if isinstance(cached, dict):
            out = dict(cached)
            out["cache_hit"] = True
            return out
    t0 = time.time()
    stage_rows: List[Dict[str, Any]] = []
    soft_timeout_hit = False
    stage_t0 = time.time()
    out_pf = bridge.fetch_binance_portfolio(profile_name=profile)
    pf_sec = time.time() - stage_t0
    stage_rows.append({"stage": "portfolio", "sec": round(pf_sec, 3), "ok": bool(out_pf.get("ok"))})
    if pf_sec > stage_soft_timeout:
        soft_timeout_hit = True
    stage_t0 = time.time()
    out_oo = bridge.list_open_binance_orders(profile_name=profile)
    oo_sec = time.time() - stage_t0
    stage_rows.append({"stage": "open_orders", "sec": round(oo_sec, 3), "ok": bool(out_oo.get("ok"))})
    if oo_sec > stage_soft_timeout:
        soft_timeout_hit = True
    stage_t0 = time.time()
    out_led = bridge.get_trade_ledger()
    led_sec = time.time() - stage_t0
    stage_rows.append({"stage": "ledger", "sec": round(led_sec, 3), "ok": bool(out_led.get("ok"))})
    if led_sec > stage_soft_timeout:
        soft_timeout_hit = True
    out_rev: Dict[str, Any] = {"ok": True, "rows": []}
    if include_review:
        if soft_timeout_hit:
            out_rev = {"ok": False, "error": "Skipped open-position review: earlier bundle stages exceeded soft timeout."}
            stage_rows.append({"stage": "review", "sec": 0.0, "ok": False, "skipped": True, "reason": "soft_timeout"})
        else:
            stage_t0 = time.time()
            out_rev = _analyze_open_positions_cached(
                profile=profile,
                timeframes=["4h", "8h", "12h", "1d"],
                display_currency="USD",
                ttl_sec=float(os.environ.get("CTMT_OPEN_POS_REVIEW_TTL_SEC", "300") or "300"),
                force_refresh=False,
            )
            stage_rows.append({"stage": "review", "sec": round(time.time() - stage_t0, 3), "ok": bool(out_rev.get("ok"))})
    out = {
        "ok": bool(out_pf.get("ok") and out_oo.get("ok") and out_led.get("ok") and (out_rev.get("ok") if include_review else True)),
        "profile": str(profile),
        "include_review": bool(include_review),
        "portfolio": out_pf,
        "orders": out_oo,
        "ledger": out_led,
        "review": out_rev,
        "cache_hit": False,
        "soft_timeout_hit": bool(soft_timeout_hit),
        "soft_timeout_sec": float(stage_soft_timeout),
        "stage_rows": stage_rows,
        "sec": round(time.time() - t0, 3),
    }
    if _is_main_streamlit_thread() and bool(out_pf.get("ok")):
        _session_cache_put(_portfolio_cache_key(profile), dict(out_pf))
    try:
        if soft_timeout_hit:
            diag.log("streamlit", "portfolio_bundle.soft_timeout", level="WARNING", profile=str(profile), include_review=bool(include_review), stage_rows=stage_rows)
    except Exception:
        pass
    if _is_main_streamlit_thread():
        _session_cache_put(key, out)
    return out


def _apply_portfolio_bundle_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    out_pf = payload.get("portfolio", {}) if isinstance(payload.get("portfolio"), dict) else {}
    out_oo = payload.get("orders", {}) if isinstance(payload.get("orders"), dict) else {}
    out_led = payload.get("ledger", {}) if isinstance(payload.get("ledger"), dict) else {}
    out_rev = payload.get("review", {}) if isinstance(payload.get("review"), dict) else {"ok": True}
    include_review = bool(payload.get("include_review", False))
    profile = str(payload.get("profile", "") or "")
    if out_pf.get("ok"):
        st.session_state.last_portfolio_df = pd.DataFrame(out_pf.get("balances", []) or [])
        if profile:
            _session_cache_put(_portfolio_cache_key(profile), dict(out_pf))
    if out_oo.get("ok"):
        st.session_state.last_open_orders_df = pd.DataFrame(out_oo.get("orders", []) or [])
    if out_led.get("ok"):
        ledger = out_led.get("ledger", {}) if isinstance(out_led.get("ledger"), dict) else {}
        entries = ledger.get("entries", []) if isinstance(ledger.get("entries"), list) else []
        st.session_state.last_ledger_df = pd.DataFrame(entries)
    if include_review and out_rev.get("ok"):
        st.session_state.last_review_df = pd.DataFrame(out_rev.get("rows", []) or [])
        st.session_state.pf_last_review_epoch = time.time()
    st.session_state.pf_last_loaded_epoch = time.time()
    return {
        "ok": bool(payload.get("ok", False)),
        "portfolio_ok": bool(out_pf.get("ok")),
        "orders_ok": bool(out_oo.get("ok")),
        "ledger_ok": bool(out_led.get("ok")),
        "review_ok": bool(out_rev.get("ok")) if include_review else True,
        "portfolio_error": str(out_pf.get("error", "")) if not out_pf.get("ok") else "",
        "orders_error": str(out_oo.get("error", "")) if not out_oo.get("ok") else "",
        "ledger_error": str(out_led.get("error", "")) if not out_led.get("ok") else "",
        "review_error": str(out_rev.get("error", "")) if include_review and not out_rev.get("ok") else "",
    }


class _QueueWriter:
    def __init__(self, q: "queue.Queue[str]") -> None:
        self.q = q
        self._buf = ""

    def write(self, s: str) -> int:
        txt = str(s or "")
        if not txt:
            return 0
        # Treat carriage-return progress updates as line boundaries so logs stream
        # instead of appearing in one large burst at task completion.
        txt = txt.replace("\r", "\n")
        self._buf += txt
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            line = line.rstrip("\r")
            if line.strip():
                try:
                    self.q.put_nowait(line)
                except Exception:
                    pass
        # If producer prints without newline for a long time, emit a chunk so UI
        # still shows forward progress.
        if len(self._buf) >= 240:
            chunk = self._buf
            self._buf = ""
            if chunk.strip():
                try:
                    self.q.put_nowait(chunk.strip())
                except Exception:
                    pass
        return len(txt)

    def flush(self) -> None:
        line = self._buf.rstrip("\r")
        self._buf = ""
        if line.strip():
            try:
                self.q.put_nowait(line)
            except Exception:
                pass


def _drain_job_log_queue(job: Dict[str, Any]) -> int:
    q = job.get("_log_queue")
    if q is None:
        return 0
    n = 0
    logs = job.get("logs", [])
    if not isinstance(logs, list):
        logs = []
    while True:
        try:
            line = q.get_nowait()
        except Exception:
            break
        n += 1
        logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] {line}")
    if n:
        job["logs"] = logs[-1200:]
        job["last_log_ts"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return n


def _run_task(name: str, fn):
    t0 = time.time()
    _log(f"START {name}")
    try:
        diag.log("streamlit", "task.start", task=name)
    except Exception:
        pass
    try:
        cap = io.StringIO()
        with redirect_stdout(cap), redirect_stderr(cap):
            out = fn()
        raw = cap.getvalue().strip()
        if raw:
            for ln in raw.splitlines():
                if ln.strip():
                    _log(f"{name}: {ln}")
        dt = time.time() - t0
        st.session_state.task_history.append({"task": name, "sec": round(dt, 3), "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "ok": True})
        st.session_state.task_history = st.session_state.task_history[-300:]
        _record_perf_event(task=name, sec=dt, ok=True)
        _log(f"DONE {name} ({dt:.2f}s)")
        try:
            diag.log("streamlit", "task.end", task=name, ok=True, elapsed_sec=round(dt, 4))
        except Exception:
            pass
        return out
    except Exception as exc:
        try:
            raw = cap.getvalue().strip()  # type: ignore[name-defined]
            if raw:
                for ln in raw.splitlines():
                    if ln.strip():
                        _log(f"{name}: {ln}")
        except Exception:
            pass
        dt = time.time() - t0
        st.session_state.task_history.append({"task": name, "sec": round(dt, 3), "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "ok": False})
        st.session_state.task_history = st.session_state.task_history[-300:]
        _record_perf_event(task=name, sec=dt, ok=False, meta={"error_type": type(exc).__name__})
        _log(f"DONE {name} (error: {type(exc).__name__}: {exc})")
        try:
            diag.log("streamlit", "task.end", level="ERROR", task=name, ok=False, elapsed_sec=round(dt, 4), error=f"{type(exc).__name__}: {exc}")
        except Exception:
            pass
        raise


def _start_bg_job(name: str, fn, meta: Optional[Dict[str, Any]] = None) -> str:
    job_id = str(uuid.uuid4())
    log_q: "queue.Queue[str]" = queue.Queue()

    def _runner():
        writer = _QueueWriter(log_q)
        with redirect_stdout(writer), redirect_stderr(writer):
            return fn()

    fut = EXECUTOR.submit(_runner)
    jobs = st.session_state.bg_jobs if isinstance(st.session_state.bg_jobs, dict) else {}
    jobs[job_id] = {
        "id": job_id,
        "name": name,
        "status": "running",
        "started_ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "started_epoch": time.time(),
        "future": fut,
        "result": None,
        "error": "",
        "state": "RUNNING",
        "elapsed_sec": 0.0,
        "logs": [],
        "_log_queue": log_q,
        "meta": dict(meta) if isinstance(meta, dict) else {},
    }
    st.session_state.bg_jobs = jobs
    _log(f"START {name} [job={job_id[:8]}]")
    try:
        diag.log("streamlit", "bg_job.start", task=name, job_id=job_id)
    except Exception:
        pass
    return job_id


def _find_running_bg_job_by_singleflight_key(singleflight_key: str) -> str:
    key = str(singleflight_key or "").strip()
    if not key:
        return ""
    jobs = st.session_state.bg_jobs if isinstance(st.session_state.get("bg_jobs"), dict) else {}
    for jid, j in (jobs.items() if isinstance(jobs, dict) else []):
        if not isinstance(j, dict):
            continue
        if str(j.get("status", "")).lower() != "running":
            continue
        meta = j.get("meta", {}) if isinstance(j.get("meta"), dict) else {}
        if str(meta.get("singleflight_key", "")).strip() == key:
            return str(jid)
    return ""


def _start_portfolio_bundle_singleflight_job(task_name: str, profile: str, include_review: bool = False) -> Dict[str, Any]:
    prof = str(profile or "").strip()
    if not prof:
        return {"started": False, "job_id": "", "reason": "missing_profile"}
    sf_key = f"pf_bundle_job::{prof.lower()}::{int(bool(include_review))}"
    existing = _find_running_bg_job_by_singleflight_key(sf_key)
    if existing:
        try:
            diag.log(
                "streamlit",
                "bg_job.singleflight_join",
                task=str(task_name or "Portfolio Bundle"),
                job_id=str(existing),
                singleflight_key=sf_key,
                profile=prof,
                include_review=bool(include_review),
            )
        except Exception:
            pass
        return {"started": False, "job_id": str(existing), "singleflight_key": sf_key}
    jid = _start_bg_job(
        str(task_name or "Portfolio Bundle"),
        lambda p=prof, r=bool(include_review): _fetch_portfolio_bundle_payload(profile=p, include_review=r),
        meta={
            "singleflight_key": sf_key,
            "profile": prof,
            "include_review": bool(include_review),
        },
    )
    return {"started": True, "job_id": str(jid), "singleflight_key": sf_key}


def _poll_bg_jobs() -> Dict[str, bool]:
    jobs = st.session_state.bg_jobs if isinstance(st.session_state.bg_jobs, dict) else {}
    dirty = False
    completed = False
    for jid, j in list(jobs.items()):
        if not isinstance(j, dict):
            continue
        if str(j.get("status", "")) != "running":
            continue
        _drain_job_log_queue(j)
        t0 = float(j.get("started_epoch", 0.0) or 0.0)
        if t0 > 0:
            j["elapsed_sec"] = round(max(0.0, time.time() - t0), 2)
            j["state"] = "RUNNING"
        fut = j.get("future")
        if fut is None or not hasattr(fut, "done"):
            continue
        if not fut.done():
            jobs[jid] = j
            continue
        dirty = True
        completed = True
        try:
            res = fut.result()
            _drain_job_log_queue(j)
            j["status"] = "done"
            j["result"] = res
            j["state"] = "DONE"
            _log(f"DONE {j.get('name', 'job')} [job={jid[:8]}]")
            try:
                diag.log(
                    "streamlit",
                    "bg_job.end",
                    task=str(j.get("name", "job")),
                    job_id=str(jid),
                    ok=True,
                    elapsed_sec=float(j.get("elapsed_sec", 0.0) or 0.0),
                )
            except Exception:
                pass
            st.session_state.task_history.append(
                {
                    "task": str(j.get("name", "job")),
                    "sec": float(j.get("elapsed_sec", 0.0) or 0.0),
                    "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "ok": True,
                }
            )
            _record_perf_event(task=str(j.get("name", "job")), sec=float(j.get("elapsed_sec", 0.0) or 0.0), ok=True)
        except Exception as exc:
            _drain_job_log_queue(j)
            j["status"] = "failed"
            j["error"] = f"{type(exc).__name__}: {exc}"
            j["state"] = "FAILED"
            _log(f"DONE {j.get('name', 'job')} [job={jid[:8]}] error={j['error']}")
            try:
                diag.log(
                    "streamlit",
                    "bg_job.end",
                    level="ERROR",
                    task=str(j.get("name", "job")),
                    job_id=str(jid),
                    ok=False,
                    elapsed_sec=float(j.get("elapsed_sec", 0.0) or 0.0),
                    error=str(j.get("error", "")),
                )
            except Exception:
                pass
            st.session_state.task_history.append(
                {
                    "task": str(j.get("name", "job")),
                    "sec": float(j.get("elapsed_sec", 0.0) or 0.0),
                    "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "ok": False,
                }
            )
            _record_perf_event(
                task=str(j.get("name", "job")),
                sec=float(j.get("elapsed_sec", 0.0) or 0.0),
                ok=False,
                meta={"error": str(j.get("error", ""))},
            )
        j["_log_queue"] = None
        jobs[jid] = j
    if dirty:
        st.session_state.bg_jobs = jobs
    return {"dirty": bool(dirty), "completed": bool(completed)}


def _running_bg_job_count() -> int:
    jobs = st.session_state.bg_jobs if isinstance(st.session_state.get("bg_jobs"), dict) else {}
    n = 0
    for j in jobs.values():
        if isinstance(j, dict) and str(j.get("status", "")).lower() == "running":
            n += 1
    return n


def _running_heavy_bg_jobs() -> List[str]:
    jobs = st.session_state.bg_jobs if isinstance(st.session_state.get("bg_jobs"), dict) else {}
    names: List[str] = []
    for j in jobs.values() if isinstance(jobs, dict) else []:
        if not isinstance(j, dict):
            continue
        if str(j.get("status", "")).lower() != "running":
            continue
        name = str(j.get("name", "") or "")
        if any(hint.lower() in name.lower() for hint in _PREFETCH_HEAVY_JOB_HINTS):
            names.append(name)
    return names


def _prefetch_defer_reason() -> str:
    running = _running_heavy_bg_jobs()
    if running:
        uniq = sorted(set(running))
        label = ", ".join(uniq[:2]) + (" ..." if len(uniq) > 2 else "")
        return f"Deferred prefetch: heavy job active ({label})"
    return ""


def _get_bg_job(job_id: str) -> Dict[str, Any]:
    jid = str(job_id or "").strip()
    if not jid:
        return {}
    jobs = st.session_state.bg_jobs if isinstance(st.session_state.get("bg_jobs"), dict) else {}
    if not isinstance(jobs, dict):
        return {}
    j = jobs.get(jid, {})
    return j if isinstance(j, dict) else {}


def _consume_bg_job(state_key: str) -> Dict[str, Any]:
    jid = str(st.session_state.get(state_key, "") or "").strip()
    if not jid:
        return {"state": "idle", "job_id": "", "job": {}}
    j = _get_bg_job(jid)
    status = str(j.get("status", "")).lower() if isinstance(j, dict) else ""
    if status == "running":
        return {"state": "running", "job_id": jid, "job": j}
    if status in ("done", "failed"):
        st.session_state[state_key] = ""
        return {"state": status, "job_id": jid, "job": j}
    st.session_state[state_key] = ""
    return {"state": "idle", "job_id": jid, "job": j}


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


def _parse_stage_events_from_logs(lines: List[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for ln in lines or []:
        s = str(ln or "")
        if "[STAGE]" not in s:
            continue
        m = re.search(r"\[STAGE\]\s+([A-Z_]+)\s+(start|done|failed)(?:\s+\(([\d.]+)s\))?(?::\s*(.*))?$", s, flags=re.IGNORECASE)
        if not m:
            continue
        stage = str(m.group(1) or "").upper()
        status = str(m.group(2) or "").lower()
        sec_raw = m.group(3)
        note = str(m.group(4) or "").strip()
        sec = 0.0
        try:
            sec = float(sec_raw) if sec_raw else 0.0
        except Exception:
            sec = 0.0
        out.append({"stage": stage, "status": status, "sec": sec, "note": note})
    return out


def _latest_stage_label(lines: List[str]) -> str:
    ev = _parse_stage_events_from_logs(lines)
    if not ev:
        return ""
    last = ev[-1]
    return f"{str(last.get('stage', '')).upper()}:{str(last.get('status', '')).lower()}"


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

    # EngineBridge.submit_binance_order does not support quote_order_qty directly.
    # Convert quote notional to qty using latest price when qty is missing.
    if qty <= 0 and q_notional > 0:
        filt_out = bridge.get_binance_symbol_filters(symbol=sym, profile_name=profile)
        min_notional = float(filt_out.get("min_notional", 0.0) or 0.0) if filt_out.get("ok") else 0.0
        buffer_pct = float(rec.get("min_notional_buffer_pct", st.session_state.get("sig_min_notional_buffer_pct", 0.0)) or 0.0)
        buffer_abs = float(rec.get("min_notional_buffer_abs", st.session_state.get("sig_min_notional_buffer_abs", 0.0)) or 0.0)
        min_target = _target_notional_with_buffer(min_notional, buffer_pct=buffer_pct, buffer_abs=buffer_abs)
        if min_target > 0 and q_notional < min_target:
            q_notional = float(min_target)
            rec["quote_notional"] = q_notional
            rec["reason"] = (
                str(rec.get("reason", ""))
                + f" | auto-bumped quote notional to buffered minNotional target {min_target:.8f}"
            )
        if side == "BUY":
            q_asset = "USDT"
            for qa in ("USDT", "USDC", "FDUSD", "BUSD", "USD", "BTC", "ETH", "BNB"):
                if sym.endswith(qa):
                    q_asset = qa
                    break
            free_q = _get_quote_free_balance(profile, q_asset)
            if free_q <= 0:
                free_q = _get_quote_free_balance(profile, "USDT")
            if q_notional > free_q > 0:
                return {"ok": False, "error": f"Insufficient quote balance for {sym}: required {q_notional:.8f}, available {free_q:.8f}"}
        lp_out = bridge.get_binance_last_price(symbol=sym, profile_name=profile)
        if not lp_out.get("ok"):
            return {"ok": False, "error": f"Unable to derive quantity from quote notional for {sym}: {lp_out.get('error', 'price unavailable')}"}
        try:
            last_px = float(lp_out.get("price", 0.0) or 0.0)
        except Exception:
            last_px = 0.0
        if last_px <= 0:
            return {"ok": False, "error": f"Invalid last price for {sym} while deriving quantity from quote notional."}
        qty = float(q_notional) / float(last_px)
        rec["quantity"] = qty

    # Map streamlit recommendation fields to EngineBridge order args.
    price_arg: Optional[float] = None
    stop_arg: Optional[float] = None
    if typ == "LIMIT":
        price_arg = lp if lp > 0 else None
    elif typ == "STOP_LOSS_LIMIT":
        price_arg = lp if lp > 0 else (sl * 0.999 if sl > 0 else None)
        stop_arg = sl if sl > 0 else None
    elif typ == "TAKE_PROFIT_LIMIT":
        # tp is treated as trigger; price uses explicit limit if provided else slight offset.
        price_arg = lp if lp > 0 else (tp * 0.999 if tp > 0 else None)
        stop_arg = tp if tp > 0 else None

    return bridge.submit_binance_order(
        symbol=sym,
        side=side,
        order_type=typ,
        quantity=qty,
        price=price_arg,
        stop_price=stop_arg,
        profile_name=profile,
    )


def _try_attach_post_buy_protection(profile: str, rec: Dict[str, Any], submit_out: Dict[str, Any]) -> Dict[str, Any]:
    side = str(rec.get("side", "")).strip().upper()
    mode = str(rec.get("post_buy_protection_mode", "none") or "none").strip().lower()
    if side != "BUY" or mode != "oco":
        return {"attempted": False}
    sym = str(rec.get("symbol", "")).strip().upper()
    if not sym:
        return {"attempted": False}
    sl = float(rec.get("stop_loss_price", 0.0) or 0.0)
    tp = float(rec.get("take_profit_price", 0.0) or 0.0)
    if sl <= 0 or tp <= 0:
        return {"attempted": True, "ok": False, "error": "Missing stop/take-profit prices for post-buy OCO."}
    try:
        qty = float(submit_out.get("normalized_quantity", rec.get("quantity", 0.0)) or 0.0)
    except Exception:
        qty = 0.0
    # Use free balance as final cap (BUY may consume fees and leave slightly less than submitted qty).
    m = re.match(r"^([A-Z0-9]+?)(USDT|USDC|FDUSD|BUSD|USD|BTC|ETH|BNB)$", sym)
    base_asset = m.group(1) if m else str(rec.get("asset", "")).strip().upper()
    if base_asset:
        free_qty = _get_asset_free_balance(profile, base_asset)
        if free_qty > 0:
            qty = min(qty if qty > 0 else free_qty, free_qty)
    if qty <= 0:
        return {"attempted": True, "ok": False, "error": f"No available filled quantity to protect for {sym}."}
    oco = bridge.submit_binance_oco_sell(
        symbol=sym,
        quantity=qty,
        take_profit_price=tp,
        stop_price=sl,
        stop_limit_price=(sl * 0.999),
        profile_name=profile,
    )
    if oco.get("ok"):
        return {"attempted": True, "ok": True, "qty": qty, "symbol": sym}
    return {"attempted": True, "ok": False, "error": str(oco.get("error", "post-buy OCO failed"))}


def _is_insufficient_balance_error(err: Any) -> bool:
    s = str(err or "").lower()
    return ("insufficient balance" in s) or ("code\":-2010" in s) or ("code -2010" in s) or ("code: -2010" in s)


def _cancel_open_sell_orders_for_symbol(profile: str, symbol: str) -> Dict[str, Any]:
    sym = str(symbol or "").strip().upper()
    if not sym:
        return {"ok": False, "error": "Missing symbol"}
    out = bridge.list_open_binance_orders(profile_name=profile, symbol=sym)
    if not out.get("ok"):
        return {"ok": False, "error": out.get("error", "Could not list open orders")}
    orders = out.get("orders", []) if isinstance(out.get("orders"), list) else []
    canceled = 0
    failed: List[str] = []
    for o in orders:
        if not isinstance(o, dict):
            continue
        if str(o.get("symbol", "")).strip().upper() != sym:
            continue
        if str(o.get("side", "")).strip().upper() != "SELL":
            continue
        try:
            oid = int(o.get("orderId", 0) or 0)
        except Exception:
            oid = 0
        if oid <= 0:
            continue
        c = bridge.cancel_binance_order(symbol=sym, order_id=oid, profile_name=profile)
        if c.get("ok"):
            canceled += 1
        else:
            failed.append(str(c.get("error", f"cancel failed for {oid}")))
    return {"ok": True, "canceled": canceled, "failed": failed}


def _submit_with_recovery(profile: str, rec: Dict[str, Any], enable_recovery: bool = True) -> Dict[str, Any]:
    out = _submit_pending_rec(profile, rec)
    if out.get("ok"):
        prot = _try_attach_post_buy_protection(profile, rec, out)
        if prot.get("attempted"):
            out["post_buy_protection"] = prot
            if prot.get("ok"):
                out["recovery_note"] = str(out.get("recovery_note", "") or "") + f" | post-buy OCO attached ({prot.get('symbol', '')} qty={float(prot.get('qty', 0.0) or 0.0):.8f})"
            else:
                out["recovery_note"] = str(out.get("recovery_note", "") or "") + f" | post-buy OCO not attached: {prot.get('error', 'unknown')}"
        return out
    if not enable_recovery:
        return out
    typ = str(rec.get("order_type", "")).strip().upper()
    side = str(rec.get("side", "")).strip().upper()
    sym = str(rec.get("symbol", "")).strip().upper()
    if not (typ == "MARKET" and side == "SELL" and sym):
        return out
    if not _is_insufficient_balance_error(out.get("error", "")):
        return out

    cancel_out = _cancel_open_sell_orders_for_symbol(profile, sym)
    canceled = int(cancel_out.get("canceled", 0) or 0)
    if canceled <= 0:
        err = str(out.get("error", "submit failed"))
        extra = str(cancel_out.get("error", "")) if not cancel_out.get("ok") else "no cancelable SELL orders found"
        return {"ok": False, "error": f"{err} | auto-retry skipped: {extra}"}

    retry = _submit_pending_rec(profile, rec)
    if retry.get("ok"):
        retry["recovered"] = True
        retry["recovery_note"] = f"Recovered by cancelling {canceled} open SELL order(s) for {sym} and retrying."
    else:
        retry["error"] = f"{retry.get('error', 'retry failed')} | canceled {canceled} open SELL order(s) but retry still failed"
    return retry


def _extract_order_ids_from_submit(out_sub: Dict[str, Any]) -> List[str]:
    ids: List[str] = []
    data = out_sub.get("data", {}) if isinstance(out_sub.get("data"), dict) else {}
    try:
        oid = int(data.get("orderId", 0) or 0)
    except Exception:
        oid = 0
    if oid > 0:
        ids.append(str(oid))
    reps = data.get("orderReports", [])
    if isinstance(reps, list):
        for r in reps:
            if not isinstance(r, dict):
                continue
            try:
                roid = int(r.get("orderId", 0) or 0)
            except Exception:
                roid = 0
            if roid > 0:
                ids.append(str(roid))
    return sorted(set(ids))


def _apply_submit_result_to_rec(rec: Dict[str, Any], out_sub: Dict[str, Any]) -> bool:
    ok = bool(out_sub.get("ok"))
    rec["last_submit_ts"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    rec["last_submit_ok"] = ok
    if ok:
        rec["status"] = "SUBMITTED"
        rec["reason"] = str(rec.get("reason", "")) + f" | {out_sub.get('recovery_note', 'submitted')}"
        ids = _extract_order_ids_from_submit(out_sub)
        if ids:
            rec["exchange_order_ids"] = ",".join(ids)
        if str(rec.get("order_type", "")).strip().upper() == "OCO_BRACKET":
            rec["status"] = "OPEN"
    else:
        rec["status"] = "FAILED"
        rec["reason"] = str(rec.get("reason", "")) + f" | {out_sub.get('error', 'submit failed')}"
    return ok


def _sync_pending_statuses(profile: str) -> Dict[str, Any]:
    open_out = bridge.list_open_binance_orders(profile_name=profile) if profile else {"ok": False, "orders": []}
    open_orders = open_out.get("orders", []) if isinstance(open_out.get("orders"), list) else []
    open_id_set = set()
    for o in open_orders:
        if not isinstance(o, dict):
            continue
        try:
            oid = int(o.get("orderId", 0) or 0)
        except Exception:
            oid = 0
        if oid > 0:
            open_id_set.add(str(oid))

    led_df = st.session_state.last_ledger_df if isinstance(st.session_state.last_ledger_df, pd.DataFrame) else pd.DataFrame()
    if led_df.empty:
        led_out = bridge.get_trade_ledger()
        if led_out.get("ok"):
            ledger = led_out.get("ledger", {}) if isinstance(led_out.get("ledger"), dict) else {}
            entries = ledger.get("entries", []) if isinstance(ledger.get("entries"), list) else []
            led_df = pd.DataFrame(entries)
            st.session_state.last_ledger_df = led_df
    fills_by_sym_side = set()
    if isinstance(led_df, pd.DataFrame) and not led_df.empty:
        rows = led_df.to_dict(orient="records")
        confirmed = _confirmed_execution_rows(rows)
        for r in confirmed:
            sym = str(r.get("symbol", "")).strip().upper()
            side = str(r.get("action", "")).strip().upper()
            if sym and side in ("BUY", "SELL"):
                fills_by_sym_side.add((sym, side))

    updated = 0
    for rec in st.session_state.pending_recs:
        if not isinstance(rec, dict):
            continue
        cur = str(rec.get("status", "PENDING") or "PENDING").strip().upper()
        sym = str(rec.get("symbol", "")).strip().upper()
        side = str(rec.get("side", "")).strip().upper()
        ids_raw = str(rec.get("exchange_order_ids", "") or "").strip()
        ids = [x.strip() for x in ids_raw.split(",") if x.strip()]
        if not ids:
            if cur in ("FAILED", "BLOCKED", "FILLED", "CANCELED", "EXPIRED"):
                continue
            if cur not in ("PENDING",):
                rec["status"] = "PENDING"
                updated += 1
            continue
        if any(i in open_id_set for i in ids):
            if cur != "OPEN":
                rec["status"] = "OPEN"
                updated += 1
            continue
        if (sym, side) in fills_by_sym_side:
            if cur != "FILLED":
                rec["status"] = "FILLED"
                updated += 1
            continue
        if cur not in ("FAILED", "BLOCKED", "FILLED", "CANCELED", "EXPIRED"):
            rec["status"] = "SUBMITTED"
            updated += 1
    return {"ok": True, "updated": updated, "open_orders": len(open_orders)}


def _split_symbol_base_quote(symbol: str) -> Tuple[str, str]:
    sym = str(symbol or "").strip().upper()
    m = re.match(r"^([A-Z0-9]+?)(USDT|USDC|FDUSD|BUSD|USD|BTC|ETH|BNB)$", sym)
    if m:
        return str(m.group(1) or ""), str(m.group(2) or "")
    return sym, ""


def _retrofit_symbol_to_oco(
    profile: str,
    symbol: str,
    stop_loss_pct: float,
    take_profit_pct: float,
    replace_existing: bool = True,
    use_total_qty: bool = True,
    explicit_stop_price: float = 0.0,
    explicit_take_profit_price: float = 0.0,
    min_notional_buffer_pct: float = 0.0,
    min_notional_buffer_abs: float = 0.0,
) -> Dict[str, Any]:
    sym = str(symbol or "").strip().upper()
    if not profile:
        return {"ok": False, "error": "No Binance profile selected."}
    if not sym:
        return {"ok": False, "error": "No symbol selected."}
    base, _ = _split_symbol_base_quote(sym)
    if not base:
        return {"ok": False, "error": f"Could not parse base asset from symbol {sym}."}

    if replace_existing:
        c = _cancel_open_sell_orders_for_symbol(profile, sym)
        if not c.get("ok"):
            return {"ok": False, "error": f"Could not cancel existing protection for {sym}: {c.get('error', 'unknown')}"}

    qty = _get_asset_total_balance(profile, base) if use_total_qty else _get_asset_free_balance(profile, base)
    if qty <= 0:
        return {"ok": False, "error": f"No available quantity for {sym} to protect."}

    lp = bridge.get_binance_last_price(symbol=sym, profile_name=profile)
    if not lp.get("ok"):
        return {"ok": False, "error": f"Could not fetch last price for {sym}: {lp.get('error', 'unknown')}"}
    try:
        px = float(lp.get("price", 0.0) or 0.0)
    except Exception:
        px = 0.0
    if px <= 0:
        return {"ok": False, "error": f"Invalid last price for {sym}."}

    filt_out = bridge.get_binance_symbol_filters(symbol=sym, profile_name=profile)
    min_notional = float(filt_out.get("min_notional", 0.0) or 0.0) if filt_out.get("ok") else 0.0
    min_target = _target_notional_with_buffer(
        min_notional,
        buffer_pct=float(min_notional_buffer_pct or 0.0),
        buffer_abs=float(min_notional_buffer_abs or 0.0),
    )
    est_notional = float(qty) * float(px)
    if min_target > 0 and est_notional < min_target:
        return {
            "ok": False,
            "error": (
                f"Position value {est_notional:.8f} is below buffered minNotional target {min_target:.8f} "
                f"(exchange min {min_notional:.8f}). Reduce buffer or increase position size."
            ),
            "symbol": sym,
            "qty": float(qty),
            "last_price": float(px),
            "estimated_notional": float(est_notional),
            "min_notional_target": float(min_target),
        }

    ex_sl = float(explicit_stop_price or 0.0)
    ex_tp = float(explicit_take_profit_price or 0.0)
    if ex_sl > 0 and ex_tp > 0:
        sl = ex_sl
        tp = ex_tp
    else:
        sl_pct = max(0.1, float(stop_loss_pct or 0.0))
        tp_pct = max(0.1, float(take_profit_pct or 0.0))
        sl = px * (1.0 - sl_pct / 100.0)
        tp = px * (1.0 + tp_pct / 100.0)

    oco = bridge.submit_binance_oco_sell(
        symbol=sym,
        quantity=float(qty),
        take_profit_price=float(tp),
        stop_price=float(sl),
        stop_limit_price=float(sl * 0.999),
        profile_name=profile,
    )
    if not oco.get("ok"):
        return {"ok": False, "error": str(oco.get("error", "OCO submit failed")), "symbol": sym, "qty": qty, "stop": sl, "tp": tp}
    return {
        "ok": True,
        "symbol": sym,
        "qty": float(oco.get("normalized_quantity", qty) or qty),
        "last_price": px,
        "stop": float(oco.get("normalized_stop_price", sl) or sl),
        "tp": float(oco.get("normalized_take_profit_price", tp) or tp),
    }


def _derive_oco_prices_live_bt_ai(
    profile: str,
    symbol: str,
    live_profile_name: str = "",
    strict_ai_values: bool = False,
    fallback_stop_pct: float = 5.0,
    fallback_tp_pct: float = 10.0,
    live_cfg: Optional[Dict[str, Any]] = None,
    bt_cfg: Optional[Dict[str, Any]] = None,
    default_tf: str = "4h",
) -> Dict[str, Any]:
    sym = str(symbol or "").strip().upper()
    if not profile or not sym:
        return {"ok": False, "error": "Missing profile or symbol."}
    try:
        live_cfg_use = dict(live_cfg) if isinstance(live_cfg, dict) else _build_live_cfg_from_state_or_profile(profile_name=live_profile_name)
        live_res = _run_live_cfg_bundle_sync(live_cfg_use)
        live_text = str(live_res.get("combined_text", "") or "").strip() if live_res.get("ok") else ""
    except Exception as ex:
        live_text = ""
        print(f"Retrofit AI live context warning: {ex}", flush=True)

    bt_cfg_use = dict(bt_cfg) if isinstance(bt_cfg, dict) else _build_backtest_cfg_from_state()
    bt_res = bridge.run_backtest(bt_cfg_use)
    bt_text = ""
    if bt_res.get("ok"):
        bt_text = "\n\n".join([str(bt_res.get("summary_text", "") or ""), str(bt_res.get("trade_text", "") or "")]).strip()

    source = "\n\n".join(
        [
            f"TARGET_SYMBOL: {sym}",
            "Task: produce a protective SELL OCO plan for this open position only.",
            "Output must include stop_loss_price and take_profit_price in JSON trade fields.",
            str(live_text),
            str(bt_text),
        ]
    ).strip()
    ai = bridge.run_ai_analysis(source, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    if not ai.get("ok"):
        if strict_ai_values:
            return {"ok": False, "error": str(ai.get("error", "AI failed for OCO derivation"))}
        return {"ok": True, "fallback": True}
    recs = _extract_trade_recs(str(ai.get("response", "") or ""), default_tf=str(default_tf or "4h"))
    pick = None
    for r in recs:
        if not isinstance(r, dict):
            continue
        rs = str(r.get("symbol", "")).strip().upper()
        if rs != sym:
            continue
        sl = float(r.get("stop_loss_price", 0.0) or 0.0)
        tp = float(r.get("take_profit_price", 0.0) or 0.0)
        if sl > 0 and tp > 0:
            pick = {"stop": sl, "tp": tp}
            break
    if pick:
        return {"ok": True, "fallback": False, "stop": float(pick["stop"]), "tp": float(pick["tp"])}
    if strict_ai_values:
        return {"ok": False, "error": f"AI did not return SL/TP for {sym}."}
    return {"ok": True, "fallback": True}


def _compute_retrofit_preview(
    profile: str,
    symbol: str,
    stop_pct: float,
    tp_pct: float,
    use_pipeline: bool,
    live_profile_name: str,
    ai_only_values: bool,
    live_cfg: Optional[Dict[str, Any]] = None,
    bt_cfg: Optional[Dict[str, Any]] = None,
    default_tf: str = "4h",
) -> Dict[str, Any]:
    base_px = 0.0
    lp_prev = bridge.get_binance_last_price(symbol=str(symbol), profile_name=profile)
    if lp_prev.get("ok"):
        try:
            base_px = float(lp_prev.get("price", 0.0) or 0.0)
        except Exception:
            base_px = 0.0
    preview_src = "fallback"
    sl_prev = base_px * (1.0 - float(stop_pct) / 100.0) if base_px > 0 else 0.0
    tp_prev = base_px * (1.0 + float(tp_pct) / 100.0) if base_px > 0 else 0.0
    if bool(use_pipeline):
        dprev = _derive_oco_prices_live_bt_ai(
            profile=profile,
            symbol=str(symbol),
            live_profile_name=str(live_profile_name or ""),
            strict_ai_values=bool(ai_only_values),
            fallback_stop_pct=float(stop_pct),
            fallback_tp_pct=float(tp_pct),
            live_cfg=(dict(live_cfg) if isinstance(live_cfg, dict) else None),
            bt_cfg=(dict(bt_cfg) if isinstance(bt_cfg, dict) else None),
            default_tf=str(default_tf or "4h"),
        )
        if dprev.get("ok"):
            if not bool(dprev.get("fallback", False)):
                preview_src = "AI"
                sl_prev = float(dprev.get("stop", 0.0) or 0.0)
                tp_prev = float(dprev.get("tp", 0.0) or 0.0)
        else:
            return {"ok": False, "error": str(dprev.get("error", "Preview derivation failed"))}
    rr = 0.0
    if base_px > 0 and sl_prev > 0 and tp_prev > 0 and (base_px - sl_prev) > 0:
        rr = (tp_prev - base_px) / (base_px - sl_prev)
    return {
        "ok": True,
        "base_px": float(base_px),
        "preview_src": str(preview_src),
        "sl_prev": float(sl_prev),
        "tp_prev": float(tp_prev),
        "rr": float(rr),
    }


def _run_retrofit_selected_oco_sync(
    profile: str,
    symbol: str,
    stop_pct: float,
    tp_pct: float,
    replace_existing: bool,
    use_total_qty: bool,
    use_pipeline: bool,
    live_profile_name: str,
    ai_only_values: bool,
    live_cfg: Optional[Dict[str, Any]] = None,
    bt_cfg: Optional[Dict[str, Any]] = None,
    default_tf: str = "4h",
    min_notional_buffer_pct: float = 0.0,
    min_notional_buffer_abs: float = 0.0,
) -> Dict[str, Any]:
    sym = str(symbol or "").strip().upper()
    if not profile:
        return {"ok": False, "error": "No Binance profile selected."}
    if not sym or sym == "(NONE)":
        return {"ok": False, "error": "Pick a symbol first."}

    ex_sl = 0.0
    ex_tp = 0.0
    pipeline_info = ""
    if bool(use_pipeline):
        d = _derive_oco_prices_live_bt_ai(
            profile=profile,
            symbol=sym,
            live_profile_name=str(live_profile_name or ""),
            strict_ai_values=bool(ai_only_values),
            fallback_stop_pct=float(stop_pct),
            fallback_tp_pct=float(tp_pct),
            live_cfg=(dict(live_cfg) if isinstance(live_cfg, dict) else None),
            bt_cfg=(dict(bt_cfg) if isinstance(bt_cfg, dict) else None),
            default_tf=str(default_tf or "4h"),
        )
        if not d.get("ok"):
            return {"ok": False, "error": str(d.get("error", "Could not derive OCO values from Live>BT>AI."))}
        if not bool(d.get("fallback", False)):
            ex_sl = float(d.get("stop", 0.0) or 0.0)
            ex_tp = float(d.get("tp", 0.0) or 0.0)
            pipeline_info = f"Using Live>BT>AI OCO values: SL={ex_sl:.8f}, TP={ex_tp:.8f}"
        else:
            pipeline_info = "Live>BT>AI did not return SL/TP for symbol; using % fallback."

    out = _retrofit_symbol_to_oco(
        profile=profile,
        symbol=sym,
        stop_loss_pct=float(stop_pct),
        take_profit_pct=float(tp_pct),
        replace_existing=bool(replace_existing),
        use_total_qty=bool(use_total_qty),
        explicit_stop_price=float(ex_sl),
        explicit_take_profit_price=float(ex_tp),
        min_notional_buffer_pct=float(min_notional_buffer_pct or 0.0),
        min_notional_buffer_abs=float(min_notional_buffer_abs or 0.0),
    )
    if pipeline_info:
        out["pipeline_info"] = pipeline_info
    return out


def _compact_pending_queue() -> Dict[str, Any]:
    rows = st.session_state.pending_recs if isinstance(st.session_state.pending_recs, list) else []
    history = st.session_state.pending_history if isinstance(st.session_state.pending_history, list) else []
    keep: List[Dict[str, Any]] = []
    removed = 0
    terminal = {"OPEN", "FILLED", "CANCELED", "EXPIRED"}
    for rec in rows:
        if not isinstance(rec, dict):
            continue
        status = str(rec.get("status", "PENDING") or "PENDING").strip().upper()
        if status in terminal:
            rr = dict(rec)
            rr["archived_ts"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            history.append(rr)
            removed += 1
        else:
            keep.append(rec)
    st.session_state.pending_recs = keep
    st.session_state.pending_history = history[-1000:]
    selected = st.session_state.selected_pending_ids if isinstance(st.session_state.selected_pending_ids, list) else []
    valid_ids = {int(r.get("id", -1)) for r in keep if isinstance(r, dict)}
    st.session_state.selected_pending_ids = [int(x) for x in selected if str(x).strip().isdigit() and int(x) in valid_ids]
    return {"ok": True, "removed": removed, "remaining": len(keep)}


def _load_binance_profiles() -> List[str]:
    out = bridge.list_binance_profiles()
    if not out.get("ok"):
        return []
    profiles = out.get("profiles", []) or []
    names = [str(p.get("name", "")).strip() for p in profiles if isinstance(p, dict) and str(p.get("name", "")).strip()]
    return sorted(names)


def _active_or_first_binance_profile() -> str:
    out = bridge.list_binance_profiles()
    if not out.get("ok"):
        return ""
    active = str(out.get("active_profile", "") or "").strip()
    profiles = out.get("profiles", []) if isinstance(out.get("profiles"), list) else []
    names = [str(p.get("name", "")).strip() for p in profiles if isinstance(p, dict) and str(p.get("name", "")).strip()]
    if active:
        return active
    return names[0] if names else ""


def _load_ai_profiles() -> Tuple[List[str], str]:
    out = bridge.list_ai_profiles()
    if not out.get("ok"):
        return [], ""
    profiles = out.get("profiles", []) or []
    names = [str(p.get("name", "")).strip() for p in profiles if isinstance(p, dict) and str(p.get("name", "")).strip()]
    return sorted(names), str(out.get("active_profile", "") or "")


def _portfolio_symbols_for_profile(profile: str, quote_pref: str = "USDT") -> List[str]:
    out = _get_portfolio_payload_cached(profile=profile)
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


def _reconcile_fills_for_profile_sync(profile: str, quote_pref: str = "USDT") -> Dict[str, Any]:
    if not str(profile or "").strip():
        return {"ok": False, "error": "No profile selected."}
    qpref = str(quote_pref or "USDT").strip().upper() or "USDT"
    syms_set = {str(s).strip().upper() for s in _portfolio_symbols_for_profile(profile=profile, quote_pref=qpref) if str(s).strip()}

    # Also reconcile symbols that are still open in local ledger even when balance is now 0.
    try:
        led_out = bridge.get_trade_ledger()
        if led_out.get("ok"):
            ledger = led_out.get("ledger", {}) if isinstance(led_out.get("ledger"), dict) else {}
            open_pos = ledger.get("open_positions", {}) if isinstance(ledger.get("open_positions"), dict) else {}
            for v in open_pos.values():
                if not isinstance(v, dict):
                    continue
                sym = str(v.get("symbol", "")).strip().upper()
                if sym:
                    syms_set.add(sym)
    except Exception:
        pass

    # Fallback discovery from open-position analysis if still empty.
    if not syms_set:
        try:
            pos_out = _analyze_open_positions_cached(
                profile=profile,
                timeframes=["4h"],
                display_currency="USD",
                ttl_sec=float(os.environ.get("CTMT_OPEN_POS_REVIEW_TTL_SEC", "300") or "300"),
                force_refresh=False,
            )
            rows = pos_out.get("rows", []) if isinstance(pos_out.get("rows"), list) else []
            for r in rows:
                if not isinstance(r, dict):
                    continue
                sym = str(r.get("symbol", "")).strip().upper()
                if sym:
                    syms_set.add(sym)
        except Exception:
            pass
    syms = sorted(syms_set)
    if not syms:
        return {"ok": False, "error": "No symbols available for reconciliation."}
    return bridge.reconcile_binance_fills(profile_name=profile, symbols=syms)


def _safe_select_option(label: str, options: List[str], preferred: str = "") -> Optional[str]:
    if not options:
        st.selectbox(label, ["(none)"], index=0, disabled=True)
        return None
    idx = options.index(preferred) if preferred in options else 0
    return st.selectbox(label, options, index=idx)


def _dataframe_column_config(df: pd.DataFrame) -> Dict[str, Any]:
    cfg: Dict[str, Any] = {}
    for c in list(df.columns):
        cl = str(c).strip().lower()
        if any(k in cl for k in ["qty", "quantity", "free", "locked", "total"]):
            cfg[str(c)] = st.column_config.NumberColumn(format="%.8f")
        elif any(k in cl for k in ["price", "stop", "tp", "entry", "current"]):
            cfg[str(c)] = st.column_config.NumberColumn(format="%.10f")
        elif any(k in cl for k in ["pnl", "return", "drawdown", "sharpe", "sortino"]):
            cfg[str(c)] = st.column_config.NumberColumn(format="%.6f")
    return cfg


def _show_df(
    df: pd.DataFrame,
    *,
    use_container_width: bool = True,
    hide_index: bool = True,
    height: Optional[int] = None,
    width: Optional[str] = None,
) -> None:
    if not width:
        width = "stretch" if bool(use_container_width) else "content"
    kwargs: Dict[str, Any] = {
        "width": width,
        "hide_index": hide_index,
    }
    if height is not None:
        kwargs["height"] = int(height)
    if not isinstance(df, pd.DataFrame):
        st.dataframe(df, **kwargs)
        return
    kwargs["column_config"] = _dataframe_column_config(df)
    st.dataframe(df, **kwargs)


def _get_quote_free_balance(profile: str, quote_asset: str) -> float:
    out = _get_portfolio_payload_cached(profile=profile)
    if not out.get("ok"):
        return 0.0
    quote = str(quote_asset or "USDT").strip().upper()
    for b in (out.get("balances", []) or []):
        if not isinstance(b, dict):
            continue
        if str(b.get("asset", "")).strip().upper() != quote:
            continue
        try:
            return float(b.get("free", 0.0) or 0.0)
        except Exception:
            return 0.0
    return 0.0


def _get_asset_total_balance(profile: str, asset: str) -> float:
    out = _get_portfolio_payload_cached(profile=profile)
    if not out.get("ok"):
        return 0.0
    a = str(asset or "").strip().upper()
    for b in (out.get("balances", []) or []):
        if not isinstance(b, dict):
            continue
        if str(b.get("asset", "")).strip().upper() != a:
            continue
        try:
            return float(b.get("total", 0.0) or 0.0)
        except Exception:
            return 0.0
    return 0.0


def _get_asset_free_balance(profile: str, asset: str) -> float:
    out = _get_portfolio_payload_cached(profile=profile)
    if not out.get("ok"):
        return 0.0
    a = str(asset or "").strip().upper()
    for b in (out.get("balances", []) or []):
        if not isinstance(b, dict):
            continue
        if str(b.get("asset", "")).strip().upper() != a:
            continue
        try:
            return float(b.get("free", 0.0) or 0.0)
        except Exception:
            return 0.0
    return 0.0


def _target_notional_with_buffer(min_notional: float, buffer_pct: float = 0.0, buffer_abs: float = 0.0) -> float:
    base = max(0.0, float(min_notional or 0.0))
    pct = max(0.0, float(buffer_pct or 0.0))
    abs_buf = max(0.0, float(buffer_abs or 0.0))
    if base <= 0:
        return 0.0 + abs_buf
    return (base * (1.0 + pct / 100.0)) + abs_buf


def _apply_signal_controls(
    recs: List[Dict[str, Any]],
    profile: str,
    quote_asset: str,
    order_type_policy: str,
    autosize_mode: str,
    fixed_quote_notional: float,
    pct_quote_balance: float,
    apply_protection: bool,
    stop_loss_pct: float,
    take_profit_pct: float,
    min_notional_buffer_pct: float = 0.0,
    min_notional_buffer_abs: float = 0.0,
    post_buy_protection_mode: str = "none",
    ai_only_oco_values: bool = False,
    log_fn=None,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    emit = log_fn if callable(log_fn) else _log
    quote = str(quote_asset or "USDT").strip().upper() or "USDT"
    quote_free = _get_quote_free_balance(profile, quote) if profile else 0.0
    for r in recs:
        if not isinstance(r, dict):
            continue
        rr = dict(r)
        sym = str(rr.get("symbol", "")).strip().upper()
        side = str(rr.get("side", "BUY")).strip().upper()
        ai_sl = float(rr.get("stop_loss_price", 0.0) or 0.0)
        ai_tp = float(rr.get("take_profit_price", 0.0) or 0.0)
        if not sym:
            asset = str(rr.get("asset", "")).strip().upper()
            if asset:
                sym = f"{asset}{quote}"
                rr["symbol"] = sym
        asset = re.sub(r"(USDT|USDC|BUSD|FDUSD|USD|BTC|ETH|BNB)$", "", sym)
        rr["asset"] = asset
        rr["min_notional_buffer_pct"] = float(max(0.0, float(min_notional_buffer_pct or 0.0)))
        rr["min_notional_buffer_abs"] = float(max(0.0, float(min_notional_buffer_abs or 0.0)))

        if order_type_policy != "as_ai":
            if side == "BUY" and order_type_policy == "OCO_BRACKET":
                rr["order_type"] = "MARKET"
                rr["reason"] = str(rr.get("reason", "")) + " | OCO override ignored for BUY; using MARKET"
            else:
                rr["order_type"] = order_type_policy
        eff_order_type = str(rr.get("order_type", "") or "").strip().upper()
        force_protection = bool(
            apply_protection
            or eff_order_type == "OCO_BRACKET"
            or str(post_buy_protection_mode or "none").strip().lower() == "oco"
        )

        px = 0.0
        lp = bridge.get_binance_last_price(symbol=sym, profile_name=profile) if profile and sym else {"ok": False}
        if lp.get("ok"):
            try:
                px = float(lp.get("price", 0.0) or 0.0)
            except Exception:
                px = 0.0

        if side == "BUY":
            qn = 0.0
            if autosize_mode == "fixed_quote_notional":
                qn = max(0.0, float(fixed_quote_notional or 0.0))
            elif autosize_mode == "percent_quote_balance":
                qn = max(0.0, quote_free * max(0.0, float(pct_quote_balance or 0.0)) / 100.0)
            if qn > 0:
                if profile and sym:
                    f = bridge.get_binance_symbol_filters(symbol=sym, profile_name=profile)
                    min_n = float(f.get("min_notional", 0.0) or 0.0) if f.get("ok") else 0.0
                    min_target = _target_notional_with_buffer(
                        min_n,
                        buffer_pct=float(min_notional_buffer_pct or 0.0),
                        buffer_abs=float(min_notional_buffer_abs or 0.0),
                    )
                    if min_target > 0 and qn < min_target:
                        if quote_free >= min_target:
                            qn = float(min_target)
                            rr["reason"] = (
                                str(rr.get("reason", ""))
                                + f" | bumped BUY notional to buffered minNotional target {min_target:.8f}"
                            )
                        else:
                            emit(
                                f"Dropped BUY {sym}: quote balance {quote_free:.8f} below buffered "
                                f"minNotional target {min_target:.8f} (exchange min {min_n:.8f})."
                            )
                            continue
                rr["quote_notional"] = round(qn, 8)
                rr["quantity"] = 0.0
                rr["reason"] = str(rr.get("reason", "")) + f" | autosized BUY using {rr['quote_notional']} {quote}"
            if force_protection and px > 0:
                if not bool(ai_only_oco_values):
                    if float(rr.get("stop_loss_price", 0.0) or 0.0) <= 0:
                        rr["stop_loss_price"] = px * (1.0 - max(0.0, float(stop_loss_pct or 0.0)) / 100.0)
                    if float(rr.get("take_profit_price", 0.0) or 0.0) <= 0:
                        rr["take_profit_price"] = px * (1.0 + max(0.0, float(take_profit_pct or 0.0)) / 100.0)
            rr["post_buy_protection_mode"] = str(post_buy_protection_mode or "none").strip().lower()
            if str(rr.get("post_buy_protection_mode", "none")).lower() == "oco":
                if bool(ai_only_oco_values) and not (ai_sl > 0 and ai_tp > 0):
                    emit(f"Dropped BUY {sym}: AI-only OCO mode requires AI-provided SL+TP.")
                    continue
                sl_b = float(rr.get("stop_loss_price", 0.0) or 0.0)
                tp_b = float(rr.get("take_profit_price", 0.0) or 0.0)
                if sl_b <= 0 or tp_b <= 0:
                    emit(f"Dropped BUY {sym}: post-BUY OCO selected but SL/TP unavailable.")
                    continue
        elif side == "SELL":
            held_total = _get_asset_total_balance(profile, asset) if profile else 0.0
            if profile and held_total <= 0:
                emit(f"Dropped SELL {sym}: no holdings in profile {profile}")
                continue
            qty = float(rr.get("quantity", 0.0) or 0.0)
            if qty <= 0 and profile:
                if autosize_mode == "percent_quote_balance":
                    qty = held_total * max(0.0, float(pct_quote_balance or 0.0)) / 100.0
                elif autosize_mode == "fixed_quote_notional" and px > 0:
                    qty = min(held_total, max(0.0, float(fixed_quote_notional or 0.0) / px))
                else:
                    qty = held_total
            if qty > 0:
                if profile and sym and px > 0:
                    f = bridge.get_binance_symbol_filters(symbol=sym, profile_name=profile)
                    min_n = float(f.get("min_notional", 0.0) or 0.0) if f.get("ok") else 0.0
                    min_target = _target_notional_with_buffer(
                        min_n,
                        buffer_pct=float(min_notional_buffer_pct or 0.0),
                        buffer_abs=float(min_notional_buffer_abs or 0.0),
                    )
                    if min_target > 0:
                        cur_notional = float(qty) * float(px)
                        if cur_notional < min_target:
                            held_notional = float(held_total) * float(px)
                            if held_notional >= min_target:
                                qty = min(float(held_total), (float(min_target) / float(px)) * 1.001)
                                rr["reason"] = (
                                    str(rr.get("reason", ""))
                                    + f" | bumped SELL qty to meet buffered minNotional target {min_target:.8f}"
                                )
                            else:
                                emit(
                                    f"Dropped SELL {sym}: holding value {held_notional:.8f} below buffered "
                                    f"minNotional target {min_target:.8f} (exchange min {min_n:.8f})."
                                )
                                continue
                rr["quantity"] = round(qty, 8)
            else:
                emit(f"Dropped SELL {sym}: computed quantity is zero")
                continue
            if force_protection and px > 0:
                if not bool(ai_only_oco_values):
                    if float(rr.get("stop_loss_price", 0.0) or 0.0) <= 0:
                        rr["stop_loss_price"] = px * (1.0 - max(0.0, float(stop_loss_pct or 0.0)) / 100.0)
                    if float(rr.get("take_profit_price", 0.0) or 0.0) <= 0:
                        rr["take_profit_price"] = px * (1.0 + max(0.0, float(take_profit_pct or 0.0)) / 100.0)

        if str(rr.get("order_type", "")).strip().upper() == "OCO_BRACKET":
            if bool(ai_only_oco_values) and not (ai_sl > 0 and ai_tp > 0):
                emit(f"Dropped {side} {sym}: AI-only OCO mode requires AI-provided SL+TP.")
                continue
            sl = float(rr.get("stop_loss_price", 0.0) or 0.0)
            tp = float(rr.get("take_profit_price", 0.0) or 0.0)
            if sl > 0 and tp > 0:
                rr["limit_price"] = float(rr.get("limit_price", 0.0) or 0.0) or (sl * 0.999)
                rr["take_profit_limit_price"] = float(rr.get("take_profit_limit_price", 0.0) or 0.0) or (tp * 0.999)
            else:
                emit(f"Dropped {side} {sym}: OCO requires both SL and TP.")
                continue
        out.append(rr)
    return out


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


def _execution_events_from_ledger_df(
    ledger_df: pd.DataFrame,
    use_confirmed_only: bool = False,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    base_cols = ["id", "ts", "symbol", "action", "qty", "price", "panel", "exchange_trade_id", "note"]
    if not isinstance(ledger_df, pd.DataFrame) or ledger_df.empty:
        return pd.DataFrame(columns=base_cols), pd.DataFrame(columns=base_cols + ["exclude_reason"])
    d = ledger_df.copy()
    if "is_execution" in d.columns:
        d = d[d["is_execution"] == True]  # noqa: E712
    d["action"] = d.get("action", "").astype(str).str.upper()
    d = d[d["action"].isin(["BUY", "SELL"])]
    d["qty"] = pd.to_numeric(d.get("qty", 0.0), errors="coerce").fillna(0.0)
    d["price"] = pd.to_numeric(d.get("price", 0.0), errors="coerce").fillna(0.0)
    d["ts"] = pd.to_datetime(d.get("ts"), errors="coerce", utc=True)
    d["panel"] = d.get("panel", "").astype(str).str.strip().str.lower()
    d["exchange_trade_id"] = d.get("exchange_trade_id", "").astype(str).str.strip()
    d["note"] = d.get("note", "").astype(str).str.strip()
    note_l = d["note"].str.lower()
    confirmed = (d["panel"] == "binance_reconcile") | (d["exchange_trade_id"] != "") | (note_l.str.contains("reconciled binance fill", na=False))

    sym = d.get("exchange_symbol", "").astype(str).str.strip().str.upper()
    asset = d.get("asset", "").astype(str).str.strip().str.upper()
    quote = d.get("quote_currency", "USDT").astype(str).str.strip().str.upper()
    fallback = asset + quote
    d["symbol"] = sym.where(sym != "", fallback)
    if "id" not in d.columns:
        d["id"] = range(1, len(d) + 1)

    d["exclude_reason"] = ""
    d.loc[d["qty"] <= 0, "exclude_reason"] = "qty<=0"
    d.loc[(d["exclude_reason"] == "") & (d["price"] <= 0), "exclude_reason"] = "price<=0"
    d.loc[(d["exclude_reason"] == "") & (d["ts"].isna()), "exclude_reason"] = "invalid_ts"
    d.loc[(d["exclude_reason"] == "") & (d["symbol"].astype(str).str.strip() == ""), "exclude_reason"] = "missing_symbol"
    if bool(use_confirmed_only):
        d.loc[(d["exclude_reason"] == "") & (~confirmed), "exclude_reason"] = "unconfirmed_execution"

    excluded = d[d["exclude_reason"] != ""].copy()
    ev = d[d["exclude_reason"] == ""].copy()
    ev = ev.sort_values(["ts", "id"]).reset_index(drop=True)
    excluded = excluded.sort_values(["ts", "id"], na_position="last").reset_index(drop=True)
    return ev[base_cols], excluded[base_cols + ["exclude_reason"]]


def _fifo_open_close_journal(
    ledger_df: pd.DataFrame,
    profile_name: str = "",
    use_confirmed_only: bool = False,
) -> Dict[str, Any]:
    ev, excluded_df = _execution_events_from_ledger_df(ledger_df, use_confirmed_only=bool(use_confirmed_only))
    if ev.empty:
        return {
            "closed": pd.DataFrame(),
            "open_lots": pd.DataFrame(),
            "unmatched_sells": pd.DataFrame(),
            "excluded": excluded_df,
            "summary": {
                "realized_quote": 0.0,
                "closed_rows": 0,
                "open_lots": 0,
                "unmatched_sell_rows": 0,
                "unmatched_sell_qty": 0.0,
                "excluded_rows": int(len(excluded_df)),
            },
        }

    lots: Dict[str, List[Dict[str, Any]]] = {}
    closed_rows: List[Dict[str, Any]] = []
    unmatched_sell_rows: List[Dict[str, Any]] = []

    for _, r in ev.iterrows():
        sym = str(r["symbol"])
        side = str(r["action"]).upper()
        qty = float(r["qty"])
        px = float(r["price"])
        ts = pd.Timestamp(r["ts"])
        eid = int(r["id"])
        book = lots.setdefault(sym, [])
        if side == "BUY":
            book.append({"entry_id": eid, "entry_ts": ts, "entry_price": px, "qty_remain": qty})
            continue

        rem = qty
        while rem > 1e-12 and book:
            lot = book[0]
            lq = float(lot.get("qty_remain", 0.0) or 0.0)
            if lq <= 1e-12:
                book.pop(0)
                continue
            take = min(rem, lq)
            entry_px = float(lot.get("entry_price", 0.0) or 0.0)
            pnl_q = (px - entry_px) * take
            pnl_pct = ((px / entry_px) - 1.0) * 100.0 if entry_px > 0 else 0.0
            hold_h = (ts - pd.Timestamp(lot.get("entry_ts"))).total_seconds() / 3600.0
            closed_rows.append(
                {
                    "symbol": sym,
                    "entry_id": int(lot.get("entry_id", 0) or 0),
                    "exit_id": eid,
                    "entry_ts": pd.Timestamp(lot.get("entry_ts")).strftime("%Y-%m-%d %H:%M:%S"),
                    "exit_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
                    "qty": take,
                    "entry_price": entry_px,
                    "exit_price": px,
                    "pnl_quote": pnl_q,
                    "pnl_pct": pnl_pct,
                    "hold_hours": hold_h,
                }
            )
            lot["qty_remain"] = lq - take
            rem -= take
            if float(lot.get("qty_remain", 0.0) or 0.0) <= 1e-12:
                book.pop(0)
        if rem > 1e-12:
            unmatched_sell_rows.append(
                {
                    "symbol": sym,
                    "exit_id": eid,
                    "exit_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
                    "sell_qty": qty,
                    "unmatched_qty": rem,
                    "exit_price": px,
                    "reason": "sell_without_prior_open_lot",
                }
            )
        lots[sym] = book

    closed_df = pd.DataFrame(closed_rows)
    if not closed_df.empty:
        closed_df = closed_df.sort_values(["exit_ts", "symbol", "entry_id"]).reset_index(drop=True)

    # Open lots with mark-to-market.
    price_cache: Dict[str, float] = {}
    open_rows: List[Dict[str, Any]] = []
    for sym, book in lots.items():
        if not isinstance(book, list):
            continue
        if sym not in price_cache:
            try:
                lp = bridge.get_binance_last_price(symbol=sym, profile_name=profile_name) if profile_name else {"ok": False}
                price_cache[sym] = float(lp.get("price", 0.0) or 0.0) if lp.get("ok") else 0.0
            except Exception:
                price_cache[sym] = 0.0
        cur = float(price_cache.get(sym, 0.0) or 0.0)
        for lot in book:
            q = float(lot.get("qty_remain", 0.0) or 0.0)
            if q <= 1e-12:
                continue
            ep = float(lot.get("entry_price", 0.0) or 0.0)
            upnl = (cur - ep) * q if (cur > 0 and ep > 0) else 0.0
            upnl_pct = ((cur / ep) - 1.0) * 100.0 if (cur > 0 and ep > 0) else 0.0
            open_rows.append(
                {
                    "symbol": sym,
                    "entry_id": int(lot.get("entry_id", 0) or 0),
                    "entry_ts": pd.Timestamp(lot.get("entry_ts")).strftime("%Y-%m-%d %H:%M:%S"),
                    "qty_open": q,
                    "entry_price": ep,
                    "current_price": cur,
                    "unrealized_quote": upnl,
                    "unrealized_pct": upnl_pct,
                }
            )
    open_df = pd.DataFrame(open_rows)
    if not open_df.empty:
        open_df = open_df.sort_values(["symbol", "entry_ts"]).reset_index(drop=True)

    unmatched_df = pd.DataFrame(unmatched_sell_rows)
    if not unmatched_df.empty:
        unmatched_df = unmatched_df.sort_values(["exit_ts", "symbol"]).reset_index(drop=True)

    realized_total = float(closed_df["pnl_quote"].sum()) if not closed_df.empty and "pnl_quote" in closed_df.columns else 0.0
    unmatched_qty_total = float(unmatched_df["unmatched_qty"].sum()) if not unmatched_df.empty and "unmatched_qty" in unmatched_df.columns else 0.0
    return {
        "closed": closed_df,
        "open_lots": open_df,
        "unmatched_sells": unmatched_df,
        "excluded": excluded_df,
        "summary": {
            "realized_quote": realized_total,
            "closed_rows": int(len(closed_df)),
            "open_lots": int(len(open_df)),
            "unmatched_sell_rows": int(len(unmatched_df)),
            "unmatched_sell_qty": float(unmatched_qty_total),
            "excluded_rows": int(len(excluded_df)),
        },
    }


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


def _position_graph_rows_compute(profile: str, quote_pref: str = "USDT") -> pd.DataFrame:
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

    pf = _get_portfolio_payload_cached(profile=profile)
    pf_ok = bool(pf.get("ok"))
    balances = pf.get("balances", []) if isinstance(pf.get("balances"), list) else []
    portfolio_qty_by_symbol: Dict[str, float] = {}
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
        portfolio_qty_by_symbol[sym] = total
        if sym not in merged:
            merged[sym] = {"symbol": sym, "tf": "spot", "qty": total, "buy_price": float(cb.get(sym, 0.0) or 0.0), "quote": quote_pref}

    rows: List[Dict[str, Any]] = []
    for sym, r in merged.items():
        qty = float(r.get("qty", 0.0) or 0.0)
        # Portfolio holdings are the source of truth for current open quantity.
        if sym in portfolio_qty_by_symbol:
            qty = float(portfolio_qty_by_symbol.get(sym, 0.0) or 0.0)
        else:
            if pf_ok:
                # When portfolio is available, treat it as source of truth.
                # Do not surface stale ledger/order-only symbols as open positions.
                continue
            # Fallback only when portfolio fetch failed: infer from open SELL order quantity.
            oq = 0.0
            for o in by_symbol.get(sym, []):
                if str(o.get("side", "")).strip().upper() != "SELL":
                    continue
                try:
                    oq = max(oq, float(o.get("origQty", 0.0) or 0.0))
                except Exception:
                    pass
            qty = max(qty, oq)
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


def _position_graph_rows(profile: str, quote_pref: str = "USDT") -> pd.DataFrame:
    ttl = float(os.environ.get("CTMT_GRAPH_ROWS_TTL_SEC", "6") or "6")
    cache_key = f"graph_rows::{str(profile)}::{str(quote_pref).upper()}"
    cached_df = _session_cache_get(cache_key, max_age_sec=max(1.0, ttl))
    if isinstance(cached_df, pd.DataFrame):
        st.session_state["_last_graph_rows_cache_hit"] = True
        return cached_df.copy()
    st.session_state["_last_graph_rows_cache_hit"] = False
    df = _position_graph_rows_compute(profile=profile, quote_pref=quote_pref)
    _session_cache_put(cache_key, df.copy())
    return df


def _fetch_portfolio_bundle_payload_uncached(profile: str, include_review: bool = False) -> Dict[str, Any]:
    t0 = time.time()
    stage_soft_timeout = float(os.environ.get("CTMT_PORTFOLIO_STAGE_SOFT_TIMEOUT_SEC", "20") or "20")
    stage_rows: List[Dict[str, Any]] = []
    soft_timeout_hit = False
    stage_t0 = time.time()
    out_pf = bridge.fetch_binance_portfolio(profile_name=profile)
    pf_sec = time.time() - stage_t0
    stage_rows.append({"stage": "portfolio", "sec": round(pf_sec, 3), "ok": bool(out_pf.get("ok"))})
    if pf_sec > stage_soft_timeout:
        soft_timeout_hit = True
    stage_t0 = time.time()
    out_oo = bridge.list_open_binance_orders(profile_name=profile)
    oo_sec = time.time() - stage_t0
    stage_rows.append({"stage": "open_orders", "sec": round(oo_sec, 3), "ok": bool(out_oo.get("ok"))})
    if oo_sec > stage_soft_timeout:
        soft_timeout_hit = True
    stage_t0 = time.time()
    out_led = bridge.get_trade_ledger()
    led_sec = time.time() - stage_t0
    stage_rows.append({"stage": "ledger", "sec": round(led_sec, 3), "ok": bool(out_led.get("ok"))})
    if led_sec > stage_soft_timeout:
        soft_timeout_hit = True
    out_rev: Dict[str, Any] = {"ok": True, "rows": []}
    if include_review:
        if soft_timeout_hit:
            out_rev = {"ok": False, "error": "Skipped open-position review: earlier bundle stages exceeded soft timeout."}
            stage_rows.append({"stage": "review", "sec": 0.0, "ok": False, "skipped": True, "reason": "soft_timeout"})
        else:
            stage_t0 = time.time()
            out_rev = _analyze_open_positions_cached(
                profile=profile,
                timeframes=["4h", "8h", "12h", "1d"],
                display_currency="USD",
                ttl_sec=float(os.environ.get("CTMT_OPEN_POS_REVIEW_TTL_SEC", "300") or "300"),
                force_refresh=False,
            )
            stage_rows.append({"stage": "review", "sec": round(time.time() - stage_t0, 3), "ok": bool(out_rev.get("ok"))})
    if _is_main_streamlit_thread() and bool(out_pf.get("ok")):
        _session_cache_put(_portfolio_cache_key(profile), dict(out_pf))
    return {
        "ok": bool(out_pf.get("ok") and out_oo.get("ok") and out_led.get("ok") and (out_rev.get("ok") if include_review else True)),
        "profile": str(profile),
        "include_review": bool(include_review),
        "portfolio": out_pf,
        "orders": out_oo,
        "ledger": out_led,
        "review": out_rev,
        "cache_hit": False,
        "soft_timeout_hit": bool(soft_timeout_hit),
        "soft_timeout_sec": float(stage_soft_timeout),
        "stage_rows": stage_rows,
        "sec": round(time.time() - t0, 3),
    }


def _build_graph_refresh_payload(profile: str, quote_pref: str = "USDT") -> Dict[str, Any]:
    t0 = time.time()
    bundle_payload = _fetch_portfolio_bundle_payload_uncached(profile=profile, include_review=False)
    graph_df = _position_graph_rows_compute(profile=profile, quote_pref=quote_pref)
    return {
        "ok": True,
        "profile": str(profile),
        "quote_pref": str(quote_pref or "USDT"),
        "bundle_payload": bundle_payload,
        "graph_df": graph_df,
        "sec": round(time.time() - t0, 3),
    }


def _render_position_graph_visuals(df: pd.DataFrame) -> None:
    if not isinstance(df, pd.DataFrame) or df.empty:
        st.info("No open positions found.")
        return

    dff = df.copy()
    for c in ["qty", "buy_price", "current", "stop", "tp", "pnl_pct", "pnl_quote"]:
        if c in dff.columns:
            dff[c] = pd.to_numeric(dff[c], errors="coerce").fillna(0.0)

    m1, m2, m3, m4 = st.columns(4)
    total_pos = int(len(dff))
    protected = int((dff["state"] == "PROTECTED").sum()) if "state" in dff.columns else 0
    partial = int((dff["state"] == "PARTIAL").sum()) if "state" in dff.columns else 0
    tot_pnl = float(dff["pnl_quote"].sum()) if "pnl_quote" in dff.columns else 0.0
    total_cost = 0.0
    total_curr = 0.0
    if all(c in dff.columns for c in ["qty", "buy_price", "current"]):
        dtmp = dff.copy()
        dtmp["qty"] = pd.to_numeric(dtmp["qty"], errors="coerce").fillna(0.0)
        dtmp["buy_price"] = pd.to_numeric(dtmp["buy_price"], errors="coerce").fillna(0.0)
        dtmp["current"] = pd.to_numeric(dtmp["current"], errors="coerce").fillna(0.0)
        dtmp = dtmp[(dtmp["qty"] > 0) & (dtmp["buy_price"] > 0)]
        total_cost = float((dtmp["qty"] * dtmp["buy_price"]).sum()) if not dtmp.empty else 0.0
        total_curr = float((dtmp["qty"] * dtmp["current"]).sum()) if not dtmp.empty else 0.0
    total_pnl_pct = ((total_curr - total_cost) / total_cost * 100.0) if total_cost > 0 else 0.0
    quote_label = "QUOTE"
    if "quote" in dff.columns:
        qset = sorted({str(x).strip().upper() for x in dff["quote"].dropna().tolist() if str(x).strip()})
        quote_label = qset[0] if len(qset) == 1 else "MIXED"
    m1.metric("Open positions", f"{total_pos}")
    m2.metric("Protected", f"{protected}")
    m3.metric("Partial", f"{partial}")
    m4.metric(f"Unrealized PnL ({quote_label})", f"{tot_pnl:,.6f}", f"{total_pnl_pct:+.2f}%")

    # PnL% bar chart
    if "pnl_pct" in dff.columns and "symbol" in dff.columns:
        pnl_df = dff.sort_values("pnl_pct", ascending=False)
        colors = ["#16a34a" if v >= 0 else "#dc2626" for v in pnl_df["pnl_pct"].tolist()]
        fig_pnl = go.Figure(
            data=[
                go.Bar(
                    x=pnl_df["symbol"],
                    y=pnl_df["pnl_pct"],
                    marker_color=colors,
                    name="PnL %",
                )
            ]
        )
        fig_pnl.update_layout(
            title="Unrealized PnL% by Position",
            xaxis_title="Symbol",
            yaxis_title="PnL %",
            height=320,
            margin=dict(l=20, r=20, t=50, b=20),
        )
        st.plotly_chart(fig_pnl, width="stretch")

    # Protection state donut
    if "state" in dff.columns:
        s = dff["state"].value_counts()
        fig_state = go.Figure(
            data=[
                go.Pie(
                    labels=list(s.index),
                    values=list(s.values),
                    hole=0.55,
                )
            ]
        )
        fig_state.update_layout(title="Protection Coverage", height=320, margin=dict(l=20, r=20, t=50, b=20))
        st.plotly_chart(fig_state, width="stretch")

    # Relative ladder chart (% move from baseline) avoids mixed-price-scale distortion.
    if all(c in dff.columns for c in ["symbol", "buy_price", "current", "stop", "tp"]):
        fig = go.Figure()
        dff2 = dff.sort_values("symbol").reset_index(drop=True)
        ycats = list(dff2["symbol"].astype(str))
        for _, r in dff2.iterrows():
            sym = str(r["symbol"])
            buy = float(r.get("buy_price", 0.0) or 0.0)
            cur = float(r.get("current", 0.0) or 0.0)
            stop = float(r.get("stop", 0.0) or 0.0)
            tp = float(r.get("tp", 0.0) or 0.0)

            # Baseline: entry when available, otherwise current.
            base = buy if buy > 0 else cur
            if base <= 0:
                continue

            def rel(v: float) -> float:
                return ((v / base) - 1.0) * 100.0 if v > 0 else float("nan")

            buy_r = 0.0 if buy > 0 else float("nan")
            cur_r = rel(cur)
            stop_r = rel(stop)
            tp_r = rel(tp)
            vals = [x for x in [buy_r, cur_r, stop_r, tp_r] if pd.notna(x)]
            if not vals:
                continue
            lo, hi = min(vals), max(vals)
            fig.add_trace(go.Scatter(x=[lo, hi], y=[sym, sym], mode="lines", line=dict(color="#64748b", width=2), name=f"{sym} range", showlegend=False))
            if pd.notna(buy_r):
                fig.add_trace(go.Scatter(x=[buy_r], y=[sym], mode="markers", marker=dict(symbol="triangle-up", size=11, color="#2563eb"), name="Entry"))
            if pd.notna(cur_r):
                fig.add_trace(go.Scatter(x=[cur_r], y=[sym], mode="markers", marker=dict(symbol="circle", size=11, color="#f59e0b"), name="Current"))
            if pd.notna(stop_r):
                fig.add_trace(go.Scatter(x=[stop_r], y=[sym], mode="markers", marker=dict(symbol="x", size=10, color="#dc2626"), name="Stop"))
            if pd.notna(tp_r):
                fig.add_trace(go.Scatter(x=[tp_r], y=[sym], mode="markers", marker=dict(symbol="star", size=11, color="#16a34a"), name="Take Profit"))

        seen = set()
        for tr in fig.data:
            n = getattr(tr, "name", "")
            if n in seen:
                tr.showlegend = False
            else:
                seen.add(n)

        fig.update_layout(
            title="Relative Position Ladder (% from Entry/Baseline)",
            xaxis_title="% vs Entry (or Current if no entry)",
            yaxis_title="Symbol",
            height=max(380, 80 + 40 * len(ycats)),
            margin=dict(l=20, r=20, t=50, b=20),
        )
        fig.add_vline(x=0.0, line_width=1, line_dash="dash", line_color="#94a3b8")
        fig.update_yaxes(categoryorder="array", categoryarray=ycats)
        st.plotly_chart(fig, width="stretch")


def _compute_pnl_breakdown(last_graph_df: pd.DataFrame, ledger_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    unreal = 0.0
    if isinstance(last_graph_df, pd.DataFrame) and not last_graph_df.empty and "pnl_quote" in last_graph_df.columns:
        unreal = float(pd.to_numeric(last_graph_df["pnl_quote"], errors="coerce").fillna(0.0).sum())

    realized_df = pd.DataFrame(columns=["ts", "pnl_quote"])
    if isinstance(ledger_df, pd.DataFrame) and not ledger_df.empty:
        d = ledger_df.copy()
        if "is_execution" in d.columns:
            d = d[d["is_execution"] == True]  # noqa: E712
        if "pnl_quote" in d.columns:
            d["pnl_quote"] = pd.to_numeric(d["pnl_quote"], errors="coerce")
            d = d[d["pnl_quote"].notna()]
        else:
            d = d.iloc[0:0]
        if "ts" in d.columns and not d.empty:
            d["ts"] = pd.to_datetime(d["ts"], errors="coerce", utc=True)
            d = d[d["ts"].notna()]
            realized_df = d[["ts", "pnl_quote"]].copy()

    now = pd.Timestamp.now(tz="UTC")
    windows = [("Daily", 1), ("Weekly", 7), ("Monthly", 30), ("Yearly", 365)]
    rows = []
    for name, days in windows:
        cutoff = now - pd.Timedelta(days=days)
        realized = float(realized_df.loc[realized_df["ts"] >= cutoff, "pnl_quote"].sum()) if not realized_df.empty else 0.0
        combined = realized + unreal
        rows.append(
            {
                "window": name,
                "realized_quote": realized,
                "unrealized_quote": unreal,
                "combined_quote": combined,
            }
        )
    summary = pd.DataFrame(rows)

    if realized_df.empty:
        empty = pd.DataFrame(columns=["period", "realized_quote"])
        return {"summary": summary, "daily": empty, "weekly": empty, "monthly": empty, "yearly": empty}

    r = realized_df.copy()
    r["day"] = r["ts"].dt.strftime("%Y-%m-%d")
    r["week"] = r["ts"].dt.to_period("W").astype(str)
    r["month"] = r["ts"].dt.to_period("M").astype(str)
    r["year"] = r["ts"].dt.year.astype(str)
    daily = r.groupby("day", as_index=False)["pnl_quote"].sum().rename(columns={"day": "period", "pnl_quote": "realized_quote"})
    weekly = r.groupby("week", as_index=False)["pnl_quote"].sum().rename(columns={"week": "period", "pnl_quote": "realized_quote"})
    monthly = r.groupby("month", as_index=False)["pnl_quote"].sum().rename(columns={"month": "period", "pnl_quote": "realized_quote"})
    yearly = r.groupby("year", as_index=False)["pnl_quote"].sum().rename(columns={"year": "period", "pnl_quote": "realized_quote"})
    return {"summary": summary, "daily": daily, "weekly": weekly, "monthly": monthly, "yearly": yearly}


def _protect_open_positions_simple(
    profile: str,
    auto_submit: bool = False,
    auto_retry_locked_sell: bool = True,
    review_ttl_sec: float = 0.0,
    force_review_refresh: bool = False,
) -> Dict[str, Any]:
    _worker_stage("REVIEW", "start")
    t0 = time.time()
    out = _analyze_open_positions_cached(
        profile=profile,
        timeframes=["4h", "8h", "12h", "1d"],
        display_currency="USD",
        ttl_sec=float(review_ttl_sec or 0.0),
        force_refresh=bool(force_review_refresh),
    )
    _worker_stage("REVIEW", "done", note=("fresh-cache-hit" if bool(out.get("cache_hit", False)) else ""), sec=(time.time() - t0))
    if not out.get("ok"):
        return {"ok": False, "error": str(out.get("error", "Open position analysis failed")), "stage": "review"}
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
    return {
        "ok": True,
        "mode": "ai_bt",
        "rows": len(rows),
        "staged_recs": staged,
        "staged_source": "protect_open_positions_ai_bt",
        "replace_sources": ["protect_open_positions_ai_bt"],
        "auto_submit_requested": bool(auto_submit),
        "auto_retry_locked_sell": bool(auto_retry_locked_sell),
        "review_cache_hit": bool(out.get("cache_hit", False)),
        "review_cache_age_sec": float(out.get("cache_age_sec", 0.0) or 0.0),
    }


def _protect_open_positions_live_bt_ai(
    profile: str,
    auto_submit: bool = False,
    live_profile_name: str = "",
    live_cfg: Optional[Dict[str, Any]] = None,
    bt_cfg: Optional[Dict[str, Any]] = None,
    default_tf: str = "4h",
    auto_retry_locked_sell: bool = True,
    review_ttl_sec: float = 0.0,
    force_review_refresh: bool = False,
) -> Dict[str, Any]:
    # Build open-position context first.
    _worker_stage("REVIEW", "start")
    t_review = time.time()
    review = _analyze_open_positions_cached(
        profile=profile,
        timeframes=["4h", "8h", "12h", "1d"],
        display_currency="USD",
        ttl_sec=float(review_ttl_sec or 0.0),
        force_refresh=bool(force_review_refresh),
    )
    _worker_stage("REVIEW", "done", note=("fresh-cache-hit" if bool(review.get("cache_hit", False)) else ""), sec=(time.time() - t_review))
    if not review.get("ok"):
        return {"ok": False, "error": str(review.get("error", "Open position review failed")), "stage": "review"}
    review_rows = review.get("rows", []) if isinstance(review.get("rows"), list) else []
    held_symbols = set(_portfolio_symbols_for_profile(profile=profile, quote_pref="USDT"))

    # Live context from current dashboard controls.
    live_cfg_use = dict(live_cfg) if isinstance(live_cfg, dict) else _build_live_cfg_from_state_or_profile(profile_name=live_profile_name)
    _worker_stage("LIVE", "start")
    t_live = time.time()
    live_res = _run_live_cfg_bundle_sync(live_cfg_use)
    _worker_stage("LIVE", ("done" if bool(live_res.get("ok")) else "failed"), note=str(live_res.get("error", "") or ""), sec=(time.time() - t_live))
    live_text = str(live_res.get("combined_text", "") or "").strip() if live_res.get("ok") else ""

    # Backtest context from current backtest controls.
    bt_cfg_use = dict(bt_cfg) if isinstance(bt_cfg, dict) else _build_backtest_cfg_from_state()
    _worker_stage("BACKTEST", "start")
    t_bt = time.time()
    bt_res = bridge.run_backtest(bt_cfg_use)
    _worker_stage("BACKTEST", ("done" if bool(bt_res.get("ok")) else "failed"), note=str(bt_res.get("error", "") or ""), sec=(time.time() - t_bt))
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
    _worker_stage("AI", "start")
    t_ai = time.time()
    ai_out = bridge.run_ai_analysis(source, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    _worker_stage("AI", ("done" if bool(ai_out.get("ok")) else "failed"), note=str(ai_out.get("error", "") or ""), sec=(time.time() - t_ai))
    if not ai_out.get("ok"):
        # Fallback to deterministic simple protection
        return _protect_open_positions_simple(
            profile=profile,
            auto_submit=auto_submit,
            auto_retry_locked_sell=auto_retry_locked_sell,
            review_ttl_sec=review_ttl_sec,
            force_review_refresh=force_review_refresh,
        )

    ai_resp = str(ai_out.get("response", "") or "")
    recs = _extract_trade_recs(ai_resp, default_tf=str(default_tf or "4h"))
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
        return _protect_open_positions_simple(
            profile=profile,
            auto_submit=auto_submit,
            auto_retry_locked_sell=auto_retry_locked_sell,
            review_ttl_sec=review_ttl_sec,
            force_review_refresh=force_review_refresh,
        )

    return {
        "ok": True,
        "mode": "live_bt_ai",
        "rows": len(review_rows),
        "staged_recs": filtered,
        "staged_source": "protect_open_positions_live_bt_ai",
        "replace_sources": ["protect_open_positions_ai_bt", "protect_open_positions_live_bt_ai"],
        "auto_submit_requested": bool(auto_submit),
        "auto_retry_locked_sell": bool(auto_retry_locked_sell),
        "review_cache_hit": bool(review.get("cache_hit", False)),
        "review_cache_age_sec": float(review.get("cache_age_sec", 0.0) or 0.0),
    }


def _refresh_portfolio_bundle(profile: str, include_review: bool = False) -> Dict[str, Any]:
    t0 = time.time()
    payload = _fetch_portfolio_bundle_payload(profile=profile, include_review=include_review)
    out = _apply_portfolio_bundle_payload(payload)
    _record_perf_event(
        task="Portfolio Bundle Refresh",
        sec=(time.time() - t0),
        ok=bool(out.get("ok", False)),
        meta={
            "cache_hit": bool(payload.get("cache_hit", False)),
            "include_review": bool(include_review),
            "profile": str(profile),
        },
    )
    return out


_init_state()
_poll_bg_jobs()

# Consume any completed background prefetch result.
try:
    _gp_jid = str(st.session_state.get("bg_prefetch_running_id", "") or "")
    if _gp_jid:
        _jobs = st.session_state.bg_jobs if isinstance(st.session_state.bg_jobs, dict) else {}
        _j = _jobs.get(_gp_jid, {}) if isinstance(_jobs, dict) else {}
        if isinstance(_j, dict) and str(_j.get("status", "")) in ("done", "failed"):
            if str(_j.get("status", "")) == "done":
                _res = _j.get("result", {}) if isinstance(_j.get("result"), dict) else {}
                _apply_portfolio_bundle_payload(_res)
            st.session_state.bg_prefetch_running_id = ""
except Exception:
    pass

# Lightweight heartbeat so global prefetch can run on cadence without manual interaction.
_hb_every = None
try:
    if bool(st.session_state.get("global_prefetch_enabled", True)):
        _hb_every = f"{max(15, int(st.session_state.get('global_prefetch_sec', 60) or 60))}s"
except Exception:
    _hb_every = None


@st.fragment(run_every=_hb_every)
def _global_prefetch_heartbeat() -> None:
    try:
        if not bool(st.session_state.get("global_prefetch_enabled", True)):
            return
        _defer_reason = _prefetch_defer_reason()
        if _defer_reason:
            now = time.time()
            st.session_state.global_prefetch_last_defer_reason = _defer_reason
            st.session_state.global_prefetch_last_defer_epoch = now
            last_log = float(st.session_state.get("global_prefetch_last_defer_log_epoch", 0.0) or 0.0)
            if now - last_log >= 20.0:
                st.session_state.global_prefetch_last_defer_log_epoch = now
                _log(_defer_reason)
                try:
                    diag.log("streamlit", "global_prefetch.deferred", reason=_defer_reason)
                except Exception:
                    pass
            return
        st.session_state.global_prefetch_last_defer_reason = ""
        _gp_sec = max(15, int(st.session_state.get("global_prefetch_sec", 60) or 60))
        _gp_last = float(st.session_state.get("pf_last_loaded_epoch", 0.0) or 0.0)
        if time.time() - _gp_last < float(_gp_sec):
            return
        _gp_running = str(st.session_state.get("bg_prefetch_running_id", "") or "")
        if _gp_running:
            _jobs = st.session_state.bg_jobs if isinstance(st.session_state.bg_jobs, dict) else {}
            _j = _jobs.get(_gp_running, {}) if isinstance(_jobs, dict) else {}
            if isinstance(_j, dict) and str(_j.get("status", "")) == "running":
                return
        _gp_profile = _active_or_first_binance_profile()
        if _gp_profile:
            _out = _start_portfolio_bundle_singleflight_job(
                task_name="Global Prefetch Bundle (async)",
                profile=_gp_profile,
                include_review=False,
            )
            _jid = str(_out.get("job_id", "") or "")
            if _jid:
                st.session_state.bg_prefetch_running_id = _jid
                if bool(_out.get("started", False)):
                    _log(f"Heartbeat prefetch started ({_gp_profile}) [job={_jid[:8]}]")
    except Exception as _hb_exc:
        _log(f"Heartbeat prefetch error: {_hb_exc}")


_global_prefetch_heartbeat()


@st.fragment(run_every="3s")
def _bg_jobs_heartbeat() -> None:
    """
    Keep async task progress moving without manual clicks.
    This avoids long jobs appearing "stuck" until user interaction.
    """
    try:
        running = _running_bg_job_count()
        if running <= 0:
            return
        polled = _poll_bg_jobs()
        # Keep this heartbeat invisible to avoid app-wide repaint/flicker.
        st.session_state["bg_jobs_running_count"] = int(running)
        # Only force a full app rerun when a job transitions to done/failed.
        # Avoiding continuous reruns prevents whole-page greying across tabs.
        if bool((polled or {}).get("completed", False)):
            now = time.time()
            last = float(st.session_state.get("_bg_jobs_last_rerun_epoch", 0.0) or 0.0)
            if now - last >= 1.0:
                st.session_state["_bg_jobs_last_rerun_epoch"] = now
                st.rerun()
    except Exception:
        return


_bg_jobs_heartbeat()


def _render_perf_hud() -> None:
    with st.sidebar:
        st.markdown("### Perf HUD")
        try:
            st.caption(f"Runtime diag: {'ON' if bool(diag.enabled) else 'OFF'} | {diag.path}")
        except Exception:
            pass
        enabled = st.checkbox(
            "Enable Perf HUD",
            value=bool(st.session_state.get("perf_hud_enabled", True)),
            key="perf_hud_enabled",
        )
        if not enabled:
            return
        hist = st.session_state.get("perf_history", [])
        if not isinstance(hist, list) or not hist:
            st.caption("No perf samples yet.")
            return
        df = pd.DataFrame(hist)
        if df.empty:
            st.caption("No perf samples yet.")
            return
        df["sec"] = pd.to_numeric(df.get("sec"), errors="coerce").fillna(0.0)
        if "ok" in df.columns:
            ok_rate = float(pd.to_numeric(df["ok"], errors="coerce").fillna(0.0).mean()) * 100.0
        else:
            ok_rate = 0.0
        last_n = df.tail(50)
        avg50 = float(last_n["sec"].mean()) if not last_n.empty else 0.0
        p95 = float(last_n["sec"].quantile(0.95)) if len(last_n) >= 3 else avg50
        running = _running_bg_job_count()
        c1, c2 = st.columns(2)
        c1.metric("Running jobs", f"{running}")
        c2.metric("Success rate", f"{ok_rate:.1f}%")
        c3, c4 = st.columns(2)
        c3.metric("Avg sec (last50)", f"{avg50:.2f}")
        c4.metric("P95 sec (last50)", f"{p95:.2f}")

        grp = (
            last_n.groupby("task", dropna=False)["sec"]
            .agg(["count", "mean", "max"])
            .reset_index()
            .sort_values("mean", ascending=False)
            .head(8)
        )
        grp = grp.rename(columns={"task": "Task", "count": "N", "mean": "Avg sec", "max": "Max sec"})
        st.caption("Top latency tasks (last 50 samples)")
        st.dataframe(grp, width="stretch", hide_index=True, height=220)

        lp = st.session_state.get("last_pipeline_perf_rows", [])
        if isinstance(lp, list) and lp:
            st.caption("Last pipeline perf breakdown")
            st.dataframe(pd.DataFrame(lp), width="stretch", hide_index=True, height=220)


_render_perf_hud()

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
        st.session_state["live_window_mode"] = str(pending_apply.get("window_mode", st.session_state.get("live_window_mode", "full")))
        st.session_state["live_window_days"] = int(pending_apply.get("window_days", st.session_state.get("live_window_days", 30)))
        st.session_state["live_window_bars"] = int(pending_apply.get("window_bars", st.session_state.get("live_window_bars", 120)))
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
    w1, w2, w3 = st.columns([2, 1, 1])
    live_window_mode = w1.selectbox(
        "Measurement window",
        ["full", "days", "bars"],
        index=["full", "days", "bars"].index(str(st.session_state.get("live_window_mode", "full")) if str(st.session_state.get("live_window_mode", "full")) in ["full", "days", "bars"] else "full"),
        key="live_window_mode",
        help="full = all available history, days = recent N days, bars = recent N candles per timeframe",
    )
    live_window_days = int(w2.number_input("Window days", min_value=1, max_value=365, value=int(st.session_state.get("live_window_days", 30)), step=1, key="live_window_days"))
    live_window_bars = int(w3.number_input("Window bars", min_value=10, max_value=5000, value=int(st.session_state.get("live_window_bars", 120)), step=10, key="live_window_bars"))
    if live_window_mode == "days":
        st.caption(f"Example: {live_window_days} days on 4h ~= {live_window_days * 6} bars per symbol.")

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
            "window_mode": str(live_window_mode),
            "window_days": int(live_window_days),
            "window_bars": int(live_window_bars),
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
    live_running_id = str(st.session_state.get("bg_live_running_id", "") or "")
    live_running = False
    if live_running_id:
        j = (st.session_state.bg_jobs or {}).get(live_running_id, {})
        live_running = isinstance(j, dict) and str(j.get("status", "")) == "running"

    if st.button("Run Live Dashboard", type="primary", disabled=live_running):
        run_tfs = [timeframe] + [x for x in tf_multi if x != timeframe]
        cfgs: List[Dict[str, Any]] = []
        for tf in run_tfs:
            cfgs.append(
                {
                    "name": f"{market}-{tf}",
                    "market": market,
                    "timeframe": tf,
                    "quote_currency": quote,
                    "top_n": int(top_n),
                    "country": country,
                    "display_currency": "USD",
                    "analysis_window_mode": str(live_window_mode),
                    "analysis_window_days": int(live_window_days),
                    "analysis_window_bars": int(live_window_bars),
                }
            )
        jid = _start_bg_job("Live Dashboard (async)", lambda ccfgs=cfgs: _run_live_panels_sync(ccfgs))
        st.session_state.bg_live_running_id = jid
        st.info("Live Dashboard started in background. You can switch tabs while it runs.")
        st.rerun()

    @st.fragment(run_every=("1s" if live_running else None))
    def _live_job_fragment() -> None:
        _poll_bg_jobs()
        _live_id = str(st.session_state.get("bg_live_running_id", "") or "")
        if not _live_id:
            return
        _j = (st.session_state.bg_jobs or {}).get(_live_id, {})
        if not isinstance(_j, dict):
            return
        _status = str(_j.get("status", ""))
        if _status == "running":
            st.info("Live Dashboard is running in background...")
            return
        if _status == "failed":
            st.error(f"Live job failed: {_j.get('error', 'unknown error')}")
            st.session_state.bg_live_running_id = ""
            st.rerun()
            return
        if _status == "done":
            _res = _j.get("result", {}) if isinstance(_j.get("result"), dict) else {}
            for _e in (_res.get("errors", []) or []):
                st.warning(str(_e))
            _runs = _res.get("runs", []) if isinstance(_res.get("runs"), list) else []
            if _runs:
                st.session_state.live_runs.extend(_runs)
                st.session_state.live_runs = st.session_state.live_runs[-20:]
                st.session_state.last_live_text = str(_res.get("combined_text", "") or "")
                st.success(f"Live run complete ({len(_runs)} panel(s)).")
            st.session_state.bg_live_running_id = ""
            st.rerun()

    _live_job_fragment()
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
    _bt_job = _consume_bg_job("bg_backtest_job_id")
    _bt_running = str(_bt_job.get("state", "")) == "running"
    if st.button("Run Backtest", type="primary", disabled=_bt_running):
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
        jid = _start_bg_job("Backtest (async)", lambda ccfg=cfg: bridge.run_backtest(ccfg))
        st.session_state.bg_backtest_job_id = str(jid)
        st.info("Backtest started in background.")
    if _bt_running:
        _elapsed = float((_bt_job.get("job", {}) or {}).get("elapsed_sec", 0.0) or 0.0)
        st.caption(f"Backtest running... {_elapsed:.1f}s")
    elif str(_bt_job.get("state", "")) == "done":
        _res = (_bt_job.get("job", {}) or {}).get("result", {})
        res = _res if isinstance(_res, dict) else {}
        if not res.get("ok"):
            st.error(res.get("error", "Backtest failed"))
        else:
            st.success("Backtest complete")
            st.session_state.last_backtest_summary = str(res.get("summary_text", "") or "")
            st.session_state.last_backtest_trades = str(res.get("trade_text", "") or "")
    elif str(_bt_job.get("state", "")) == "failed":
        _err = str((_bt_job.get("job", {}) or {}).get("error", "") or "Backtest failed")
        st.error(_err)
    if st.session_state.last_backtest_summary:
        st.text(st.session_state.last_backtest_summary)
    if st.session_state.last_backtest_trades:
        st.text(st.session_state.last_backtest_trades)

with tab_ai:
    st.subheader("AI Analysis")
    _pending_ai_src = str(st.session_state.get("pending_ai_source_text", "") or "").strip()
    if _pending_ai_src:
        st.session_state.ai_source_text = _pending_ai_src
        # This assignment is safe here because the widget is instantiated later in this block.
        st.session_state.ai_source_text_area = _pending_ai_src
        st.session_state.pending_ai_source_text = ""
    ai_names, ai_active = _load_ai_profiles()
    ai_cols = st.columns([2, 1, 1, 1])
    ai_profile = _safe_select_option("AI profile", ai_names, preferred=ai_active)
    sig_ctrl = st.expander("Signal Test Controls", expanded=True)
    with sig_ctrl:
        sc1, sc2, sc3, sc4 = st.columns(4)
        bn_names_sig = _load_binance_profiles()
        signal_profile = _safe_select_option("Execution profile", bn_names_sig, preferred=(bn_names_sig[0] if bn_names_sig else ""))
        st.session_state["sig_exec_profile"] = str(signal_profile or "")
        signal_quote = sc2.selectbox("Quote token", ["USDT", "USDC", "FDUSD", "BUSD", "USTC", "USD"], index=0, key="sig_quote")
        signal_order_type = sc3.selectbox(
            "Order type override",
            ["as_ai", "MARKET", "LIMIT", "STOP_LOSS_LIMIT", "TAKE_PROFIT_LIMIT", "OCO_BRACKET"],
            index=0,
            key="sig_order_type",
        )
        signal_autosize = sc4.selectbox(
            "Autosize",
            ["none", "fixed_quote_notional", "percent_quote_balance"],
            index=2,
            key="sig_autosize",
        )
        sc5, sc6, sc7, sc8 = st.columns(4)
        signal_fixed_notional = float(sc5.number_input("Fixed quote amount", min_value=0.0, value=10.0, step=1.0, key="sig_fixed_notional"))
        signal_pct_balance = float(sc6.number_input("% of quote balance", min_value=0.0, max_value=100.0, value=30.0, step=1.0, key="sig_pct_balance"))
        signal_protect = bool(sc7.checkbox("Add default SL/TP", value=True, key="sig_add_protect"))
        signal_auto_submit = bool(sc8.checkbox("Auto-submit after stage", value=False, key="sig_auto_submit"))
        sc9, sc10, sc11, sc12 = st.columns(4)
        signal_sl_pct = float(sc9.number_input("Default stop-loss %", min_value=0.1, max_value=80.0, value=5.0, step=0.5, key="sig_sl_pct"))
        signal_tp_pct = float(sc10.number_input("Default take-profit %", min_value=0.1, max_value=300.0, value=10.0, step=1.0, key="sig_tp_pct"))
        signal_post_buy_mode = str(
            sc11.selectbox(
                "Post-BUY protection",
                ["none", "oco"],
                index=0,
                key="sig_post_buy_mode",
                help="none = no automatic bracket after buy; oco = submit SELL OCO after buy fill using SL/TP values.",
            )
        )
        signal_ai_only_oco = bool(
            sc12.checkbox(
                "AI-only OCO values",
                value=False,
                key="sig_ai_only_oco",
                help="When ON, OCO/post-BUY-OCO requires AI-provided SL/TP. No fallback SL/TP auto-fill.",
            )
        )
        sc13, sc14, _, _ = st.columns(4)
        sc13.number_input(
            "MinNotional buffer %",
            min_value=0.0,
            max_value=100.0,
            value=float(st.session_state.get("sig_min_notional_buffer_pct", 0.0) or 0.0),
            step=0.5,
            key="sig_min_notional_buffer_pct",
            help="Adds a percentage safety margin above exchange minNotional for staged orders.",
        )
        sc14.number_input(
            "MinNotional buffer (abs)",
            min_value=0.0,
            value=float(st.session_state.get("sig_min_notional_buffer_abs", 0.0) or 0.0),
            step=0.1,
            key="sig_min_notional_buffer_abs",
            help="Adds an absolute quote-value safety margin above exchange minNotional.",
        )
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
            recs = _apply_signal_controls(
                recs=recs,
                profile=str(signal_profile or ""),
                quote_asset=str(signal_quote or "USDT"),
                order_type_policy=str(signal_order_type or "as_ai"),
                autosize_mode=str(signal_autosize or "none"),
                fixed_quote_notional=float(signal_fixed_notional or 0.0),
                pct_quote_balance=float(signal_pct_balance or 0.0),
                apply_protection=bool(signal_protect),
                stop_loss_pct=float(signal_sl_pct or 0.0),
                take_profit_pct=float(signal_tp_pct or 0.0),
                min_notional_buffer_pct=float(st.session_state.get("sig_min_notional_buffer_pct", 0.0) or 0.0),
                min_notional_buffer_abs=float(st.session_state.get("sig_min_notional_buffer_abs", 0.0) or 0.0),
                post_buy_protection_mode=signal_post_buy_mode,
                ai_only_oco_values=bool(signal_ai_only_oco),
            )
            seq0 = int(st.session_state.pending_rec_seq or 0)
            n = _append_pending(recs, source="ai_interpretation")
            _log(f"AI staged {n} recommendation(s).")
            st.success(f"Staged {n} recommendation(s).")
            if signal_auto_submit and signal_profile and n > 0:
                new_ids = set(range(seq0 + 1, seq0 + n + 1))
                ok_n, fail_n = 0, 0
                for rec in st.session_state.pending_recs:
                    if int(rec.get("id", -1)) not in new_ids:
                        continue
                    out_sub = _submit_with_recovery(str(signal_profile), rec, enable_recovery=bool(st.session_state.get("auto_retry_locked_sell", True)))
                    if _apply_submit_result_to_rec(rec, out_sub):
                        ok_n += 1
                    else:
                        fail_n += 1
                st.info(f"Auto-submit results: submitted={ok_n}, failed={fail_n}")
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
    pipe_cols = st.columns([2, 1, 1, 2])
    bn_names_for_pipe = _load_binance_profiles()
    pipe_profile = _safe_select_option("Binance profile for auto-submit", bn_names_for_pipe, preferred=(bn_names_for_pipe[0] if bn_names_for_pipe else ""))
    pipe_auto_submit = pipe_cols[1].checkbox("Auto-submit staged", value=False, key="ai_pipe_auto_submit")
    lp_all = _load_live_profiles()
    lp_names = sorted(lp_all.keys()) if isinstance(lp_all, dict) else []
    pipe_live_profile_pick = pipe_cols[3].selectbox(
        "Live profile source",
        ["(current live settings)"] + lp_names,
        index=0,
        key="ai_pipe_live_profile_pick",
    )
    pipeline_running_id = str(st.session_state.get("bg_pipeline_running_id", "") or "")
    pipeline_running = False
    if pipeline_running_id:
        pj = (st.session_state.bg_jobs or {}).get(pipeline_running_id, {})
        pipeline_running = isinstance(pj, dict) and str(pj.get("status", "")) == "running"
    run_pipeline = pipe_cols[2].button("Run Live > BT > AI > Stage", key="ai_pipe_run_btn", type="primary", disabled=pipeline_running)
    if run_pipeline:
        picked_lp = "" if pipe_live_profile_pick == "(current live settings)" else str(pipe_live_profile_pick)
        live_cfg = _build_live_cfg_from_state_or_profile(profile_name=picked_lp)
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
        jid = _start_bg_job(
            "Pipeline Live->BT->AI (async)",
            lambda lcfg=live_cfg, bcfg=bt_cfg, dctx=dt_context: _run_full_pipeline_sync(
                lcfg,
                bcfg,
                dctx,
                bridge_obj=EngineBridge(Path(REPO_ROOT)),
            ),
        )
        jobs = st.session_state.bg_jobs if isinstance(st.session_state.bg_jobs, dict) else {}
        j = jobs.get(jid, {})
        if isinstance(j, dict):
            j["meta"] = {
                "pipe_profile": str(pipe_profile or signal_profile or ""),
                "pipe_auto_submit": bool(pipe_auto_submit),
                "signal_quote": str(signal_quote or "USDT"),
                "signal_order_type": str(signal_order_type or "as_ai"),
                "signal_autosize": str(signal_autosize or "none"),
                "signal_fixed_notional": float(signal_fixed_notional or 0.0),
                "signal_pct_balance": float(signal_pct_balance or 0.0),
                "signal_protect": bool(signal_protect),
                "signal_sl_pct": float(signal_sl_pct or 0.0),
                "signal_tp_pct": float(signal_tp_pct or 0.0),
                "signal_min_notional_buffer_pct": float(st.session_state.get("sig_min_notional_buffer_pct", 0.0) or 0.0),
                "signal_min_notional_buffer_abs": float(st.session_state.get("sig_min_notional_buffer_abs", 0.0) or 0.0),
                "signal_post_buy_mode": str(signal_post_buy_mode or "none"),
                "signal_ai_only_oco": bool(signal_ai_only_oco),
                "default_tf": str(st.session_state.get("bt_tf", "4h")),
            }
            jobs[jid] = j
            st.session_state.bg_jobs = jobs
        st.session_state.bg_pipeline_running_id = jid
        st.info("Pipeline started in background. You can switch tabs and monitor in Task Monitor.")
        st.rerun()

    if pipeline_running:
        st.info("Pipeline is running in background...")
        try:
            _pj = (st.session_state.bg_jobs or {}).get(pipeline_running_id, {})
            _logs = _pj.get("logs", []) if isinstance(_pj, dict) else []
            _events = _parse_stage_events_from_logs(_logs if isinstance(_logs, list) else [])
            if _events:
                _df = pd.DataFrame(_events)
                st.caption("Pipeline stage progress (live)")
                _show_df(_df.tail(12), width="stretch", hide_index=True)
        except Exception:
            pass

    # consume completed pipeline
    pipeline_done_id = str(st.session_state.get("bg_pipeline_running_id", "") or "")
    if pipeline_done_id:
        pj = (st.session_state.bg_jobs or {}).get(pipeline_done_id, {})
        if isinstance(pj, dict) and str(pj.get("status", "")) in ("done", "failed"):
            if str(pj.get("status", "")) == "failed":
                st.error(f"Pipeline failed: {pj.get('error', 'unknown error')}")
            else:
                pres = pj.get("result", {}) if isinstance(pj.get("result"), dict) else {}
                meta = pj.get("meta", {}) if isinstance(pj.get("meta"), dict) else {}
                stage_events = list(pres.get("stage_events", []) if isinstance(pres, dict) else [])
                perf_rows = list(pres.get("perf_rows", []) if isinstance(pres, dict) else [])
                if isinstance(stage_events, list):
                    st.session_state.last_pipeline_stage_events = stage_events
                if isinstance(perf_rows, list):
                    st.session_state.last_pipeline_perf_rows = perf_rows
                if not pres.get("ok"):
                    st.error(f"Pipeline failed at {pres.get('stage', 'unknown')}: {pres.get('error', 'unknown error')}")
                else:
                    live_block = str(pres.get("live_block", "") or "")
                    st.session_state.live_runs.append(
                        {"name": _build_live_cfg_from_state_or_profile().get("name", "pipeline-live"), "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "text": live_block}
                    )
                    st.session_state.live_runs = st.session_state.live_runs[-20:]
                    st.session_state.last_live_text = live_block
                    st.session_state.last_backtest_summary = str(pres.get("bt_summary", "") or "")
                    st.session_state.last_backtest_trades = str(pres.get("bt_trades", "") or "")
                    pipe_source = "\n\n".join([live_block, st.session_state.last_backtest_summary, st.session_state.last_backtest_trades]).strip()
                    st.session_state.ai_source_text = pipe_source
                    st.session_state.ai_source_text_area = pipe_source
                    st.session_state.ai_response = str(pres.get("ai_response", "") or "")
                    recs = _extract_trade_recs(st.session_state.ai_response, default_tf=str(meta.get("default_tf", "4h")))
                    recs = _apply_signal_controls(
                        recs=recs,
                        profile=str(meta.get("pipe_profile", "")),
                        quote_asset=str(meta.get("signal_quote", "USDT")),
                        order_type_policy=str(meta.get("signal_order_type", "as_ai")),
                        autosize_mode=str(meta.get("signal_autosize", "none")),
                        fixed_quote_notional=float(meta.get("signal_fixed_notional", 0.0) or 0.0),
                        pct_quote_balance=float(meta.get("signal_pct_balance", 0.0) or 0.0),
                        apply_protection=bool(meta.get("signal_protect", False)),
                        stop_loss_pct=float(meta.get("signal_sl_pct", 0.0) or 0.0),
                        take_profit_pct=float(meta.get("signal_tp_pct", 0.0) or 0.0),
                        min_notional_buffer_pct=float(meta.get("signal_min_notional_buffer_pct", 0.0) or 0.0),
                        min_notional_buffer_abs=float(meta.get("signal_min_notional_buffer_abs", 0.0) or 0.0),
                        post_buy_protection_mode=str(meta.get("signal_post_buy_mode", "none") or "none"),
                        ai_only_oco_values=bool(meta.get("signal_ai_only_oco", False)),
                    )
                    if recs:
                        seq0 = int(st.session_state.pending_rec_seq or 0)
                        n = _append_pending(recs, source="pipeline_live_bt_ai")
                        stage_events.append({"stage": "STAGE", "status": "done", "sec": 0.0, "note": f"staged={int(n)}"})
                        st.success(f"Pipeline complete. Staged {n} recommendation(s).")
                        if bool(meta.get("pipe_auto_submit", False)):
                            prof = str(meta.get("pipe_profile", "") or "")
                            if not prof:
                                st.warning("No Binance profile selected for auto-submit.")
                                stage_events.append({"stage": "SUBMIT", "status": "failed", "sec": 0.0, "note": "No Binance profile selected for auto-submit."})
                            else:
                                new_ids = set(range(seq0 + 1, seq0 + n + 1))
                                ok_n, fail_n = 0, 0
                                for rec in st.session_state.pending_recs:
                                    if int(rec.get("id", -1)) not in new_ids:
                                        continue
                                    out_sub = _submit_with_recovery(prof, rec, enable_recovery=bool(st.session_state.get("auto_retry_locked_sell", True)))
                                    if _apply_submit_result_to_rec(rec, out_sub):
                                        ok_n += 1
                                    else:
                                        fail_n += 1
                                st.info(f"Auto-submit results: submitted={ok_n}, failed={fail_n}")
                                stage_events.append({"stage": "SUBMIT", "status": "done" if fail_n == 0 else "failed", "sec": 0.0, "note": f"submitted={int(ok_n)}, failed={int(fail_n)}"})
                    else:
                        stage_events.append({"stage": "STAGE", "status": "failed", "sec": 0.0, "note": "No structured trade recommendations to stage."})
                        st.warning("Pipeline AI returned no structured trade recommendations to stage.")
                if isinstance(stage_events, list) and stage_events:
                    st.session_state.last_pipeline_stage_events = stage_events
                    st.caption("Pipeline stage timings")
                    _show_df(pd.DataFrame(stage_events), width="stretch", hide_index=True)
                if isinstance(perf_rows, list) and perf_rows:
                    st.caption("Pipeline performance breakdown")
                    _show_df(pd.DataFrame(perf_rows), width="stretch", hide_index=True)
            st.session_state.bg_pipeline_running_id = ""
    elif isinstance(st.session_state.get("last_pipeline_stage_events"), list) and st.session_state.last_pipeline_stage_events:
        st.caption("Last pipeline stage timings")
        try:
            _show_df(pd.DataFrame(st.session_state.last_pipeline_stage_events), width="stretch", hide_index=True)
        except Exception:
            pass
        if isinstance(st.session_state.get("last_pipeline_perf_rows"), list) and st.session_state.last_pipeline_perf_rows:
            st.caption("Last pipeline performance breakdown")
            try:
                _show_df(pd.DataFrame(st.session_state.last_pipeline_perf_rows), width="stretch", hide_index=True)
            except Exception:
                pass

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
    _ai_job = _consume_bg_job("bg_ai_run_job_id")
    _ai_running = str(_ai_job.get("state", "")) == "running"
    if st.button("Run AI Analysis", type="primary", disabled=_ai_running):
        if not current_src:
            st.warning("Provide source text first.")
        else:
            jid = _start_bg_job("AI Analysis (async)", lambda src=current_src, dtx=dt_context: bridge.run_ai_analysis(src, dtx))
            st.session_state.bg_ai_run_job_id = str(jid)
            st.info("AI analysis started in background.")
    if _ai_running:
        _elapsed = float((_ai_job.get("job", {}) or {}).get("elapsed_sec", 0.0) or 0.0)
        st.caption(f"AI analysis running... {_elapsed:.1f}s")
    elif str(_ai_job.get("state", "")) == "done":
        _res = (_ai_job.get("job", {}) or {}).get("result", {})
        res = _res if isinstance(_res, dict) else {}
        if not res.get("ok"):
            st.error(res.get("error", "AI failed"))
        else:
            st.session_state.ai_response = str(res.get("response", "") or "")
            st.success("AI response received")
    elif str(_ai_job.get("state", "")) == "failed":
        _err = str((_ai_job.get("job", {}) or {}).get("error", "") or "AI failed")
        st.error(_err)
    if st.session_state.ai_response:
        st.text_area("AI Response", value=st.session_state.ai_response, height=420)
    follow = st.text_input("Follow-up")
    _ai_fu_job = _consume_bg_job("bg_ai_followup_job_id")
    _ai_fu_running = str(_ai_fu_job.get("state", "")) == "running"
    if st.button("Run Follow-up", disabled=_ai_fu_running) and follow.strip():
        compound = (st.session_state.ai_source_text or "") + "\n\nPrevious response:\n" + (st.session_state.ai_response or "") + "\n\nFollow-up:\n" + follow
        jid = _start_bg_job("AI Follow-up (async)", lambda cmp=compound, dtx=dt_context: bridge.run_ai_analysis(cmp, dtx))
        st.session_state.bg_ai_followup_job_id = str(jid)
        st.info("AI follow-up started in background.")
    if _ai_fu_running:
        _elapsed_fu = float((_ai_fu_job.get("job", {}) or {}).get("elapsed_sec", 0.0) or 0.0)
        st.caption(f"AI follow-up running... {_elapsed_fu:.1f}s")
    elif str(_ai_fu_job.get("state", "")) == "done":
        _res_fu = (_ai_fu_job.get("job", {}) or {}).get("result", {})
        out_fu = _res_fu if isinstance(_res_fu, dict) else {}
        if out_fu.get("ok"):
            st.session_state.ai_response = str(out_fu.get("response", "") or "")
        else:
            st.error(out_fu.get("error", "Follow-up failed"))
    elif str(_ai_fu_job.get("state", "")) == "failed":
        _err_fu = str((_ai_fu_job.get("job", {}) or {}).get("error", "") or "Follow-up failed")
        st.error(_err_fu)

with tab_portfolio:
    st.subheader("Portfolio & Ledger")
    names = _load_binance_profiles()
    profile = _safe_select_option("Binance profile", names, preferred=(names[0] if names else ""))
    auto_cols = st.columns([1, 1, 1, 1])
    pf_auto_load = auto_cols[0].checkbox("Auto-load on tab open", value=True, key="pf_auto_load")
    pf_auto_include_review = auto_cols[1].checkbox("Include MTF review", value=False, key="pf_auto_include_review")
    pf_auto_refresh_sec = int(auto_cols[2].number_input("Auto-load cadence (sec)", min_value=10, max_value=600, value=45, step=5, key="pf_auto_refresh_sec"))
    pf_force_bundle = auto_cols[3].button("Refresh All Now", key="pf_refresh_all_now", disabled=not bool(profile))

    now_epoch = time.time()
    last_epoch = float(st.session_state.get("pf_last_loaded_epoch", 0.0) or 0.0)
    need_initial = (
        not isinstance(st.session_state.last_portfolio_df, pd.DataFrame)
        or st.session_state.last_portfolio_df.empty
        or not isinstance(st.session_state.last_open_orders_df, pd.DataFrame)
        or not isinstance(st.session_state.last_ledger_df, pd.DataFrame)
        or st.session_state.last_ledger_df.empty
    )
    should_auto_bundle = bool(profile) and bool(pf_auto_load) and (need_initial or (now_epoch - last_epoch >= float(pf_auto_refresh_sec)))
    gp_running_id = str(st.session_state.get("bg_prefetch_running_id", "") or "")
    gp_running = False
    if gp_running_id:
        _jobs = st.session_state.bg_jobs if isinstance(st.session_state.bg_jobs, dict) else {}
        _j = _jobs.get(gp_running_id, {}) if isinstance(_jobs, dict) else {}
        gp_running = isinstance(_j, dict) and str(_j.get("status", "")) == "running"

    if pf_force_bundle and bool(profile):
        if gp_running:
            st.info("Portfolio refresh already running in background.")
        else:
            _out = _start_portfolio_bundle_singleflight_job(
                task_name="Portfolio Auto Bundle Refresh (async)",
                profile=str(profile),
                include_review=bool(pf_auto_include_review),
            )
            _jid = str(_out.get("job_id", "") or "")
            if _jid:
                st.session_state.bg_prefetch_running_id = _jid
                if bool(_out.get("started", False)):
                    st.info("Portfolio bundle refresh started in background.")
                else:
                    st.info(f"Portfolio bundle refresh already running [job={_jid[:8]}].")
    elif should_auto_bundle and bool(profile):
        if not gp_running:
            _out = _start_portfolio_bundle_singleflight_job(
                task_name="Portfolio Auto Bundle Refresh (async)",
                profile=str(profile),
                include_review=bool(pf_auto_include_review),
            )
            _jid = str(_out.get("job_id", "") or "")
            if _jid:
                st.session_state.bg_prefetch_running_id = _jid
                if bool(_out.get("started", False)):
                    st.caption("Auto-refresh kicked off in background.")

    c1, c2, c3, c4, c5 = st.columns(5)
    do_pf = c1.button("Refresh Portfolio", type="primary", disabled=not bool(profile))
    do_oo = c2.button("Refresh Open Orders", disabled=not bool(profile))
    do_led = c3.button("Refresh Ledger")
    do_rec = c4.button("Reconcile Fills", disabled=not bool(profile))
    do_review = c5.button("Review Open Positions (MTF)", disabled=not bool(profile))
    force_review_refresh = st.checkbox(
        "Force fresh open-position analysis (ignore TTL cache)",
        value=False,
        key="force_open_pos_review_refresh",
    )
    pcols = st.columns(5)
    auto_send_protect = pcols[0].checkbox(
        "Unified cycle auto-submit protection orders",
        value=False,
        help="When ON, protection recommendations are submitted to Binance during Unified/Protect runs. When OFF, they are staged as PENDING for manual review.",
    )
    do_protect = pcols[1].button("Protect Open Positions (AI+BT)", disabled=not bool(profile))
    do_protect_pipeline = pcols[2].button("Protect Open Positions (Live > BT > AI)", disabled=not bool(profile))
    auto_submit_unified_new = pcols[3].checkbox(
        "Unified cycle auto-submit new entries",
        value=False,
        help="When Unified Cycle runs, auto-submit newly discovered opportunity trades.",
    )
    do_unified = pcols[4].button("Unified Cycle (Manage + Discover)", disabled=not bool(profile))
    do_prune = st.button("Prune Signal History")
    uctl = st.columns(2)
    unified_signal_order_type = uctl[0].selectbox(
        "Unified new-entry order type",
        ["as_ai", "MARKET", "LIMIT", "STOP_LOSS_LIMIT", "TAKE_PROFIT_LIMIT", "OCO_BRACKET"],
        index=max(
            0,
            ["as_ai", "MARKET", "LIMIT", "STOP_LOSS_LIMIT", "TAKE_PROFIT_LIMIT", "OCO_BRACKET"].index(
                str(st.session_state.get("unified_signal_order_type", "as_ai") or "as_ai")
            ),
        )
        if str(st.session_state.get("unified_signal_order_type", "as_ai") or "as_ai")
        in ["as_ai", "MARKET", "LIMIT", "STOP_LOSS_LIMIT", "TAKE_PROFIT_LIMIT", "OCO_BRACKET"]
        else 0,
        key="unified_signal_order_type",
        help="Controls order type applied to Unified Cycle new-entry recommendations.",
    )
    unified_post_buy_mode = uctl[1].selectbox(
        "Unified BUY follow-up protection",
        ["none", "oco"],
        index=0 if str(st.session_state.get("unified_post_buy_mode", "none") or "none") == "none" else 1,
        key="unified_post_buy_mode",
        help="none = plain BUY only; oco = after BUY fill, submit SELL OCO using configured/default SL/TP.",
    )
    live_profiles_for_protect = _load_live_profiles()
    live_profile_names_for_protect = sorted(live_profiles_for_protect.keys()) if isinstance(live_profiles_for_protect, dict) else []
    protect_live_profile_pick = st.selectbox(
        "Live profile source for Live > BT > AI protection",
        ["(current live settings)"] + live_profile_names_for_protect,
        index=0,
        key="protect_live_profile_pick",
    )
    if auto_send_protect:
        st.info("Protect behavior: Stage + Auto-submit to Binance.")
    else:
        st.info("Protect behavior: Stage only (manual review and submit from Pending Recommendations).")
    st.caption(
        f"Unified new-entry behavior: order type `{str(unified_signal_order_type)}` | BUY follow-up `{str(unified_post_buy_mode)}`"
    )

    st.markdown("### Retrofit Selected Position to OCO")
    rcols = st.columns([2, 1, 1, 2, 2])
    _retro_syms = _portfolio_symbols_for_profile(profile=profile, quote_pref=str(st.session_state.get("live_quote", "USDT") or "USDT")) if profile else []
    _retro_pick = rcols[0].selectbox(
        "Open symbol",
        ["(none)"] + sorted(_retro_syms),
        index=0,
        key="retrofit_oco_symbol",
        help="Pick an existing held symbol to apply/replace with an OCO bracket.",
    )
    _retro_sl = float(
        rcols[1].number_input(
            "Stop %",
            min_value=0.1,
            max_value=80.0,
            value=float(st.session_state.get("sig_sl_pct", 5.0) or 5.0),
            step=0.5,
            key="retrofit_oco_sl_pct",
        )
    )
    _retro_tp = float(
        rcols[2].number_input(
            "TP %",
            min_value=0.1,
            max_value=300.0,
            value=float(st.session_state.get("sig_tp_pct", 10.0) or 10.0),
            step=1.0,
            key="retrofit_oco_tp_pct",
        )
    )
    _retro_replace = bool(
        rcols[3].checkbox(
            "Replace existing protection",
            value=True,
            key="retrofit_oco_replace_existing",
            help="Cancels existing SELL protection orders for this symbol before placing fresh OCO.",
        )
    )
    _retro_use_total = bool(
        rcols[4].checkbox(
            "Use total position qty",
            value=True,
            key="retrofit_oco_use_total",
            help="ON uses total held qty. OFF uses free qty only.",
        )
    )
    rcols2 = st.columns([2, 2, 2])
    _retro_use_pipeline = bool(
        rcols2[0].checkbox(
            "Use Live > BT > AI values",
            value=False,
            key="retrofit_oco_use_live_bt_ai",
            help="Derive stop/take-profit from Live+Backtest+AI context for this symbol.",
        )
    )
    _retro_ai_only = bool(
        rcols2[1].checkbox(
            "AI-only values (no fallback)",
            value=bool(st.session_state.get("sig_ai_only_oco", False)),
            key="retrofit_oco_ai_only",
            help="If ON and AI does not return SL/TP, retrofit fails instead of using percentage fallback.",
        )
    )
    rcols2[2].caption("Tip: if AI mode is OFF, SL/TP percentages are used.")
    do_preview_retrofit = st.button("Preview OCO Values", disabled=(not bool(profile) or _retro_pick == "(none)"))
    do_retrofit_oco = st.button("Retrofit Selected to OCO", disabled=(not bool(profile) or _retro_pick == "(none)"))

    if do_preview_retrofit and profile and _retro_pick != "(none)":
        picked_lp = "" if protect_live_profile_pick == "(current live settings)" else str(protect_live_profile_pick)
        retro_live_cfg = _build_live_cfg_from_state_or_profile(profile_name=picked_lp)
        retro_bt_cfg = _build_backtest_cfg_from_state()
        retro_default_tf = str(st.session_state.get("bt_tf", "4h") or "4h")
        jid = _start_bg_job(
            "Preview OCO Values (async)",
            lambda p=profile, s=str(_retro_pick), sl=float(_retro_sl), tp=float(_retro_tp), up=bool(_retro_use_pipeline), lp=picked_lp, ao=bool(_retro_ai_only), lcfg=retro_live_cfg, bcfg=retro_bt_cfg, dtf=retro_default_tf: _compute_retrofit_preview(
                profile=p,
                symbol=s,
                stop_pct=sl,
                tp_pct=tp,
                use_pipeline=up,
                live_profile_name=lp,
                ai_only_values=ao,
                live_cfg=lcfg,
                bt_cfg=bcfg,
                default_tf=dtf,
            ),
        )
        st.session_state.bg_preview_retrofit_job_id = str(jid)
        st.info("OCO preview started in background.")
    _prev_job = _consume_bg_job("bg_preview_retrofit_job_id")
    if str(_prev_job.get("state", "")) == "running":
        _prev_elapsed = float((_prev_job.get("job", {}) or {}).get("elapsed_sec", 0.0) or 0.0)
        st.caption(f"OCO preview running... {_prev_elapsed:.1f}s")
    elif str(_prev_job.get("state", "")) == "done":
        _prev_res = (_prev_job.get("job", {}) or {}).get("result", {})
        dprev = _prev_res if isinstance(_prev_res, dict) else {}
        if dprev.get("ok"):
            st.session_state.last_retrofit_preview = dict(dprev)
        else:
            st.error(str(dprev.get("error", "Preview derivation failed")))
    elif str(_prev_job.get("state", "")) == "failed":
        st.error(str((_prev_job.get("job", {}) or {}).get("error", "") or "Preview derivation failed"))

    _prev = st.session_state.get("last_retrofit_preview", {})
    if isinstance(_prev, dict) and _prev:
        base_px = float(_prev.get("base_px", 0.0) or 0.0)
        sl_prev = float(_prev.get("sl_prev", 0.0) or 0.0)
        tp_prev = float(_prev.get("tp_prev", 0.0) or 0.0)
        rr = float(_prev.get("rr", 0.0) or 0.0)
        preview_src = str(_prev.get("preview_src", "fallback") or "fallback")
        pc_prev = st.columns(4)
        pc_prev[0].metric("Preview source", preview_src.upper())
        pc_prev[1].metric("Last price", f"{base_px:.8f}" if base_px > 0 else "n/a")
        pc_prev[2].metric("Stop / TP", f"{sl_prev:.8f} / {tp_prev:.8f}" if sl_prev > 0 and tp_prev > 0 else "n/a")
        pc_prev[3].metric("Est. R:R", f"{rr:.2f}" if rr > 0 else "n/a")

    if do_pf and profile:
        jid = _start_bg_job("Portfolio Refresh (async)", lambda p=profile: bridge.fetch_binance_portfolio(profile_name=p))
        st.session_state.bg_portfolio_refresh_job_id = str(jid)
        st.info("Portfolio refresh started in background.")
    _pf_job = _consume_bg_job("bg_portfolio_refresh_job_id")
    if str(_pf_job.get("state", "")) == "running":
        _pf_elapsed = float((_pf_job.get("job", {}) or {}).get("elapsed_sec", 0.0) or 0.0)
        st.caption(f"Portfolio refresh running... {_pf_elapsed:.1f}s")
    elif str(_pf_job.get("state", "")) == "done":
        _pf_res = (_pf_job.get("job", {}) or {}).get("result", {})
        out = _pf_res if isinstance(_pf_res, dict) else {}
        if not out.get("ok"):
            st.error(out.get("error", "Portfolio fetch failed"))
        else:
            st.success(f"Portfolio: {profile}")
            st.write(f"Estimated Total (USD): **${float(out.get('estimated_total_usd', 0.0) or 0.0):,.2f}**")
            df = pd.DataFrame(out.get("balances", []) or [])
            st.session_state.last_portfolio_df = df
            _session_cache_put(_portfolio_cache_key(profile), dict(out))
            if bool(out.get("circuit_open", False)):
                st.warning(
                    f"Portfolio fetch circuit is open ({float(out.get('retry_in_sec', 0.0) or 0.0):.1f}s retry window). "
                    f"Last error: {str(out.get('error', '') or '').strip()}"
                )
            if not df.empty:
                _show_df(df, width="stretch", hide_index=True)
    elif str(_pf_job.get("state", "")) == "failed":
        st.error(str((_pf_job.get("job", {}) or {}).get("error", "") or "Portfolio refresh failed"))

    if do_oo and profile:
        jid = _start_bg_job("Open Orders Refresh (async)", lambda p=profile: bridge.list_open_binance_orders(profile_name=p))
        st.session_state.bg_open_orders_refresh_job_id = str(jid)
        st.info("Open orders refresh started in background.")
    _oo_job = _consume_bg_job("bg_open_orders_refresh_job_id")
    if str(_oo_job.get("state", "")) == "running":
        _oo_elapsed = float((_oo_job.get("job", {}) or {}).get("elapsed_sec", 0.0) or 0.0)
        st.caption(f"Open orders refresh running... {_oo_elapsed:.1f}s")
    elif str(_oo_job.get("state", "")) == "done":
        _oo_res = (_oo_job.get("job", {}) or {}).get("result", {})
        out = _oo_res if isinstance(_oo_res, dict) else {}
        if not out.get("ok"):
            st.error(out.get("error", "Open orders fetch failed"))
        else:
            st.success("Open orders loaded")
            df = pd.DataFrame(out.get("orders", []) or [])
            st.session_state.last_open_orders_df = df
            if not df.empty:
                _show_df(df, width="stretch", hide_index=True)
            else:
                st.info("No open orders.")
            _sync_pending_statuses(profile)
    elif str(_oo_job.get("state", "")) == "failed":
        st.error(str((_oo_job.get("job", {}) or {}).get("error", "") or "Open orders refresh failed"))

    if do_led:
        jid = _start_bg_job("Ledger Refresh (async)", lambda: bridge.get_trade_ledger())
        st.session_state.bg_ledger_refresh_job_id = str(jid)
        st.info("Ledger refresh started in background.")
    _led_job = _consume_bg_job("bg_ledger_refresh_job_id")
    if str(_led_job.get("state", "")) == "running":
        _led_elapsed = float((_led_job.get("job", {}) or {}).get("elapsed_sec", 0.0) or 0.0)
        st.caption(f"Ledger refresh running... {_led_elapsed:.1f}s")
    elif str(_led_job.get("state", "")) == "done":
        _led_res = (_led_job.get("job", {}) or {}).get("result", {})
        out = _led_res if isinstance(_led_res, dict) else {}
        if not out.get("ok"):
            st.error(out.get("error", "Ledger fetch failed"))
        else:
            ledger = out.get("ledger", {}) if isinstance(out.get("ledger"), dict) else {}
            entries = ledger.get("entries", []) if isinstance(ledger.get("entries"), list) else []
            st.write(f"Entries: **{len(entries)}**")
            if entries:
                df = pd.DataFrame(entries)
                st.session_state.last_ledger_df = df
                _show_df(df, width="stretch", hide_index=True)
    elif str(_led_job.get("state", "")) == "failed":
        st.error(str((_led_job.get("job", {}) or {}).get("error", "") or "Ledger refresh failed"))
    if do_rec and profile:
        rec_quote = str(st.session_state.get("live_quote", "USDT") or "USDT")
        jid = _start_bg_job(
            "Reconcile Fills (async)",
            lambda p=profile, q=rec_quote: _reconcile_fills_for_profile_sync(profile=p, quote_pref=q),
        )
        st.session_state.bg_reconcile_job_id = str(jid)
        st.info("Reconcile started in background.")
    _rec_job = _consume_bg_job("bg_reconcile_job_id")
    if str(_rec_job.get("state", "")) == "running":
        _rec_elapsed = float((_rec_job.get("job", {}) or {}).get("elapsed_sec", 0.0) or 0.0)
        st.caption(f"Reconcile running... {_rec_elapsed:.1f}s")
    elif str(_rec_job.get("state", "")) == "done":
        _rec_res = (_rec_job.get("job", {}) or {}).get("result", {})
        out = _rec_res if isinstance(_rec_res, dict) else {}
        if out.get("ok"):
            st.success(f"Reconcile added={int(out.get('added', 0) or 0)}, dup={int(out.get('duplicates', 0) or 0)}")
            _sync_pending_statuses(profile)
        else:
            st.error(out.get("error", "Reconcile failed"))
    elif str(_rec_job.get("state", "")) == "failed":
        st.error(str((_rec_job.get("job", {}) or {}).get("error", "") or "Reconcile failed"))
    if do_review and profile:
        review_ttl = float(st.session_state.get("open_pos_review_ttl_sec", 300) or 300)
        review_force = bool(force_review_refresh)
        jid = _start_bg_job(
            "Open Position Review (MTF) (async)",
            lambda p=profile, ttl=review_ttl, force=review_force: _analyze_open_positions_cached(
                profile=p,
                timeframes=["4h", "8h", "12h", "1d"],
                display_currency="USD",
                ttl_sec=ttl,
                force_refresh=force,
            ),
        )
        st.session_state.bg_review_job_id = str(jid)
        st.info("MTF review started in background.")
    _rev_job = _consume_bg_job("bg_review_job_id")
    if str(_rev_job.get("state", "")) == "running":
        _rev_elapsed = float((_rev_job.get("job", {}) or {}).get("elapsed_sec", 0.0) or 0.0)
        st.caption(f"MTF review running... {_rev_elapsed:.1f}s")
    elif str(_rev_job.get("state", "")) == "done":
        _rev_res = (_rev_job.get("job", {}) or {}).get("result", {})
        out = _rev_res if isinstance(_rev_res, dict) else {}
        if out.get("ok"):
            rdf = pd.DataFrame(out.get("rows", []) or [])
            st.session_state.last_review_df = rdf
            if bool(out.get("cache_hit", False)):
                st.caption(f"Review reused fresh cache ({float(out.get('cache_age_sec', 0.0) or 0.0):.1f}s old).")
            _show_df(rdf, width="stretch", hide_index=True)
        else:
            st.error(out.get("error", "Review failed"))
    elif str(_rev_job.get("state", "")) == "failed":
        st.error(str((_rev_job.get("job", {}) or {}).get("error", "") or "Review failed"))
    if do_retrofit_oco and profile:
        pick_sym = str(st.session_state.get("retrofit_oco_symbol", "(none)") or "(none)")
        if pick_sym == "(none)":
            st.warning("Pick a symbol first.")
        else:
            picked_lp = "" if protect_live_profile_pick == "(current live settings)" else str(protect_live_profile_pick)
            retro_live_cfg = _build_live_cfg_from_state_or_profile(profile_name=picked_lp)
            retro_bt_cfg = _build_backtest_cfg_from_state()
            retro_default_tf = str(st.session_state.get("bt_tf", "4h") or "4h")
            jid = _start_bg_job(
                "Retrofit Selected Position to OCO (async)",
                lambda p=profile, s=pick_sym, sl=float(_retro_sl), tp=float(_retro_tp), rep=bool(_retro_replace), ut=bool(_retro_use_total), upl=bool(_retro_use_pipeline), lp=picked_lp, ao=bool(_retro_ai_only), lcfg=retro_live_cfg, bcfg=retro_bt_cfg, dtf=retro_default_tf, mnp=float(st.session_state.get("sig_min_notional_buffer_pct", 0.0) or 0.0), mna=float(st.session_state.get("sig_min_notional_buffer_abs", 0.0) or 0.0): _run_retrofit_selected_oco_sync(
                    profile=p,
                    symbol=s,
                    stop_pct=sl,
                    tp_pct=tp,
                    replace_existing=rep,
                    use_total_qty=ut,
                    use_pipeline=upl,
                    live_profile_name=lp,
                    ai_only_values=ao,
                    live_cfg=lcfg,
                    bt_cfg=bcfg,
                    default_tf=dtf,
                    min_notional_buffer_pct=mnp,
                    min_notional_buffer_abs=mna,
                ),
            )
            st.session_state.bg_retrofit_oco_job_id = str(jid)
            st.info("Retrofit OCO started in background.")
    _retro_job = _consume_bg_job("bg_retrofit_oco_job_id")
    if str(_retro_job.get("state", "")) == "running":
        _retro_elapsed = float((_retro_job.get("job", {}) or {}).get("elapsed_sec", 0.0) or 0.0)
        st.caption(f"Retrofit OCO running... {_retro_elapsed:.1f}s")
    elif str(_retro_job.get("state", "")) == "done":
        _retro_res = (_retro_job.get("job", {}) or {}).get("result", {})
        out = _retro_res if isinstance(_retro_res, dict) else {}
        _msg = str(out.get("pipeline_info", "") or "").strip()
        if _msg:
            st.info(_msg)
        if out.get("ok"):
            st.success(
                f"OCO set for {out.get('symbol')} qty={float(out.get('qty', 0.0) or 0.0):.8f} | "
                f"SL={float(out.get('stop', 0.0) or 0.0):.8f} TP={float(out.get('tp', 0.0) or 0.0):.8f}"
            )
            bridge.list_open_binance_orders(profile_name=profile)
            _sync_pending_statuses(profile)
        else:
            st.error(str(out.get("error", "Retrofit OCO failed")))
    elif str(_retro_job.get("state", "")) == "failed":
        st.error(str((_retro_job.get("job", {}) or {}).get("error", "") or "Retrofit OCO failed"))
    if do_protect and profile:
        review_ttl = float(st.session_state.get("open_pos_review_ttl_sec", 300) or 300)
        force_review = bool(force_review_refresh)
        auto_retry = bool(st.session_state.get("auto_retry_locked_sell", True))
        jid = _start_bg_job(
            "Protect Open Positions (AI+BT) (async)",
            lambda p=profile, a=bool(auto_send_protect), ar=auto_retry, ttl=review_ttl, force=force_review: _protect_open_positions_simple(
                profile=p,
                auto_submit=a,
                auto_retry_locked_sell=ar,
                review_ttl_sec=ttl,
                force_review_refresh=force,
            ),
        )
        st.session_state.bg_protect_job_id = str(jid)
        st.info("Protection (AI+BT) started in background.")
    _prot_job = _consume_bg_job("bg_protect_job_id")
    if str(_prot_job.get("state", "")) == "running":
        _prot_elapsed = float((_prot_job.get("job", {}) or {}).get("elapsed_sec", 0.0) or 0.0)
        st.caption(f"Protection (AI+BT) running... {_prot_elapsed:.1f}s")
    elif str(_prot_job.get("state", "")) == "done":
        _prot_res = (_prot_job.get("job", {}) or {}).get("result", {})
        out = _prot_res if isinstance(_prot_res, dict) else {}
        if out.get("ok"):
            applied = _apply_pending_payload(profile, out)
            mode_txt = str(out.get("mode", "ai_bt"))
            st.success(f"Protection ({mode_txt}) staged={int(applied.get('staged', 0) or 0)}, submitted={int(applied.get('submitted', 0) or 0)}")
            if bool(out.get("review_cache_hit", False)):
                st.caption(f"Protection review used fresh cache ({float(out.get('review_cache_age_sec', 0.0) or 0.0):.1f}s old).")
        else:
            st.error(out.get("error", "Protect failed"))
    elif str(_prot_job.get("state", "")) == "failed":
        st.error(str((_prot_job.get("job", {}) or {}).get("error", "") or "Protect failed"))

    if do_protect_pipeline and profile:
        picked_lp = "" if protect_live_profile_pick == "(current live settings)" else str(protect_live_profile_pick)
        protect_live_cfg = _build_live_cfg_from_state_or_profile(profile_name=picked_lp)
        protect_bt_cfg = _build_backtest_cfg_from_state()
        protect_default_tf = str(st.session_state.get("bt_tf", "4h") or "4h")
        review_ttl = float(st.session_state.get("open_pos_review_ttl_sec", 300) or 300)
        force_review = bool(force_review_refresh)
        auto_retry = bool(st.session_state.get("auto_retry_locked_sell", True))
        jid = _start_bg_job(
            "Protect Open Positions (Live->BT->AI) (async)",
            lambda p=profile, a=bool(auto_send_protect), lp=picked_lp, lcfg=protect_live_cfg, bcfg=protect_bt_cfg, dtf=protect_default_tf, ar=auto_retry, ttl=review_ttl, force=force_review: _protect_open_positions_live_bt_ai(
                profile=p,
                auto_submit=a,
                live_profile_name=lp,
                live_cfg=lcfg,
                bt_cfg=bcfg,
                default_tf=dtf,
                auto_retry_locked_sell=ar,
                review_ttl_sec=ttl,
                force_review_refresh=force,
            ),
        )
        st.session_state.bg_protect_pipeline_job_id = str(jid)
        st.info("Protection (Live->BT->AI) started in background.")
    _prot_pipe_job = _consume_bg_job("bg_protect_pipeline_job_id")
    if str(_prot_pipe_job.get("state", "")) == "running":
        _prot_pipe_elapsed = float((_prot_pipe_job.get("job", {}) or {}).get("elapsed_sec", 0.0) or 0.0)
        st.caption(f"Protection (Live->BT->AI) running... {_prot_pipe_elapsed:.1f}s")
    elif str(_prot_pipe_job.get("state", "")) == "done":
        _prot_pipe_res = (_prot_pipe_job.get("job", {}) or {}).get("result", {})
        out = _prot_pipe_res if isinstance(_prot_pipe_res, dict) else {}
        if out.get("ok"):
            applied = _apply_pending_payload(profile, out)
            mode_txt = str(out.get("mode", "live_bt_ai"))
            st.success(f"Protection ({mode_txt}) staged={int(applied.get('staged', 0) or 0)}, submitted={int(applied.get('submitted', 0) or 0)}")
            if bool(out.get("review_cache_hit", False)):
                st.caption(f"Protection review used fresh cache ({float(out.get('review_cache_age_sec', 0.0) or 0.0):.1f}s old).")
        else:
            st.error(out.get("error", "Protect failed"))
    elif str(_prot_pipe_job.get("state", "")) == "failed":
        st.error(str((_prot_pipe_job.get("job", {}) or {}).get("error", "") or "Protect failed"))
    if do_unified and profile:
        picked_lp = "" if protect_live_profile_pick == "(current live settings)" else str(protect_live_profile_pick)
        unified_live_cfg = _build_live_cfg_from_state_or_profile(profile_name=picked_lp)
        unified_bt_cfg = _build_backtest_cfg_from_state()
        unified_signal_ctx = _build_signal_controls_from_state(
            default_profile=str(profile or ""),
            signal_order_type=str(unified_signal_order_type or "as_ai"),
            post_buy_protection_mode=str(unified_post_buy_mode or "none"),
            ai_only_oco_values=bool(st.session_state.get("sig_ai_only_oco", False)),
        )
        unified_default_tf = str(st.session_state.get("bt_tf", "4h") or "4h")
        unified_quote_pref = str(unified_signal_ctx.get("quote_asset", st.session_state.get("live_quote", "USDT")) or "USDT")
        unified_auto_retry = bool(st.session_state.get("auto_retry_locked_sell", True))
        review_ttl = float(st.session_state.get("open_pos_review_ttl_sec", 300) or 300)
        force_review = bool(force_review_refresh)
        jid = _start_bg_job(
            "Unified Cycle (Manage+Discover) (async)",
            lambda p=profile, lp=picked_lp, asp=bool(auto_send_protect), asn=bool(auto_submit_unified_new), lcfg=unified_live_cfg, bcfg=unified_bt_cfg, sctx=unified_signal_ctx, dtf=unified_default_tf, qpf=unified_quote_pref, ar=unified_auto_retry, ttl=review_ttl, force=force_review: _run_unified_cycle_sync(
                profile=p,
                live_profile_name=lp,
                auto_send_protect=asp,
                auto_submit_new=asn,
                dt_context=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                signal_order_type=str(unified_signal_order_type or "as_ai"),
                post_buy_protection_mode=str(unified_post_buy_mode or "none"),
                ai_only_oco_values=bool(st.session_state.get("sig_ai_only_oco", False)),
                live_cfg=lcfg,
                bt_cfg=bcfg,
                signal_controls=sctx,
                default_tf=dtf,
                quote_pref=qpf,
                auto_retry_locked_sell=ar,
                review_ttl_sec=ttl,
                force_review_refresh=force,
            ),
        )
        st.session_state.bg_unified_manual_job_id = str(jid)
        st.info("Unified cycle started in background.")
    _unified_job = _consume_bg_job("bg_unified_manual_job_id")
    if str(_unified_job.get("state", "")) == "running":
        _unified_elapsed = float((_unified_job.get("job", {}) or {}).get("elapsed_sec", 0.0) or 0.0)
        st.caption(f"Unified cycle running... {_unified_elapsed:.1f}s")
    elif str(_unified_job.get("state", "")) == "done":
        _unified_res = (_unified_job.get("job", {}) or {}).get("result", {})
        out = _apply_unified_payload(profile, _unified_res if isinstance(_unified_res, dict) else {})
        if out.get("ok"):
            st.success(f"Unified complete: {str(out.get('summary_text', '') or '').strip()}")
        else:
            st.error(f"Unified Cycle failed at {out.get('stage', 'unknown')}: {out.get('error', 'unknown error')}")
    elif str(_unified_job.get("state", "")) == "failed":
        st.error(str((_unified_job.get("job", {}) or {}).get("error", "") or "Unified cycle failed"))

    st.markdown("### Unified Monitor")
    um1, um2, um3, um4, um5 = st.columns([1, 1, 1, 1, 2])
    unified_interval = int(
        um1.number_input(
            "Every (min)",
            min_value=5,
            max_value=1440,
            value=int(st.session_state.get("unified_monitor_interval_min", 30) or 30),
            step=5,
            key="unified_monitor_interval_min",
        )
    )
    unified_24x7 = bool(
        um2.checkbox(
            "24x7",
            value=bool(st.session_state.get("unified_monitor_24x7", True)),
            key="unified_monitor_24x7",
            help="If off, monitor runs only during 06:00-23:00 local time.",
        )
    )
    start_unified = um3.button("Start Unified Monitor", disabled=not bool(profile))
    stop_unified = um4.button("Stop Unified Monitor")
    if start_unified and profile:
        picked_lp = "" if protect_live_profile_pick == "(current live settings)" else str(protect_live_profile_pick)
        st.session_state.unified_monitor_enabled = True
        st.session_state.unified_monitor_next_epoch = time.time() + 2.0
        st.session_state.unified_monitor_last_epoch = 0.0
        st.session_state.unified_monitor_busy = False
        st.session_state.unified_monitor_job_id = ""
        st.session_state.unified_monitor_live_profile = picked_lp
        st.session_state.unified_monitor_auto_send_protect = bool(auto_send_protect)
        st.session_state.unified_monitor_auto_submit_new = bool(auto_submit_unified_new)
        st.session_state.unified_monitor_signal_order_type = str(unified_signal_order_type or "as_ai")
        st.session_state.unified_monitor_post_buy_mode = str(unified_post_buy_mode or "none")
        st.session_state.unified_monitor_ai_only_oco = bool(st.session_state.get("sig_ai_only_oco", False))
        st.success("Unified Monitor started.")
        st.rerun()
    if stop_unified:
        st.session_state.unified_monitor_enabled = False
        st.session_state.unified_monitor_busy = False
        st.session_state.unified_monitor_job_id = ""
        st.info("Unified Monitor stopped.")
        st.rerun()
    um_on = bool(st.session_state.get("unified_monitor_enabled", False))
    um_busy = bool(st.session_state.get("unified_monitor_busy", False))
    um_next = float(st.session_state.get("unified_monitor_next_epoch", 0.0) or 0.0)
    um_started = float(st.session_state.get("unified_monitor_started_epoch", 0.0) or 0.0)
    if um_busy:
        elapsed = max(0.0, time.time() - um_started) if um_started > 0 else 0.0
        um5.caption(f"Status: RUNNING ({elapsed:.1f}s) | Next run: after current cycle completes")
    else:
        um_next_txt = datetime.fromtimestamp(um_next).strftime("%Y-%m-%d %H:%M:%S") if um_next > 0 else "-"
        um5.caption(f"Status: {'ACTIVE' if um_on else 'OFF'} | Next run: {um_next_txt}")
    um_last = str(st.session_state.get("unified_monitor_last_summary", "") or "").strip()
    if um_last:
        st.caption(f"Last run: {um_last}")
    hist = st.session_state.get("unified_monitor_history", [])
    if isinstance(hist, list) and hist:
        st.caption("Recent monitor runs")
        _show_df(pd.DataFrame(hist[-20:]), width="stretch", hide_index=True, height=220)

    @st.fragment(run_every=("5s" if um_on else None))
    def _unified_monitor_fragment() -> None:
        if not bool(st.session_state.get("unified_monitor_enabled", False)):
            return
        running_job_id = str(st.session_state.get("unified_monitor_job_id", "") or "")
        if bool(st.session_state.get("unified_monitor_busy", False)):
            if not running_job_id:
                return
            jobs = st.session_state.bg_jobs if isinstance(st.session_state.get("bg_jobs"), dict) else {}
            job = jobs.get(running_job_id, {}) if isinstance(jobs, dict) else {}
            jstatus = str(job.get("status", "") or "").lower() if isinstance(job, dict) else ""
            if jstatus == "running":
                return
            # Completed (done/failed): consume result and finalize without blocking UI.
            stamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            out = job.get("result", {}) if isinstance(job.get("result"), dict) else {}
            if jstatus == "done":
                out = _apply_unified_payload(str(profile or ""), out)
            if jstatus == "done" and bool(out.get("ok", False)):
                st.session_state.unified_monitor_last_summary = f"{stamp} {str(out.get('summary_text', '') or '').strip()}"
                h = st.session_state.get("unified_monitor_history", [])
                if not isinstance(h, list):
                    h = []
                h.append(
                    {
                        "ts": stamp,
                        "status": "OK",
                        "protect_staged": int(out.get("protect_staged", 0) or 0),
                        "protect_submitted": int(out.get("protect_submitted", 0) or 0),
                        "new_parsed": int(out.get("new_parsed", 0) or 0),
                        "new_eligible": int(out.get("new_eligible", 0) or 0),
                        "new_staged": int(out.get("new_staged", 0) or 0),
                        "new_submitted": int(out.get("new_submitted", 0) or 0),
                        "new_failed": int(out.get("new_failed", 0) or 0),
                        "note": str(out.get("note", "") or ""),
                    }
                )
                st.session_state.unified_monitor_history = h[-200:]
            else:
                err = str(job.get("error", "") or "")
                stage = str(out.get("stage", "unknown") or "unknown")
                detail = str(out.get("error", "") or err or "unknown error")
                st.session_state.unified_monitor_last_summary = f"{stamp} failed at {stage}: {detail}"
                h = st.session_state.get("unified_monitor_history", [])
                if not isinstance(h, list):
                    h = []
                h.append(
                    {
                        "ts": stamp,
                        "status": "FAILED",
                        "protect_staged": int(out.get("protect_staged", 0) or 0),
                        "protect_submitted": int(out.get("protect_submitted", 0) or 0),
                        "new_parsed": int(out.get("new_parsed", 0) or 0),
                        "new_eligible": int(out.get("new_eligible", 0) or 0),
                        "new_staged": int(out.get("new_staged", 0) or 0),
                        "new_submitted": int(out.get("new_submitted", 0) or 0),
                        "new_failed": int(out.get("new_failed", 0) or 0),
                        "note": f"{stage}: {detail}",
                    }
                )
                st.session_state.unified_monitor_history = h[-200:]
            st.session_state.unified_monitor_last_epoch = time.time()
            st.session_state.unified_monitor_next_epoch = time.time() + (max(5, int(st.session_state.get("unified_monitor_interval_min", 30) or 30)) * 60)
            st.session_state.unified_monitor_busy = False
            st.session_state.unified_monitor_started_epoch = 0.0
            st.session_state.unified_monitor_job_id = ""
            return
        if not bool(st.session_state.get("unified_monitor_24x7", True)):
            hour = datetime.now().hour
            if hour < 6 or hour >= 23:
                return
        nxt = float(st.session_state.get("unified_monitor_next_epoch", 0.0) or 0.0)
        if nxt > 0 and time.time() < nxt:
            return
        prof = str(profile or "")
        if not prof:
            return
        mon_live_profile = str(st.session_state.get("unified_monitor_live_profile", "") or "")
        mon_auto_send = bool(st.session_state.get("unified_monitor_auto_send_protect", False))
        mon_auto_submit = bool(st.session_state.get("unified_monitor_auto_submit_new", False))
        mon_order_type = str(st.session_state.get("unified_monitor_signal_order_type", "as_ai") or "as_ai")
        mon_post_buy = str(st.session_state.get("unified_monitor_post_buy_mode", "none") or "none")
        mon_ai_only_oco = bool(st.session_state.get("unified_monitor_ai_only_oco", False))
        mon_live_cfg = _build_live_cfg_from_state_or_profile(profile_name=mon_live_profile)
        mon_bt_cfg = _build_backtest_cfg_from_state()
        mon_signal_ctx = _build_signal_controls_from_state(
            default_profile=prof,
            signal_order_type=mon_order_type,
            post_buy_protection_mode=mon_post_buy,
            ai_only_oco_values=mon_ai_only_oco,
        )
        mon_default_tf = str(st.session_state.get("bt_tf", "4h") or "4h")
        mon_quote_pref = str(mon_signal_ctx.get("quote_asset", st.session_state.get("live_quote", "USDT")) or "USDT")
        mon_auto_retry = bool(st.session_state.get("auto_retry_locked_sell", True))
        mon_review_ttl = float(st.session_state.get("open_pos_review_ttl_sec", 300) or 300)
        st.session_state.unified_monitor_busy = True
        st.session_state.unified_monitor_started_epoch = time.time()
        jid = _start_bg_job(
            "Unified Cycle (scheduled)",
            lambda p=prof, lp=mon_live_profile, asp=mon_auto_send, asn=mon_auto_submit, ot=mon_order_type, pb=mon_post_buy, aoo=mon_ai_only_oco, lcfg=mon_live_cfg, bcfg=mon_bt_cfg, sctx=mon_signal_ctx, dtf=mon_default_tf, qpf=mon_quote_pref, ar=mon_auto_retry, ttl=mon_review_ttl: _run_unified_cycle_sync(
                profile=p,
                live_profile_name=lp,
                auto_send_protect=asp,
                auto_submit_new=asn,
                dt_context=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                signal_order_type=ot,
                post_buy_protection_mode=pb,
                ai_only_oco_values=aoo,
                live_cfg=lcfg,
                bt_cfg=bcfg,
                signal_controls=sctx,
                default_tf=dtf,
                quote_pref=qpf,
                auto_retry_locked_sell=ar,
                review_ttl_sec=ttl,
            ),
        )
        st.session_state.unified_monitor_job_id = str(jid)
        # UI remains responsive; completion handled in subsequent fragment ticks.
        return

    _unified_monitor_fragment()
    if do_prune:
        out = _run_task("Prune Signal History", lambda: bridge.prune_signal_only_history(keep_last_signals=0))
        if out.get("ok"):
            st.success("Signal history pruned.")
        else:
            st.error(out.get("error", "Prune failed"))
    last_pf = float(st.session_state.get("pf_last_loaded_epoch", 0.0) or 0.0)
    if last_pf > 0:
        st.caption(f"Last live bundle refresh: {datetime.fromtimestamp(last_pf).strftime('%Y-%m-%d %H:%M:%S')}")

    st.markdown("### Pending Recommendations")
    pdf = _pending_df()
    if not pdf.empty:
        all_ids = [int(x) for x in pdf["id"].tolist()]
        _all_id_set = set(all_ids)
        _raw_selected = st.session_state.selected_pending_ids if isinstance(st.session_state.selected_pending_ids, list) else []
        _selected_defaults = [int(x) for x in _raw_selected if str(x).strip().isdigit() and int(x) in _all_id_set]
        select_mask = [int(i) in set(_selected_defaults) for i in all_ids]
        pending_ui = pdf.copy()
        pending_ui.insert(0, "select", select_mask)
        edited = st.data_editor(
            pending_ui,
            width="stretch",
            hide_index=True,
            num_rows="fixed",
            disabled=[c for c in pending_ui.columns if c != "select"],
            column_config={"select": st.column_config.CheckboxColumn("Select", help="Tick rows to submit/remove.")},
            key="pending_recs_editor",
        )
        selected = []
        try:
            selected = [int(x) for x in edited.loc[edited["select"] == True, "id"].tolist()]  # noqa: E712
        except Exception:
            selected = []
        st.session_state.selected_pending_ids = selected
        st.caption(f"Selected: {len(selected)} of {len(all_ids)}")
        pc = st.columns(6)
        if pc[0].button("Submit Selected", disabled=not bool(profile)):
            ok_n = 0
            fail_n = 0
            for rid in selected:
                rec = next((r for r in st.session_state.pending_recs if int(r.get("id", -1)) == int(rid)), None)
                if not rec:
                    continue
                out_sub = _submit_with_recovery(profile, rec, enable_recovery=bool(st.session_state.get("auto_retry_locked_sell", True)))
                if _apply_submit_result_to_rec(rec, out_sub):
                    ok_n += 1
                else:
                    fail_n += 1
            if profile:
                _sync_pending_statuses(profile)
                _compact_pending_queue()
            st.success(f"Submitted={ok_n}, failed={fail_n}")
        if pc[1].button("Remove Selected"):
            ids = set(int(x) for x in selected)
            st.session_state.pending_recs = [r for r in st.session_state.pending_recs if int(r.get("id", -1)) not in ids]
            st.success(f"Removed {len(ids)} selected.")
            st.rerun()
        if pc[2].button("Clear Pending"):
            st.session_state.pending_recs = []
            st.session_state.selected_pending_ids = []
            st.success("Pending cleared.")
            st.rerun()
        if pc[3].button("Download Pending CSV"):
            st.download_button("Save pending.csv", data=pdf.to_csv(index=False), file_name="pending_recommendations.csv", mime="text/csv")
        if pc[4].button("Sync Pending Status", disabled=not bool(profile)):
            sout = _sync_pending_statuses(profile)
            if sout.get("ok"):
                st.success(f"Pending sync updated={int(sout.get('updated', 0) or 0)}, open_orders={int(sout.get('open_orders', 0) or 0)}")
            else:
                st.error(str(sout.get("error", "Pending sync failed")))
        if pc[5].button("Clean Queue"):
            cout = _compact_pending_queue()
            st.success(f"Queue cleaned: removed={int(cout.get('removed', 0) or 0)}, remaining={int(cout.get('remaining', 0) or 0)}")
            st.rerun()
    else:
        st.info("No pending recommendations.")
    st.markdown("### Cached Views")
    cv1, cv2, cv3 = st.columns(3)
    with cv1:
        st.caption("Portfolio (cached)")
        if isinstance(st.session_state.last_portfolio_df, pd.DataFrame) and not st.session_state.last_portfolio_df.empty:
            _show_df(st.session_state.last_portfolio_df, width="stretch", height=220, hide_index=True)
    with cv2:
        st.caption("Open Orders (cached)")
        if isinstance(st.session_state.last_open_orders_df, pd.DataFrame) and not st.session_state.last_open_orders_df.empty:
            _show_df(st.session_state.last_open_orders_df, width="stretch", height=220, hide_index=True)
    with cv3:
        st.caption("Open Position Review (cached)")
        if isinstance(st.session_state.last_review_df, pd.DataFrame) and not st.session_state.last_review_df.empty:
            _show_df(st.session_state.last_review_df, width="stretch", height=220, hide_index=True)
    st.caption("Ledger (cached)")
    if isinstance(st.session_state.last_ledger_df, pd.DataFrame) and not st.session_state.last_ledger_df.empty:
        _show_df(st.session_state.last_ledger_df, width="stretch", height=260, hide_index=True)

    st.markdown("### FIFO Open-to-Close PnL Tracker")
    fifo_profile = profile or _active_or_first_binance_profile()
    fifo_mode_col1, fifo_mode_col2 = st.columns([2, 3])
    fifo_confirmed_only = bool(
        fifo_mode_col1.checkbox(
            "Use reconciled-only rows",
            value=bool(st.session_state.get("fifo_confirmed_only", False)),
            key="fifo_confirmed_only",
            help="OFF (recommended): ledger-truth mode uses all execution rows with valid qty/price. "
            "ON: only reconciled/confirmed rows are used.",
        )
    )
    fifo_mode_col2.caption("Ledger-truth mode treats ledger execution rows as source of truth.")
    fifo = _fifo_open_close_journal(
        st.session_state.last_ledger_df,
        profile_name=str(fifo_profile or ""),
        use_confirmed_only=bool(fifo_confirmed_only),
    )
    fs = fifo.get("summary", {}) if isinstance(fifo.get("summary"), dict) else {}
    fc = fifo.get("closed", pd.DataFrame())
    fo = fifo.get("open_lots", pd.DataFrame())
    fu = fifo.get("unmatched_sells", pd.DataFrame())
    fe = fifo.get("excluded", pd.DataFrame())
    f1, f2, f3, f4 = st.columns(4)
    f1.metric("Closed matches (FIFO)", f"{int(fs.get('closed_rows', 0) or 0)}")
    f2.metric("Open lots (FIFO)", f"{int(fs.get('open_lots', 0) or 0)}")
    f3.metric("Realized PnL (quote)", f"{float(fs.get('realized_quote', 0.0) or 0.0):,.6f}")
    f4.metric("Unmatched SELL rows", f"{int(fs.get('unmatched_sell_rows', 0) or 0)}")
    st.caption(
        f"Excluded execution rows: {int(fs.get('excluded_rows', 0) or 0)} | "
        f"Unmatched SELL qty total: {float(fs.get('unmatched_sell_qty', 0.0) or 0.0):.8f}"
    )
    if isinstance(st.session_state.last_ledger_df, pd.DataFrame) and not st.session_state.last_ledger_df.empty and "ts" in st.session_state.last_ledger_df.columns:
        try:
            _mx = pd.to_datetime(st.session_state.last_ledger_df["ts"], errors="coerce", utc=True).max()
            if pd.notna(_mx):
                st.caption(f"Ledger rows loaded through: {pd.Timestamp(_mx).strftime('%Y-%m-%d %H:%M:%S UTC')}")
        except Exception:
            pass
    t1, t2, t3, t4 = st.tabs(["Closed (Open->Close)", "Open Lots", "Unmatched SELL", "Excluded Rows"])
    with t1:
        if isinstance(fc, pd.DataFrame) and not fc.empty:
            _show_df(fc, width="stretch", hide_index=True, height=320)
            st.download_button("Export FIFO Closed CSV", data=fc.to_csv(index=False), file_name="fifo_closed_pnl.csv", mime="text/csv", key="fifo_closed_export")
        else:
            st.info("No closed FIFO matches yet.")
    with t2:
        if isinstance(fo, pd.DataFrame) and not fo.empty:
            _show_df(fo, width="stretch", hide_index=True, height=320)
            st.download_button("Export FIFO Open Lots CSV", data=fo.to_csv(index=False), file_name="fifo_open_lots.csv", mime="text/csv", key="fifo_open_export")
        else:
            st.info("No open FIFO lots.")
    with t3:
        if isinstance(fu, pd.DataFrame) and not fu.empty:
            _show_df(fu, width="stretch", hide_index=True, height=260)
            st.download_button("Export FIFO Unmatched SELL CSV", data=fu.to_csv(index=False), file_name="fifo_unmatched_sells.csv", mime="text/csv", key="fifo_unmatched_export")
        else:
            st.info("No unmatched SELL rows.")
    with t4:
        if isinstance(fe, pd.DataFrame) and not fe.empty:
            _show_df(fe, width="stretch", hide_index=True, height=260)
            st.download_button("Export FIFO Excluded Rows CSV", data=fe.to_csv(index=False), file_name="fifo_excluded_rows.csv", mime="text/csv", key="fifo_excluded_export")
        else:
            st.info("No excluded execution rows.")

with tab_graph:
    st.subheader("Position Graph Data")
    names = _load_binance_profiles()
    profile = _safe_select_option("Binance profile (graph)", names, preferred=(names[0] if names else ""))
    quote_pref = st.selectbox("Quote", ["USDT", "USD", "USDC", "BUSD", "BTC", "ETH", "BNB"], index=0, key="graph_quote")
    g1, g2, g3 = st.columns([1, 1, 2])
    graph_auto = g1.checkbox("Auto-refresh", value=bool(st.session_state.get("graph_auto_refresh", False)), key="graph_auto_refresh")
    graph_interval = int(g2.number_input("Every (sec)", min_value=5, max_value=300, value=20, step=5, key="graph_interval_sec"))
    refresh_now = g3.button("Refresh Position Graph Data", type="primary", disabled=not bool(profile), key="graph_refresh_btn")

    def _start_graph_refresh_job(p: str, q: str) -> None:
        jid = _start_bg_job(
            "Position Graph Refresh (async)",
            lambda pp=p, qq=q: _build_graph_refresh_payload(profile=pp, quote_pref=qq),
        )
        st.session_state.graph_refresh_job_id = str(jid)
        st.session_state.graph_refresh_profile = str(p)
        st.session_state.graph_refresh_quote = str(q)
        # Force a fresh non-blocking ledger bootstrap for PnL breakdown on graph tab.
        st.session_state.graph_ledger_bootstrap_done = False

    # manual refresh -> async kickoff
    if refresh_now and profile:
        running_jid = str(st.session_state.get("graph_refresh_job_id", "") or "")
        jobs = st.session_state.bg_jobs if isinstance(st.session_state.get("bg_jobs"), dict) else {}
        running = False
        if running_jid and isinstance(jobs, dict):
            j = jobs.get(running_jid, {})
            running = isinstance(j, dict) and str(j.get("status", "")).lower() == "running"
        if running:
            st.info("Graph refresh already running.")
        else:
            _start_graph_refresh_job(str(profile), str(quote_pref))
            st.info("Graph refresh started in background.")

    run_every = f"{int(graph_interval)}s" if graph_auto else None

    @st.fragment(run_every=run_every)
    def _graph_fragment() -> None:
        # Auto trigger
        if graph_auto and profile:
            nxt = float(st.session_state.get("graph_next_refresh_epoch", 0.0) or 0.0)
            rid = str(st.session_state.get("graph_refresh_job_id", "") or "")
            jobs = st.session_state.bg_jobs if isinstance(st.session_state.get("bg_jobs"), dict) else {}
            is_running = False
            if rid and isinstance(jobs, dict):
                j = jobs.get(rid, {})
                is_running = isinstance(j, dict) and str(j.get("status", "")).lower() == "running"
            if (not is_running) and (nxt <= 0 or time.time() >= nxt):
                _start_graph_refresh_job(str(profile), str(quote_pref))
                st.session_state.graph_next_refresh_epoch = time.time() + float(max(5, int(graph_interval)))

        # Consume completed graph job result
        rid = str(st.session_state.get("graph_refresh_job_id", "") or "")
        if rid:
            jobs = st.session_state.bg_jobs if isinstance(st.session_state.get("bg_jobs"), dict) else {}
            j = jobs.get(rid, {}) if isinstance(jobs, dict) else {}
            status = str(j.get("status", "")).lower() if isinstance(j, dict) else ""
            if status == "running":
                elapsed = float(j.get("elapsed_sec", 0.0) or 0.0) if isinstance(j, dict) else 0.0
                st.caption(f"Graph refresh running... {elapsed:.1f}s")
            elif status in ("done", "failed"):
                if status == "done":
                    res = j.get("result", {}) if isinstance(j.get("result"), dict) else {}
                    bundle = res.get("bundle_payload", {}) if isinstance(res.get("bundle_payload"), dict) else {}
                    try:
                        _apply_portfolio_bundle_payload(bundle)
                    except Exception:
                        pass
                    df = res.get("graph_df", pd.DataFrame())
                    if isinstance(df, pd.DataFrame):
                        st.session_state.last_graph_df = df.copy()
                        st.session_state["_last_graph_rows_cache_hit"] = False
                        _session_cache_put(
                            f"graph_rows::{str(st.session_state.get('graph_refresh_profile', profile))}::{str(st.session_state.get('graph_refresh_quote', quote_pref)).upper()}",
                            df.copy(),
                        )
                        st.session_state.graph_last_refresh_epoch = time.time()
                        _record_perf_event(
                            task="Position Graph Refresh",
                            sec=float(res.get("sec", 0.0) or 0.0),
                            ok=True,
                            meta={
                                "cache_hit": False,
                                "rows": int(len(df)),
                                "profile": str(st.session_state.get("graph_refresh_profile", profile)),
                            },
                        )
                else:
                    st.warning(f"Graph refresh failed: {str(j.get('error', 'unknown error') or 'unknown error')}")
                st.session_state.graph_refresh_job_id = ""
        if isinstance(st.session_state.last_graph_df, pd.DataFrame) and not st.session_state.last_graph_df.empty:
            st.caption("Latest position graph data")
            _show_df(st.session_state.last_graph_df, width="stretch", hide_index=True)
            _render_position_graph_visuals(st.session_state.last_graph_df)
        else:
            st.info("No cached graph data yet. Click refresh.")

    _graph_fragment()
    glast = float(st.session_state.get("graph_last_refresh_epoch", 0.0) or 0.0)
    if glast > 0:
        st.caption(f"Last graph refresh: {datetime.fromtimestamp(glast).strftime('%Y-%m-%d %H:%M:%S')}")

    st.markdown("### PnL Breakdown (Open + Closed)")
    led_df = st.session_state.last_ledger_df if isinstance(st.session_state.last_ledger_df, pd.DataFrame) else pd.DataFrame()
    if (not isinstance(led_df, pd.DataFrame)) or led_df.empty:
        _gled_job = _consume_bg_job("bg_graph_ledger_load_job_id")
        _gled_state = str(_gled_job.get("state", "")).lower()
        if _gled_state == "running":
            _gled_elapsed = float((_gled_job.get("job", {}) or {}).get("elapsed_sec", 0.0) or 0.0)
            st.caption(f"Loading ledger for PnL breakdown... {_gled_elapsed:.1f}s")
        elif _gled_state == "done":
            _gled_res = (_gled_job.get("job", {}) or {}).get("result", {})
            lout = _gled_res if isinstance(_gled_res, dict) else {}
            if lout.get("ok"):
                ldg = lout.get("ledger", {}) if isinstance(lout.get("ledger"), dict) else {}
                ent = ldg.get("entries", []) if isinstance(ldg.get("entries"), list) else []
                led_df = pd.DataFrame(ent)
                st.session_state.last_ledger_df = led_df
            st.session_state.graph_ledger_bootstrap_done = True
        elif _gled_state == "failed":
            st.warning(str((_gled_job.get("job", {}) or {}).get("error", "") or "Ledger load failed for PnL breakdown."))
            st.session_state.graph_ledger_bootstrap_done = True

        _boot_done = bool(st.session_state.get("graph_ledger_bootstrap_done", False))
        if ((not _boot_done) and _gled_state != "running"):
            jid = _start_bg_job("Graph Ledger Bootstrap (async)", lambda: bridge.get_trade_ledger())
            st.session_state.bg_graph_ledger_load_job_id = str(jid)
            st.caption("Loading ledger in background for PnL breakdown...")
        elif _boot_done and ((not isinstance(led_df, pd.DataFrame)) or led_df.empty):
            if st.button("Retry Ledger Load", key="graph_retry_ledger_load"):
                st.session_state.graph_ledger_bootstrap_done = False
                st.rerun()

    if isinstance(st.session_state.last_graph_df, pd.DataFrame) and not st.session_state.last_graph_df.empty:
        pb = _compute_pnl_breakdown(st.session_state.last_graph_df, led_df)
        s = pb.get("summary", pd.DataFrame())
        if isinstance(s, pd.DataFrame) and not s.empty:
            _show_df(s, width="stretch", hide_index=True)
            fig_sum = go.Figure()
            fig_sum.add_trace(go.Bar(name="Realized", x=s["window"], y=s["realized_quote"]))
            fig_sum.add_trace(go.Bar(name="Unrealized (current)", x=s["window"], y=s["unrealized_quote"]))
            fig_sum.add_trace(go.Bar(name="Combined", x=s["window"], y=s["combined_quote"]))
            fig_sum.update_layout(
                title="Windowed PnL (Quote)",
                barmode="group",
                height=330,
                margin=dict(l=20, r=20, t=50, b=20),
                xaxis_title="Window",
                yaxis_title="PnL (quote)",
            )
            st.plotly_chart(fig_sum, width="stretch")

        t1, t2, t3, t4 = st.tabs(["Daily", "Weekly", "Monthly", "Yearly"])
        with t1:
            d = pb.get("daily", pd.DataFrame())
            if isinstance(d, pd.DataFrame) and not d.empty:
                _show_df(d, width="stretch", hide_index=True)
                st.bar_chart(d.set_index("period")["realized_quote"])
            else:
                st.info("No realized daily history yet.")
        with t2:
            d = pb.get("weekly", pd.DataFrame())
            if isinstance(d, pd.DataFrame) and not d.empty:
                _show_df(d, width="stretch", hide_index=True)
                st.bar_chart(d.set_index("period")["realized_quote"])
            else:
                st.info("No realized weekly history yet.")
        with t3:
            d = pb.get("monthly", pd.DataFrame())
            if isinstance(d, pd.DataFrame) and not d.empty:
                _show_df(d, width="stretch", hide_index=True)
                st.bar_chart(d.set_index("period")["realized_quote"])
            else:
                st.info("No realized monthly history yet.")
        with t4:
            d = pb.get("yearly", pd.DataFrame())
            if isinstance(d, pd.DataFrame) and not d.empty:
                _show_df(d, width="stretch", hide_index=True)
                st.bar_chart(d.set_index("period")["realized_quote"])
            else:
                st.info("No realized yearly history yet.")
    else:
        st.info("Refresh Position Graph Data first to compute open + closed PnL breakdown.")

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
        jid = _start_bg_job("Standard Research (async)", lambda: bridge.run_standard_research())
        st.session_state.bg_standard_research_job_id = str(jid)
        st.info("Standard research started in background.")
    if st.button("Run Comprehensive Research"):
        scenarios = []
        for tf in (scen_tfs or ["1d"]):
            scenarios.append({"market": scen_market, "timeframe": tf, "top_n": int(scen_topn), "months": int(scen_months)})
        jid = _start_bg_job(
            "Comprehensive Research (async)",
            lambda ss=scenarios, tr=int(trials), jb=int(jobs): bridge.run_comprehensive_research(scenarios=ss, optuna_trials=tr, optuna_jobs=jb),
        )
        st.session_state.bg_comprehensive_research_job_id = str(jid)
        st.info("Comprehensive research started in background.")

    def _record_research_history(run_type: str, status: str, elapsed: float, summary: str, output_text: str = "") -> None:
        hist = st.session_state.get("research_history", [])
        if not isinstance(hist, list):
            hist = []
        hist.append(
            {
                "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "run_type": str(run_type),
                "status": str(status),
                "elapsed_sec": round(float(elapsed or 0.0), 2),
                "summary": str(summary or ""),
                "output_chars": int(len(str(output_text or ""))),
            }
        )
        st.session_state.research_history = hist[-200:]

    _sr_job = _consume_bg_job("bg_standard_research_job_id")
    if str(_sr_job.get("state", "")) == "running":
        _sr_elapsed = float((_sr_job.get("job", {}) or {}).get("elapsed_sec", 0.0) or 0.0)
        st.caption(f"Standard research running... {_sr_elapsed:.1f}s")
    elif str(_sr_job.get("state", "")) == "done":
        _sr_res = (_sr_job.get("job", {}) or {}).get("result", {})
        out = _sr_res if isinstance(_sr_res, dict) else {}
        if out.get("ok"):
            st.success("Standard research completed.")
            _txt = str(out.get("output", "") or out.get("stdout", "") or "")
            st.session_state.last_research_text = _txt
            _record_research_history(
                run_type="standard",
                status="DONE",
                elapsed=float((_sr_job.get("job", {}) or {}).get("elapsed_sec", 0.0) or 0.0),
                summary="Standard research completed",
                output_text=_txt,
            )
        else:
            st.error(out.get("error", "Standard research failed"))
            _record_research_history(
                run_type="standard",
                status="FAILED",
                elapsed=float((_sr_job.get("job", {}) or {}).get("elapsed_sec", 0.0) or 0.0),
                summary=str(out.get("error", "Standard research failed")),
                output_text=str(out.get("output", "") or out.get("stdout", "") or ""),
            )
    elif str(_sr_job.get("state", "")) == "failed":
        _err = str((_sr_job.get("job", {}) or {}).get("error", "") or "Standard research failed")
        st.error(_err)
        _record_research_history(
            run_type="standard",
            status="FAILED",
            elapsed=float((_sr_job.get("job", {}) or {}).get("elapsed_sec", 0.0) or 0.0),
            summary=_err,
            output_text="",
        )
    _cr_job = _consume_bg_job("bg_comprehensive_research_job_id")
    if str(_cr_job.get("state", "")) == "running":
        _cr_elapsed = float((_cr_job.get("job", {}) or {}).get("elapsed_sec", 0.0) or 0.0)
        st.caption(f"Comprehensive research running... {_cr_elapsed:.1f}s")
    elif str(_cr_job.get("state", "")) == "done":
        _cr_res = (_cr_job.get("job", {}) or {}).get("result", {})
        out = _cr_res if isinstance(_cr_res, dict) else {}
        if out.get("ok"):
            st.success("Comprehensive research completed.")
            _txt = str(out.get("output", "") or out.get("stdout", "") or "")
            st.session_state.last_research_text = _txt
            _record_research_history(
                run_type="comprehensive",
                status="DONE",
                elapsed=float((_cr_job.get("job", {}) or {}).get("elapsed_sec", 0.0) or 0.0),
                summary="Comprehensive research completed",
                output_text=_txt,
            )
        else:
            st.error(out.get("error", "Comprehensive research failed"))
            _record_research_history(
                run_type="comprehensive",
                status="FAILED",
                elapsed=float((_cr_job.get("job", {}) or {}).get("elapsed_sec", 0.0) or 0.0),
                summary=str(out.get("error", "Comprehensive research failed")),
                output_text=str(out.get("output", "") or out.get("stdout", "") or ""),
            )
    elif str(_cr_job.get("state", "")) == "failed":
        _err = str((_cr_job.get("job", {}) or {}).get("error", "") or "Comprehensive research failed")
        st.error(_err)
        _record_research_history(
            run_type="comprehensive",
            status="FAILED",
            elapsed=float((_cr_job.get("job", {}) or {}).get("elapsed_sec", 0.0) or 0.0),
            summary=_err,
            output_text="",
        )

    st.markdown("### Research Process Monitor")
    _jobs = st.session_state.bg_jobs if isinstance(st.session_state.get("bg_jobs"), dict) else {}
    _active = []
    for _jid, _j in (_jobs.items() if isinstance(_jobs, dict) else []):
        if not isinstance(_j, dict):
            continue
        _nm = str(_j.get("name", ""))
        if "research" not in _nm.lower():
            continue
        if str(_j.get("status", "")).lower() != "running":
            continue
        _active.append(
            {
                "job_id": str(_jid)[:8],
                "name": _nm,
                "stage": _latest_stage_label(_j.get("logs", []) if isinstance(_j.get("logs"), list) else []),
                "elapsed_sec": float(_j.get("elapsed_sec", 0.0) or 0.0),
                "started_ts": str(_j.get("started_ts", "")),
            }
        )
    if _active:
        _show_df(pd.DataFrame(_active), width="stretch", hide_index=True, height=180)
    else:
        st.caption("No research jobs running.")

    rh1, rh2 = st.columns([1, 4])
    if rh1.button("Clear Research History", key="research_clear_history"):
        st.session_state.research_history = []
        st.rerun()
    rhist = st.session_state.get("research_history", [])
    if isinstance(rhist, list) and rhist:
        _hdf = pd.DataFrame(rhist)
        _show_df(_hdf.iloc[::-1], width="stretch", hide_index=True, height=220)
        _latest = _hdf.iloc[-1].to_dict()
        rh2.caption(
            f"Latest: {str(_latest.get('run_type', '')).title()} | "
            f"{_latest.get('status', '')} | {float(_latest.get('elapsed_sec', 0.0) or 0.0):.2f}s"
        )
    else:
        rh2.caption("No past research runs yet.")

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
    auto_mon = st.checkbox("Live monitor refresh (1s)", value=bool(st.session_state.get("task_monitor_auto_refresh", False)), key="task_monitor_auto_refresh")
    mon_every = "1s" if auto_mon else None

    @st.fragment(run_every=mon_every)
    def _task_monitor_fragment() -> None:
        _poll_bg_jobs()
        jobs = st.session_state.bg_jobs if isinstance(st.session_state.bg_jobs, dict) else {}
        active_rows = []
        for jid, j in jobs.items():
            if not isinstance(j, dict):
                continue
            active_rows.append(
                {
                    "job_id": str(jid)[:8],
                    "name": str(j.get("name", "")),
                    "state": str(j.get("state", j.get("status", ""))),
                    "status": str(j.get("status", "")),
                    "stage": _latest_stage_label(j.get("logs", []) if isinstance(j.get("logs"), list) else []),
                    "started_ts": str(j.get("started_ts", "")),
                    "elapsed_sec": float(j.get("elapsed_sec", 0.0) or 0.0),
                    "last_log_ts": str(j.get("last_log_ts", "")),
                }
            )
        if active_rows:
            st.caption("Background jobs (exact state)")
            _show_df(pd.DataFrame(active_rows), width="stretch", hide_index=True)
            jid_pick = st.selectbox(
                "Verbose job output",
                ["(none)"] + [str(r["job_id"]) for r in active_rows],
                index=0,
                key="task_job_pick",
            )
            if jid_pick != "(none)":
                full_id = next((str(k) for k in jobs.keys() if str(k).startswith(jid_pick)), "")
                j = jobs.get(full_id, {}) if full_id else {}
                logs = j.get("logs", []) if isinstance(j, dict) else []
                if isinstance(logs, list) and logs:
                    st.text_area("Selected job verbose log", value="\n".join(str(x) for x in logs[-800:]), height=260)
                else:
                    st.info("No verbose lines captured yet for this job.")
        st.text_area("Global Task Log", value="\n".join(st.session_state.task_logs), height=260)
        hist_df = pd.DataFrame(st.session_state.task_history) if st.session_state.task_history else pd.DataFrame(columns=["task", "sec", "ts", "ok"])
        _show_df(hist_df, width="stretch", hide_index=True)

    _task_monitor_fragment()

with tab_settings:
    st.subheader("Settings")
    st.caption("Profile management for AI and Binance.")

    st.markdown("### Concurrency")
    cw1, cw2, cw3 = st.columns([1, 1, 2])
    bg_workers = int(
        cw1.number_input(
            "Background workers",
            min_value=1,
            max_value=16,
            value=int(st.session_state.get("bg_max_workers", EXECUTOR_WORKERS) or EXECUTOR_WORKERS),
            step=1,
            key="set_bg_max_workers",
        )
    )
    cw2.caption(f"Current: {int(EXECUTOR_WORKERS)}")
    if cw2.button("Apply Workers", key="set_bg_workers_apply"):
        _set_bg_executor_workers(int(bg_workers))
        st.session_state.bg_max_workers = int(bg_workers)
        st.success(f"Background workers set to {int(bg_workers)}")
        st.rerun()
    cw3.caption("Increase if you want more concurrent long-running async jobs.")

    st.markdown("### Data Prefetch")
    gp1, gp2, gp3, gp4, gp5 = st.columns([1, 1, 1, 1, 1])
    st.session_state.global_prefetch_enabled = gp1.checkbox(
        "Global portfolio prefetch",
        value=bool(st.session_state.get("global_prefetch_enabled", True)),
        help="Keep shared portfolio/open-orders/ledger cache warm for all tabs/workflows.",
        key="set_global_prefetch_enabled",
    )
    st.session_state.global_prefetch_sec = int(
        gp2.number_input(
            "Prefetch cadence (sec)",
            min_value=15,
            max_value=900,
            value=int(st.session_state.get("global_prefetch_sec", 60) or 60),
            step=5,
            key="set_global_prefetch_sec",
        )
    )
    st.session_state.live_panel_cache_ttl_sec = int(
        gp4.number_input(
            "Live cache TTL (sec)",
            min_value=30,
            max_value=3600,
            value=int(st.session_state.get("live_panel_cache_ttl_sec", int(getattr(bridge, "_cache_ttl_live_sec", 180) or 180)) or 180),
            step=15,
            key="set_live_panel_cache_ttl_sec",
            help="Controls live panel result-cache freshness window.",
        )
    )
    st.session_state.open_pos_review_ttl_sec = int(
        gp5.number_input(
            "Open-pos review TTL (sec)",
            min_value=30,
            max_value=3600,
            value=int(st.session_state.get("open_pos_review_ttl_sec", 300) or 300),
            step=15,
            key="set_open_pos_review_ttl_sec",
            help="Skip expensive re-analysis while cached open-position review remains fresh.",
        )
    )
    try:
        bridge._cache_ttl_live_sec = float(st.session_state.get("live_panel_cache_ttl_sec", 180) or 180)
    except Exception:
        pass
    if gp3.button("Run Prefetch Now", key="set_global_prefetch_now"):
        prof = _active_or_first_binance_profile()
        if not prof:
            st.warning("No active Binance profile found for prefetch.")
        else:
            d_reason = _prefetch_defer_reason()
            if d_reason:
                st.info(d_reason)
                prof = ""
        if prof:
            _out = _start_portfolio_bundle_singleflight_job(
                task_name="Global Prefetch Bundle (async)",
                profile=str(prof),
                include_review=False,
            )
            _jid = str(_out.get("job_id", "") or "")
            st.session_state.bg_global_prefetch_job_id = _jid
            if _jid:
                if bool(_out.get("started", False)):
                    st.info(f"Prefetch started in background for profile: {prof}")
                else:
                    st.info(f"Prefetch already running for profile: {prof} [job={_jid[:8]}]")
    _gp_job = _consume_bg_job("bg_global_prefetch_job_id")
    if str(_gp_job.get("state", "")) == "running":
        _gp_elapsed = float((_gp_job.get("job", {}) or {}).get("elapsed_sec", 0.0) or 0.0)
        st.caption(f"Global prefetch running... {_gp_elapsed:.1f}s")
    elif str(_gp_job.get("state", "")) == "done":
        _gp_res = (_gp_job.get("job", {}) or {}).get("result", {})
        out = _gp_res if isinstance(_gp_res, dict) else {}
        if out.get("ok"):
            _apply_portfolio_bundle_payload(out)
            st.success("Prefetch complete.")
        else:
            st.warning("Prefetch completed with warnings.")
        _stages = out.get("stage_rows", []) if isinstance(out.get("stage_rows"), list) else []
        if _stages:
            _txt = " | ".join(
                f"{str(r.get('stage', 'stage'))}:{float(r.get('sec', 0.0) or 0.0):.2f}s"
                for r in _stages
                if isinstance(r, dict)
            )
            if _txt:
                st.caption(f"Prefetch stages: {_txt}")
        if bool(out.get("soft_timeout_hit", False)):
            st.warning(
                f"Prefetch soft-timeout triggered ({float(out.get('soft_timeout_sec', 0.0) or 0.0):.1f}s stage budget). "
                "Review stage may be skipped."
            )
    elif str(_gp_job.get("state", "")) == "failed":
        st.error(str((_gp_job.get("job", {}) or {}).get("error", "") or "Global prefetch failed"))
    _defer_reason = str(st.session_state.get("global_prefetch_last_defer_reason", "") or "").strip()
    if _defer_reason:
        _defer_ts = float(st.session_state.get("global_prefetch_last_defer_epoch", 0.0) or 0.0)
        _when = datetime.fromtimestamp(_defer_ts).strftime("%Y-%m-%d %H:%M:%S") if _defer_ts > 0 else "n/a"
        st.caption(f"{_defer_reason} | last seen: {_when}")
    try:
        cb_state = bridge.get_portfolio_circuit_state()
    except Exception:
        cb_state = {"ok": False}
    if isinstance(cb_state, dict) and cb_state.get("ok"):
        if bool(cb_state.get("circuit_open", False)):
            st.warning(
                "Portfolio API circuit is OPEN | "
                f"retry in {float(cb_state.get('retry_in_sec', 0.0) or 0.0):.1f}s | "
                f"failures {int(cb_state.get('fail_count', 0) or 0)}/{int(cb_state.get('threshold', 0) or 0)}"
            )
        elif int(cb_state.get("fail_count", 0) or 0) > 0:
            st.caption(
                "Portfolio API circuit recovering | "
                f"failure streak {int(cb_state.get('fail_count', 0) or 0)}/{int(cb_state.get('threshold', 0) or 0)}"
            )
    st.session_state.auto_retry_locked_sell = gp2.checkbox(
        "Auto-retry SELL MARKET (cancel protective orders first)",
        value=bool(st.session_state.get("auto_retry_locked_sell", True)),
        help="If a SELL MARKET fails with insufficient balance due to locked qty, cancel open SELL protective orders for that symbol and retry once.",
        key="set_auto_retry_locked_sell",
    )

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
