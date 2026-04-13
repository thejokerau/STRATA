from __future__ import annotations

from datetime import datetime
import os
import json
import re
import time
import tempfile
import concurrent.futures
import uuid
import io
import queue
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from contextlib import redirect_stdout, redirect_stderr

import pandas as pd
import streamlit as st
import plotly.graph_objects as go

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


def _run_live_panels_sync(configs: List[Dict[str, Any]]) -> Dict[str, Any]:
    combined: List[str] = []
    runs: List[Dict[str, str]] = []
    errors: List[str] = []
    for cfg in configs:
        tf = str(cfg.get("timeframe", ""))
        res = bridge.run_live_panel(cfg)
        if not res.get("ok"):
            errors.append(f"{tf}: {res.get('error', 'Live run failed')}")
            continue
        t = res.get("table_text", "")
        b = res.get("breakdown_text", "")
        c = res.get("context_text", "")
        block = "\n\n".join([str(t), str(b), str(c)]).strip()
        runs.append({"name": str(cfg.get("name", f"panel-{tf}")), "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "text": block})
        combined.append(block)
    return {"ok": bool(combined), "runs": runs, "combined_text": "\n\n".join(combined).strip(), "errors": errors}


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


def _run_live_cfg_bundle_sync(live_cfg: Dict[str, Any]) -> Dict[str, Any]:
    cfgs = _expand_live_cfgs(live_cfg)
    return _run_live_panels_sync(cfgs)


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


def _run_full_pipeline_sync(live_cfg: Dict[str, Any], bt_cfg: Dict[str, Any], dt_context: str) -> Dict[str, Any]:
    live_res = _run_live_cfg_bundle_sync(live_cfg)
    if not live_res.get("ok"):
        return {"ok": False, "stage": "live", "error": str(live_res.get("error", "Live failed"))}
    live_block = str(live_res.get("combined_text", "") or "").strip()
    bt_res = bridge.run_backtest(bt_cfg)
    if not bt_res.get("ok"):
        return {"ok": False, "stage": "backtest", "error": str(bt_res.get("error", "Backtest failed")), "live_block": live_block}
    bt_summary = str(bt_res.get("summary_text", "") or "")
    bt_trades = str(bt_res.get("trade_text", "") or "")
    src = "\n\n".join([live_block, bt_summary, bt_trades]).strip()
    ai_res = bridge.run_ai_analysis(src, dt_context)
    if not ai_res.get("ok"):
        return {"ok": False, "stage": "ai", "error": str(ai_res.get("error", "AI failed")), "live_block": live_block, "bt_summary": bt_summary, "bt_trades": bt_trades}
    return {
        "ok": True,
        "live_block": live_block,
        "bt_summary": bt_summary,
        "bt_trades": bt_trades,
        "ai_response": str(ai_res.get("response", "") or ""),
    }


def _run_unified_cycle_sync(
    profile: str,
    live_profile_name: str = "",
    auto_send_protect: bool = False,
    auto_submit_new: bool = False,
    dt_context: str = "",
) -> Dict[str, Any]:
    # 1) Manage existing positions.
    protect_out = _protect_open_positions_live_bt_ai(
        profile=profile,
        auto_submit=bool(auto_send_protect),
        live_profile_name=live_profile_name,
    )
    if not protect_out.get("ok"):
        return {"ok": False, "stage": "protect", "error": str(protect_out.get("error", "Protection failed"))}

    # 2) Discover new opportunities (Live -> BT -> AI -> Stage).
    live_cfg = _build_live_cfg_from_state_or_profile(profile_name=live_profile_name)
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
    pipe_out = _run_full_pipeline_sync(live_cfg=live_cfg, bt_cfg=bt_cfg, dt_context=(dt_context or datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
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

    # Keep shared visible outputs in sync.
    st.session_state.live_runs.append(
        {"name": _build_live_cfg_from_state_or_profile(profile_name=live_profile_name).get("name", "unified-live"), "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "text": live_block}
    )
    st.session_state.live_runs = st.session_state.live_runs[-20:]
    st.session_state.last_live_text = live_block
    st.session_state.last_backtest_summary = bt_summary
    st.session_state.last_backtest_trades = bt_trades
    st.session_state.pending_ai_source_text = "\n\n".join([live_block, bt_summary, bt_trades]).strip()
    st.session_state.ai_response = ai_response

    # Stage recommendations as "new opportunities".
    default_tf = str(st.session_state.get("bt_tf", "4h"))
    recs = _extract_trade_recs(ai_response, default_tf=default_tf)
    recs = _apply_signal_controls(
        recs=recs,
        profile=str(st.session_state.get("sig_exec_profile", profile) or profile),
        quote_asset=str(st.session_state.get("sig_quote", "USDT") or "USDT"),
        order_type_policy=str(st.session_state.get("sig_order_type", "as_ai") or "as_ai"),
        autosize_mode=str(st.session_state.get("sig_autosize", "none") or "none"),
        fixed_quote_notional=float(st.session_state.get("sig_fixed_notional", 0.0) or 0.0),
        pct_quote_balance=float(st.session_state.get("sig_pct_balance", 0.0) or 0.0),
        apply_protection=bool(st.session_state.get("sig_add_protect", False)),
        stop_loss_pct=float(st.session_state.get("sig_sl_pct", 0.0) or 0.0),
        take_profit_pct=float(st.session_state.get("sig_tp_pct", 0.0) or 0.0),
    )
    staged = 0
    submitted = 0
    failed = 0
    if recs:
        seq0 = int(st.session_state.pending_rec_seq or 0)
        staged = _append_pending(recs, source="unified_new_opportunity")
        if auto_submit_new and staged > 0:
            new_ids = set(range(seq0 + 1, seq0 + staged + 1))
            for rec in st.session_state.pending_recs:
                if int(rec.get("id", -1)) not in new_ids:
                    continue
                out_sub = _submit_with_recovery(profile, rec, enable_recovery=bool(st.session_state.get("auto_retry_locked_sell", True)))
                rec["status"] = "SUBMITTED" if out_sub.get("ok") else "FAILED"
                rec["reason"] = str(rec.get("reason", "")) + (
                    f" | {out_sub.get('recovery_note', 'submitted')}" if out_sub.get("ok") else f" | {out_sub.get('error', 'submit failed')}"
                )
                if out_sub.get("ok"):
                    submitted += 1
                else:
                    failed += 1

    return {
        "ok": True,
        "mode": "unified_manage_and_discover",
        "protect_staged": int(protect_out.get("staged", 0) or 0),
        "protect_submitted": int(protect_out.get("submitted", 0) or 0),
        "new_staged": int(staged),
        "new_submitted": int(submitted),
        "new_failed": int(failed),
    }


def _init_state() -> None:
    defaults = {
        "task_logs": [],
        "task_history": [],
        "pending_recs": [],
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
        "live_profile_name": "default",
        "pending_live_profile_apply": None,
        "bg_jobs": {},
        "bg_live_running_id": "",
        "bg_pipeline_running_id": "",
        "bg_unified_running_id": "",
        "pf_last_loaded_epoch": 0.0,
        "pf_last_review_epoch": 0.0,
        "global_prefetch_enabled": True,
        "global_prefetch_sec": 60,
        "auto_retry_locked_sell": True,
        "graph_auto_refresh": False,
        "task_monitor_auto_refresh": False,
        "bg_max_workers": int(EXECUTOR_WORKERS),
        "bg_prefetch_running_id": "",
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


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
    out_pf = bridge.fetch_binance_portfolio(profile_name=profile)
    out_oo = bridge.list_open_binance_orders(profile_name=profile)
    out_led = bridge.get_trade_ledger()
    out_rev: Dict[str, Any] = {"ok": True, "rows": []}
    if include_review:
        out_rev = bridge.analyze_open_positions_multi_tf(
            profile_name=profile, timeframes=["4h", "8h", "12h", "1d"], display_currency="USD"
        )
    return {
        "ok": bool(out_pf.get("ok") and out_oo.get("ok") and out_led.get("ok") and (out_rev.get("ok") if include_review else True)),
        "profile": str(profile),
        "include_review": bool(include_review),
        "portfolio": out_pf,
        "orders": out_oo,
        "ledger": out_led,
        "review": out_rev,
    }


def _apply_portfolio_bundle_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    out_pf = payload.get("portfolio", {}) if isinstance(payload.get("portfolio"), dict) else {}
    out_oo = payload.get("orders", {}) if isinstance(payload.get("orders"), dict) else {}
    out_led = payload.get("ledger", {}) if isinstance(payload.get("ledger"), dict) else {}
    out_rev = payload.get("review", {}) if isinstance(payload.get("review"), dict) else {"ok": True}
    include_review = bool(payload.get("include_review", False))
    if out_pf.get("ok"):
        st.session_state.last_portfolio_df = pd.DataFrame(out_pf.get("balances", []) or [])
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
        self._buf += txt
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            line = line.rstrip("\r")
            if line.strip():
                try:
                    self.q.put_nowait(line)
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
        _log(f"DONE {name} ({dt:.2f}s)")
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
        _log(f"DONE {name} (error: {type(exc).__name__}: {exc})")
        raise


def _start_bg_job(name: str, fn) -> str:
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
    }
    st.session_state.bg_jobs = jobs
    _log(f"START {name} [job={job_id[:8]}]")
    return job_id


def _poll_bg_jobs() -> None:
    jobs = st.session_state.bg_jobs if isinstance(st.session_state.bg_jobs, dict) else {}
    dirty = False
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
        try:
            res = fut.result()
            _drain_job_log_queue(j)
            j["status"] = "done"
            j["result"] = res
            j["state"] = "DONE"
            _log(f"DONE {j.get('name', 'job')} [job={jid[:8]}]")
            st.session_state.task_history.append(
                {
                    "task": str(j.get("name", "job")),
                    "sec": float(j.get("elapsed_sec", 0.0) or 0.0),
                    "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "ok": True,
                }
            )
        except Exception as exc:
            _drain_job_log_queue(j)
            j["status"] = "failed"
            j["error"] = f"{type(exc).__name__}: {exc}"
            j["state"] = "FAILED"
            _log(f"DONE {j.get('name', 'job')} [job={jid[:8]}] error={j['error']}")
            st.session_state.task_history.append(
                {
                    "task": str(j.get("name", "job")),
                    "sec": float(j.get("elapsed_sec", 0.0) or 0.0),
                    "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "ok": False,
                }
            )
        j["_log_queue"] = None
        jobs[jid] = j
    if dirty:
        st.session_state.bg_jobs = jobs


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

    # EngineBridge.submit_binance_order does not support quote_order_qty directly.
    # Convert quote notional to qty using latest price when qty is missing.
    if qty <= 0 and q_notional > 0:
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
    if out.get("ok") or not enable_recovery:
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
    out = bridge.fetch_binance_portfolio(profile_name=profile)
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
    out = bridge.fetch_binance_portfolio(profile_name=profile)
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
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    quote = str(quote_asset or "USDT").strip().upper() or "USDT"
    quote_free = _get_quote_free_balance(profile, quote) if profile else 0.0
    for r in recs:
        if not isinstance(r, dict):
            continue
        rr = dict(r)
        sym = str(rr.get("symbol", "")).strip().upper()
        side = str(rr.get("side", "BUY")).strip().upper()
        if not sym:
            asset = str(rr.get("asset", "")).strip().upper()
            if asset:
                sym = f"{asset}{quote}"
                rr["symbol"] = sym
        asset = re.sub(r"(USDT|USDC|BUSD|FDUSD|USD|BTC|ETH|BNB)$", "", sym)
        rr["asset"] = asset

        if order_type_policy != "as_ai":
            if side == "BUY" and order_type_policy == "OCO_BRACKET":
                rr["order_type"] = "MARKET"
                rr["reason"] = str(rr.get("reason", "")) + " | OCO override ignored for BUY; using MARKET"
            else:
                rr["order_type"] = order_type_policy

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
                rr["quote_notional"] = round(qn, 8)
                rr["quantity"] = 0.0
                rr["reason"] = str(rr.get("reason", "")) + f" | autosized BUY using {rr['quote_notional']} {quote}"
            if apply_protection and px > 0:
                if float(rr.get("stop_loss_price", 0.0) or 0.0) <= 0:
                    rr["stop_loss_price"] = px * (1.0 - max(0.0, float(stop_loss_pct or 0.0)) / 100.0)
                if float(rr.get("take_profit_price", 0.0) or 0.0) <= 0:
                    rr["take_profit_price"] = px * (1.0 + max(0.0, float(take_profit_pct or 0.0)) / 100.0)
        elif side == "SELL":
            qty = float(rr.get("quantity", 0.0) or 0.0)
            if qty <= 0 and profile:
                if autosize_mode == "percent_quote_balance":
                    held = _get_asset_total_balance(profile, asset)
                    qty = held * max(0.0, float(pct_quote_balance or 0.0)) / 100.0
                elif autosize_mode == "fixed_quote_notional" and px > 0:
                    qty = max(0.0, float(fixed_quote_notional or 0.0) / px)
                else:
                    qty = _get_asset_total_balance(profile, asset)
            if qty > 0:
                rr["quantity"] = round(qty, 8)
            if apply_protection and px > 0:
                if float(rr.get("stop_loss_price", 0.0) or 0.0) <= 0:
                    rr["stop_loss_price"] = px * (1.0 - max(0.0, float(stop_loss_pct or 0.0)) / 100.0)
                if float(rr.get("take_profit_price", 0.0) or 0.0) <= 0:
                    rr["take_profit_price"] = px * (1.0 + max(0.0, float(take_profit_pct or 0.0)) / 100.0)

        if str(rr.get("order_type", "")).strip().upper() == "OCO_BRACKET":
            sl = float(rr.get("stop_loss_price", 0.0) or 0.0)
            tp = float(rr.get("take_profit_price", 0.0) or 0.0)
            if sl > 0 and tp > 0:
                rr["limit_price"] = float(rr.get("limit_price", 0.0) or 0.0) or (sl * 0.999)
                rr["take_profit_limit_price"] = float(rr.get("take_profit_limit_price", 0.0) or 0.0) or (tp * 0.999)
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


def _execution_events_from_ledger_df(ledger_df: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(ledger_df, pd.DataFrame) or ledger_df.empty:
        return pd.DataFrame(columns=["id", "ts", "symbol", "action", "qty", "price"])
    d = ledger_df.copy()
    if "is_execution" in d.columns:
        d = d[d["is_execution"] == True]  # noqa: E712
    d["action"] = d.get("action", "").astype(str).str.upper()
    d = d[d["action"].isin(["BUY", "SELL"])]
    d["qty"] = pd.to_numeric(d.get("qty", 0.0), errors="coerce").fillna(0.0)
    d["price"] = pd.to_numeric(d.get("price", 0.0), errors="coerce").fillna(0.0)
    d = d[(d["qty"] > 0) & (d["price"] > 0)]
    d["ts"] = pd.to_datetime(d.get("ts"), errors="coerce", utc=True)
    d = d[d["ts"].notna()]
    sym = d.get("exchange_symbol", "").astype(str).str.strip().str.upper()
    asset = d.get("asset", "").astype(str).str.strip().str.upper()
    quote = d.get("quote_currency", "USDT").astype(str).str.strip().str.upper()
    fallback = asset + quote
    d["symbol"] = sym.where(sym != "", fallback)
    d = d[d["symbol"].astype(str).str.strip() != ""]
    if "id" not in d.columns:
        d["id"] = range(1, len(d) + 1)
    d = d.sort_values(["ts", "id"]).reset_index(drop=True)
    return d[["id", "ts", "symbol", "action", "qty", "price"]]


def _fifo_open_close_journal(ledger_df: pd.DataFrame, profile_name: str = "") -> Dict[str, Any]:
    ev = _execution_events_from_ledger_df(ledger_df)
    if ev.empty:
        return {"closed": pd.DataFrame(), "open_lots": pd.DataFrame(), "summary": {"realized_quote": 0.0, "closed_rows": 0, "open_lots": 0}}

    lots: Dict[str, List[Dict[str, Any]]] = {}
    closed_rows: List[Dict[str, Any]] = []

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

    realized_total = float(closed_df["pnl_quote"].sum()) if not closed_df.empty and "pnl_quote" in closed_df.columns else 0.0
    return {
        "closed": closed_df,
        "open_lots": open_df,
        "summary": {
            "realized_quote": realized_total,
            "closed_rows": int(len(closed_df)),
            "open_lots": int(len(open_df)),
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
            out_sub = _submit_with_recovery(profile, rec, enable_recovery=bool(st.session_state.get("auto_retry_locked_sell", True)))
            rec["status"] = "SUBMITTED" if out_sub.get("ok") else "FAILED"
            rec["reason"] = str(rec.get("reason", "")) + (
                f" | {out_sub.get('recovery_note', 'submitted')}" if out_sub.get("ok") else f" | {out_sub.get('error', 'submit failed')}"
            )
            if out_sub.get("ok"):
                submitted += 1
    return {"ok": True, "staged": added, "submitted": submitted, "rows": len(rows)}


def _protect_open_positions_live_bt_ai(profile: str, auto_submit: bool = False, live_profile_name: str = "") -> Dict[str, Any]:
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
    live_cfg = _build_live_cfg_from_state_or_profile(profile_name=live_profile_name)
    live_res = _run_live_cfg_bundle_sync(live_cfg)
    live_text = str(live_res.get("combined_text", "") or "").strip() if live_res.get("ok") else ""

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
            out_sub = _submit_with_recovery(profile, rec, enable_recovery=bool(st.session_state.get("auto_retry_locked_sell", True)))
            rec["status"] = "SUBMITTED" if out_sub.get("ok") else "FAILED"
            rec["reason"] = str(rec.get("reason", "")) + (
                f" | {out_sub.get('recovery_note', 'submitted')}" if out_sub.get("ok") else f" | {out_sub.get('error', 'submit failed')}"
            )
            if out_sub.get("ok"):
                submitted += 1
    return {"ok": True, "staged": added, "submitted": submitted, "rows": len(review_rows), "mode": "live_bt_ai"}


def _refresh_portfolio_bundle(profile: str, include_review: bool = False) -> Dict[str, Any]:
    payload = _fetch_portfolio_bundle_payload(profile=profile, include_review=include_review)
    return _apply_portfolio_bundle_payload(payload)


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
            _jid = _start_bg_job(
                "Global Prefetch Bundle (async)",
                lambda p=_gp_profile: _fetch_portfolio_bundle_payload(profile=p, include_review=False),
            )
            st.session_state.bg_prefetch_running_id = _jid
            _log(f"Heartbeat prefetch started ({_gp_profile}) [job={_jid[:8]}]")
    except Exception as _hb_exc:
        _log(f"Heartbeat prefetch error: {_hb_exc}")


_global_prefetch_heartbeat()

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

    if live_running:
        st.info("Live Dashboard is running in background...")

    # consume completed live job result
    live_done_id = str(st.session_state.get("bg_live_running_id", "") or "")
    if live_done_id:
        j = (st.session_state.bg_jobs or {}).get(live_done_id, {})
        if isinstance(j, dict) and str(j.get("status", "")) in ("done", "failed"):
            if str(j.get("status", "")) == "failed":
                st.error(f"Live job failed: {j.get('error', 'unknown error')}")
            else:
                res = j.get("result", {}) if isinstance(j.get("result"), dict) else {}
                for e in (res.get("errors", []) or []):
                    st.warning(str(e))
                runs = res.get("runs", []) if isinstance(res.get("runs"), list) else []
                if runs:
                    st.session_state.live_runs.extend(runs)
                    st.session_state.live_runs = st.session_state.live_runs[-20:]
                    st.session_state.last_live_text = str(res.get("combined_text", "") or "")
                    st.success(f"Live run complete ({len(runs)} panel(s)).")
            st.session_state.bg_live_running_id = ""
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
        sc9, sc10 = st.columns(2)
        signal_sl_pct = float(sc9.number_input("Default stop-loss %", min_value=0.1, max_value=80.0, value=5.0, step=0.5, key="sig_sl_pct"))
        signal_tp_pct = float(sc10.number_input("Default take-profit %", min_value=0.1, max_value=300.0, value=10.0, step=1.0, key="sig_tp_pct"))
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
                    rec["status"] = "SUBMITTED" if out_sub.get("ok") else "FAILED"
                    rec["reason"] = str(rec.get("reason", "")) + (
                        f" | {out_sub.get('recovery_note', 'submitted')}" if out_sub.get("ok") else f" | {out_sub.get('error', 'submit failed')}"
                    )
                    if out_sub.get("ok"):
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
            lambda lcfg=live_cfg, bcfg=bt_cfg, dctx=dt_context: _run_full_pipeline_sync(lcfg, bcfg, dctx),
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
                "default_tf": str(st.session_state.get("bt_tf", "4h")),
            }
            jobs[jid] = j
            st.session_state.bg_jobs = jobs
        st.session_state.bg_pipeline_running_id = jid
        st.info("Pipeline started in background. You can switch tabs and monitor in Task Monitor.")
        st.rerun()

    if pipeline_running:
        st.info("Pipeline is running in background...")

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
                    )
                    if recs:
                        seq0 = int(st.session_state.pending_rec_seq or 0)
                        n = _append_pending(recs, source="pipeline_live_bt_ai")
                        st.success(f"Pipeline complete. Staged {n} recommendation(s).")
                        if bool(meta.get("pipe_auto_submit", False)):
                            prof = str(meta.get("pipe_profile", "") or "")
                            if not prof:
                                st.warning("No Binance profile selected for auto-submit.")
                            else:
                                new_ids = set(range(seq0 + 1, seq0 + n + 1))
                                ok_n, fail_n = 0, 0
                                for rec in st.session_state.pending_recs:
                                    if int(rec.get("id", -1)) not in new_ids:
                                        continue
                                    out_sub = _submit_with_recovery(prof, rec, enable_recovery=bool(st.session_state.get("auto_retry_locked_sell", True)))
                                    rec["status"] = "SUBMITTED" if out_sub.get("ok") else "FAILED"
                                    rec["reason"] = str(rec.get("reason", "")) + (
                                        f" | {out_sub.get('recovery_note', 'submitted')}" if out_sub.get("ok") else f" | {out_sub.get('error', 'submit failed')}"
                                    )
                                    if out_sub.get("ok"):
                                        ok_n += 1
                                    else:
                                        fail_n += 1
                                st.info(f"Auto-submit results: submitted={ok_n}, failed={fail_n}")
                    else:
                        st.warning("Pipeline AI returned no structured trade recommendations to stage.")
            st.session_state.bg_pipeline_running_id = ""

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
    if pf_force_bundle:
        out_bundle = _run_task("Portfolio Auto Bundle Refresh", lambda: _refresh_portfolio_bundle(profile, include_review=bool(pf_auto_include_review)))
        if out_bundle.get("ok"):
            st.success("Portfolio bundle refreshed.")
        else:
            st.warning(
                "Portfolio bundle refreshed with issues: "
                + ", ".join(
                    [
                        x
                        for x in [
                            out_bundle.get("portfolio_error", ""),
                            out_bundle.get("orders_error", ""),
                            out_bundle.get("ledger_error", ""),
                            out_bundle.get("review_error", ""),
                        ]
                        if str(x).strip()
                    ]
                )
            )
    elif should_auto_bundle:
        _run_task("Portfolio Auto Bundle Refresh", lambda: _refresh_portfolio_bundle(profile, include_review=bool(pf_auto_include_review)))
        st.caption("Auto-loaded portfolio, open orders, and ledger.")

    c1, c2, c3, c4, c5 = st.columns(5)
    do_pf = c1.button("Refresh Portfolio", type="primary", disabled=not bool(profile))
    do_oo = c2.button("Refresh Open Orders", disabled=not bool(profile))
    do_led = c3.button("Refresh Ledger")
    do_rec = c4.button("Reconcile Fills", disabled=not bool(profile))
    do_review = c5.button("Review Open Positions (MTF)", disabled=not bool(profile))
    pcols = st.columns(5)
    auto_send_protect = pcols[0].checkbox(
        "Auto-submit protection orders to Binance",
        value=False,
        help="ON: generate protection and submit immediately. OFF: generate protection only and leave as PENDING for manual review/submit.",
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
                _show_df(df, width="stretch", hide_index=True)

    if do_oo and profile:
        out = _run_task("Open Orders Refresh", lambda: bridge.list_open_binance_orders(profile_name=profile))
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
                _show_df(df, width="stretch", hide_index=True)
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
            _show_df(rdf, width="stretch", hide_index=True)
        else:
            st.error(out.get("error", "Review failed"))
    if do_protect and profile:
        out = _run_task("Protect Open Positions (AI+BT)", lambda: _protect_open_positions_simple(profile, auto_submit=bool(auto_send_protect)))
        if out.get("ok"):
            mode_txt = str(out.get("mode", "ai_bt"))
            st.success(f"Protection ({mode_txt}) staged={int(out.get('staged', 0) or 0)}, submitted={int(out.get('submitted', 0) or 0)}")
        else:
            st.error(out.get("error", "Protect failed"))

    if do_protect_pipeline and profile:
        picked_lp = "" if protect_live_profile_pick == "(current live settings)" else str(protect_live_profile_pick)
        out = _run_task(
            "Protect Open Positions (Live->BT->AI)",
            lambda: _protect_open_positions_live_bt_ai(profile, auto_submit=bool(auto_send_protect), live_profile_name=picked_lp),
        )
        if out.get("ok"):
            mode_txt = str(out.get("mode", "live_bt_ai"))
            st.success(f"Protection ({mode_txt}) staged={int(out.get('staged', 0) or 0)}, submitted={int(out.get('submitted', 0) or 0)}")
        else:
            st.error(out.get("error", "Protect failed"))
    if do_unified and profile:
        picked_lp = "" if protect_live_profile_pick == "(current live settings)" else str(protect_live_profile_pick)
        out = _run_task(
            "Unified Cycle (Manage+Discover)",
            lambda p=profile, lp=picked_lp, asp=bool(auto_send_protect), asn=bool(auto_submit_unified_new): _run_unified_cycle_sync(
                profile=p,
                live_profile_name=lp,
                auto_send_protect=asp,
                auto_submit_new=asn,
                dt_context=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )
        if out.get("ok"):
            st.success(
                "Unified complete: "
                f"protect(staged={int(out.get('protect_staged', 0) or 0)}, submitted={int(out.get('protect_submitted', 0) or 0)}), "
                f"new(staged={int(out.get('new_staged', 0) or 0)}, submitted={int(out.get('new_submitted', 0) or 0)}, failed={int(out.get('new_failed', 0) or 0)})"
            )
        else:
            st.error(f"Unified Cycle failed at {out.get('stage', 'unknown')}: {out.get('error', 'unknown error')}")
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
        _show_df(pdf, width="stretch", hide_index=True)
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
                out_sub = _submit_with_recovery(profile, rec, enable_recovery=bool(st.session_state.get("auto_retry_locked_sell", True)))
                rec["status"] = "SUBMITTED" if out_sub.get("ok") else "FAILED"
                rec["reason"] = str(rec.get("reason", "")) + (
                    f" | {out_sub.get('recovery_note', 'submitted')}" if out_sub.get("ok") else f" | {out_sub.get('error', 'submit failed')}"
                )
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
    fifo = _fifo_open_close_journal(st.session_state.last_ledger_df, profile_name=str(fifo_profile or ""))
    fs = fifo.get("summary", {}) if isinstance(fifo.get("summary"), dict) else {}
    fc = fifo.get("closed", pd.DataFrame())
    fo = fifo.get("open_lots", pd.DataFrame())
    f1, f2, f3 = st.columns(3)
    f1.metric("Closed matches (FIFO)", f"{int(fs.get('closed_rows', 0) or 0)}")
    f2.metric("Open lots (FIFO)", f"{int(fs.get('open_lots', 0) or 0)}")
    f3.metric("Realized PnL (quote)", f"{float(fs.get('realized_quote', 0.0) or 0.0):,.6f}")
    t1, t2 = st.tabs(["Closed (Open?Close)", "Open Lots"])
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

with tab_graph:
    st.subheader("Position Graph Data")
    names = _load_binance_profiles()
    profile = _safe_select_option("Binance profile (graph)", names, preferred=(names[0] if names else ""))
    quote_pref = st.selectbox("Quote", ["USDT", "USD", "USDC", "BUSD", "BTC", "ETH", "BNB"], index=0, key="graph_quote")
    g1, g2, g3 = st.columns([1, 1, 2])
    graph_auto = g1.checkbox("Auto-refresh", value=bool(st.session_state.get("graph_auto_refresh", True)), key="graph_auto_refresh")
    graph_interval = int(g2.number_input("Every (sec)", min_value=5, max_value=300, value=20, step=5, key="graph_interval_sec"))
    refresh_now = g3.button("Refresh Position Graph Data", type="primary", disabled=not bool(profile), key="graph_refresh_btn")

    def _refresh_graph_once() -> None:
        if not profile:
            st.warning("Select a Binance profile first.")
            return
        # Keep graph aligned with latest portfolio/open-order/ledger cache.
        try:
            _refresh_portfolio_bundle(profile, include_review=False)
        except Exception:
            pass
        with st.spinner("Building position graph data..."):
            df = _run_task("Position Graph Refresh", lambda: _position_graph_rows(profile=profile, quote_pref=quote_pref))
        if df.empty:
            st.session_state.last_graph_df = pd.DataFrame()
        else:
            st.session_state.last_graph_df = df.copy()
        st.session_state.graph_last_refresh_epoch = time.time()

    if refresh_now:
        _refresh_graph_once()

    run_every = f"{int(graph_interval)}s" if graph_auto else None

    @st.fragment(run_every=run_every)
    def _graph_fragment() -> None:
        if graph_auto and profile:
            _refresh_graph_once()
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
        # Best-effort load when user opens graph without visiting portfolio tab first.
        try:
            lout = bridge.get_trade_ledger()
            if lout.get("ok"):
                ldg = lout.get("ledger", {}) if isinstance(lout.get("ledger"), dict) else {}
                ent = ldg.get("entries", []) if isinstance(ldg.get("entries"), list) else []
                led_df = pd.DataFrame(ent)
                st.session_state.last_ledger_df = led_df
        except Exception:
            led_df = pd.DataFrame()

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
    gp1, gp2, gp3, gp4 = st.columns([1, 1, 1, 1])
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
    if gp3.button("Run Prefetch Now", key="set_global_prefetch_now"):
        prof = _active_or_first_binance_profile()
        if not prof:
            st.warning("No active Binance profile found for prefetch.")
        else:
            out = _run_task("Global Prefetch Bundle", lambda: _refresh_portfolio_bundle(prof, include_review=False))
            if out.get("ok"):
                st.success(f"Prefetch complete for profile: {prof}")
            else:
                st.warning("Prefetch completed with warnings.")
    st.session_state.auto_retry_locked_sell = gp4.checkbox(
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
