import threading
import tkinter as tk
from collections import deque
from dataclasses import dataclass, asdict
from datetime import datetime
import json
from pathlib import Path
import re
from tkinter import ttk, messagebox, filedialog
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .engine_bridge import EngineBridge
from .state import load_state, save_state


@dataclass
class LivePanelConfig:
    name: str
    market: str = "crypto"
    timeframe: str = "1d"
    quote_currency: str = "USD"
    top_n: int = 20
    country: str = "2"
    display_currency: str = "USD"


COUNTRY_LABELS = {
    "1": "Australia",
    "2": "United States",
    "3": "United Kingdom",
    "4": "Europe",
    "5": "Canada",
    "6": "Other / Manual",
}

AI_PROVIDER_OPTIONS = ["xai", "openai", "anthropic", "ollama", "openai_compatible", "openclaw"]
COMMON_CRYPTO_BASES = {
    "BTC", "ETH", "BNB", "SOL", "XRP", "ADA", "DOGE", "TRX", "TON", "AVAX",
    "LINK", "BCH", "DOT", "LTC", "XLM", "ATOM", "ETC", "NEAR", "APT", "ARB",
    "OP", "UNI", "AAVE", "SUI", "PEPE", "SHIB", "MATIC", "FIL", "INJ", "KAS",
    "HBAR", "CRO", "RUNE", "FTM", "VET", "ALGO", "ICP", "MKR", "SNX", "GRT",
    "EGLD", "IMX", "SEI", "TIA", "JUP", "WIF", "RENDER", "TAO", "RNDR", "ZEC",
}


def country_display_values() -> List[str]:
    return [f"{name} ({code})" for code, name in COUNTRY_LABELS.items()]


def country_code_to_display(code: str) -> str:
    c = str(code).strip()
    if c in COUNTRY_LABELS:
        return f"{COUNTRY_LABELS[c]} ({c})"
    return f"Manual ({c})"


def parse_country_code(display_or_code: str, manual_code: str = "") -> str:
    manual = str(manual_code).strip()
    if manual:
        return manual
    raw = str(display_or_code).strip()
    if not raw:
        return "2"
    if raw.isdigit():
        return raw
    m = re.search(r"\((\d+)\)\s*$", raw)
    if m:
        return m.group(1)
    for code, label in COUNTRY_LABELS.items():
        if raw.lower() == label.lower():
            return code
    return "2"


class StrataGuiApp:
    def __init__(self, root: tk.Tk, repo_root: Path) -> None:
        self.root = root
        self.repo_root = repo_root
        self.root.title("STRATA GUI (gui-nightly)")
        self.root.geometry("1400x900")

        self.state = load_state()
        self.bridge = EngineBridge(repo_root=self.repo_root)
        self.auto_refresh_job: Optional[str] = None
        self.auto_refresh_running = False
        self.busy = False
        self.running_tasks = 0
        self.running_task_names: List[str] = []
        self._task_id_seq = 0
        self._running_tasks: Dict[int, str] = {}
        self.task_queue = deque()
        self.queue_paused = False
        self.ai_last_source_text = ""
        self.ai_conversation: List[Dict[str, str]] = []
        self.latest_live_output_text = ""
        self.latest_live_panel_texts: Dict[str, str] = {}
        self.task_monitor_window: Optional[tk.Toplevel] = None
        self.task_monitor_text: Optional[tk.Text] = None
        self.task_monitor_job: Optional[str] = None
        self.task_tab_job: Optional[str] = None
        self._ai_profiles_cache: Dict[str, Dict[str, Any]] = {}
        self._binance_profiles_cache: Dict[str, Dict[str, Any]] = {}
        self.latest_portfolio_snapshot: Dict[str, Any] = {}
        self.pending_recommendations: List[Dict[str, Any]] = []
        self._pending_rec_seq = 0
        self.pipeline_job: Optional[str] = None
        self.protection_monitor_job: Optional[str] = None
        self.portfolio_auto_refresh_job: Optional[str] = None
        self.portfolio_auto_refresh_running = False
        self.agent_last_staged_ids: List[int] = []
        self.agent_context: Dict[str, Any] = dict(self.state.get("agent_context", {})) if isinstance(self.state.get("agent_context", {}), dict) else {}
        self.agent_context.setdefault("timeframe", "4h")
        self.agent_context.setdefault("top_n", 10)
        self.agent_context.setdefault("quote_asset", self._effective_crypto_quote("USDT"))
        self.agent_context.setdefault("stop_pct", 5.0)
        self.agent_context.setdefault("exclude_assets", [])

        self.live_panels: List[LivePanelConfig] = []
        for p in self.state.get("live_panels", []):
            if isinstance(p, dict):
                self.live_panels.append(LivePanelConfig(**{k: p.get(k) for k in asdict(LivePanelConfig("x")).keys()}))
        if not self.live_panels:
            self.live_panels = [LivePanelConfig(name="Crypto 1d", market="crypto", timeframe="1d", quote_currency="USD", top_n=20)]

        self._build_ui()
        self._refresh_panel_list()
        self._update_run_controls_and_status()

    def _build_ui(self) -> None:
        self.nb = ttk.Notebook(self.root)
        self.nb.pack(fill="both", expand=True)
        self._tab_scroll_canvases: Dict[str, tk.Canvas] = {}
        self.live_tab = self._create_scrollable_tab("Live Dashboard", "live")
        self.backtest_tab = self._create_scrollable_tab("Backtest", "backtest")
        self.ai_tab = self._create_scrollable_tab("AI Analysis", "ai")
        self.agent_tab = self._create_scrollable_tab("Agent Console", "agent")
        self.portfolio_tab = self._create_scrollable_tab("Portfolio & Ledger", "portfolio")
        self.research_tab = self._create_scrollable_tab("Auto-Research", "research")
        self.task_tab = self._create_scrollable_tab("Task Monitor", "task")
        self.settings_tab = self._create_scrollable_tab("Settings", "settings")

        self._build_live_tab()
        self._build_backtest_tab()
        self._build_ai_tab()
        self._build_agent_tab()
        self._build_portfolio_tab()
        self._build_research_tab()
        self._build_task_tab()
        self._build_settings_tab()
        self._build_status_bar()
        self.nb.bind("<<NotebookTabChanged>>", self._on_notebook_tab_changed)

    def _create_scrollable_tab(self, title: str, key: str) -> ttk.Frame:
        container = ttk.Frame(self.nb)
        canvas = tk.Canvas(container, highlightthickness=0)
        ybar = ttk.Scrollbar(container, orient="vertical", command=canvas.yview)
        xbar = ttk.Scrollbar(container, orient="horizontal", command=canvas.xview)
        canvas.configure(yscrollcommand=ybar.set, xscrollcommand=xbar.set)
        inner = ttk.Frame(canvas)
        window_id = canvas.create_window((0, 0), window=inner, anchor="nw")

        def _sync_scrollregion(_event=None):
            canvas.configure(scrollregion=canvas.bbox("all"))

        def _on_canvas_resize(event):
            try:
                req_w = inner.winfo_reqwidth()
            except Exception:
                req_w = event.width
            target_w = event.width if req_w <= event.width else req_w
            canvas.itemconfigure(window_id, width=target_w)
            canvas.configure(scrollregion=canvas.bbox("all"))

        inner.bind("<Configure>", _sync_scrollregion)
        canvas.bind("<Configure>", _on_canvas_resize)

        container.rowconfigure(0, weight=1)
        container.columnconfigure(0, weight=1)
        canvas.grid(row=0, column=0, sticky="nsew")
        ybar.grid(row=0, column=1, sticky="ns")
        xbar.grid(row=1, column=0, sticky="ew")

        self.nb.add(container, text=title)
        self._tab_scroll_canvases[key] = canvas
        return inner

    def _build_status_bar(self) -> None:
        bar = ttk.Frame(self.root, padding=(8, 4))
        bar.pack(side="bottom", fill="x")
        self.status_var = tk.StringVar(value="Ready")
        self.status_label = ttk.Label(bar, textvariable=self.status_var)
        self.status_label.pack(side="left")
        ttk.Button(bar, text="Tasks", command=self._show_task_status).pack(side="right", padx=(8, 0))
        self.status_progress = ttk.Progressbar(bar, mode="indeterminate", length=180)
        self.status_progress.pack(side="right")

    def _build_live_tab(self) -> None:
        split = self._create_paned(self.live_tab, orient="horizontal")
        left = ttk.Frame(split, padding=8)
        right = ttk.Frame(split, padding=8)
        split.add(left, weight=1)
        split.add(right, weight=3)

        ttk.Label(left, text="Panels").pack(anchor="w")
        self.panel_list = tk.Listbox(left, width=40, height=18, selectmode="extended")
        self.panel_list.pack(fill="y", pady=(4, 8))
        self.panel_list.bind("<<ListboxSelect>>", lambda e: self._load_selected_panel_to_form())

        self.market_var = tk.StringVar(value="crypto")
        self.timeframe_var = tk.StringVar(value="1d")
        self.quote_var = tk.StringVar(value="USD")
        self.topn_var = tk.StringVar(value="20")
        self.country_var = tk.StringVar(value=country_code_to_display("2"))
        self.country_manual_var = tk.StringVar(value="")
        self.panel_name_var = tk.StringVar(value="Panel")

        form = ttk.LabelFrame(left, text="Panel Config", padding=8)
        form.pack(fill="x")
        self._labeled_entry(form, "Name", self.panel_name_var)
        self._labeled_combo(form, "Market", self.market_var, ["crypto", "traditional"])
        self._labeled_combo(form, "Timeframe", self.timeframe_var, ["1d", "4h", "8h", "12h"])
        self._labeled_combo(form, "Quote (crypto)", self.quote_var, ["USD", "USDT", "BTC", "ETH", "BNB"])
        self._labeled_combo(form, "Top N", self.topn_var, ["10", "20", "50", "100"])
        self.country_combo_live = self._labeled_combo(
            form,
            "Country (trad)",
            self.country_var,
            country_display_values(),
            state="normal",
            filterable=True,
        )
        self._labeled_entry(form, "Manual code", self.country_manual_var)

        btns = ttk.Frame(left)
        btns.pack(fill="x", pady=8)
        ttk.Button(btns, text="Add Panel", command=self._add_panel).pack(fill="x")
        ttk.Button(btns, text="Update Panel", command=self._update_panel).pack(fill="x", pady=2)
        ttk.Button(btns, text="Remove Panel", command=self._remove_panel).pack(fill="x")
        self.btn_run_selected = ttk.Button(btns, text="Run Selected", command=self._run_selected_panel)
        self.btn_run_selected.pack(fill="x", pady=(8, 2))
        self.btn_run_all = ttk.Button(btns, text="Run All Panels", command=self._run_all_panels)
        self.btn_run_all.pack(fill="x")

        auto = ttk.LabelFrame(left, text="Auto Refresh", padding=8)
        auto.pack(fill="x", pady=8)
        self.refresh_secs_var = tk.StringVar(value=str(self.state.get("auto_refresh_seconds", 120)))
        self._labeled_entry(auto, "Seconds", self.refresh_secs_var)
        ttk.Button(auto, text="Start Auto", command=self._start_auto_refresh).pack(fill="x")
        ttk.Button(auto, text="Stop Auto", command=self._stop_auto_refresh).pack(fill="x", pady=2)

        presets = ttk.LabelFrame(left, text="Dashboard Presets", padding=8)
        presets.pack(fill="x", pady=8)
        self.live_profile_name_var = tk.StringVar(value="default")
        self._labeled_entry(presets, "Profile Name", self.live_profile_name_var)
        ttk.Button(presets, text="Save/Update from Selected Panels", command=self._save_live_profile_from_selected).pack(fill="x", pady=2)
        ttk.Button(presets, text="Save/Update from All Panels", command=self._save_live_profile_from_all).pack(fill="x", pady=2)
        ttk.Label(presets, text="Saved Profiles (multi-select)").pack(anchor="w", pady=(6, 2))
        self.live_profile_list = tk.Listbox(presets, height=5, selectmode="extended")
        self.live_profile_list.pack(fill="x")
        ttk.Button(presets, text="Load Selected Profile (Replace)", command=self._load_selected_profile_replace).pack(fill="x", pady=2)
        ttk.Button(presets, text="Merge Selected Profiles (Append)", command=self._merge_selected_profiles).pack(fill="x", pady=2)
        ttk.Button(presets, text="Delete Selected Profiles", command=self._delete_selected_profiles).pack(fill="x", pady=2)
        self._refresh_saved_profile_list()

        live_frame, self.live_output = self._create_scrolled_text(right, wrap="none")
        live_frame.pack(fill="both", expand=True)
        self._configure_dashboard_tags(self.live_output)
        live_copy_row = ttk.Frame(right)
        live_copy_row.pack(fill="x", pady=(6, 0))
        ttk.Button(live_copy_row, text="Copy Output", command=lambda: self._copy_text_widget(self.live_output, "Live output")).pack(side="left")
        ttk.Button(live_copy_row, text="Clear Output", command=lambda: self.live_output.delete("1.0", tk.END)).pack(side="left", padx=6)

    def _build_backtest_tab(self) -> None:
        split = self._create_paned(self.backtest_tab, orient="vertical")
        top = ttk.Frame(split, padding=8)
        out = ttk.Frame(split, padding=8)
        split.add(top, weight=1)
        split.add(out, weight=3)

        self.bt_market = tk.StringVar(value="crypto")
        self.bt_tf = tk.StringVar(value="1d")
        self.bt_months = tk.StringVar(value="12")
        self.bt_topn = tk.StringVar(value="20")
        self.bt_quote = tk.StringVar(value="USD")
        self.bt_country = tk.StringVar(value=country_code_to_display("2"))
        self.bt_country_manual = tk.StringVar(value="")
        self.bt_initial = tk.StringVar(value="10000")

        self._labeled_combo(top, "Market", self.bt_market, ["crypto", "traditional"])
        self._labeled_combo(top, "Timeframe", self.bt_tf, ["1d", "4h", "8h", "12h"])
        self._labeled_combo(top, "Months", self.bt_months, ["1", "3", "6", "12", "18", "24"])
        self._labeled_combo(top, "Top N", self.bt_topn, ["10", "20", "50", "100"])
        self._labeled_combo(top, "Quote (crypto)", self.bt_quote, ["USD", "USDT", "BTC", "ETH", "BNB"])
        self.bt_country_combo = self._labeled_combo(
            top,
            "Country (trad)",
            self.bt_country,
            country_display_values(),
            state="normal",
            filterable=True,
        )
        self._labeled_entry(top, "Manual code", self.bt_country_manual)
        self._labeled_entry(top, "Initial USD", self.bt_initial)
        self.bt_stop_loss = tk.StringVar(value="8")
        self.bt_take_profit = tk.StringVar(value="20")
        self.bt_max_hold_days = tk.StringVar(value="45")
        self.bt_min_hold_bars = tk.StringVar(value="2")
        self.bt_cooldown_bars = tk.StringVar(value="1")
        self.bt_same_asset_cooldown = tk.StringVar(value="3")
        self.bt_max_same_asset_entries = tk.StringVar(value="3")
        self.bt_fee_pct = tk.StringVar(value="0.10")
        self.bt_slippage_pct = tk.StringVar(value="0.05")
        self.bt_position_size_pct = tk.StringVar(value="30")
        self.bt_atr_mult = tk.StringVar(value="2.2")
        self.bt_adx_threshold = tk.StringVar(value="25")
        self.bt_cmf_threshold = tk.StringVar(value="0.02")
        self.bt_obv_threshold = tk.StringVar(value="0")
        self.bt_max_dd_target_pct = tk.StringVar(value="35")
        self.bt_max_exposure_pct = tk.StringVar(value="40")
        self.bt_cache_workers = tk.StringVar(value="8")
        self.bt_buy_threshold = tk.StringVar(value="2")
        self.bt_sell_threshold = tk.StringVar(value="-2")

        self._labeled_entry(top, "Stop loss %", self.bt_stop_loss)
        self._labeled_entry(top, "Take profit %", self.bt_take_profit)
        self._labeled_entry(top, "Max hold days", self.bt_max_hold_days)
        self._labeled_entry(top, "Min hold bars", self.bt_min_hold_bars)
        self._labeled_entry(top, "Cooldown bars", self.bt_cooldown_bars)
        self._labeled_entry(top, "Re-entry cooldown", self.bt_same_asset_cooldown)
        self._labeled_entry(top, "Max same-asset entries", self.bt_max_same_asset_entries)
        self._labeled_entry(top, "Fee % per leg", self.bt_fee_pct)
        self._labeled_entry(top, "Slippage % per leg", self.bt_slippage_pct)
        self._labeled_entry(top, "Position size %", self.bt_position_size_pct)
        self._labeled_entry(top, "ATR multiplier", self.bt_atr_mult)
        self._labeled_entry(top, "ADX threshold", self.bt_adx_threshold)
        self._labeled_entry(top, "CMF threshold", self.bt_cmf_threshold)
        self._labeled_entry(top, "OBV slope threshold", self.bt_obv_threshold)
        self._labeled_entry(top, "Max DD target %", self.bt_max_dd_target_pct)
        self._labeled_entry(top, "Max exposure %", self.bt_max_exposure_pct)
        self._labeled_entry(top, "Cache workers", self.bt_cache_workers)
        self._labeled_entry(top, "Buy threshold", self.bt_buy_threshold)
        self._labeled_entry(top, "Sell threshold", self.bt_sell_threshold)

        self.btn_run_backtest = ttk.Button(top, text="Run Backtest", command=self._run_backtest)
        self.btn_run_backtest.pack(side="left", padx=8)

        bt_summary_frame, self.bt_summary = self._create_scrolled_text(out, height=10, wrap="none")
        bt_summary_frame.pack(fill="x")
        self._configure_dashboard_tags(self.bt_summary)
        bt_btns = ttk.Frame(out)
        bt_btns.pack(fill="x", pady=(4, 4))
        ttk.Button(bt_btns, text="Copy Summary", command=lambda: self._copy_text_widget(self.bt_summary, "Backtest summary")).pack(side="left")
        bt_trades_frame, self.bt_trades = self._create_scrolled_text(out, wrap="none")
        bt_trades_frame.pack(fill="both", expand=True, pady=(8, 0))
        self._configure_dashboard_tags(self.bt_trades)
        ttk.Button(bt_btns, text="Copy Trades", command=lambda: self._copy_text_widget(self.bt_trades, "Backtest trades")).pack(side="left", padx=6)

    def _build_ai_tab(self) -> None:
        split = self._create_paned(self.ai_tab, orient="vertical")
        top = ttk.Frame(split, padding=8)
        body = ttk.Frame(split, padding=8)
        split.add(top, weight=1)
        split.add(body, weight=3)

        self.ai_source = tk.StringVar(value="live")
        self.ai_datetime = tk.StringVar(value=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        self.ai_prompt_mode = tk.StringVar(value="preset_dashboard")
        self.ai_require_confirm = tk.BooleanVar(value=False)
        self.ai_auto_stage_var = tk.BooleanVar(value=True)
        self.ai_log_signals_var = tk.BooleanVar(value=True)
        self.pipeline_interval_min_var = tk.StringVar(value="30")
        self.ai_backtest_path = tk.StringVar(
            value=str(self.repo_root / "experiments" / "backtest_snapshots" / "latest_backtest.txt")
        )
        ttk.Label(top, text="Date/Time Context").pack(side="left")
        ttk.Entry(top, textvariable=self.ai_datetime, width=24).pack(side="left", padx=4)
        ttk.Label(top, text="Source").pack(side="left", padx=(12, 0))
        ttk.Combobox(
            top,
            textvariable=self.ai_source,
            values=["live", "live_all_panels", "backtest_latest", "backtest_file", "paste"],
            width=16,
            state="readonly",
        ).pack(side="left", padx=4)
        ttk.Label(top, text="Prompt").pack(side="left", padx=(12, 0))
        ttk.Combobox(
            top,
            textvariable=self.ai_prompt_mode,
            values=["preset_dashboard", "custom_prompt"],
            width=16,
            state="readonly",
        ).pack(side="left", padx=4)
        ttk.Checkbutton(top, text="Confirm before send", variable=self.ai_require_confirm).pack(side="left", padx=(6, 0))
        ttk.Checkbutton(top, text="Auto-stage signals", variable=self.ai_auto_stage_var).pack(side="left", padx=(6, 0))
        ttk.Checkbutton(top, text="Log AI signals to ledger", variable=self.ai_log_signals_var).pack(side="left", padx=(6, 0))
        self.btn_run_ai = ttk.Button(top, text="Run AI Analysis", command=self._run_ai_analysis)
        self.btn_run_ai.pack(side="left", padx=8)
        ttk.Button(top, text="Show Prompt", command=self._preview_ai_prompt).pack(side="left")
        ttk.Button(top, text="Stage Recommendations", command=self._stage_ai_recommendations).pack(side="left", padx=6)
        ttk.Button(top, text="Clear Pending", command=self._clear_pending_recommendations).pack(side="left")
        ttk.Label(top, text="Pipeline min").pack(side="left", padx=(10, 0))
        ttk.Entry(top, textvariable=self.pipeline_interval_min_var, width=6).pack(side="left", padx=4)
        ttk.Button(top, text="Run Live->AI Pipeline", command=self._run_live_ai_pipeline_now).pack(side="left", padx=4)
        ttk.Button(top, text="Run Live->Backtest->AI", command=self._run_live_backtest_ai_pipeline).pack(side="left", padx=4)
        ttk.Button(top, text="Start Pipeline Scheduler", command=self._start_pipeline_scheduler).pack(side="left", padx=4)
        ttk.Button(top, text="Stop Scheduler", command=self._stop_pipeline_scheduler).pack(side="left", padx=4)

        ai_input_frame, self.ai_input = self._create_scrolled_text(body, height=10, wrap="none")
        ai_input_frame.pack(fill="x")
        ai_file_row = ttk.Frame(body)
        ai_file_row.pack(fill="x", pady=(6, 0))
        ttk.Label(ai_file_row, text="Backtest file").pack(side="left")
        ttk.Entry(ai_file_row, textvariable=self.ai_backtest_path, width=90).pack(side="left", padx=6, fill="x", expand=True)
        ttk.Button(ai_file_row, text="Browse...", command=self._browse_ai_backtest_file).pack(side="left")
        ttk.Label(body, text="Custom prompt (used when Prompt=custom_prompt)").pack(anchor="w", pady=(8, 0))
        ai_custom_frame, self.ai_custom_prompt = self._create_scrolled_text(body, height=8, wrap="none")
        ai_custom_frame.pack(fill="x")
        follow_row = ttk.Frame(body)
        follow_row.pack(fill="x", pady=(8, 0))
        self.ai_followup_var = tk.StringVar(value="")
        ttk.Label(follow_row, text="Follow-up").pack(side="left")
        ttk.Entry(follow_row, textvariable=self.ai_followup_var, width=100).pack(side="left", padx=6, fill="x", expand=True)
        ttk.Button(follow_row, text="Send Follow-up", command=self._run_ai_followup).pack(side="left")
        ai_output_frame, self.ai_output = self._create_scrolled_text(body, wrap="none")
        ai_output_frame.pack(fill="both", expand=True, pady=(8, 0))
        ai_btns = ttk.Frame(body)
        ai_btns.pack(fill="x", pady=(6, 0))
        ttk.Button(ai_btns, text="Copy AI Output", command=lambda: self._copy_text_widget(self.ai_output, "AI output")).pack(side="left")
        ttk.Button(ai_btns, text="Clear AI Output", command=lambda: self.ai_output.delete("1.0", tk.END)).pack(side="left", padx=6)

    def _build_agent_tab(self) -> None:
        split = self._create_paned(self.agent_tab, orient="vertical")
        top = ttk.Frame(split, padding=8)
        body = ttk.Frame(split, padding=8)
        split.add(top, weight=1)
        split.add(body, weight=3)

        self.agent_mode_var = tk.StringVar(value="plan")
        self.agent_cmd_var = tk.StringVar(value="find me the best buys across the top 10 crypto coins on 4h and keep risk tight")
        self.agent_ai_fallback_var = tk.BooleanVar(value=bool(self.state.get("agent_ai_fallback_enabled", True)))
        self.agent_guard_enabled_var = tk.BooleanVar(value=bool(self.state.get("agent_guard_enabled", True)))
        self.agent_guard_require_stop_var = tk.BooleanVar(value=bool(self.state.get("agent_guard_require_stop", True)))
        self.agent_guard_max_daily_loss_var = tk.StringVar(value=str(self.state.get("agent_guard_max_daily_loss_pct", 5.0)))
        self.agent_guard_max_trades_var = tk.StringVar(value=str(self.state.get("agent_guard_max_trades_per_day", 8)))
        self.agent_guard_max_exposure_var = tk.StringVar(value=str(self.state.get("agent_guard_max_exposure_pct", 40.0)))
        ttk.Label(top, text="Mode").pack(side="left")
        ttk.Combobox(top, textvariable=self.agent_mode_var, values=["plan", "semi_auto", "auto_execute"], width=12, state="readonly").pack(side="left", padx=4)
        ttk.Label(top, text="Command").pack(side="left", padx=(10, 0))
        ttk.Entry(top, textvariable=self.agent_cmd_var, width=120).pack(side="left", padx=6, fill="x", expand=True)
        ttk.Button(top, text="Run Command", command=self._run_agent_command).pack(side="left", padx=6)
        ttk.Checkbutton(top, text="AI fallback", variable=self.agent_ai_fallback_var).pack(side="left", padx=(6, 0))

        guard = ttk.LabelFrame(self.agent_tab, text="Agent Guardrails", padding=8)
        guard.pack(fill="x", padx=8, pady=(0, 8))
        ttk.Checkbutton(guard, text="Enable guardrails", variable=self.agent_guard_enabled_var).pack(side="left", padx=(0, 10))
        ttk.Checkbutton(guard, text="Require stop for BUY execution", variable=self.agent_guard_require_stop_var).pack(side="left", padx=(0, 10))
        ttk.Label(guard, text="Max daily loss %").pack(side="left")
        ttk.Entry(guard, textvariable=self.agent_guard_max_daily_loss_var, width=6).pack(side="left", padx=4)
        ttk.Label(guard, text="Max trades/day").pack(side="left")
        ttk.Entry(guard, textvariable=self.agent_guard_max_trades_var, width=6).pack(side="left", padx=4)
        ttk.Label(guard, text="Max exposure %").pack(side="left")
        ttk.Entry(guard, textvariable=self.agent_guard_max_exposure_var, width=6).pack(side="left", padx=4)

        agent_frame, self.agent_output = self._create_scrolled_text(body, wrap="none")
        agent_frame.pack(fill="both", expand=True)
        btns = ttk.Frame(body)
        btns.pack(fill="x", pady=(6, 0))
        ttk.Button(btns, text="Copy Output", command=lambda: self._copy_text_widget(self.agent_output, "Agent output")).pack(side="left")
        ttk.Button(btns, text="Clear Output", command=lambda: self.agent_output.delete("1.0", tk.END)).pack(side="left", padx=6)
        ttk.Button(btns, text="Execute Last Staged", command=self._execute_last_agent_staged).pack(side="left", padx=6)
        ttk.Button(btns, text="Show Agent Status", command=self._show_agent_status).pack(side="left", padx=6)

    def _build_portfolio_tab(self) -> None:
        top = ttk.Frame(self.portfolio_tab, padding=8)
        top.pack(fill="x")
        body = ttk.Frame(self.portfolio_tab, padding=8)
        body.pack(fill="both", expand=True)

        self.pf_binance_profile_var = tk.StringVar(value="")
        self.pf_exec_mode_var = tk.StringVar(value="semi_auto")
        self.pf_quote_var = tk.StringVar(value="USDT")
        self.pf_cooldown_min_var = tk.StringVar(value="240")
        self.pf_track_hold_var = tk.BooleanVar(value=True)
        self.pf_guard_hold_var = tk.BooleanVar(value=False)
        self.pf_manual_asset_var = tk.StringVar(value="")
        self.pf_manual_action_var = tk.StringVar(value="BUY")
        self.pf_manual_price_var = tk.StringVar(value="")
        self.pf_manual_qty_var = tk.StringVar(value="")
        self.pf_manual_tf_var = tk.StringVar(value="1d")
        self.pf_manual_note_var = tk.StringVar(value="")
        self.pf_pending_qty_var = tk.StringVar(value="0")
        self.pf_pending_type_var = tk.StringVar(value="MARKET")
        self.pf_auto_buy_pct_var = tk.StringVar(value="10")
        self.pf_auto_sell_pct_var = tk.StringVar(value="100")
        self.pf_auto_confidence_var = tk.BooleanVar(value=True)
        self.pf_protect_interval_min_var = tk.StringVar(value="30")
        self.pf_protect_auto_send_var = tk.BooleanVar(value=False)
        self.pf_auto_refresh_var = tk.BooleanVar(value=bool(self.state.get("portfolio_auto_refresh_enabled", True)))
        self.pf_auto_refresh_secs_var = tk.StringVar(value=str(self.state.get("portfolio_auto_refresh_seconds", 45)))

        ttk.Label(top, text="Binance Profile").pack(side="left")
        self.pf_binance_profile_combo = ttk.Combobox(top, textvariable=self.pf_binance_profile_var, values=[], width=22, state="readonly")
        self.pf_binance_profile_combo.pack(side="left", padx=4)
        ttk.Label(top, text="Mode").pack(side="left")
        ttk.Combobox(top, textvariable=self.pf_exec_mode_var, values=["manual", "semi_auto", "full_auto"], width=10, state="readonly").pack(side="left", padx=4)
        ttk.Label(top, text="Quote").pack(side="left")
        ttk.Combobox(top, textvariable=self.pf_quote_var, values=["USDT", "USD", "BTC", "ETH", "BNB"], width=8, state="readonly").pack(side="left", padx=4)
        ttk.Button(top, text="Refresh Profiles", command=self._refresh_binance_profiles).pack(side="left", padx=(4, 8))
        ttk.Button(top, text="Refresh Portfolio", command=self._refresh_portfolio).pack(side="left")
        ttk.Button(top, text="Reconcile Fills", command=self._reconcile_binance_fills).pack(side="left", padx=(6, 0))
        ttk.Button(top, text="Review Open Positions (MTF)", command=self._review_open_positions_mtf).pack(side="left", padx=(6, 0))
        ttk.Button(top, text="Protect Open Positions (AI+BT)", command=self._run_protect_open_positions_ai).pack(side="left", padx=(6, 0))
        ttk.Label(top, text="Protect every (min)").pack(side="left", padx=(10, 0))
        ttk.Entry(top, textvariable=self.pf_protect_interval_min_var, width=6).pack(side="left", padx=4)
        ttk.Checkbutton(top, text="Auto-send protection", variable=self.pf_protect_auto_send_var).pack(side="left", padx=(6, 0))
        ttk.Button(top, text="Start Protect Monitor", command=self._start_protection_monitor).pack(side="left", padx=(6, 0))
        ttk.Button(top, text="Stop Protect Monitor", command=self._stop_protection_monitor).pack(side="left", padx=(6, 0))
        ttk.Label(top, text="Signal Cooldown (min)").pack(side="left", padx=(12, 0))
        ttk.Entry(top, textvariable=self.pf_cooldown_min_var, width=8).pack(side="left", padx=4)
        ttk.Checkbutton(top, text="Track HOLD signals", variable=self.pf_track_hold_var).pack(side="left", padx=(8, 0))
        ttk.Checkbutton(top, text="Guard HOLD duplicates", variable=self.pf_guard_hold_var).pack(side="left", padx=(8, 0))
        ttk.Button(top, text="Import Signals from Live", command=self._import_signals_from_live).pack(side="left", padx=8)
        ttk.Button(top, text="Refresh Ledger", command=self._refresh_ledger_view).pack(side="left")
        ttk.Button(top, text="Prune Signal History", command=self._prune_signal_history).pack(side="left", padx=(6, 0))
        ttk.Label(top, text="Auto refresh (s)").pack(side="left", padx=(12, 0))
        ttk.Entry(top, textvariable=self.pf_auto_refresh_secs_var, width=6).pack(side="left", padx=4)
        ttk.Checkbutton(top, text="While on this tab", variable=self.pf_auto_refresh_var).pack(side="left", padx=(4, 0))
        ttk.Button(top, text="Refresh All Now", command=self._refresh_portfolio_suite).pack(side="left", padx=(6, 0))

        pending_frame = ttk.LabelFrame(body, text="Pending Recommendations (Review & Approve)", padding=8)
        pending_frame.pack(fill="x", expand=False, padx=8, pady=(0, 8))
        pending_tree_frame, self.pending_tree = self._create_scrolled_tree(
            pending_frame,
            columns=("id", "symbol", "side", "type", "qty", "stop", "tf", "conf", "status", "reason"),
            show="headings",
            height=6,
            selectmode="extended",
        )
        for c, w in [
            ("id", 46),
            ("symbol", 90),
            ("side", 60),
            ("type", 70),
            ("qty", 80),
            ("stop", 90),
            ("tf", 50),
            ("conf", 60),
            ("status", 90),
            ("reason", 520),
        ]:
            self.pending_tree.heading(c, text=c.upper())
            self.pending_tree.column(c, width=w, anchor="w")
        pending_tree_frame.pack(fill="x", expand=False)
        pending_btns_row1 = ttk.Frame(pending_frame)
        pending_btns_row1.pack(fill="x", pady=(6, 2))
        ttk.Label(pending_btns_row1, text="Set qty").pack(side="left")
        ttk.Entry(pending_btns_row1, textvariable=self.pf_pending_qty_var, width=10).pack(side="left", padx=4)
        ttk.Label(pending_btns_row1, text="type").pack(side="left")
        ttk.Combobox(pending_btns_row1, textvariable=self.pf_pending_type_var, values=["MARKET", "LIMIT", "STOP_LOSS_LIMIT"], width=14, state="readonly").pack(side="left", padx=4)
        ttk.Button(pending_btns_row1, text="Apply to Selected", command=self._apply_pending_edit_to_selected).pack(side="left", padx=6)
        ttk.Label(pending_btns_row1, text="Auto BUY %").pack(side="left", padx=(10, 0))
        ttk.Entry(pending_btns_row1, textvariable=self.pf_auto_buy_pct_var, width=6).pack(side="left", padx=4)
        ttk.Label(pending_btns_row1, text="Auto SELL %").pack(side="left")
        ttk.Entry(pending_btns_row1, textvariable=self.pf_auto_sell_pct_var, width=6).pack(side="left", padx=4)
        ttk.Checkbutton(pending_btns_row1, text="Weight by confidence", variable=self.pf_auto_confidence_var).pack(side="left", padx=(6, 0))
        ttk.Button(pending_btns_row1, text="Auto-size Selected", command=self._auto_size_selected_pending_orders).pack(side="left", padx=6)
        pending_btns_row2 = ttk.Frame(pending_frame)
        pending_btns_row2.pack(fill="x")
        ttk.Button(pending_btns_row2, text="Submit Selected", command=self._submit_selected_pending_orders).pack(side="left")
        ttk.Button(pending_btns_row2, text="Remove Selected", command=self._remove_selected_pending_orders).pack(side="left", padx=6)
        ttk.Button(pending_btns_row2, text="Clear All", command=self._clear_pending_recommendations).pack(side="left")

        manual = ttk.LabelFrame(body, text="Manual Ledger Event", padding=8)
        manual.pack(fill="x", padx=8, pady=(0, 8))
        self._labeled_entry(manual, "Asset", self.pf_manual_asset_var)
        self._labeled_combo(manual, "Action", self.pf_manual_action_var, ["BUY", "SELL", "HOLD"], state="readonly")
        self._labeled_combo(manual, "Timeframe", self.pf_manual_tf_var, ["1d", "4h", "8h", "12h"], state="readonly")
        self._labeled_entry(manual, "Price", self.pf_manual_price_var)
        self._labeled_entry(manual, "Qty", self.pf_manual_qty_var)
        self._labeled_entry(manual, "Note", self.pf_manual_note_var)
        ttk.Button(manual, text="Add Manual Event", command=self._add_manual_ledger_event).pack(fill="x", pady=(6, 0))

        orders = ttk.LabelFrame(body, text="Open Binance Orders (Cancel via GUI)", padding=8)
        orders.pack(fill="x", expand=False, padx=8, pady=(0, 8))
        order_top = ttk.Frame(orders)
        order_top.pack(fill="x")
        self.pf_open_symbol_filter_var = tk.StringVar(value="")
        ttk.Label(order_top, text="Symbol filter").pack(side="left")
        ttk.Entry(order_top, textvariable=self.pf_open_symbol_filter_var, width=14).pack(side="left", padx=4)
        ttk.Button(order_top, text="Refresh Open Orders", command=self._refresh_open_orders).pack(side="left")
        ttk.Button(order_top, text="Cancel Selected", command=self._cancel_selected_open_orders).pack(side="left", padx=6)
        open_orders_frame, self.open_orders_tree = self._create_scrolled_tree(
            orders,
            columns=("symbol", "orderId", "side", "type", "status", "price", "origQty", "executedQty"),
            show="headings",
            height=6,
            selectmode="extended",
        )
        for c, w in [
            ("symbol", 90),
            ("orderId", 100),
            ("side", 60),
            ("type", 70),
            ("status", 90),
            ("price", 100),
            ("origQty", 90),
            ("executedQty", 100),
        ]:
            self.open_orders_tree.heading(c, text=c)
            self.open_orders_tree.column(c, width=w, anchor="w")
        open_orders_frame.pack(fill="x", expand=False, pady=(6, 0))

        cols = ttk.Frame(body)
        cols.pack(fill="both", expand=False, pady=(0, 8))
        cols.configure(height=220)
        cols.pack_propagate(False)
        left = ttk.LabelFrame(cols, text="Current Portfolio (Binance)", padding=6)
        right = ttk.LabelFrame(cols, text="Open Positions (Ledger)", padding=6)
        left.pack(side="left", fill="both", expand=True, padx=(0, 6))
        right.pack(side="left", fill="both", expand=True)

        pf_portfolio_frame, self.pf_portfolio_text = self._create_scrolled_text(left, wrap="none", height=8)
        pf_portfolio_frame.pack(fill="both", expand=True)
        self._configure_dashboard_tags(self.pf_portfolio_text)

        pf_open_frame, self.pf_open_positions_text = self._create_scrolled_text(right, wrap="none", height=8)
        pf_open_frame.pack(fill="both", expand=True)
        self._configure_dashboard_tags(self.pf_open_positions_text)

        bottom = ttk.LabelFrame(body, text="Trade Ledger (Signal + Execution Views)", padding=6)
        bottom.pack(fill="both", expand=True, pady=(8, 0))
        ledger_nb = ttk.Notebook(bottom)
        ledger_nb.pack(fill="both", expand=True)
        tab_overall = ttk.Frame(ledger_nb)
        tab_signal = ttk.Frame(ledger_nb)
        tab_exec = ttk.Frame(ledger_nb)
        ledger_nb.add(tab_overall, text="Overall")
        ledger_nb.add(tab_signal, text="Signal Journal")
        ledger_nb.add(tab_exec, text="Execution Ledger")
        pf_ledger_frame, self.pf_ledger_text = self._create_scrolled_text(tab_overall, wrap="none")
        pf_ledger_frame.pack(fill="both", expand=True)
        self._configure_dashboard_tags(self.pf_ledger_text)
        pf_signal_frame, self.pf_signal_text = self._create_scrolled_text(tab_signal, wrap="none")
        pf_signal_frame.pack(fill="both", expand=True)
        self._configure_dashboard_tags(self.pf_signal_text)
        pf_exec_frame, self.pf_execution_text = self._create_scrolled_text(tab_exec, wrap="none")
        pf_exec_frame.pack(fill="both", expand=True)
        self._configure_dashboard_tags(self.pf_execution_text)

        self._refresh_binance_profiles()
        self._refresh_ledger_view()
        self._refresh_pending_recommendations_view()
        self.root.after(300, self._refresh_portfolio_suite)

    def _build_research_tab(self) -> None:
        split = self._create_paned(self.research_tab, orient="vertical")
        top = ttk.Frame(split, padding=8)
        split.add(top, weight=1)
        self.rs_market_scope = tk.StringVar(value="both")
        self.rs_quote = tk.StringVar(value="USD")
        self.rs_country = tk.StringVar(value=country_code_to_display("2"))
        self.rs_country_manual = tk.StringVar(value="")
        self.rs_trials = tk.StringVar(value="10")
        self.rs_jobs = tk.StringVar(value="4")

        self._labeled_combo(top, "Scope", self.rs_market_scope, ["crypto", "traditional", "both"])
        self._labeled_combo(top, "Quote (crypto)", self.rs_quote, ["USD", "USDT", "BTC", "ETH", "BNB"])
        self.rs_country_combo = self._labeled_combo(
            top,
            "Country (trad)",
            self.rs_country,
            country_display_values(),
            state="normal",
            filterable=True,
        )
        self._labeled_entry(top, "Manual code", self.rs_country_manual)
        self._labeled_entry(top, "Trials", self.rs_trials)
        self._labeled_entry(top, "Jobs", self.rs_jobs)
        self.btn_run_research_std = ttk.Button(top, text="Run Standard", command=self._run_standard_research)
        self.btn_run_research_std.pack(side="left", padx=8)
        self.btn_run_research_comp = ttk.Button(top, text="Run Comprehensive", command=self._run_comprehensive_research)
        self.btn_run_research_comp.pack(side="left")

        rs_host = ttk.Frame(split, padding=8)
        split.add(rs_host, weight=3)
        rs_frame, self.rs_output = self._create_scrolled_text(rs_host, wrap="none")
        rs_frame.pack(fill="both", expand=True, padx=8, pady=8)

    def _build_task_tab(self) -> None:
        split = self._create_paned(self.task_tab, orient="vertical")
        top = ttk.Frame(split, padding=8)
        body = ttk.Frame(split, padding=8)
        split.add(top, weight=1)
        split.add(body, weight=4)

        ttk.Button(top, text="Refresh", command=self._refresh_task_tab).pack(side="left")
        self.btn_pause_queue = ttk.Button(top, text="Pause Queue", command=self._toggle_queue_pause)
        self.btn_pause_queue.pack(side="left", padx=4)
        ttk.Button(top, text="Stop/Remove Selected", command=self._stop_selected_task).pack(side="left")
        ttk.Button(top, text="Move Up", command=lambda: self._reprioritize_queue(-1)).pack(side="left", padx=4)
        ttk.Button(top, text="Move Down", command=lambda: self._reprioritize_queue(1)).pack(side="left")

        cols = ttk.Frame(body)
        cols.pack(fill="both", expand=True)
        left = ttk.LabelFrame(cols, text="Running", padding=6)
        right = ttk.LabelFrame(cols, text="Queued", padding=6)
        cols_split = self._create_paned(cols, orient="horizontal")
        cols_split.pack(fill="both", expand=True)
        cols_split.add(left, weight=1)
        cols_split.add(right, weight=1)

        self.running_list = tk.Listbox(left, height=12)
        self.running_list.pack(fill="both", expand=True)
        self.queued_list = tk.Listbox(right, height=12)
        self.queued_list.pack(fill="both", expand=True)

        logs_split = self._create_paned(body, orient="vertical")
        logs_split.pack(fill="both", expand=True, pady=(8, 0))
        task_out_frame, self.task_tab_output = self._create_scrolled_text(logs_split, height=4, wrap="none")
        logs_split.add(task_out_frame, weight=1)
        task_term_frame, self.task_terminal = self._create_scrolled_text(
            logs_split,
            height=12,
            wrap="none",
            bg="#101315",
            fg="#9CF5C6",
            insertbackground="#9CF5C6",
        )
        logs_split.add(task_term_frame, weight=3)
        self.task_terminal.insert("1.0", "STRATA Task Terminal\n")
        term_btns = ttk.Frame(body)
        term_btns.pack(fill="x", pady=(6, 0))
        ttk.Button(term_btns, text="Copy Terminal", command=lambda: self._copy_text_widget(self.task_terminal, "Task terminal")).pack(side="left")
        ttk.Button(term_btns, text="Clear Terminal", command=lambda: self.task_terminal.delete("1.0", tk.END)).pack(side="left", padx=6)
        self._refresh_task_tab()
        self._schedule_task_tab_refresh()

    def _build_settings_tab(self) -> None:
        split = self._create_paned(self.settings_tab, orient="vertical")
        frame = ttk.Frame(split, padding=8)
        split.add(frame, weight=3)

        self.display_currency_var = tk.StringVar(value=self.state.get("display_currency", "USD"))
        self._labeled_combo(frame, "Display Currency", self.display_currency_var, ["USD", "AUD", "EUR", "GBP", "CAD", "JPY", "NZD", "SGD", "HKD", "CHF"])
        self.primary_quote_var = tk.StringVar(value=self.state.get("primary_quote_asset", "USDT"))
        self.quote_lock_var = tk.BooleanVar(value=bool(self.state.get("lock_primary_quote", False)))
        self._labeled_combo(frame, "Primary Quote", self.primary_quote_var, ["USDT", "USDC", "BUSD", "FDUSD", "USD", "BTC", "ETH", "BNB"])
        ttk.Checkbutton(frame, text="Lock primary quote across crypto dashboards/trades", variable=self.quote_lock_var).pack(anchor="w", pady=(2, 6))
        self.parallel_mode_var = tk.BooleanVar(value=bool(self.state.get("parallel_mode_enabled", False)))
        self.parallel_jobs_var = tk.StringVar(value=str(self.state.get("parallel_max_jobs", 2)))
        self.verbose_logging_var = tk.BooleanVar(value=bool(self.state.get("verbose_terminal_logging", True)))
        ttk.Checkbutton(
            frame,
            text="Advanced Mode: Parallel jobs (experimental)",
            variable=self.parallel_mode_var,
            command=lambda: self._update_run_controls_and_status(),
        ).pack(anchor="w", pady=(8, 2))
        ttk.Checkbutton(
            frame,
            text="Verbose terminal logging (debug mode)",
            variable=self.verbose_logging_var,
        ).pack(anchor="w", pady=(2, 2))
        self._labeled_entry(frame, "Max parallel jobs", self.parallel_jobs_var)

        ai_frame = ttk.LabelFrame(frame, text="AI Profiles", padding=8)
        ai_frame.pack(fill="x", pady=8)
        self.ai_profile_var = tk.StringVar(value="")
        self.ai_provider_var = tk.StringVar(value="xai")
        self.ai_model_var = tk.StringVar(value="")
        self.ai_endpoint_var = tk.StringVar(value="")
        self.ai_internet_var = tk.BooleanVar(value=True)
        self.ai_temp_var = tk.StringVar(value="0.2")
        self.ai_key_var = tk.StringVar(value="")

        self.ai_profile_combo = self._labeled_combo(ai_frame, "Profile", self.ai_profile_var, [], state="readonly")
        self._labeled_combo(ai_frame, "Provider", self.ai_provider_var, AI_PROVIDER_OPTIONS, state="readonly")
        self._labeled_entry(ai_frame, "Model", self.ai_model_var)
        self._labeled_entry(ai_frame, "Endpoint", self.ai_endpoint_var)
        self._labeled_entry(ai_frame, "Temperature", self.ai_temp_var)
        key_row = ttk.Frame(ai_frame)
        key_row.pack(fill="x", pady=2)
        ttk.Label(key_row, text="API key", width=16).pack(side="left")
        ttk.Entry(key_row, textvariable=self.ai_key_var, show="*", width=32).pack(side="left")
        ttk.Checkbutton(ai_frame, text="Internet-enabled profile", variable=self.ai_internet_var).pack(anchor="w")

        ai_btns = ttk.Frame(ai_frame)
        ai_btns.pack(fill="x", pady=4)
        ttk.Button(ai_btns, text="Refresh", command=self._refresh_ai_profiles).pack(side="left")
        ttk.Button(ai_btns, text="Save Profile", command=self._save_ai_profile_from_form).pack(side="left", padx=4)
        ttk.Button(ai_btns, text="Set Active", command=self._set_active_ai_profile_from_form).pack(side="left")
        ttk.Button(ai_btns, text="Delete", command=self._delete_ai_profile_from_form).pack(side="left", padx=4)
        ttk.Button(ai_btns, text="Set Key", command=self._set_ai_key_from_form).pack(side="left")
        ttk.Button(ai_btns, text="Remove Key", command=self._remove_ai_key_from_form).pack(side="left", padx=4)
        ttk.Button(ai_btns, text="Test", command=self._test_ai_profile_from_form).pack(side="left")

        bn_frame = ttk.LabelFrame(frame, text="Binance Profiles", padding=8)
        bn_frame.pack(fill="x", pady=8)
        self.bn_profile_var = tk.StringVar(value="")
        self.bn_endpoint_var = tk.StringVar(value="https://api.binance.com")
        self.bn_key_env_var = tk.StringVar(value="BINANCE_API_KEY")
        self.bn_secret_env_var = tk.StringVar(value="BINANCE_API_SECRET")
        self.bn_key_var = tk.StringVar(value="")
        self.bn_secret_var = tk.StringVar(value="")

        self.bn_profile_combo = self._labeled_combo(bn_frame, "Profile", self.bn_profile_var, [], state="normal")
        self._labeled_entry(bn_frame, "Endpoint", self.bn_endpoint_var)
        self._labeled_entry(bn_frame, "API key env", self.bn_key_env_var)
        self._labeled_entry(bn_frame, "API secret env", self.bn_secret_env_var)
        key_row = ttk.Frame(bn_frame)
        key_row.pack(fill="x", pady=2)
        ttk.Label(key_row, text="API key", width=16).pack(side="left")
        ttk.Entry(key_row, textvariable=self.bn_key_var, show="*", width=32).pack(side="left")
        sec_row = ttk.Frame(bn_frame)
        sec_row.pack(fill="x", pady=2)
        ttk.Label(sec_row, text="API secret", width=16).pack(side="left")
        ttk.Entry(sec_row, textvariable=self.bn_secret_var, show="*", width=32).pack(side="left")

        bn_btns = ttk.Frame(bn_frame)
        bn_btns.pack(fill="x", pady=4)
        ttk.Button(bn_btns, text="Refresh", command=self._refresh_binance_profiles_from_settings).pack(side="left")
        ttk.Button(bn_btns, text="Save Profile", command=self._save_binance_profile_from_form).pack(side="left", padx=4)
        ttk.Button(bn_btns, text="Set Active", command=self._set_active_binance_profile_from_form).pack(side="left")
        ttk.Button(bn_btns, text="Delete", command=self._delete_binance_profile_from_form).pack(side="left", padx=4)
        ttk.Button(bn_btns, text="Set Keys", command=self._set_binance_keys_from_form).pack(side="left")
        ttk.Button(bn_btns, text="Remove Keys", command=self._remove_binance_keys_from_form).pack(side="left", padx=4)
        ttk.Button(bn_btns, text="Test", command=self._test_binance_profile_from_form).pack(side="left")

        ttk.Button(frame, text="Save Settings", command=self._persist_state).pack(fill="x")

        settings_frame, self.settings_output = self._create_scrolled_text(split, height=8, wrap="none")
        split.add(settings_frame, weight=1)
        settings_btns = ttk.Frame(frame)
        settings_btns.pack(fill="x", pady=(6, 0))
        ttk.Button(settings_btns, text="Copy Settings Log", command=lambda: self._copy_text_widget(self.settings_output, "Settings log")).pack(side="left")
        self._append_settings("Settings are stored under %USERPROFILE%\\.ctmt\\gui\\gui_state.json")
        self.ai_profile_combo.bind("<<ComboboxSelected>>", lambda _e: self._load_ai_profile_into_form())
        self.bn_profile_combo.bind("<<ComboboxSelected>>", lambda _e: self._load_binance_profile_into_form())
        self._refresh_ai_profiles()
        self._refresh_binance_profiles_from_settings()

    def _labeled_entry(self, parent, label: str, var: tk.StringVar) -> None:
        row = ttk.Frame(parent)
        row.pack(fill="x", pady=2)
        ttk.Label(row, text=label, width=16).pack(side="left")
        ttk.Entry(row, textvariable=var, width=18).pack(side="left")

    def _create_paned(self, parent, orient: str = "horizontal") -> ttk.Panedwindow:
        o = tk.HORIZONTAL if str(orient).lower().startswith("h") else tk.VERTICAL
        pane = ttk.Panedwindow(parent, orient=o)
        pane.pack(fill="both", expand=True)
        return pane

    def _create_scrolled_text(self, parent, **text_kwargs):
        frame = ttk.Frame(parent)
        text = tk.Text(frame, **text_kwargs)
        ybar = ttk.Scrollbar(frame, orient="vertical", command=text.yview)
        xbar = ttk.Scrollbar(frame, orient="horizontal", command=text.xview)
        text.configure(yscrollcommand=ybar.set, xscrollcommand=xbar.set)
        frame.rowconfigure(0, weight=1)
        frame.columnconfigure(0, weight=1)
        text.grid(row=0, column=0, sticky="nsew")
        ybar.grid(row=0, column=1, sticky="ns")
        xbar.grid(row=1, column=0, sticky="ew")
        self._install_text_console_bindings(text)
        return frame, text

    def _create_scrolled_tree(self, parent, **tree_kwargs):
        frame = ttk.Frame(parent)
        tree = ttk.Treeview(frame, **tree_kwargs)
        ybar = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
        xbar = ttk.Scrollbar(frame, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=ybar.set, xscrollcommand=xbar.set)
        frame.rowconfigure(0, weight=1)
        frame.columnconfigure(0, weight=1)
        tree.grid(row=0, column=0, sticky="nsew")
        ybar.grid(row=0, column=1, sticky="ns")
        xbar.grid(row=1, column=0, sticky="ew")
        self._install_tree_bindings(tree)
        return frame, tree

    def _install_text_console_bindings(self, text: tk.Text) -> None:
        menu = tk.Menu(text, tearoff=0)
        menu.add_command(label="Copy", command=lambda: text.event_generate("<<Copy>>"))
        menu.add_command(label="Select All", command=lambda: text.tag_add("sel", "1.0", tk.END))
        menu.add_separator()
        menu.add_command(label="Clear", command=lambda: text.delete("1.0", tk.END))

        def _popup(event):
            try:
                menu.tk_popup(event.x_root, event.y_root)
            finally:
                menu.grab_release()

        text.bind("<Button-3>", _popup)
        text.bind("<Control-a>", lambda e: (text.tag_add("sel", "1.0", tk.END), "break"))
        text.bind("<Control-A>", lambda e: (text.tag_add("sel", "1.0", tk.END), "break"))

    def _install_tree_bindings(self, tree: ttk.Treeview) -> None:
        menu = tk.Menu(tree, tearoff=0)
        menu.add_command(label="Copy Selected Rows", command=lambda: self._copy_tree_selection(tree, "Tree rows"))

        def _popup(event):
            try:
                menu.tk_popup(event.x_root, event.y_root)
            finally:
                menu.grab_release()

        tree.bind("<Button-3>", _popup)
        tree.bind("<Control-c>", lambda e: (self._copy_tree_selection(tree, "Tree rows"), "break"))
        tree.bind("<Control-C>", lambda e: (self._copy_tree_selection(tree, "Tree rows"), "break"))

    def _copy_text_widget(self, widget: tk.Text, label: str = "Text") -> None:
        try:
            txt = widget.get("1.0", tk.END).strip()
        except Exception:
            txt = ""
        if not txt:
            self._append_task_terminal(f"{label}: nothing to copy.")
            return
        self.root.clipboard_clear()
        self.root.clipboard_append(txt)
        self._append_task_terminal(f"Copied {label} to clipboard ({len(txt)} chars).")

    def _copy_tree_selection(self, tree: ttk.Treeview, label: str = "Rows") -> None:
        rows = []
        cols = list(tree["columns"]) if "columns" in tree.keys() else []
        for iid in tree.selection():
            vals = tree.item(iid, "values")
            rows.append("\t".join([str(v) for v in vals]))
        if not rows:
            self._append_task_terminal(f"{label}: no selected rows to copy.")
            return
        head = "\t".join([str(c) for c in cols]) if cols else ""
        payload = (head + "\n" if head else "") + "\n".join(rows)
        self.root.clipboard_clear()
        self.root.clipboard_append(payload)
        self._append_task_terminal(f"Copied {label} to clipboard ({len(rows)} row(s)).")

    def _human_readable_ts(self, value: Any) -> str:
        raw = str(value or "").strip()
        if not raw:
            return raw
        try:
            ts = pd.to_datetime(raw, utc=True, errors="raise")
        except Exception:
            return raw
        try:
            local_ts = ts.tz_convert(datetime.now().astimezone().tzinfo)
            return local_ts.strftime("%Y-%m-%d %I:%M:%S %p %Z")
        except Exception:
            return str(ts)

    def _humanize_df_timestamps(self, df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
        out = df.copy()
        for c in cols:
            if c in out.columns:
                out[c] = out[c].apply(self._human_readable_ts)
        return out

    def _configure_dashboard_tags(self, widget: tk.Text) -> None:
        # Section/header emphasis
        widget.tag_configure("hdr", foreground="#7FDBFF")
        widget.tag_configure("subhdr", foreground="#FFD166")
        # Action emphasis
        widget.tag_configure("buy", foreground="#2ECC71")
        widget.tag_configure("hold", foreground="#F39C12")
        widget.tag_configure("sell", foreground="#E74C3C")
        # Open position protection emphasis
        widget.tag_configure("protected", foreground="#2ECC71")
        widget.tag_configure("stale_protected", foreground="#F39C12")
        widget.tag_configure("unprotected", foreground="#E74C3C")
        # Result emphasis
        widget.tag_configure("okline", foreground="#2ECC71")
        widget.tag_configure("errline", foreground="#FF6B6B")

    def _apply_color_tags(self, widget: tk.Text) -> None:
        if widget is None:
            return
        # Clear prior highlights
        for tag in (
            "hdr",
            "subhdr",
            "buy",
            "hold",
            "sell",
            "protected",
            "stale_protected",
            "unprotected",
            "okline",
            "errline",
        ):
            widget.tag_remove(tag, "1.0", tk.END)

        def _tag_all(needle: str, tag: str, nocase: bool = True) -> None:
            start = "1.0"
            while True:
                idx = widget.search(needle, start, stopindex=tk.END, nocase=nocase)
                if not idx:
                    break
                end = f"{idx}+{len(needle)}c"
                widget.tag_add(tag, idx, end)
                start = end

        # Headers and sections commonly present in CLI-style output
        for k in ("LIVE DASHBOARD", "RISK SCORE BREAKDOWN", "PORTFOLIO CONTEXT", "TRADE HISTORY", "Final Value", "Total Return"):
            _tag_all(k, "hdr")
        for k in ("Assets loaded", "Running", "Queued"):
            _tag_all(k, "subhdr")

        # Action words (including emoji variants)
        for k in ("🟢 BUY", " BUY "):
            _tag_all(k, "buy")
        for k in ("🟠 HOLD", " HOLD "):
            _tag_all(k, "hold")
        for k in ("🔴 SELL", " SELL "):
            _tag_all(k, "sell")

        # Open-position protection markers
        for k in ("PROTECTED",):
            _tag_all(k, "protected", nocase=False)
        for k in ("STALE_PROTECTED",):
            _tag_all(k, "stale_protected", nocase=False)
        for k in ("UNPROTECTED",):
            _tag_all(k, "unprotected", nocase=False)

        # Outcome markers
        for k in ("DONE", "SUCCESS", "OK"):
            _tag_all(k, "okline")
        for k in ("ERROR", "FAILED", "Traceback"):
            _tag_all(k, "errline")

    def _labeled_combo(
        self,
        parent,
        label: str,
        var: tk.StringVar,
        values: List[str],
        state: str = "readonly",
        filterable: bool = False,
    ):
        row = ttk.Frame(parent)
        row.pack(fill="x", pady=2)
        ttk.Label(row, text=label, width=16).pack(side="left")
        combo = ttk.Combobox(row, textvariable=var, values=values, width=24, state=state)
        combo.pack(side="left")
        if filterable:
            self._bind_combo_filter(combo, values)
        return combo

    def _bind_combo_filter(self, combo: ttk.Combobox, base_values: List[str]) -> None:
        vals = list(base_values)

        def _on_key(_event):
            q = combo.get().strip().lower()
            if not q:
                combo["values"] = vals
                return
            filt = [v for v in vals if q in v.lower()]
            combo["values"] = filt if filt else vals

        combo.bind("<KeyRelease>", _on_key)

    def _refresh_panel_list(self) -> None:
        self.panel_list.delete(0, tk.END)
        for i, p in enumerate(self.live_panels):
            self.panel_list.insert(tk.END, f"[{i+1}] {p.name} | {p.market} | {p.timeframe} | top{p.top_n} | {p.quote_currency}")
        if self.live_panels:
            self.panel_list.selection_set(0)
            self._load_selected_panel_to_form()

    def _selected_panel_index(self) -> Optional[int]:
        sel = self.panel_list.curselection()
        if not sel:
            return None
        return int(sel[0])

    def _selected_panel_indices(self) -> List[int]:
        sel = list(self.panel_list.curselection())
        if not sel:
            return []
        out: List[int] = []
        for i in sel:
            ii = int(i)
            if 0 <= ii < len(self.live_panels):
                out.append(ii)
        return out

    def _refresh_saved_profile_list(self) -> None:
        if not hasattr(self, "live_profile_list"):
            return
        self.live_profile_list.delete(0, tk.END)
        dashboards = self.state.get("saved_dashboards", {})
        if not isinstance(dashboards, dict):
            dashboards = {}
        for name in sorted(dashboards.keys()):
            self.live_profile_list.insert(tk.END, name)

    def _selected_profile_names(self) -> List[str]:
        if not hasattr(self, "live_profile_list"):
            return []
        sel = list(self.live_profile_list.curselection())
        names: List[str] = []
        for i in sel:
            try:
                names.append(str(self.live_profile_list.get(i)))
            except Exception:
                continue
        return names

    def _save_live_profile_from_selected(self) -> None:
        name = (self.live_profile_name_var.get().strip() if hasattr(self, "live_profile_name_var") else "") or "default"
        indices = self._selected_panel_indices()
        if not indices:
            messagebox.showinfo("Dashboard Presets", "Select one or more panels first.")
            return
        dashboards = self.state.get("saved_dashboards", {})
        if not isinstance(dashboards, dict):
            dashboards = {}
        dashboards[name] = [asdict(self.live_panels[i]) for i in indices]
        self.state["saved_dashboards"] = dashboards
        self._persist_state()
        self._refresh_saved_profile_list()
        self._append_task_terminal(f"Saved dashboard profile `{name}` from {len(indices)} selected panel(s).")

    def _save_live_profile_from_all(self) -> None:
        name = (self.live_profile_name_var.get().strip() if hasattr(self, "live_profile_name_var") else "") or "default"
        dashboards = self.state.get("saved_dashboards", {})
        if not isinstance(dashboards, dict):
            dashboards = {}
        dashboards[name] = [asdict(p) for p in self.live_panels]
        self.state["saved_dashboards"] = dashboards
        self._persist_state()
        self._refresh_saved_profile_list()
        self._append_task_terminal(f"Saved dashboard profile `{name}` from all panels ({len(self.live_panels)}).")

    def _load_selected_profile_replace(self) -> None:
        names = self._selected_profile_names()
        if not names:
            messagebox.showinfo("Dashboard Presets", "Select one profile to load.")
            return
        if len(names) > 1:
            messagebox.showinfo("Dashboard Presets", "Select only one profile for replace-load.")
            return
        dashboards = self.state.get("saved_dashboards", {})
        if not isinstance(dashboards, dict) or names[0] not in dashboards:
            messagebox.showerror("Dashboard Presets", f"Profile not found: {names[0]}")
            return
        panels = dashboards.get(names[0], [])
        if not isinstance(panels, list) or not panels:
            messagebox.showerror("Dashboard Presets", f"Profile is empty: {names[0]}")
            return
        self.live_panels = [LivePanelConfig(**p) for p in panels if isinstance(p, dict)]
        self._refresh_panel_list()
        self._persist_state()
        self._append_task_terminal(f"Loaded dashboard profile `{names[0]}` (replace).")

    def _merge_selected_profiles(self) -> None:
        names = self._selected_profile_names()
        if not names:
            messagebox.showinfo("Dashboard Presets", "Select one or more profiles to merge.")
            return
        dashboards = self.state.get("saved_dashboards", {})
        if not isinstance(dashboards, dict):
            dashboards = {}
        merged: List[LivePanelConfig] = []
        for n in names:
            panels = dashboards.get(n, [])
            if not isinstance(panels, list):
                continue
            for p in panels:
                if isinstance(p, dict):
                    try:
                        merged.append(LivePanelConfig(**p))
                    except Exception:
                        continue
        if not merged:
            messagebox.showerror("Dashboard Presets", "No panels found to merge from selected profiles.")
            return
        self.live_panels.extend(merged)
        self._refresh_panel_list()
        self._persist_state()
        self._append_task_terminal(f"Merged {len(names)} profile(s), appended {len(merged)} panel(s).")

    def _delete_selected_profiles(self) -> None:
        names = self._selected_profile_names()
        if not names:
            messagebox.showinfo("Dashboard Presets", "Select one or more profiles to delete.")
            return
        ok = messagebox.askyesno("Dashboard Presets", f"Delete {len(names)} selected profile(s)?")
        if not ok:
            return
        dashboards = self.state.get("saved_dashboards", {})
        if not isinstance(dashboards, dict):
            dashboards = {}
        for n in names:
            dashboards.pop(n, None)
        self.state["saved_dashboards"] = dashboards
        self._persist_state()
        self._refresh_saved_profile_list()
        self._append_task_terminal(f"Deleted {len(names)} dashboard profile(s).")

    def _load_selected_panel_to_form(self) -> None:
        idx = self._selected_panel_index()
        if idx is None or idx >= len(self.live_panels):
            return
        p = self.live_panels[idx]
        self.panel_name_var.set(p.name)
        self.market_var.set(p.market)
        self.timeframe_var.set(p.timeframe)
        self.quote_var.set(p.quote_currency)
        self.topn_var.set(str(p.top_n))
        self.country_var.set(country_code_to_display(p.country))
        self.country_manual_var.set("")

    def _panel_from_form(self) -> LivePanelConfig:
        p = LivePanelConfig(
            name=self.panel_name_var.get().strip() or "Panel",
            market=self.market_var.get().strip() or "crypto",
            timeframe=self.timeframe_var.get().strip() or "1d",
            quote_currency=self.quote_var.get().strip() or "USD",
            top_n=max(1, int(self.topn_var.get().strip() or "20")),
            country=self.country_var.get().strip() or "2",
            display_currency=self.display_currency_var.get().strip() or "USD",
        )
        p.country = parse_country_code(self.country_var.get(), self.country_manual_var.get())
        if p.market.lower() == "crypto" and self._is_primary_quote_locked():
            p.quote_currency = self._primary_quote_asset()
        return p

    def _primary_quote_asset(self) -> str:
        return str(getattr(self, "primary_quote_var", tk.StringVar(value="USDT")).get() or "USDT").strip().upper() or "USDT"

    def _is_primary_quote_locked(self) -> bool:
        v = getattr(self, "quote_lock_var", None)
        return bool(v.get()) if v is not None else False

    def _effective_crypto_quote(self, fallback: str = "USDT") -> str:
        if self._is_primary_quote_locked():
            return self._primary_quote_asset()
        try:
            q = str(self.quote_var.get() or "").strip().upper()
            return q or fallback
        except Exception:
            return fallback

    def _notify_busy(self, task_name: str) -> None:
        messagebox.showinfo("Task Queued", f"Task queued: {task_name}")

    def _task_limit(self) -> int:
        mode_var = getattr(self, "parallel_mode_var", None)
        jobs_var = getattr(self, "parallel_jobs_var", None)
        if mode_var is not None and bool(mode_var.get()):
            try:
                raw = str(jobs_var.get()).strip() if jobs_var is not None else "2"
                return max(1, int(raw or "2"))
            except Exception:
                return 2
        return 1

    def _queue_if_busy(self, task_name: str, starter) -> bool:
        if self.running_tasks >= self._task_limit():
            self.task_queue.append((task_name, starter))
            self._update_run_controls_and_status()
            self._notify_busy(task_name)
            return True
        return False

    def _set_busy(self, busy: bool, task_name: str = "") -> None:
        if busy:
            self._task_id_seq += 1
            task_id = int(self._task_id_seq)
            self._running_tasks[task_id] = task_name or "Task"
        else:
            task_id = None
            # Backward-compatible no-op return var for non-start calls.
        self.running_task_names = [self._running_tasks[k] for k in sorted(self._running_tasks.keys())]
        self.running_tasks = len(self._running_tasks)
        self.busy = self.running_tasks > 0
        limit = self._task_limit()
        disable = self.running_tasks > 0 and limit <= 1
        run_state = "disabled" if disable else "normal"
        for btn_name in [
            "btn_run_selected",
            "btn_run_all",
            "btn_run_backtest",
            "btn_run_ai",
            "btn_run_research_std",
            "btn_run_research_comp",
        ]:
            btn = getattr(self, btn_name, None)
            if btn is not None:
                btn.configure(state=run_state)
        if (not busy) and (not self.queue_paused) and self.task_queue and self.running_tasks < limit:
            slots = limit - self.running_tasks
            for _ in range(min(slots, len(self.task_queue))):
                next_name, next_job = self.task_queue.popleft()
                self.status_var.set(f"Starting queued task: {next_name} ({len(self.task_queue)} remaining)")
                self.root.after(100, next_job)
        if self.running_tasks > 0:
            label = f"Running {self.running_tasks}/{limit}"
            if self.task_queue:
                label += f" | queued {len(self.task_queue)}"
            if task_name:
                label += f" | {task_name}"
            self.status_var.set(label)
            self.status_progress.start(10)
        else:
            self.status_progress.stop()
            self.status_var.set("Ready")
        self._refresh_task_tab()
        return task_id

    def _finish_task(self, task_id: Optional[int], task_name: str = "") -> None:
        if task_id is not None and task_id in self._running_tasks:
            del self._running_tasks[task_id]
        elif task_name:
            for k in sorted(self._running_tasks.keys()):
                if self._running_tasks.get(k) == task_name:
                    del self._running_tasks[k]
                    break
        elif self._running_tasks:
            # Fallback: remove oldest active task.
            oldest = sorted(self._running_tasks.keys())[0]
            del self._running_tasks[oldest]
        self._set_busy(False, task_name=task_name)

    def _update_run_controls_and_status(self) -> None:
        limit = self._task_limit()
        disable = self.running_tasks > 0 and limit <= 1
        run_state = "disabled" if disable else "normal"
        for btn_name in [
            "btn_run_selected",
            "btn_run_all",
            "btn_run_backtest",
            "btn_run_ai",
            "btn_run_research_std",
            "btn_run_research_comp",
        ]:
            btn = getattr(self, btn_name, None)
            if btn is not None:
                btn.configure(state=run_state)
        if self.running_tasks > 0:
            label = f"Running {self.running_tasks}/{limit}"
            if self.task_queue:
                label += f" | queued {len(self.task_queue)}"
            self.status_var.set(label)
            self.status_progress.start(10)
        else:
            self.status_progress.stop()
            self.status_var.set("Ready")
        self._refresh_task_tab()

    def _task_snapshot(self) -> Dict[str, List[str]]:
        running = list(self.running_task_names)
        queued = [str(item[0]) for item in list(self.task_queue)]
        return {"running": running, "queued": queued}

    def _show_task_status(self) -> None:
        if self.task_monitor_window is not None and self.task_monitor_window.winfo_exists():
            self.task_monitor_window.lift()
            self.task_monitor_window.focus_force()
            return
        win = tk.Toplevel(self.root)
        win.title("Task Monitor")
        win.geometry("640x360")
        self.task_monitor_window = win
        text_frame, text = self._create_scrolled_text(win, wrap="none")
        text_frame.pack(fill="both", expand=True, padx=8, pady=8)
        self.task_monitor_text = text

        btn_row = ttk.Frame(win)
        btn_row.pack(fill="x", padx=8, pady=(0, 8))
        ttk.Button(btn_row, text="Refresh Now", command=self._refresh_task_monitor).pack(side="left")
        ttk.Button(btn_row, text="Close", command=self._close_task_monitor).pack(side="right")

        win.protocol("WM_DELETE_WINDOW", self._close_task_monitor)
        self._refresh_task_monitor()

    def _open_task_monitor_tab(self) -> None:
        self.nb.select(self.task_tab)
        self._refresh_task_tab()

    def _refresh_task_monitor(self) -> None:
        if self.task_monitor_window is None or not self.task_monitor_window.winfo_exists():
            return
        snap = self._task_snapshot()
        running = snap["running"]
        queued = snap["queued"]
        lines: List[str] = []
        lines.append(f"Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"Running slots: {self.running_tasks}/{self._task_limit()}")
        lines.append("")
        lines.append("Current Running Tasks:")
        if running:
            for i, name in enumerate(running, 1):
                lines.append(f"{i}. {name}")
        else:
            lines.append("None")
        lines.append("")
        lines.append("Queued Tasks:")
        if queued:
            for i, name in enumerate(queued, 1):
                lines.append(f"{i}. {name}")
        else:
            lines.append("None")
        if self.task_monitor_text is not None:
            self.task_monitor_text.delete("1.0", tk.END)
            self.task_monitor_text.insert("1.0", "\n".join(lines))
        if self.task_monitor_job:
            try:
                self.root.after_cancel(self.task_monitor_job)
            except Exception:
                pass
        self.task_monitor_job = self.root.after(1000, self._refresh_task_monitor)

    def _close_task_monitor(self) -> None:
        if self.task_monitor_job:
            try:
                self.root.after_cancel(self.task_monitor_job)
            except Exception:
                pass
            self.task_monitor_job = None
        if self.task_monitor_window is not None and self.task_monitor_window.winfo_exists():
            self.task_monitor_window.destroy()
        self.task_monitor_window = None
        self.task_monitor_text = None

    def _refresh_task_tab(self) -> None:
        if not hasattr(self, "running_list") or not hasattr(self, "queued_list"):
            return
        self.running_list.delete(0, tk.END)
        for i, name in enumerate(self.running_task_names, 1):
            self.running_list.insert(tk.END, f"{i}. {name}")
        self.queued_list.delete(0, tk.END)
        for i, item in enumerate(list(self.task_queue), 1):
            qname = str(item[0])
            self.queued_list.insert(tk.END, f"{i}. {qname}")
        if hasattr(self, "btn_pause_queue"):
            self.btn_pause_queue.configure(text="Resume Queue" if self.queue_paused else "Pause Queue")
        if hasattr(self, "task_tab_output"):
            self.task_tab_output.delete("1.0", tk.END)
            self.task_tab_output.insert(
                "1.0",
                f"Running: {self.running_tasks}/{self._task_limit()} | Queued: {len(self.task_queue)} | Queue paused: {self.queue_paused}",
            )

    def _append_task_terminal(self, line: str) -> None:
        if not hasattr(self, "task_terminal"):
            return
        ts = datetime.now().strftime("%H:%M:%S")
        msg = f"[{ts}] {line}\n"
        self.task_terminal.insert("end", msg)
        self.task_terminal.see("end")
        # Keep terminal reasonably bounded in GUI memory.
        max_lines = 1500
        current_lines = int(self.task_terminal.index("end-1c").split(".")[0])
        if current_lines > max_lines:
            drop = current_lines - max_lines
            self.task_terminal.delete("1.0", f"{drop}.0")

    def _is_verbose_logging(self) -> bool:
        v = getattr(self, "verbose_logging_var", None)
        if v is not None:
            try:
                return bool(v.get())
            except Exception:
                pass
        return bool(self.state.get("verbose_terminal_logging", True))

    def _vlog(self, line: str) -> None:
        if self._is_verbose_logging():
            self._append_task_terminal(f"VERBOSE {line}")

    def _append_task_terminal_from_worker(self, line: str) -> None:
        self.root.after(0, lambda: self._append_task_terminal(line))

    def _schedule_task_tab_refresh(self) -> None:
        if self.task_tab_job:
            try:
                self.root.after_cancel(self.task_tab_job)
            except Exception:
                pass
        self._refresh_task_tab()
        self.task_tab_job = self.root.after(1000, self._schedule_task_tab_refresh)

    def _toggle_queue_pause(self) -> None:
        self.queue_paused = not self.queue_paused
        self._append_task_terminal(f"QUEUE {'PAUSED' if self.queue_paused else 'RESUMED'}")
        self._refresh_task_tab()
        if (not self.queue_paused) and self.running_tasks < self._task_limit() and self.task_queue:
            self._set_busy(False, task_name="Queue resumed")

    def _stop_selected_task(self) -> None:
        qsel = self.queued_list.curselection() if hasattr(self, "queued_list") else ()
        if qsel:
            idx = int(qsel[0])
            items = list(self.task_queue)
            if 0 <= idx < len(items):
                removed = items.pop(idx)
                self.task_queue = deque(items)
                self._append_settings(f"Removed queued task: {removed[0]}")
                self._append_task_terminal(f"REMOVED queued task: {removed[0]}")
                self._refresh_task_tab()
                return
        rsel = self.running_list.curselection() if hasattr(self, "running_list") else ()
        if rsel:
            messagebox.showinfo("Task Control", "Stopping active running tasks is not supported yet. You can remove queued tasks.")
            return
        messagebox.showinfo("Task Control", "Select a queued or running task first.")

    def _reprioritize_queue(self, direction: int) -> None:
        qsel = self.queued_list.curselection() if hasattr(self, "queued_list") else ()
        if not qsel:
            messagebox.showinfo("Queue Priority", "Select a queued task first.")
            return
        idx = int(qsel[0])
        items = list(self.task_queue)
        new_idx = idx + int(direction)
        if new_idx < 0 or new_idx >= len(items):
            return
        items[idx], items[new_idx] = items[new_idx], items[idx]
        self.task_queue = deque(items)
        moved = str(items[new_idx][0]) if 0 <= new_idx < len(items) else "task"
        self._append_task_terminal(f"REPRIORITIZED queued task: {moved} -> position {new_idx + 1}")
        self._refresh_task_tab()
        self.queued_list.selection_set(new_idx)

    def _bridge_for_task(self) -> EngineBridge:
        if self._task_limit() <= 1:
            return self.bridge
        return EngineBridge(repo_root=self.repo_root)

    def _browse_ai_backtest_file(self) -> None:
        initial_dir = self.repo_root / "experiments" / "backtest_snapshots"
        path = filedialog.askopenfilename(
            title="Select Backtest Result File",
            initialdir=str(initial_dir),
            filetypes=[("Text Files", "*.txt"), ("All Files", "*.*")],
        )
        if path:
            self.ai_backtest_path.set(path)

    def _add_panel(self) -> None:
        self.live_panels.append(self._panel_from_form())
        self._refresh_panel_list()
        self._persist_state()

    def _update_panel(self) -> None:
        idx = self._selected_panel_index()
        if idx is None:
            return
        self.live_panels[idx] = self._panel_from_form()
        self._refresh_panel_list()
        self.panel_list.selection_set(idx)
        self._persist_state()

    def _remove_panel(self) -> None:
        idx = self._selected_panel_index()
        if idx is None:
            return
        del self.live_panels[idx]
        self._refresh_panel_list()
        self._persist_state()

    def _run_selected_panel(self) -> None:
        idx = self._selected_panel_index()
        if idx is None:
            return
        self._run_live_job(self.live_panels[idx], selected_only=True)

    def _run_all_panels(self) -> None:
        if self._queue_if_busy("Run All Panels", self._start_run_all_panels):
            return
        self._start_run_all_panels()

    def _start_run_all_panels(self) -> None:
        task_name = "Live Dashboard (All Panels)"
        task_id = self._set_busy(True, task_name)
        self._append_task_terminal("START Live Dashboard (All Panels)")
        # Keep prior dashboard visible while refresh runs; replace only on completion.

        def worker():
            bridge = self._bridge_for_task()
            chunks = []
            panel_text_map: Dict[str, str] = {}
            for p in self.live_panels:
                cfg = asdict(p)
                cfg["display_currency"] = self.display_currency_var.get().strip() or "USD"
                if str(cfg.get("market", "crypto")).strip().lower() == "crypto" and self._is_primary_quote_locked():
                    cfg["quote_currency"] = self._primary_quote_asset()
                self._append_task_terminal_from_worker(
                    f"VERBOSE Live panel start: name={cfg.get('name')} market={cfg.get('market')} tf={cfg.get('timeframe')} quote={cfg.get('quote_currency')} top_n={cfg.get('top_n')}"
                )
                res = bridge.run_live_panel(cfg)
                log = (res.get("log", "") or "").strip()
                if log:
                    self._append_task_terminal_from_worker(f"LOG [{p.name}] {log[:4000]}")
                if not res.get("ok"):
                    chunks.append(f"=== {p.name} ===\nERROR: {res.get('error', 'unknown')}\n")
                    self._append_task_terminal_from_worker(f"VERBOSE Live panel failed: {p.name} -> {res.get('error', 'unknown')}")
                    continue
                text = [
                    f"=== {p.name} ({res['market']} {res['timeframe']}) ===",
                    f"Assets loaded: {res['loaded_assets']}/{res['requested_assets']}",
                    "",
                    res["table_text"],
                    "",
                    "RISK SCORE BREAKDOWN",
                    res["risk_text"],
                ]
                notes = res.get("notes", [])
                if notes:
                    text.append("")
                    text.append("PORTFOLIO CONTEXT")
                    for n in notes:
                        text.append(f"- {n}")
                panel_blob = "\n".join(text) + "\n"
                panel_text_map[p.name] = panel_blob
                chunks.append(panel_blob)
                self._append_task_terminal_from_worker(
                    f"VERBOSE Live panel done: {p.name} loaded={res.get('loaded_assets')}/{res.get('requested_assets')}"
                )

            out = "\n".join(chunks)
            self.root.after(0, lambda: self._finish_live_output(out, task_name, task_id, panel_text_map))

        threading.Thread(target=worker, daemon=True).start()

    def _run_live_job(self, panel: LivePanelConfig, selected_only: bool = False) -> None:
        if self._queue_if_busy(
            f"Live Dashboard ({panel.name})",
            lambda p=panel, s=selected_only: self._start_run_live_job(p, s),
        ):
            return
        self._start_run_live_job(panel, selected_only)

    def _start_run_live_job(self, panel: LivePanelConfig, selected_only: bool = False) -> None:
        task_name = f"Live Dashboard ({panel.name})"
        task_id = self._set_busy(True, task_name)
        self._append_task_terminal(f"START {task_name}")
        # Keep prior panel output visible until new payload is ready.

        def worker():
            bridge = self._bridge_for_task()
            cfg = asdict(panel)
            cfg["display_currency"] = self.display_currency_var.get().strip() or "USD"
            if str(cfg.get("market", "crypto")).strip().lower() == "crypto" and self._is_primary_quote_locked():
                cfg["quote_currency"] = self._primary_quote_asset()
            self._append_task_terminal_from_worker(
                f"VERBOSE Live single start: name={cfg.get('name')} market={cfg.get('market')} tf={cfg.get('timeframe')} quote={cfg.get('quote_currency')} top_n={cfg.get('top_n')}"
            )
            res = bridge.run_live_panel(cfg)
            log = (res.get("log", "") or "").strip()
            if log:
                self._append_task_terminal_from_worker(f"LOG [{panel.name}] {log[:4000]}")
            if not res.get("ok"):
                out = f"ERROR: {res.get('error', 'unknown')}"
            else:
                lines = [
                    f"=== {panel.name} ({res['market']} {res['timeframe']}) ===",
                    f"Assets loaded: {res['loaded_assets']}/{res['requested_assets']}",
                    "",
                    res["table_text"],
                    "",
                    "RISK SCORE BREAKDOWN",
                    res["risk_text"],
                ]
                notes = res.get("notes", [])
                if notes:
                    lines.append("")
                    lines.append("PORTFOLIO CONTEXT")
                    for n in notes:
                        lines.append(f"- {n}")
                out = "\n".join(lines)
            self.root.after(0, lambda: self._finish_live_output(out, task_name, task_id))

        threading.Thread(target=worker, daemon=True).start()

    def _finish_live_output(
        self,
        text: str,
        task_name: str = "Live Dashboard",
        task_id: Optional[int] = None,
        panel_text_map: Optional[Dict[str, str]] = None,
    ) -> None:
        self.live_output.delete("1.0", tk.END)
        self.live_output.insert("1.0", text)
        self._apply_color_tags(self.live_output)
        self.latest_live_output_text = text
        if panel_text_map is not None:
            self.latest_live_panel_texts = dict(panel_text_map)
        self._append_task_terminal(f"DONE {task_name}")
        self._finish_task(task_id, task_name=task_name)
        self._persist_state()

    def _start_auto_refresh(self) -> None:
        self.auto_refresh_running = True
        self._schedule_next_refresh()

    def _stop_auto_refresh(self) -> None:
        self.auto_refresh_running = False
        if self.auto_refresh_job:
            self.root.after_cancel(self.auto_refresh_job)
            self.auto_refresh_job = None

    def _schedule_next_refresh(self) -> None:
        if not self.auto_refresh_running:
            return
        try:
            secs = max(10, int(self.refresh_secs_var.get().strip() or "120"))
        except Exception:
            secs = 120
        self._run_all_panels()
        self.auto_refresh_job = self.root.after(secs * 1000, self._schedule_next_refresh)
        self.state["auto_refresh_seconds"] = secs
        self._persist_state()

    def _run_backtest(self) -> None:
        if self._queue_if_busy("Backtest", self._start_run_backtest):
            return
        self._start_run_backtest()

    def _start_run_backtest(self) -> None:
        task_name = "Backtest"
        task_id = self._set_busy(True, task_name)
        self._append_task_terminal("START Backtest")
        self.bt_summary.delete("1.0", tk.END)
        self.bt_trades.delete("1.0", tk.END)

        def _to_int(v: str, default: int) -> int:
            try:
                return int(str(v).strip())
            except Exception:
                return default

        def _to_float(v: str, default: float) -> float:
            try:
                return float(str(v).strip())
            except Exception:
                return default

        bt_quote = self.bt_quote.get()
        if self.bt_market.get().strip().lower() == "crypto" and self._is_primary_quote_locked():
            bt_quote = self._primary_quote_asset()
        cfg = {
            "market": self.bt_market.get(),
            "timeframe": self.bt_tf.get(),
            "months": _to_int(self.bt_months.get(), 12),
            "top_n": _to_int(self.bt_topn.get(), 20),
            "quote_currency": bt_quote,
            "country": parse_country_code(self.bt_country.get(), self.bt_country_manual.get()),
            "initial_capital": _to_float(self.bt_initial.get(), 10000.0),
            "stop_loss_pct": _to_float(self.bt_stop_loss.get(), 8.0),
            "take_profit_pct": _to_float(self.bt_take_profit.get(), 20.0),
            "max_hold_days": _to_int(self.bt_max_hold_days.get(), 45),
            "min_hold_bars": _to_int(self.bt_min_hold_bars.get(), 2),
            "cooldown_bars": _to_int(self.bt_cooldown_bars.get(), 1),
            "same_asset_cooldown_bars": _to_int(self.bt_same_asset_cooldown.get(), 3),
            "max_consecutive_same_asset_entries": _to_int(self.bt_max_same_asset_entries.get(), 3),
            "fee_pct": _to_float(self.bt_fee_pct.get(), 0.10),
            "slippage_pct": _to_float(self.bt_slippage_pct.get(), 0.05),
            "position_size": max(0.01, min(1.0, _to_float(self.bt_position_size_pct.get(), 30.0) / 100.0)),
            "atr_multiplier": _to_float(self.bt_atr_mult.get(), 2.2),
            "adx_threshold": _to_float(self.bt_adx_threshold.get(), 25.0),
            "cmf_threshold": _to_float(self.bt_cmf_threshold.get(), 0.02),
            "obv_slope_threshold": _to_float(self.bt_obv_threshold.get(), 0.0),
            "max_drawdown_limit_pct": _to_float(self.bt_max_dd_target_pct.get(), 35.0),
            "max_exposure_pct": max(0.01, min(1.0, _to_float(self.bt_max_exposure_pct.get(), 40.0) / 100.0)),
            "cache_workers": _to_int(self.bt_cache_workers.get(), 8),
            "buy_threshold": _to_int(self.bt_buy_threshold.get(), 2),
            "sell_threshold": _to_int(self.bt_sell_threshold.get(), -2),
            "display_currency": self.display_currency_var.get() or "USD",
        }
        try:
            self._vlog("Backtest config: " + json.dumps(cfg, indent=2))
        except Exception:
            self._vlog(f"Backtest config built (market={cfg.get('market')} tf={cfg.get('timeframe')}).")

        def worker():
            bridge = self._bridge_for_task()
            res = bridge.run_backtest(cfg)
            log = (res.get("log", "") or "").strip()
            if log:
                self._append_task_terminal_from_worker(f"LOG [Backtest] {log[:6000]}")
            self.root.after(0, lambda: self._finish_backtest_output(res, task_id))

        threading.Thread(target=worker, daemon=True).start()

    def _finish_backtest_output(self, res: Dict[str, Any], task_id: Optional[int] = None) -> None:
        if not res.get("ok"):
            self.bt_summary.insert("1.0", f"ERROR: {res.get('error', 'unknown')}")
            self._append_task_terminal(f"DONE Backtest (error: {res.get('error', 'unknown')})")
        else:
            self.bt_summary.insert("1.0", res.get("summary_text", ""))
            self.bt_trades.insert("1.0", res.get("trades_text", ""))
            self._append_task_terminal("DONE Backtest")
        self._apply_color_tags(self.bt_summary)
        self._apply_color_tags(self.bt_trades)
        self._finish_task(task_id, task_name="Backtest")

    def _run_ai_analysis(self) -> None:
        if self._queue_if_busy("AI Analysis", self._start_run_ai_analysis):
            return
        self._start_run_ai_analysis()

    def _run_agent_command(self) -> None:
        if self._queue_if_busy("Agent Command", self._start_run_agent_command):
            return
        self._start_run_agent_command()

    def _start_run_agent_command(self) -> None:
        task_name = "Agent Command"
        task_id = self._set_busy(True, task_name)
        cmd = str(self.agent_cmd_var.get() or "").strip()
        mode = str(self.agent_mode_var.get() or "plan").strip().lower()
        if not cmd:
            self.agent_output.delete("1.0", tk.END)
            self.agent_output.insert("1.0", "No command provided.")
            self._finish_task(task_id, task_name=task_name)
            return
        self.agent_output.delete("1.0", tk.END)
        self.agent_output.insert("1.0", f"Running command in {mode} mode...\n")
        self._append_task_terminal(f"START Agent Command ({mode})")

        def worker():
            bridge = self._bridge_for_task()
            try:
                out = self._execute_agent_command(bridge, cmd, mode)
            except Exception as exc:
                out = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
            self.root.after(0, lambda: self._finish_agent_command(out, task_id))

        threading.Thread(target=worker, daemon=True).start()

    def _finish_agent_command(self, out: Dict[str, Any], task_id: Optional[int]) -> None:
        if not out.get("ok"):
            self.agent_output.delete("1.0", tk.END)
            self.agent_output.insert("1.0", f"Agent command failed: {out.get('error', 'unknown')}")
            self._append_task_terminal(f"DONE Agent Command (error: {out.get('error', 'unknown')})")
            self._finish_task(task_id, task_name="Agent Command")
            return
        text = str(out.get("text", "") or "").strip() or "Done."
        self.agent_output.delete("1.0", tk.END)
        self.agent_output.insert("1.0", text)
        staged = out.get("staged_recs", []) or []
        if staged:
            self.pending_recommendations.extend(staged)
            self.agent_last_staged_ids = [int(r.get("id", 0) or 0) for r in staged if int(r.get("id", 0) or 0) > 0]
            self._refresh_pending_recommendations_view()
            self._append_task_terminal(f"Agent staged {len(staged)} recommendation(s).")
        if out.get("auto_size", False) and staged:
            ids = [int(r.get("id", 0) or 0) for r in staged]
            try:
                self.pending_tree.selection_set(*[str(i) for i in ids if i > 0])
            except Exception:
                pass
            self._auto_size_selected_pending_orders()
            if out.get("auto_execute", False):
                if bool(self.agent_guard_enabled_var.get()):
                    buy_count = sum([1 for r in staged if str(r.get("side", "")).upper() == "BUY"])
                    try:
                        max_loss = float((self.agent_guard_max_daily_loss_var.get() or "5").strip())
                    except Exception:
                        max_loss = 5.0
                    try:
                        max_trades = int((self.agent_guard_max_trades_var.get() or "8").strip())
                    except Exception:
                        max_trades = 8
                    try:
                        max_exposure = float((self.agent_guard_max_exposure_var.get() or "40").strip())
                    except Exception:
                        max_exposure = 40.0
                    pol = self.bridge.evaluate_agent_policy(
                        profile_name=self.pf_binance_profile_var.get().strip() or None,
                        display_currency=self.display_currency_var.get().strip() or "USD",
                        max_daily_loss_pct=max_loss,
                        max_trades_per_day=max_trades,
                        max_exposure_pct=max_exposure,
                        pending_buy_count=buy_count,
                    )
                    if not pol.get("ok"):
                        reasons = pol.get("reasons", []) or [pol.get("error", "Policy check failed.")]
                        self._append_task_terminal("Agent execution blocked by policy: " + " | ".join([str(x) for x in reasons]))
                        messagebox.showwarning("Agent Guardrails", "Execution blocked:\n- " + "\n- ".join([str(x) for x in reasons]))
                        self._finish_task(task_id, task_name="Agent Command")
                        return
                if bool(self.agent_guard_require_stop_var.get()):
                    missing = []
                    for r in staged:
                        if str(r.get("side", "")).upper() != "BUY":
                            continue
                        try:
                            sl = float(r.get("stop_loss_price", 0.0) or 0.0)
                        except Exception:
                            sl = 0.0
                        if sl <= 0:
                            missing.append(str(r.get("symbol", "")))
                    if missing:
                        self._append_task_terminal("Agent execution blocked: missing stop on BUY -> " + ", ".join(missing))
                        messagebox.showwarning("Agent Guardrails", "Execution blocked. Missing stop for BUY symbols:\n" + "\n".join(missing))
                        self._finish_task(task_id, task_name="Agent Command")
                        return
                self._submit_selected_pending_orders()
        if out.get("refresh_ledger", False):
            self._refresh_ledger_view()
        if bool(out.get("execute_last_staged", False)):
            self._execute_last_agent_staged()
        sched = str(out.get("scheduler_action", "") or "").strip().lower()
        if sched == "start":
            mins = out.get("scheduler_minutes")
            if isinstance(mins, (int, float)) and float(mins) > 0:
                self.pipeline_interval_min_var.set(str(int(float(mins))))
            self._start_pipeline_scheduler()
        elif sched in ("stop", "pause"):
            self._stop_pipeline_scheduler()
        view = str(out.get("open_view", "") or "").strip().lower()
        if view in ("portfolio", "ledger", "detailed"):
            try:
                self.nb.select(self.portfolio_tab)
            except Exception:
                pass
        self._append_task_terminal("DONE Agent Command")
        self._finish_task(task_id, task_name="Agent Command")

    def _show_agent_status(self) -> None:
        excl = self.agent_context.get("exclude_assets", []) or []
        ex_txt = ", ".join([str(x) for x in excl[:12]]) if excl else "none"
        sched = "running" if bool(self.pipeline_job) else "stopped"
        msg = (
            "Agent Status\n"
            f"- Context timeframe: {self.agent_context.get('timeframe', '4h')}\n"
            f"- Context top_n: {self.agent_context.get('top_n', 10)}\n"
            f"- Context quote: {self.agent_context.get('quote_asset', self._effective_crypto_quote('USDT'))}\n"
            f"- Context stop %: {self.agent_context.get('stop_pct', 5.0)}\n"
            f"- Exclusions: {ex_txt}\n"
            f"- Scheduler: {sched} (interval {self.pipeline_interval_min_var.get().strip() or '30'} min)\n"
            f"- Last staged ids: {len(self.agent_last_staged_ids)}"
        )
        self.agent_output.delete("1.0", tk.END)
        self.agent_output.insert("1.0", msg)

    def _execute_last_agent_staged(self) -> None:
        ids = [int(x) for x in (self.agent_last_staged_ids or []) if int(x) > 0]
        if not ids:
            messagebox.showinfo("Agent", "No staged recommendations from the latest agent run.")
            return
        sel = []
        for rec in self.pending_recommendations:
            try:
                rid = int(rec.get("id", 0) or 0)
            except Exception:
                rid = 0
            if rid in ids:
                sel.append(str(rid))
        if not sel:
            messagebox.showinfo("Agent", "Latest staged recommendations are no longer pending.")
            return
        try:
            self.pending_tree.selection_set(*sel)
        except Exception:
            pass
        self._auto_size_selected_pending_orders()
        if str(self.agent_mode_var.get() or "").strip().lower() == "auto_execute":
            self._submit_selected_pending_orders()
            self._refresh_ledger_view()
            self._append_task_terminal(f"Agent executed {len(sel)} staged recommendation(s).")
            return
        self._append_task_terminal(f"Agent prepared {len(sel)} staged recommendation(s). Review then Submit Selected.")

    def _resolve_agent_quote_from_text(self, cmd_lower: str) -> str:
        m = re.search(r"\b(usdt|usdc|fdusd|busd|usd|btc|eth|bnb)\b", cmd_lower)
        if m:
            q = m.group(1).upper()
            if q == "USD" and self._is_primary_quote_locked():
                return self._primary_quote_asset()
            return q
        return self._effective_crypto_quote("USDT")

    def _build_agent_plan_card(self, intent: Dict[str, Any]) -> str:
        lines = ["Agent Plan"]
        lines.append(f"- Intent: {intent.get('intent', 'unknown')}")
        tf = str(intent.get("timeframe", "") or "").strip()
        if tf:
            lines.append(f"- Timeframe: {tf}")
        q = str(intent.get("quote_asset", "") or "").strip()
        if q:
            lines.append(f"- Quote Asset: {q}")
        top_n = intent.get("top_n")
        if isinstance(top_n, int) and top_n > 0:
            lines.append(f"- Universe Size: top {top_n}")
        stop_pct = intent.get("stop_pct")
        if isinstance(stop_pct, (int, float)) and stop_pct > 0:
            lines.append(f"- Stop Loss: {float(stop_pct):.2f}%")
        if intent.get("amount_mode") == "quote_amount":
            lines.append(f"- Spend: {float(intent.get('amount_value', 0.0) or 0.0):.4f} {q or 'QUOTE'}")
        if intent.get("amount_mode") == "quote_percent":
            lines.append(f"- Spend: {float(intent.get('amount_value', 0.0) or 0.0):.2f}% of free {q or 'QUOTE'}")
        if bool(intent.get("expand_until_found", False)):
            lines.append("- Search Strategy: expand top-N tiers until BUY signal(s) found")
        excl = intent.get("exclude_assets")
        if isinstance(excl, list) and excl:
            lines.append("- Exclusions: " + ", ".join([str(x) for x in excl[:12]]))
        lines.append(
            "- Execution: "
            + ("auto-submit allowed" if bool(intent.get("execute_requested", False)) else "stage/plan first")
        )
        return "\n".join(lines)

    def _build_agent_ai_intent_prompt(self, cmd: str, mode: str) -> str:
        ctx = self.agent_context if isinstance(self.agent_context, dict) else {}
        payload = {
            "command": str(cmd or "").strip(),
            "mode": str(mode or "plan").strip().lower(),
            "context": {
                "timeframe": str(ctx.get("timeframe", "4h")),
                "top_n": int(ctx.get("top_n", 10) or 10),
                "quote_asset": str(ctx.get("quote_asset", self._effective_crypto_quote("USDT"))),
                "stop_pct": float(ctx.get("stop_pct", 5.0) or 5.0),
                "exclude_assets": ctx.get("exclude_assets", []),
            },
            "allowed_intents": [
                "buy",
                "scan_allocate",
                "portfolio_pnl",
                "portfolio_balance",
                "open_view",
                "scheduler",
                "status",
                "set_context",
                "execute_staged",
                "unknown",
            ],
            "allowed_timeframes": ["1d", "4h", "8h", "12h"],
            "allowed_quote_assets": ["USDT", "USDC", "FDUSD", "BUSD", "USD", "BTC", "ETH", "BNB"],
        }
        return (
            "Interpret the user trading command into strict JSON for the STRATA agent.\n"
            "Return ONLY one JSON object between the markers below.\n"
            "Do not include commentary.\n\n"
            "BEGIN_STRATA_AGENT_INTENT_JSON\n"
            "{\n"
            "  \"intent\": \"buy|scan_allocate|portfolio_pnl|portfolio_balance|open_view|scheduler|status|set_context|execute_staged|unknown\",\n"
            "  \"asset\": \"BTC\",\n"
            "  \"symbol\": \"BTCUSDT\",\n"
            "  \"timeframe\": \"4h\",\n"
            "  \"top_n\": 10,\n"
            "  \"quote_asset\": \"USDT\",\n"
            "  \"stop_pct\": 5.0,\n"
            "  \"amount_mode\": \"quote_amount|quote_percent\",\n"
            "  \"amount_value\": 10.0,\n"
            "  \"auto_size\": true,\n"
            "  \"execute_requested\": false,\n"
            "  \"expand_until_found\": false,\n"
            "  \"scheduler_action\": \"start|stop\",\n"
            "  \"scheduler_minutes\": 30,\n"
            "  \"open_view\": \"portfolio\",\n"
            "  \"context_updates\": {\"exclude_assets\": [\"TRX\"]},\n"
            "  \"exclude_assets\": [\"TRX\"],\n"
            "  \"error\": \"\"\n"
            "}\n"
            "END_STRATA_AGENT_INTENT_JSON\n\n"
            "User command + runtime context JSON:\n"
            + json.dumps(payload, indent=2)
        )

    def _parse_agent_intent_ai_response(self, text: str) -> Dict[str, Any]:
        raw = str(text or "")
        if not raw.strip():
            return {"intent": "unknown", "error": "Empty AI response."}
        m = re.search(
            r"BEGIN_STRATA_AGENT_INTENT_JSON\s*(\{[\s\S]*?\})\s*END_STRATA_AGENT_INTENT_JSON",
            raw,
            flags=re.IGNORECASE,
        )
        blob = ""
        if m:
            blob = m.group(1).strip()
        else:
            m2 = re.search(r"(\{[\s\S]*\})", raw)
            if m2:
                blob = m2.group(1).strip()
        if not blob:
            return {"intent": "unknown", "error": "AI intent JSON not found in response."}
        try:
            parsed = json.loads(blob)
        except Exception as exc:
            return {"intent": "unknown", "error": f"AI intent JSON parse failed: {type(exc).__name__}"}
        if not isinstance(parsed, dict):
            return {"intent": "unknown", "error": "AI intent payload is not an object."}
        intent = str(parsed.get("intent", "unknown")).strip().lower()
        allowed = {"buy", "scan_allocate", "portfolio_pnl", "portfolio_balance", "open_view", "scheduler", "status", "set_context", "execute_staged", "unknown"}
        if intent not in allowed:
            return {"intent": "unknown", "error": f"AI returned unsupported intent: {intent}"}
        parsed["intent"] = intent
        return parsed

    def _parse_agent_intent(self, cmd: str, mode: str) -> Dict[str, Any]:
        c_raw = str(cmd or "").strip()
        c = c_raw.lower()
        tf = str(self.agent_context.get("timeframe", "4h")).strip().lower() or "4h"
        m_tf = re.search(r"\b(1d|4h|8h|12h)\b", c)
        if m_tf:
            tf = m_tf.group(1)
        top_n = int(self.agent_context.get("top_n", 10) or 10)
        m_top = re.search(r"top\s+(\d+)", c)
        if m_top:
            try:
                top_n = max(1, min(100, int(m_top.group(1))))
            except Exception:
                top_n = 10
        stop_pct = float(self.agent_context.get("stop_pct", 5.0) or 5.0)
        if re.search(r"\b(no stop|without stop)\b", c):
            stop_pct = 0.0
        else:
            m_stop = re.search(r"stop(?:\s*loss)?[^0-9]{0,8}([0-9]+(?:\.[0-9]+)?)\s*%?", c)
            if m_stop:
                try:
                    stop_pct = max(0.0, min(25.0, float(m_stop.group(1))))
                except Exception:
                    stop_pct = 5.0
        quote = str(self.agent_context.get("quote_asset", self._resolve_agent_quote_from_text(c))).strip().upper()
        if re.search(r"\b(usdt|usdc|fdusd|busd|usd|btc|eth|bnb)\b", c):
            quote = self._resolve_agent_quote_from_text(c)
        exec_words = ("execute", "submit", "place order", "buy now", "transaction", "go live")
        execute_requested = any([w in c for w in exec_words])
        scan_words = ("best buy", "best buys", "look for signals", "search for", "allocate", "invest in")
        expand_words = ("expand search", "until found", "comprehensive")
        excl = self.agent_context.get("exclude_assets", [])
        if not isinstance(excl, list):
            excl = []

        m_every = re.search(r"\brun\s+every\s+(\d+)\s*(m|min|mins|minute|minutes)\b", c)
        if m_every:
            mins = max(1, min(240, int(m_every.group(1))))
            return {"intent": "scheduler", "scheduler_action": "start", "scheduler_minutes": mins}
        if re.search(r"\b(pause|stop)\b.*\b(scheduler|pipeline|auto)\b", c) or c in {"pause", "stop"}:
            return {"intent": "scheduler", "scheduler_action": "stop"}
        if re.search(r"\b(status|show status|agent status)\b", c):
            return {"intent": "status"}
        if re.search(r"\b(pnl|profit|performance|return)\b", c):
            window = "all"
            n = 20
            m_last = re.search(r"\blast\s+(\d+)\b", c)
            if m_last:
                window = "last_n"
                try:
                    n = max(1, min(500, int(m_last.group(1))))
                except Exception:
                    n = 20
            if re.search(r"\b(today|24h)\b", c):
                window = "today"
            return {"intent": "portfolio_pnl", "window": window, "last_n": n}
        if re.search(r"\b(how much|balance|holdings?|position|portfolio)\b", c):
            asset = ""
            m_asset = re.search(r"\b(?:of|for|in)\s+([a-z0-9]{2,12})\b", c)
            if m_asset:
                asset = str(m_asset.group(1) or "").upper()
            if not asset:
                tokens = [t.upper() for t in re.findall(r"\b[a-z0-9]{2,12}\b", c)]
                ignore = {
                    "HOW", "MUCH", "DO", "I", "HAVE", "MY", "BALANCE", "HOLDING", "HOLDINGS",
                    "POSITION", "PORTFOLIO", "SHOW", "IN", "OF", "FOR", "THE", "A", "AN", "PLEASE",
                }
                for t in tokens:
                    if t in ignore:
                        continue
                    if t in COMMON_CRYPTO_BASES or t in {"USDT", "USDC", "FDUSD", "BUSD", "USD", "BTC", "ETH", "BNB"}:
                        asset = t
                        break
            return {"intent": "portfolio_balance", "asset": asset}

        m_excl = re.search(r"\bexclude\s+([a-z0-9,\s]+)$", c)
        if m_excl:
            raw = m_excl.group(1)
            tokens = [x.strip().upper() for x in re.split(r"[,\s]+", raw) if x.strip()]
            tokens = [t for t in tokens if re.fullmatch(r"[A-Z0-9]{2,12}", t)]
            new_excl = sorted(list(set(excl + tokens)))
            return {"intent": "set_context", "context_updates": {"exclude_assets": new_excl}}
        if re.search(r"\b(clear exclusions|reset exclusions)\b", c):
            return {"intent": "set_context", "context_updates": {"exclude_assets": []}}

        if ("open detailed view" in c) or ("open detail view" in c) or ("open portfolio" in c):
            return {"intent": "open_view", "open_view": "portfolio"}
        if re.search(r"\b(execute|submit)\s+(last\s+)?staged\b", c):
            return {"intent": "execute_staged"}

        if any([w in c for w in scan_words]):
            return {
                "intent": "scan_allocate",
                "timeframe": tf,
                "top_n": top_n,
                "quote_asset": quote,
                "stop_pct": stop_pct,
                "expand_until_found": any([w in c for w in expand_words]),
                "auto_size": ("allocate" in c) or ("current " + quote.lower() in c) or ("with my current" in c),
                "execute_requested": execute_requested and mode in ("semi_auto", "auto_execute"),
                "exclude_assets": excl,
            }

        if "buy" in c:
            m_amt = re.search(
                r"\bbuy\s+([0-9]+(?:\.[0-9]+)?)\s*(usd|usdt|usdc|fdusd|busd)?\s*(?:of\s+)?([a-z0-9]{2,12})\b",
                c,
            )
            m_sym = re.search(r"\bbuy\s+([a-z0-9]{2,12})\b", c)
            base = ""
            amount_mode = "quote_percent"
            amount_value = 10.0
            if m_amt:
                base = str(m_amt.group(3) or "").upper()
                amount_mode = "quote_amount"
                try:
                    amount_value = max(0.01, float(m_amt.group(1)))
                except Exception:
                    amount_value = 10.0
                if m_amt.group(2):
                    qtxt = str(m_amt.group(2)).upper()
                    if qtxt == "USD" and self._is_primary_quote_locked():
                        qtxt = self._primary_quote_asset()
                    quote = qtxt
            elif m_sym:
                base = str(m_sym.group(1) or "").upper()
            banned = {"SIGNAL", "SIGNALS", "STRATEGY", "ORDER", "ORDERS", "VIEW", "DETAIL", "DETAILED"}
            if (not base) or (base in banned):
                return {"intent": "unknown", "error": "Could not parse buy symbol."}
            m_pct = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*%\s*(?:of\s+my\s+)?(usdt|usd|usdc|fdusd|busd|balance|capital)?", c)
            if amount_mode != "quote_amount" and m_pct:
                amount_mode = "quote_percent"
                try:
                    amount_value = max(0.1, min(100.0, float(m_pct.group(1))))
                except Exception:
                    amount_value = 10.0
                pool = str(m_pct.group(2) or "").strip().upper()
                if pool in {"USDT", "USD", "USDC", "FDUSD", "BUSD"}:
                    quote = self._primary_quote_asset() if (pool == "USD" and self._is_primary_quote_locked()) else pool
            return {
                "intent": "buy",
                "asset": base,
                "symbol": f"{base}{quote}",
                "timeframe": tf,
                "quote_asset": quote,
                "stop_pct": stop_pct,
                "amount_mode": amount_mode,
                "amount_value": amount_value,
                "execute_requested": execute_requested and mode in ("semi_auto", "auto_execute"),
                "exclude_assets": excl,
            }

        return {"intent": "unknown"}

    def _execute_agent_command(self, bridge: EngineBridge, cmd: str, mode: str) -> Dict[str, Any]:
        parsed = self._parse_agent_intent(cmd, mode)
        intent = str(parsed.get("intent", "unknown")).strip().lower()
        ai_used = False
        ai_fallback_enabled = bool(self.agent_ai_fallback_var.get()) if hasattr(self, "agent_ai_fallback_var") else False
        if intent == "unknown" and ai_fallback_enabled:
            dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ai_prompt = self._build_agent_ai_intent_prompt(cmd, mode)
            ai_res = bridge.run_ai_analysis(
                dashboard_text=str(cmd or ""),
                datetime_context=dt,
                prompt_override=ai_prompt,
                system_prompt_override=(
                    "You are a strict command parser for a trading copilot. "
                    "Output only the requested JSON object between markers."
                ),
            )
            if ai_res.get("ok"):
                parsed_ai = self._parse_agent_intent_ai_response(ai_res.get("response", ""))
                if str(parsed_ai.get("intent", "unknown")).strip().lower() != "unknown":
                    parsed = parsed_ai
                    intent = str(parsed.get("intent", "unknown")).strip().lower()
                    ai_used = True
            else:
                self._append_task_terminal(f"AI fallback unavailable: {ai_res.get('error', 'unknown')}")
        if intent == "set_context":
            upd = parsed.get("context_updates", {})
            if not isinstance(upd, dict):
                upd = {}
            for k, v in upd.items():
                self.agent_context[k] = v
            self.state["agent_context"] = self.agent_context
            save_state(self.state)
            return {"ok": True, "text": self._build_agent_plan_card({"intent": "context_updated", **self.agent_context})}
        if intent == "status":
            excl = self.agent_context.get("exclude_assets", []) or []
            ex_txt = ", ".join([str(x) for x in excl[:12]]) if excl else "none"
            sched = "running" if bool(self.pipeline_job) else "stopped"
            return {
                "ok": True,
                "text": (
                    "Agent Status\n"
                    f"- Timeframe: {self.agent_context.get('timeframe', '4h')}\n"
                    f"- Top-N: {self.agent_context.get('top_n', 10)}\n"
                    f"- Quote: {self.agent_context.get('quote_asset', self._effective_crypto_quote('USDT'))}\n"
                    f"- Stop %: {self.agent_context.get('stop_pct', 5.0)}\n"
                    f"- Exclusions: {ex_txt}\n"
                    f"- Scheduler: {sched} ({self.pipeline_interval_min_var.get().strip() or '30'} min)"
                ),
            }
        if intent == "scheduler":
            act = str(parsed.get("scheduler_action", "")).strip().lower()
            mins = parsed.get("scheduler_minutes")
            if act == "start":
                return {
                    "ok": True,
                    "text": f"Starting Live->AI pipeline scheduler every {int(float(mins or 30))} minutes.",
                    "scheduler_action": "start",
                    "scheduler_minutes": int(float(mins or 30)),
                }
            return {"ok": True, "text": "Stopping Live->AI pipeline scheduler.", "scheduler_action": "stop"}
        if intent == "portfolio_balance":
            profile = self.pf_binance_profile_var.get().strip() or None
            if not profile:
                return {"ok": False, "error": "Select a Binance profile first."}
            fres = bridge.fetch_binance_portfolio(profile_name=profile)
            if not fres.get("ok"):
                return {"ok": False, "error": str(fres.get("error", "Failed to fetch portfolio."))}
            asset = str(parsed.get("asset", "") or "").strip().upper()
            balances = fres.get("balances", []) if isinstance(fres.get("balances"), list) else []
            total_est_usd = float(fres.get("total_est_usd", 0.0) or 0.0)
            if asset:
                row = None
                for b in balances:
                    if isinstance(b, dict) and str(b.get("asset", "")).upper() == asset:
                        row = b
                        break
                if row is None:
                    return {
                        "ok": True,
                        "text": (
                            f"Holding check ({profile})\n"
                            f"- {asset}: free=0, locked=0, total=0\n"
                            f"- Portfolio est USD: {total_est_usd:,.2f}"
                        ),
                        "open_view": "portfolio",
                    }
                free = float(row.get("free", 0.0) or 0.0)
                locked = float(row.get("locked", 0.0) or 0.0)
                total = float(row.get("total", 0.0) or 0.0)
                est_usd = float(row.get("est_usd", 0.0) or 0.0)
                return {
                    "ok": True,
                    "text": (
                        f"Holding check ({profile})\n"
                        f"- {asset}: free={free:.8f}, locked={locked:.8f}, total={total:.8f}\n"
                        f"- Est USD value: {est_usd:,.2f}\n"
                        f"- Portfolio est USD: {total_est_usd:,.2f}"
                    ),
                    "open_view": "portfolio",
                }
            lines = [
                f"Portfolio balance summary ({profile})",
                f"- Total est USD: {total_est_usd:,.2f}",
                "- Top holdings:",
            ]
            for b in balances[:10]:
                if not isinstance(b, dict):
                    continue
                a = str(b.get("asset", "")).upper()
                t = float(b.get("total", 0.0) or 0.0)
                u = float(b.get("est_usd", 0.0) or 0.0)
                lines.append(f"  - {a}: total={t:.8f} (~${u:,.2f})")
            return {"ok": True, "text": "\n".join(lines), "open_view": "portfolio"}
        if intent == "portfolio_pnl":
            out = bridge.get_trade_ledger()
            if not out.get("ok"):
                return {"ok": False, "error": str(out.get("error", "Failed to load trade ledger."))}
            execution_entries = out.get("execution_entries", [])
            if not isinstance(execution_entries, list):
                execution_entries = []
            rows: List[Dict[str, Any]] = []
            for e in execution_entries:
                if not isinstance(e, dict):
                    continue
                pq_raw = e.get("pnl_quote", None)
                pd_raw = e.get("pnl_display", None)
                if pq_raw is None and pd_raw is None:
                    continue
                ee = dict(e)
                try:
                    ee["_pnl_quote"] = float(pq_raw or 0.0)
                except Exception:
                    ee["_pnl_quote"] = 0.0
                try:
                    ee["_pnl_display"] = float(pd_raw or 0.0)
                except Exception:
                    ee["_pnl_display"] = ee["_pnl_quote"]
                ee["_ts"] = pd.to_datetime(ee.get("ts", ""), utc=True, errors="coerce")
                rows.append(ee)
            if not rows:
                return {"ok": True, "text": "PnL summary: no realized execution PnL entries yet.", "open_view": "portfolio"}

            window = str(parsed.get("window", "all") or "all").strip().lower()
            last_n = int(parsed.get("last_n", 20) or 20)
            filt = rows
            if window == "today":
                local_today = pd.Timestamp.now(tz="Australia/Sydney").date()
                tmp = []
                for r in rows:
                    ts = r.get("_ts")
                    if isinstance(ts, pd.Timestamp) and not pd.isna(ts):
                        try:
                            if ts.tz_convert("Australia/Sydney").date() == local_today:
                                tmp.append(r)
                        except Exception:
                            pass
                filt = tmp
            elif window == "last_n":
                filt = sorted(rows, key=lambda x: x.get("_ts", pd.Timestamp.min), reverse=True)[:last_n]

            if not filt:
                if window == "today":
                    return {"ok": True, "text": "PnL summary (today): no realized entries today.", "open_view": "portfolio"}
                return {"ok": True, "text": f"PnL summary: no entries for selected window ({window}).", "open_view": "portfolio"}

            qsum = float(sum([float(r.get("_pnl_quote", 0.0) or 0.0) for r in filt]))
            dsum = float(sum([float(r.get("_pnl_display", 0.0) or 0.0) for r in filt]))
            wins = sum([1 for r in filt if float(r.get("_pnl_quote", 0.0) or 0.0) > 0])
            losses = sum([1 for r in filt if float(r.get("_pnl_quote", 0.0) or 0.0) < 0])
            total = len(filt)
            win_rate = (wins / total * 100.0) if total > 0 else 0.0
            scope = "all realized executions"
            if window == "today":
                scope = "today"
            elif window == "last_n":
                scope = f"last {last_n} realized executions"
            txt = (
                "PnL Summary\n"
                f"- Scope: {scope}\n"
                f"- Trades: {total} (wins={wins}, losses={losses}, win-rate={win_rate:.1f}%)\n"
                f"- Realized PnL (Quote): {qsum:,.4f}\n"
                f"- Realized PnL (Display): {dsum:,.4f}"
            )
            return {"ok": True, "text": txt, "open_view": "portfolio"}
        if intent == "execute_staged":
            return {"ok": True, "text": "Executing latest staged recommendations.", "execute_last_staged": True}
        if intent == "open_view":
            txt = "Opening Portfolio & Ledger detailed view."
            if ai_used:
                txt = "[AI intent parsed]\n" + txt
            return {"ok": True, "text": txt, "open_view": parsed.get("open_view", "portfolio")}
        if intent == "unknown":
            err = str(parsed.get("error", "")).strip()
            if err:
                return {"ok": False, "error": err + " Try: 'buy 10 usdt of btc with stop loss 5%'."}
            return {
                "ok": True,
                "text": (
                    "Command understood, but no executable intent matched yet.\n"
                    "Try:\n"
                    "- find best buys top 10 crypto 4h and allocate\n"
                    "- buy 10 usdt of btc with stop loss 5%\n"
                    "- buy btc with 30% of my usdt\n"
                    "- open detailed view"
                ),
            }

        plan_card = self._build_agent_plan_card(parsed)
        tf = str(parsed.get("timeframe", "4h")).strip().lower() or "4h"
        quote = str(parsed.get("quote_asset", self._effective_crypto_quote("USDT"))).strip().upper()
        stop_pct = float(parsed.get("stop_pct", 5.0) or 0.0)
        self.agent_context["timeframe"] = tf
        self.agent_context["quote_asset"] = quote
        self.agent_context["stop_pct"] = stop_pct
        try:
            self.agent_context["top_n"] = int(parsed.get("top_n", self.agent_context.get("top_n", 10)) or 10)
        except Exception:
            self.agent_context["top_n"] = 10
        if isinstance(parsed.get("exclude_assets"), list):
            self.agent_context["exclude_assets"] = parsed.get("exclude_assets")
        self.state["agent_context"] = self.agent_context
        save_state(self.state)

        if intent == "scan_allocate":
            top_n = int(parsed.get("top_n", 10) or 10)
            exclude_assets = {str(x).strip().upper() for x in (parsed.get("exclude_assets", []) or []) if str(x).strip()}
            tiers = [top_n]
            if bool(parsed.get("expand_until_found", False)):
                for t in [10, 20, 50, 100]:
                    if t not in tiers:
                        tiers.append(t)
            buys: List[Dict[str, Any]] = []
            matched_top = top_n
            for n in tiers:
                live_cfg = {
                    "name": "Agent Scan",
                    "market": "crypto",
                    "timeframe": tf,
                    "quote_currency": quote,
                    "top_n": n,
                    "display_currency": self.display_currency_var.get().strip() or "USD",
                }
                live = bridge.run_live_panel(live_cfg)
                if not live.get("ok"):
                    continue
                rows = live.get("table_rows", []) if isinstance(live.get("table_rows"), list) else []
                staged: List[Dict[str, Any]] = []
                for r in rows:
                    if not isinstance(r, dict):
                        continue
                    act = str(r.get("Action", "")).upper()
                    if "BUY" not in act:
                        continue
                    asset = str(r.get("Asset", "")).strip().upper()
                    if not asset:
                        continue
                    if asset in exclude_assets:
                        continue
                    try:
                        px = float(r.get("Price", 0.0) or 0.0)
                    except Exception:
                        px = 0.0
                    sl = px * (1.0 - (stop_pct / 100.0)) if (px > 0 and stop_pct > 0) else 0.0
                    score_raw = str(r.get("Raw Score", "") or r.get("Score", "")).strip()
                    conf = ""
                    m_sc = re.search(r"(-?\d+)\s*/\s*5", score_raw)
                    if m_sc:
                        try:
                            sc = int(m_sc.group(1))
                            conf = str(max(1.0, min(99.0, 50.0 + (sc * 10.0))))
                        except Exception:
                            conf = ""
                    self._pending_rec_seq += 1
                    staged.append(
                        {
                            "id": self._pending_rec_seq,
                            "symbol": f"{asset}{quote}",
                            "asset": asset,
                            "side": "BUY",
                            "order_type": "MARKET",
                            "quantity": 0.0,
                            "stop_loss_price": (round(sl, 8) if sl > 0 else 0.0),
                            "timeframe": tf,
                            "confidence": conf,
                            "status": "PENDING",
                            "reason": f"Agent scan allocation ({tf}, top {n})",
                        }
                    )
                if staged:
                    buys = staged
                    matched_top = n
                    break
            if not buys:
                tiers_txt = ", ".join([str(t) for t in tiers])
                return {"ok": True, "text": plan_card + f"\n\nNo BUY signals found (searched top tiers: {tiers_txt}) on {tf}.", "staged_recs": []}
            lines = [plan_card, "", f"Signal scan found {len(buys)} BUY candidate(s) on {tf} (top {matched_top}, quote {quote})."]
            for b in buys[:10]:
                lines.append(f"- {b['symbol']} stop~{b.get('stop_loss_price', 0)}")
            auto_size = bool(parsed.get("auto_size", False))
            auto_exec = bool(parsed.get("execute_requested", False)) and mode == "auto_execute"
            if auto_size:
                lines.append("Allocation step: will auto-size recommendations from available balances.")
            if auto_exec:
                lines.append("Execution step: will submit immediately (auto_execute mode).")
            out = {
                "ok": True,
                "text": "\n".join(lines),
                "staged_recs": buys,
                "auto_size": auto_size,
                "auto_execute": auto_exec,
                "refresh_ledger": auto_exec,
            }
            if ai_used:
                out["text"] = "[AI intent parsed]\n" + str(out.get("text", ""))
            return out

        if intent == "buy":
            base = str(parsed.get("asset", "")).strip().upper()
            symbol = str(parsed.get("symbol", f"{base}{quote}")).strip().upper()
            amount_mode = str(parsed.get("amount_mode", "quote_percent")).strip().lower()
            amount_value = float(parsed.get("amount_value", 0.0) or 0.0)
            execute_requested = bool(parsed.get("execute_requested", False))
            profile = self.pf_binance_profile_var.get().strip() or None
            if not profile:
                return {"ok": False, "error": "Select a Binance profile first."}
            fres = bridge.fetch_binance_portfolio(profile_name=profile)
            if not fres.get("ok"):
                return {"ok": False, "error": str(fres.get("error", "Failed to fetch portfolio."))}
            free_quote = 0.0
            for b in fres.get("balances", []) or []:
                if isinstance(b, dict) and str(b.get("asset", "")).upper() == quote:
                    try:
                        free_quote = float(b.get("free", 0.0) or 0.0)
                    except Exception:
                        free_quote = 0.0
                    break
            if free_quote <= 0:
                return {"ok": False, "error": f"No free {quote} balance available."}
            p = bridge.get_binance_last_price(symbol=symbol, profile_name=profile)
            if not p.get("ok"):
                return {"ok": False, "error": str(p.get("error", "Failed to fetch symbol price."))}
            px = float(p.get("price", 0.0) or 0.0)
            if px <= 0:
                return {"ok": False, "error": "Invalid market price."}
            if amount_mode == "quote_amount":
                spend = max(0.01, amount_value)
            else:
                pct = max(0.1, min(100.0, amount_value if amount_value > 0 else 10.0))
                spend = free_quote * (pct / 100.0)
            if spend > free_quote:
                return {"ok": False, "error": f"Requested spend {spend:.4f} {quote} exceeds free balance {free_quote:.4f} {quote}."}
            qty = spend / px
            v = bridge.validate_binance_order(symbol=symbol, side="BUY", order_type="MARKET", quantity=qty, profile_name=profile)
            if not v.get("ok"):
                return {"ok": False, "error": str(v.get("error", "Order validation failed."))}
            nq = float(v.get("normalized_quantity", 0.0) or 0.0)
            sl = px * (1.0 - (stop_pct / 100.0)) if stop_pct > 0 else 0.0
            self._pending_rec_seq += 1
            rec = {
                "id": self._pending_rec_seq,
                "symbol": symbol,
                "asset": base,
                "side": "BUY",
                "order_type": "MARKET",
                "quantity": nq,
                "stop_loss_price": round(sl, 8) if sl > 0 else 0.0,
                "timeframe": tf,
                "confidence": "",
                "status": "PENDING",
                "reason": (
                    f"Agent BUY plan using {spend:.4f} {quote}"
                    + (f" ({amount_value:.2f}% balance)" if amount_mode == "quote_percent" else "")
                    + (f"; stop {stop_pct:.2f}%" if stop_pct > 0 else "; no stop")
                ),
            }
            if mode == "plan" or not execute_requested:
                out = {
                    "ok": True,
                    "text": plan_card + f"\n\nPlanned BUY {symbol}: qty={nq:.8f}, est spend={spend:.4f} {quote}, stop~{sl:.8f}",
                    "staged_recs": [rec],
                }
                if ai_used:
                    out["text"] = "[AI intent parsed]\n" + str(out.get("text", ""))
                return out

            if bool(self.agent_guard_enabled_var.get()):
                try:
                    max_loss = float((self.agent_guard_max_daily_loss_var.get() or "5").strip())
                except Exception:
                    max_loss = 5.0
                try:
                    max_trades = int((self.agent_guard_max_trades_var.get() or "8").strip())
                except Exception:
                    max_trades = 8
                try:
                    max_exposure = float((self.agent_guard_max_exposure_var.get() or "40").strip())
                except Exception:
                    max_exposure = 40.0
                pol = bridge.evaluate_agent_policy(
                    profile_name=profile,
                    display_currency=self.display_currency_var.get().strip() or "USD",
                    max_daily_loss_pct=max_loss,
                    max_trades_per_day=max_trades,
                    max_exposure_pct=max_exposure,
                    pending_buy_count=1,
                )
                if not pol.get("ok"):
                    reasons = pol.get("reasons", []) or [pol.get("error", "Policy check failed.")]
                    return {"ok": False, "error": "Agent policy blocked execution: " + " | ".join([str(x) for x in reasons])}
            if bool(self.agent_guard_require_stop_var.get()) and stop_pct <= 0:
                return {"ok": False, "error": "Agent policy requires a stop loss for BUY execution."}

            out = bridge.submit_binance_order(symbol=symbol, side="BUY", order_type="MARKET", quantity=nq, profile_name=profile)
            if not out.get("ok"):
                return {"ok": False, "error": str(out.get("error", "Submit failed."))}
            stop_msg = "No protective stop requested."
            if sl > 0:
                stop_limit = sl * 0.995
                stop_out = bridge.submit_binance_order(
                    symbol=symbol,
                    side="SELL",
                    order_type="STOP_LOSS_LIMIT",
                    quantity=float(out.get("normalized_quantity", nq) or nq),
                    profile_name=profile,
                    price=stop_limit,
                    stop_price=sl,
                )
                if stop_out.get("ok"):
                    stop_msg = f"Protective stop set at {sl:.8f}."
                else:
                    stop_msg = f"Stop placement failed: {stop_out.get('error', 'unknown')}."
            bridge.record_signal_event(
                {
                    "market": "crypto",
                    "timeframe": tf,
                    "panel": "agent_console",
                    "asset": base,
                    "action": "BUY",
                    "price": float(out.get("normalized_price", 0.0) or 0.0),
                    "qty": float(out.get("normalized_quantity", nq) or nq),
                    "quote_currency": quote,
                    "display_currency": self.display_currency_var.get().strip() or "USD",
                    "note": f"Agent BUY ({symbol})",
                    "is_execution": True,
                },
                allow_duplicate=True,
            )
            msg = (
                plan_card
                + f"\n\nExecuted BUY {symbol}: qty={float(out.get('normalized_quantity', nq) or nq):.8f}, "
                + f"est spend={spend:.4f} {quote}. {stop_msg}"
            )
            out = {"ok": True, "text": msg, "refresh_ledger": True}
            if ai_used:
                out["text"] = "[AI intent parsed]\n" + str(out.get("text", ""))
            return out

        return {"ok": False, "error": "Unhandled agent intent."}

    def _start_run_ai_analysis(self) -> None:
        self._start_run_ai_analysis_internal(source_override=None, force_no_confirm=False)

    def _start_run_ai_analysis_internal(self, source_override: Optional[str], force_no_confirm: bool) -> None:
        task_name = "AI Analysis"
        task_id = self._set_busy(True, task_name)
        self._append_task_terminal("START AI Analysis")
        self.ai_output.delete("1.0", tk.END)
        self.ai_output.insert("1.0", "Preparing AI request...\n")
        original_source = self.ai_source.get().strip()
        if source_override:
            self.ai_source.set(source_override)
        text = self._resolve_ai_source_text()
        if source_override:
            self.ai_source.set(original_source)
        if not text.strip():
            self.ai_output.insert("1.0", "No source text available.")
            self._append_task_terminal("DONE AI Analysis (no source text)")
            messagebox.showinfo(
                "AI Analysis",
                "No source text available.\n\n"
                "Tip: run a Live/Backtest first or switch AI Source to 'paste' and provide input text.",
            )
            self._finish_task(task_id, task_name=task_name)
            return
        self.ai_last_source_text = text
        dt = self.ai_datetime.get().strip() or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            prompt = self._build_ai_prompt(text, dt)
        except Exception as exc:
            self.ai_output.delete("1.0", tk.END)
            self.ai_output.insert("1.0", f"Failed to build AI prompt: {type(exc).__name__}: {exc}")
            self._append_task_terminal(f"DONE AI Analysis (prompt build error: {type(exc).__name__})")
            messagebox.showerror("AI Prompt Error", f"{type(exc).__name__}: {exc}")
            self._finish_task(task_id, task_name=task_name)
            return
        self._vlog(
            f"AI request prepared: source={source_override or self.ai_source.get().strip()} "
            f"prompt_mode={self.ai_prompt_mode.get().strip()} chars_source={len(text)} chars_prompt={len(prompt)}"
        )
        if self.ai_require_confirm.get() and (not force_no_confirm):
            preview = prompt[:2000]
            ok = messagebox.askyesno("Confirm AI Request", f"Send this prompt?\n\n{preview}")
            if not ok:
                self._finish_task(task_id, task_name=task_name)
                self.ai_output.insert("1.0", "AI request canceled by user.")
                self._append_task_terminal("DONE AI Analysis (canceled)")
                return

        def worker():
            bridge = self._bridge_for_task()
            res = bridge.run_ai_analysis(text, dt, prompt_override=prompt)
            log = (res.get("log", "") or "").strip()
            if log:
                self._append_task_terminal_from_worker(f"LOG [AI] {log[:6000]}")
            self.root.after(0, lambda: self._finish_ai_output(res, task_id))

        threading.Thread(target=worker, daemon=True).start()

    def _run_live_ai_pipeline_now(self) -> None:
        # Chain: run all live panels -> queue AI analysis on combined live panels.
        self._append_task_terminal("Pipeline requested: Live dashboards -> AI analysis/staging.")
        self._run_all_panels()
        # AI will run after live task due to queue lock.
        self._queue_if_busy(
            "AI Analysis (Pipeline)",
            lambda: self._start_run_ai_analysis_internal(source_override="live_all_panels", force_no_confirm=True),
        )

    def _run_live_backtest_ai_pipeline(self) -> None:
        if self._queue_if_busy("Live->Backtest->AI Pipeline", self._start_live_backtest_ai_pipeline):
            return
        self._start_live_backtest_ai_pipeline()

    def _start_live_backtest_ai_pipeline(self) -> None:
        task_name = "Live->Backtest->AI Pipeline"
        task_id = self._set_busy(True, task_name)
        self._append_task_terminal("START Live->Backtest->AI Pipeline")
        self.ai_output.delete("1.0", tk.END)
        self.ai_output.insert("1.0", "Running live/backtest/AI pipeline...\n")

        def worker():
            bridge = self._bridge_for_task()
            live_chunks: List[str] = []
            signal_map: Dict[Tuple[str, str], set] = {}
            for p in self.live_panels:
                cfg = asdict(p)
                cfg["display_currency"] = self.display_currency_var.get().strip() or "USD"
                if str(cfg.get("market", "crypto")).strip().lower() == "crypto" and self._is_primary_quote_locked():
                    cfg["quote_currency"] = self._primary_quote_asset()
                res = bridge.run_live_panel(cfg)
                if not res.get("ok"):
                    self._append_task_terminal_from_worker(f"LOG [Pipeline Live] {p.name} failed: {res.get('error', 'unknown')}")
                    continue
                blob = "\n".join(
                    [
                        f"=== {p.name} ({res['market']} {res['timeframe']}) ===",
                        f"Assets loaded: {res['loaded_assets']}/{res['requested_assets']}",
                        "",
                        res["table_text"],
                        "",
                        "RISK SCORE BREAKDOWN",
                        res["risk_text"],
                    ]
                )
                live_chunks.append(blob)
                for s in self._extract_signals_from_live_text(blob):
                    mkt = str(s.get("market", "crypto")).strip().lower()
                    tf = str(s.get("timeframe", "1d")).strip().lower()
                    asset = str(s.get("asset", "")).strip().upper()
                    if not asset:
                        continue
                    signal_map.setdefault((mkt, tf), set()).add(asset)

            live_text = "\n\n".join(live_chunks).strip()
            if not live_text:
                self.root.after(
                    0,
                    lambda: self._finish_live_backtest_ai_pipeline(
                        {"ok": False, "error": "No live panel output available for pipeline."},
                        task_id,
                        task_name,
                    ),
                )
                return

            def _to_int(v: str, default: int) -> int:
                try:
                    return int(str(v).strip())
                except Exception:
                    return default

            def _to_float(v: str, default: float) -> float:
                try:
                    return float(str(v).strip())
                except Exception:
                    return default

            months = _to_int(self.bt_months.get(), 12)
            quote = self._effective_crypto_quote("USDT")
            targeted_parts: List[str] = []
            for (mkt, tf), assets in sorted(signal_map.items()):
                aset = sorted(list(assets))
                if not aset:
                    continue
                if mkt == "crypto":
                    tks = [f"{a}-{quote}" for a in aset]
                else:
                    tks = aset
                bt_cfg = {
                    "market": mkt,
                    "timeframe": tf,
                    "months": months,
                    "top_n": max(1, len(tks)),
                    "tickers": tks,
                    "quote_currency": quote,
                    "country": parse_country_code(self.bt_country.get(), self.bt_country_manual.get()),
                    "initial_capital": _to_float(self.bt_initial.get(), 10000.0),
                    "stop_loss_pct": _to_float(self.bt_stop_loss.get(), 8.0),
                    "take_profit_pct": _to_float(self.bt_take_profit.get(), 20.0),
                    "max_hold_days": _to_int(self.bt_max_hold_days.get(), 45),
                    "min_hold_bars": _to_int(self.bt_min_hold_bars.get(), 2),
                    "cooldown_bars": _to_int(self.bt_cooldown_bars.get(), 1),
                    "same_asset_cooldown_bars": _to_int(self.bt_same_asset_cooldown.get(), 3),
                    "max_consecutive_same_asset_entries": _to_int(self.bt_max_same_asset_entries.get(), 3),
                    "fee_pct": _to_float(self.bt_fee_pct.get(), 0.10),
                    "slippage_pct": _to_float(self.bt_slippage_pct.get(), 0.05),
                    "position_size": max(0.01, min(1.0, _to_float(self.bt_position_size_pct.get(), 30.0) / 100.0)),
                    "atr_multiplier": _to_float(self.bt_atr_mult.get(), 2.2),
                    "adx_threshold": _to_float(self.bt_adx_threshold.get(), 25.0),
                    "cmf_threshold": _to_float(self.bt_cmf_threshold.get(), 0.02),
                    "obv_slope_threshold": _to_float(self.bt_obv_threshold.get(), 0.0),
                    "max_drawdown_limit_pct": _to_float(self.bt_max_dd_target_pct.get(), 35.0),
                    "max_exposure_pct": max(0.01, min(1.0, _to_float(self.bt_max_exposure_pct.get(), 40.0) / 100.0)),
                    "cache_workers": _to_int(self.bt_cache_workers.get(), 8),
                    "buy_threshold": _to_int(self.bt_buy_threshold.get(), 2),
                    "sell_threshold": _to_int(self.bt_sell_threshold.get(), -2),
                    "display_currency": self.display_currency_var.get() or "USD",
                }
                bt_res = bridge.run_backtest(bt_cfg)
                if not bt_res.get("ok"):
                    targeted_parts.append(f"## {mkt.upper()} {tf} (assets={len(tks)})\nBACKTEST ERROR: {bt_res.get('error', 'unknown')}\n")
                else:
                    targeted_parts.append(
                        "\n".join(
                            [
                                f"## {mkt.upper()} {tf} (assets={len(tks)})",
                                bt_res.get("summary_text", ""),
                                "",
                                "TRADE SAMPLE",
                                bt_res.get("trades_text", ""),
                            ]
                        )
                    )

            combined_source = (
                "LIVE DASHBOARD SNAPSHOT\n"
                + ("=" * 80)
                + "\n"
                + live_text
                + "\n\nTARGETED BACKTEST REVIEW\n"
                + ("=" * 80)
                + "\n"
                + ("\n\n".join(targeted_parts) if targeted_parts else "No targeted backtest output.")
            )
            dt = self.ai_datetime.get().strip() or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            prompt = self._build_ai_prompt(combined_source, dt)
            ai_res = bridge.run_ai_analysis(combined_source, dt, prompt_override=prompt)
            ai_res["combined_source"] = combined_source
            self.root.after(0, lambda: self._finish_live_backtest_ai_pipeline(ai_res, task_id, task_name))

        threading.Thread(target=worker, daemon=True).start()

    def _finish_live_backtest_ai_pipeline(self, res: Dict[str, Any], task_id: Optional[int], task_name: str) -> None:
        if not res.get("ok"):
            self.ai_output.delete("1.0", tk.END)
            self.ai_output.insert("1.0", f"Pipeline failed: {res.get('error', 'unknown')}")
            self._append_task_terminal(f"DONE {task_name} (error)")
            self._finish_task(task_id, task_name=task_name)
            return
        response = str(res.get("response", "") or "").strip()
        self.ai_last_source_text = str(res.get("combined_source", "") or "")
        self.ai_output.delete("1.0", tk.END)
        self.ai_output.insert("1.0", response or "AI returned empty response.")
        used_prompt = str(res.get("prompt", "") or "")
        if used_prompt:
            self.ai_conversation.append({"role": "user", "content": used_prompt})
        if response:
            self.ai_conversation.append({"role": "assistant", "content": response})
        if bool(self.ai_auto_stage_var.get()) and response:
            staged = self._stage_ai_recommendations(silent=True)
            self._append_task_terminal(f"Auto-stage after pipeline: {staged} recommendation(s).")
        self._append_task_terminal(f"DONE {task_name}")
        self._finish_task(task_id, task_name=task_name)

    def _pipeline_tick(self) -> None:
        self._run_live_ai_pipeline_now()
        if self.pipeline_job:
            try:
                mins = max(1, int((self.pipeline_interval_min_var.get() or "30").strip()))
            except Exception:
                mins = 30
            self.pipeline_job = self.root.after(mins * 60 * 1000, self._pipeline_tick)

    def _start_pipeline_scheduler(self) -> None:
        self._stop_pipeline_scheduler()
        try:
            mins = max(1, int((self.pipeline_interval_min_var.get() or "30").strip()))
        except Exception:
            mins = 30
        self._append_task_terminal(f"Started Live->AI pipeline scheduler ({mins} min interval).")
        self.pipeline_job = self.root.after(1000, self._pipeline_tick)

    def _stop_pipeline_scheduler(self) -> None:
        if self.pipeline_job:
            try:
                self.root.after_cancel(self.pipeline_job)
            except Exception:
                pass
            self.pipeline_job = None
            self._append_task_terminal("Stopped Live->AI pipeline scheduler.")

    def _finish_ai_output(self, res: Dict[str, Any], task_id: Optional[int] = None) -> None:
        if not res.get("ok"):
            err = str(res.get("error", "") or "").strip()
            self.ai_output.insert("1.0", "AI analysis failed or returned empty response.\n")
            if err:
                self.ai_output.insert("end", f"\nError: {err}\n")
            self.ai_output.insert("end", "\nPrompt preview:\n\n")
            self.ai_output.insert("end", (res.get("prompt", "") or "")[:4000])
            self._append_task_terminal("DONE AI Analysis (failed/empty)")
            if err:
                messagebox.showerror("AI Analysis Failed", err)
            self._vlog(f"AI failed: {err or 'empty response'}")
        else:
            response = res.get("response", "")
            self.ai_output.insert("1.0", response)
            used_prompt = res.get("prompt", "")
            if used_prompt:
                self.ai_conversation.append({"role": "user", "content": used_prompt})
            self.ai_conversation.append({"role": "assistant", "content": response})
            if bool(self.ai_auto_stage_var.get()):
                staged = self._stage_ai_recommendations(silent=True)
                self._append_task_terminal(f"Auto-stage after AI run: {staged} recommendation(s).")
            self._vlog(f"AI success: response_chars={len(response)}")
            self._append_task_terminal("DONE AI Analysis")
        self._finish_task(task_id, task_name="AI Analysis")

    def _run_ai_followup(self) -> None:
        if self._queue_if_busy("AI Follow-up", self._start_run_ai_followup):
            return
        self._start_run_ai_followup()

    def _start_run_ai_followup(self) -> None:
        follow = self.ai_followup_var.get().strip()
        if not follow:
            messagebox.showinfo("Follow-up", "Enter a follow-up question first.")
            return
        if not self.ai_last_source_text.strip() and not self.ai_conversation:
            messagebox.showinfo("Follow-up", "Run an initial AI analysis first.")
            return
        task_name = "AI Follow-up"
        task_id = self._set_busy(True, task_name)
        self._append_task_terminal("START AI Follow-up")
        dt = self.ai_datetime.get().strip() or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        history = self.ai_conversation[-6:]
        lines = [
            "Continue the prior analysis conversation using the same style and constraints.",
            f"Date/time context: {dt}",
        ]
        if self.ai_last_source_text.strip():
            lines.extend(["", "Reference dashboard/backtest text:", self.ai_last_source_text[:12000]])
        if history:
            lines.append("")
            lines.append("Recent conversation:")
            for turn in history:
                role = turn.get("role", "user").upper()
                lines.append(f"{role}:")
                lines.append(turn.get("content", ""))
                lines.append("")
        lines.append("New user follow-up:")
        lines.append(follow)
        prompt = "\n".join(lines)

        if self.ai_require_confirm.get():
            preview = prompt[:2000]
            ok = messagebox.askyesno("Confirm AI Follow-up", f"Send this follow-up prompt?\n\n{preview}")
            if not ok:
                self._finish_task(task_id, task_name=task_name)
                self._append_task_terminal("DONE AI Follow-up (canceled)")
                return

        def worker():
            bridge = self._bridge_for_task()
            res = bridge.run_ai_analysis(
                self.ai_last_source_text,
                dt,
                prompt_override=prompt,
            )
            log = (res.get("log", "") or "").strip()
            if log:
                self._append_task_terminal_from_worker(f"LOG [AI Follow-up] {log[:6000]}")
            self.root.after(0, lambda: self._finish_ai_followup(res, follow, task_id))

        threading.Thread(target=worker, daemon=True).start()

    def _finish_ai_followup(self, res: Dict[str, Any], follow_text: str, task_id: Optional[int] = None) -> None:
        if not res.get("ok"):
            self.ai_output.insert("1.0", "AI follow-up failed.\n\n")
            self.ai_output.insert("end", (res.get("prompt", "") or "")[:4000])
            self._append_task_terminal("DONE AI Follow-up (failed)")
        else:
            response = res.get("response", "")
            current = self.ai_output.get("1.0", tk.END).strip()
            stitched = (
                (current + "\n\n" if current else "")
                + f"FOLLOW-UP QUESTION:\n{follow_text}\n\nFOLLOW-UP RESPONSE:\n{response}"
            )
            self.ai_output.delete("1.0", tk.END)
            self.ai_output.insert("1.0", stitched)
            self.ai_conversation.append({"role": "user", "content": follow_text})
            self.ai_conversation.append({"role": "assistant", "content": response})
            self.ai_followup_var.set("")
            self._append_task_terminal("DONE AI Follow-up")
        self._finish_task(task_id, task_name="AI Follow-up")

    def _resolve_ai_source_text(self) -> str:
        source = self.ai_source.get().strip().lower()
        if source == "live":
            live_text = (self.latest_live_output_text or "").strip()
            if live_text:
                return live_text
            path = self.repo_root / "experiments" / "live_snapshots" / "latest_live_dashboard.txt"
            return path.read_text(encoding="utf-8") if path.exists() else ""
        if source == "live_all_panels":
            if self.latest_live_panel_texts:
                blocks = []
                for name in sorted(self.latest_live_panel_texts.keys()):
                    blocks.append(self.latest_live_panel_texts[name])
                return "\n".join(blocks).strip()
            live_text = (self.latest_live_output_text or "").strip()
            if live_text:
                return live_text
            path = self.repo_root / "experiments" / "live_snapshots" / "latest_live_dashboard.txt"
            return path.read_text(encoding="utf-8") if path.exists() else ""
        if source == "backtest_latest":
            path = self.repo_root / "experiments" / "backtest_snapshots" / "latest_backtest.txt"
            return path.read_text(encoding="utf-8") if path.exists() else ""
        if source == "backtest_file":
            path = Path(self.ai_backtest_path.get().strip())
            return path.read_text(encoding="utf-8") if path.exists() else ""
        return self.ai_input.get("1.0", tk.END).strip()

    def _build_ai_prompt(self, source_text: str, datetime_context: str) -> str:
        mode = self.ai_prompt_mode.get().strip().lower()
        if mode == "custom_prompt":
            custom = self.ai_custom_prompt.get("1.0", tk.END).strip()
            if custom:
                return "\n\n".join(
                    [
                        custom,
                        f"Date/time context: {datetime_context}",
                        "Input text:",
                        source_text,
                    ]
                )
        return self.bridge.build_dashboard_prompt(source_text, datetime_context)

    def _preview_ai_prompt(self) -> None:
        source_text = self._resolve_ai_source_text()
        if not source_text.strip():
            self.ai_output.delete("1.0", tk.END)
            self.ai_output.insert("1.0", "No source text available for prompt preview.")
            self._append_task_terminal("PROMPT PREVIEW skipped (no source text)")
            messagebox.showinfo(
                "Show Prompt",
                "No source text available for prompt preview.\n\n"
                "Run a Live/Backtest first, load a backtest file, or use Source='paste'.",
            )
            return
        dt = self.ai_datetime.get().strip() or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            prompt = self._build_ai_prompt(source_text, dt)
        except Exception as exc:
            self.ai_output.delete("1.0", tk.END)
            self.ai_output.insert("1.0", f"Failed to build prompt: {type(exc).__name__}: {exc}")
            self._append_task_terminal(f"PROMPT PREVIEW failed ({type(exc).__name__})")
            messagebox.showerror("Show Prompt Failed", f"{type(exc).__name__}: {exc}")
            return
        self.ai_output.delete("1.0", tk.END)
        self.ai_output.insert("1.0", "PROMPT PREVIEW\n" + ("=" * 60) + "\n" + prompt)
        self._append_task_terminal("PROMPT PREVIEW generated")

    def _extract_trade_recommendations_from_ai_text(self, text: str) -> List[Dict[str, Any]]:
        recs: List[Dict[str, Any]] = []
        if not text.strip():
            return recs
        # Preferred path: structured JSON payload at bottom of AI response.
        structured = self._extract_structured_trade_plan_from_ai_text(text)
        if structured:
            return structured
        quote_default = self._effective_crypto_quote("USDT")
        allowed_assets: set[str] = set()
        for s in self._extract_signals_from_live_text((self.latest_live_output_text or "").strip()):
            a = str(s.get("asset", "")).strip().upper()
            if a:
                allowed_assets.add(a)

        lines = text.splitlines()
        patt = re.compile(
            r"\b(BUY|SELL)\b[^A-Z0-9]{0,8}\b([A-Z][A-Z0-9]{1,11})(?:[-/ ]?(USDT|USD|BTC|ETH|BNB))?\b",
            re.IGNORECASE,
        )
        seen: set[Tuple[str, str]] = set()
        for ln in lines:
            m = patt.search(ln)
            if not m:
                continue
            side = str(m.group(1)).upper()
            base = str(m.group(2)).upper()
            if not re.fullmatch(r"[A-Z0-9]{2,8}", base):
                continue
            if allowed_assets:
                if base not in allowed_assets:
                    continue
            else:
                if base not in COMMON_CRYPTO_BASES:
                    continue
            q = str(m.group(3) or quote_default).upper()
            symbol = f"{base}{q}"
            symbol = self._normalize_symbol_quote(symbol)
            key = (symbol, side)
            if key in seen:
                continue
            seen.add(key)
            conf = ""
            cm = re.search(r"(?:confidence|conviction)\s*[:=]?\s*([0-9]+(?:\.[0-9]+)?)", ln, re.IGNORECASE)
            if cm:
                conf = cm.group(1)
            self._pending_rec_seq += 1
            recs.append(
                {
                    "id": self._pending_rec_seq,
                    "symbol": symbol,
                    "asset": base,
                    "side": side,
                    "order_type": "MARKET",
                    "quantity": 0.0,
                    "stop_loss_price": 0.0,
                    "timeframe": (self.timeframe_var.get().strip() or "1d"),
                    "confidence": conf,
                    "status": "PENDING",
                    "reason": ln.strip()[:220],
                }
            )
        return recs

    def _extract_structured_trade_plan_from_ai_text(self, text: str) -> List[Dict[str, Any]]:
        payload: Optional[Dict[str, Any]] = None

        # First preference: explicit STRATA markers.
        marker_re = re.compile(
            r"BEGIN_STRATA_TRADE_PLAN_JSON\s*(\{[\s\S]*?\})\s*END_STRATA_TRADE_PLAN_JSON",
            re.IGNORECASE,
        )
        mm = marker_re.search(text)
        if mm:
            try:
                obj = json.loads(mm.group(1))
                if isinstance(obj, dict):
                    payload = obj
            except Exception:
                payload = None

        # Fallback: last json fenced block containing trades list.
        if payload is None:
            code_blocks = re.findall(r"```json\s*([\s\S]*?)\s*```", text, flags=re.IGNORECASE)
            for block in reversed(code_blocks):
                try:
                    obj = json.loads(block)
                except Exception:
                    continue
                if isinstance(obj, dict) and isinstance(obj.get("trades", None), list):
                    payload = obj
                    break

        if payload is None:
            return []

        trades = payload.get("trades", [])
        if not isinstance(trades, list):
            return []

        quote_default = self._effective_crypto_quote("USDT")
        allowed_assets: set[str] = set()
        for s in self._extract_signals_from_live_text((self.latest_live_output_text or "").strip()):
            a = str(s.get("asset", "")).strip().upper()
            if a:
                allowed_assets.add(a)

        recs: List[Dict[str, Any]] = []
        seen: set[Tuple[str, str]] = set()
        for tr in trades:
            if not isinstance(tr, dict):
                continue
            symbol = str(tr.get("symbol", "") or "").strip().upper()
            asset = str(tr.get("asset", "") or "").strip().upper()
            side = str(tr.get("side", "") or "").strip().upper()
            if side not in ("BUY", "SELL"):
                continue

            if not symbol and asset:
                symbol = f"{asset}{quote_default}"
            symbol = self._normalize_symbol_quote(symbol)
            if not asset and symbol:
                asset = self._base_asset_from_symbol(symbol)
            if not symbol or not asset:
                continue
            if not re.fullmatch(r"[A-Z0-9]{2,12}", asset):
                continue
            if allowed_assets:
                if asset not in allowed_assets:
                    continue
            else:
                if asset not in COMMON_CRYPTO_BASES:
                    continue

            key = (symbol, side)
            if key in seen:
                continue
            seen.add(key)

            order_type = str(tr.get("order_type", "MARKET") or "MARKET").strip().upper()
            if order_type not in ("MARKET", "LIMIT"):
                order_type = "MARKET"
            timeframe = str(tr.get("timeframe", "") or "").strip().lower() or (self.timeframe_var.get().strip() or "1d")
            confidence = str(tr.get("confidence", "") or "").strip()
            reason = str(tr.get("reason", "") or "").strip()
            invalidation = str(tr.get("invalidation", "") or "").strip()
            qty = 0.0
            for qk in ("quantity", "qty", "size_qty"):
                try:
                    qv = float(tr.get(qk, 0.0) or 0.0)
                except Exception:
                    qv = 0.0
                if qv > 0:
                    qty = qv
                    break
            stop_loss = 0.0
            try:
                stop_loss = float(tr.get("stop_loss_price", 0.0) or 0.0)
            except Exception:
                stop_loss = 0.0
            if stop_loss <= 0 and invalidation:
                mm = re.search(r"([0-9]+(?:\.[0-9]+)?)", invalidation.replace(",", ""))
                if mm:
                    try:
                        stop_loss = float(mm.group(1))
                    except Exception:
                        stop_loss = 0.0

            self._pending_rec_seq += 1
            recs.append(
                {
                    "id": self._pending_rec_seq,
                    "symbol": symbol,
                    "asset": asset,
                    "side": side,
                    "order_type": order_type,
                    "quantity": qty,
                    "timeframe": timeframe,
                    "confidence": confidence,
                    "status": "PENDING",
                    "reason": (reason or "Structured trade-plan recommendation")[:300],
                    "invalidation": invalidation,
                    "stop_loss_price": (stop_loss if stop_loss > 0 else 0.0),
                }
            )
        return recs

    def _stage_ai_recommendations(self, silent: bool = False) -> int:
        text = self.ai_output.get("1.0", tk.END).strip()
        if not text:
            if not silent:
                messagebox.showinfo("AI Recommendations", "Run AI analysis first (or load AI output).")
            return 0
        recs = self._extract_trade_recommendations_from_ai_text(text)
        if not recs:
            if not silent:
                messagebox.showinfo("AI Recommendations", "No BUY/SELL recommendations were detected in AI output.")
            return 0
        recs, skipped_sell = self._filter_sells_without_holdings(recs)
        self.pending_recommendations.extend(recs)
        self._refresh_pending_recommendations_view()
        self._append_task_terminal(
            f"Staged {len(recs)} AI recommendation(s) into pending orders."
            + (f" Skipped SELL (no holdings): {skipped_sell}." if skipped_sell > 0 else "")
        )
        if bool(self.ai_log_signals_var.get()):
            try:
                cooldown = max(1, int((self.pf_cooldown_min_var.get() or "240").strip()))
            except Exception:
                cooldown = 240
            logged = 0
            for r in recs:
                out = self.bridge.record_signal_event(
                    {
                        "market": "crypto",
                        "timeframe": str(r.get("timeframe", "1d")),
                        "panel": "ai_interpretation",
                        "asset": str(r.get("asset", "")),
                        "action": str(r.get("side", "")),
                        "qty": 0.0,
                        "quote_currency": self._quote_asset_from_symbol(str(r.get("symbol", ""))),
                        "display_currency": self.display_currency_var.get().strip() or "USD",
                        "note": "AI interpretation signal",
                    },
                    cooldown_minutes=cooldown,
                    allow_duplicate=False,
                    guard_hold_signals=bool(self.pf_guard_hold_var.get()),
                )
                if out.get("ok"):
                    logged += 1
            self._append_task_terminal(f"Logged {logged}/{len(recs)} AI signal(s) to ledger.")
            self._refresh_ledger_view()
        if self.pf_exec_mode_var.get().strip().lower() == "full_auto":
            self._append_task_terminal("Execution mode FULL_AUTO: attempting auto-submit for newly staged recommendations.")
            if hasattr(self, "pending_tree"):
                self.pending_tree.selection_set(*[str(r.get("id")) for r in recs])
            self._submit_selected_pending_orders()
            return len(recs)
        if not silent:
            msg = f"Staged {len(recs)} recommendation(s)."
            if skipped_sell > 0:
                msg += f"\nSkipped SELL (no holdings): {skipped_sell}"
            messagebox.showinfo("AI Recommendations", msg)
        return len(recs)

    def _filter_sells_without_holdings(self, recs: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
        profile = self.pf_binance_profile_var.get().strip() if hasattr(self, "pf_binance_profile_var") else ""
        if not profile:
            return recs, 0
        # Ensure we have fresh holdings for filtering.
        out = self.bridge.fetch_binance_portfolio(profile_name=profile)
        if out.get("ok"):
            self.latest_portfolio_snapshot = out
        balances = self.latest_portfolio_snapshot.get("balances", []) if isinstance(self.latest_portfolio_snapshot, dict) else []
        if not isinstance(balances, list) or not balances:
            return recs, 0
        free_by_asset: Dict[str, float] = {}
        for b in balances:
            if not isinstance(b, dict):
                continue
            a = str(b.get("asset", "")).strip().upper()
            if not a:
                continue
            try:
                free_by_asset[a] = float(b.get("free", 0.0) or 0.0)
            except Exception:
                free_by_asset[a] = 0.0
        kept: List[Dict[str, Any]] = []
        skipped = 0
        for r in recs:
            side = str(r.get("side", "")).strip().upper()
            if side != "SELL":
                kept.append(r)
                continue
            sym = str(r.get("symbol", "")).strip().upper()
            base = self._base_asset_from_symbol(sym)
            held = float(free_by_asset.get(base, 0.0) or 0.0)
            if held <= 0:
                skipped += 1
                self._vlog(f"Skipped SELL recommendation (no holdings): {sym}")
                continue
            kept.append(r)
        return kept, skipped

    def _clear_pending_recommendations(self) -> None:
        self.pending_recommendations = []
        self._refresh_pending_recommendations_view()
        self._append_task_terminal("Cleared pending recommendations.")

    def _refresh_binance_profiles(self) -> None:
        res = self.bridge.list_binance_profiles()
        if not res.get("ok"):
            self._append_settings(f"Binance profile refresh failed: {res.get('error', 'unknown')}")
            return
        profiles = res.get("profiles", []) or []
        self._binance_profiles_cache = {p.get("name", ""): p for p in profiles if isinstance(p, dict)}
        names = list(self._binance_profiles_cache.keys())
        if hasattr(self, "pf_binance_profile_combo"):
            self.pf_binance_profile_combo["values"] = names
        active = str(res.get("active_profile", "") or "")
        if active and active in self._binance_profiles_cache:
            self.pf_binance_profile_var.set(active)
        elif names:
            self.pf_binance_profile_var.set(names[0])
        else:
            self.pf_binance_profile_var.set("")
        self._append_settings(f"Binance profiles loaded: {len(names)} (active: {self.pf_binance_profile_var.get() or 'none'})")

    def _refresh_pending_recommendations_view(self) -> None:
        if not hasattr(self, "pending_tree"):
            return
        self.pending_tree.delete(*self.pending_tree.get_children())
        for r in self.pending_recommendations:
            self.pending_tree.insert(
                "",
                "end",
                iid=str(r.get("id", "")),
                values=(
                    r.get("id", ""),
                    r.get("symbol", ""),
                    r.get("side", ""),
                    r.get("order_type", "MARKET"),
                    r.get("quantity", 0),
                    r.get("stop_loss_price", ""),
                    r.get("timeframe", ""),
                    r.get("confidence", ""),
                    r.get("status", "PENDING"),
                    r.get("reason", ""),
                ),
            )

    def _selected_pending_ids(self) -> List[int]:
        if not hasattr(self, "pending_tree"):
            return []
        out: List[int] = []
        for iid in self.pending_tree.selection():
            try:
                out.append(int(iid))
            except Exception:
                continue
        return out

    def _remove_selected_pending_orders(self) -> None:
        ids = set(self._selected_pending_ids())
        if not ids:
            messagebox.showinfo("Pending Orders", "Select one or more pending rows.")
            return
        self.pending_recommendations = [r for r in self.pending_recommendations if int(r.get("id", -1)) not in ids]
        self._refresh_pending_recommendations_view()
        self._append_task_terminal(f"Removed {len(ids)} pending recommendation(s).")

    def _apply_pending_edit_to_selected(self) -> None:
        ids = set(self._selected_pending_ids())
        if not ids:
            messagebox.showinfo("Pending Orders", "Select rows first.")
            return
        try:
            qty = float((self.pf_pending_qty_var.get() or "0").strip())
        except Exception:
            qty = 0.0
        otype = str(self.pf_pending_type_var.get() or "MARKET").strip().upper()
        changed = 0
        for r in self.pending_recommendations:
            if int(r.get("id", -1)) in ids:
                if qty > 0:
                    r["quantity"] = qty
                r["order_type"] = otype
                if str(r.get("status", "")).upper() == "BLOCKED":
                    r["status"] = "PENDING"
                changed += 1
        self._refresh_pending_recommendations_view()
        self._append_task_terminal(f"Updated {changed} pending recommendation(s).")

    def _base_asset_from_symbol(self, symbol: str) -> str:
        s = str(symbol).strip().upper()
        for q in ["USDT", "USDC", "BUSD", "USD", "BTC", "ETH", "BNB"]:
            if s.endswith(q) and len(s) > len(q):
                return s[: -len(q)]
        return s

    def _quote_asset_from_symbol(self, symbol: str) -> str:
        s = str(symbol).strip().upper()
        for q in ["USDT", "USDC", "BUSD", "USD", "BTC", "ETH", "BNB"]:
            if s.endswith(q) and len(s) > len(q):
                return q
        if self._is_primary_quote_locked():
            return self._primary_quote_asset()
        return str(self.pf_quote_var.get() or self._effective_crypto_quote("USDT")).strip().upper()

    def _normalize_symbol_quote(self, symbol: str) -> str:
        s = str(symbol).strip().upper()
        if not s:
            return s
        if not self._is_primary_quote_locked():
            return s
        base = self._base_asset_from_symbol(s)
        if not base or base == s:
            return s
        return f"{base}{self._primary_quote_asset()}"

    def _parse_min_notional_from_error(self, err_text: str) -> Optional[float]:
        txt = str(err_text or "")
        m = re.search(r"minNotional\s+([0-9]+(?:\.[0-9]+)?)", txt, flags=re.IGNORECASE)
        if not m:
            return None
        try:
            return float(m.group(1))
        except Exception:
            return None

    def _balances_free_map(self) -> Dict[str, float]:
        out: Dict[str, float] = {}
        snap = self.latest_portfolio_snapshot if isinstance(self.latest_portfolio_snapshot, dict) else {}
        rows = snap.get("balances", []) if isinstance(snap, dict) else []
        if not isinstance(rows, list):
            return out
        for b in rows:
            if not isinstance(b, dict):
                continue
            a = str(b.get("asset", "")).strip().upper()
            if not a:
                continue
            try:
                out[a] = float(b.get("free", 0.0) or 0.0)
            except Exception:
                out[a] = 0.0
        return out

    def _attempt_min_notional_qty_adjustment(
        self,
        symbol: str,
        side: str,
        order_type: str,
        min_notional: float,
        profile: Optional[str],
        free_by_asset: Dict[str, float],
    ) -> Tuple[Optional[float], str]:
        if min_notional <= 0:
            return None, "Invalid minNotional."
        p = self.bridge.get_binance_last_price(symbol=symbol, profile_name=profile)
        if not p.get("ok"):
            return None, str(p.get("error", "Failed to fetch market price for minNotional adjustment."))
        try:
            px = float(p.get("price", 0.0) or 0.0)
        except Exception:
            px = 0.0
        if px <= 0:
            return None, "Invalid market price for minNotional adjustment."
        # Small buffer above minNotional to avoid rounding underflow.
        target_notional = float(min_notional) * 1.02
        qty_guess = target_notional / px
        v = self.bridge.validate_binance_order(
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=qty_guess,
            profile_name=profile,
        )
        if not v.get("ok"):
            return None, str(v.get("error", "Adjusted quantity still fails validation."))
        try:
            nq = float(v.get("normalized_quantity", 0.0) or 0.0)
        except Exception:
            nq = 0.0
        if nq <= 0:
            return None, "Adjusted quantity normalized to zero."

        quote = self._quote_asset_from_symbol(symbol)
        base = self._base_asset_from_symbol(symbol)
        if side == "BUY":
            need_quote = nq * px
            free_q = float(free_by_asset.get(quote, 0.0) or 0.0)
            if free_q > 0 and need_quote > free_q:
                return None, f"Need ~{need_quote:.6f} {quote} for minNotional-adjusted BUY, available {free_q:.6f}."
        if side == "SELL":
            free_b = float(free_by_asset.get(base, 0.0) or 0.0)
            if free_b > 0 and nq > free_b:
                return None, f"Need ~{nq:.8f} {base} for minNotional-adjusted SELL, available {free_b:.8f}."
        return nq, f"Auto-adjusted qty to satisfy minNotional ({min_notional}) at px {px:.6f}."

    def _confidence_multiplier(self, raw: Any) -> float:
        if not bool(self.pf_auto_confidence_var.get()):
            return 1.0
        try:
            s = str(raw or "").strip().replace("%", "")
            if not s:
                return 1.0
            v = float(s)
            if v > 1.0:
                v = v / 100.0
            v = max(0.0, min(1.0, v))
            # Keep sizing bounded but responsive.
            return max(0.5, min(1.25, 0.5 + (v * 0.75)))
        except Exception:
            return 1.0

    def _auto_size_selected_pending_orders(self) -> None:
        ids = self._selected_pending_ids()
        if not ids:
            messagebox.showinfo("Auto-size", "Select one or more pending rows first.")
            return
        profile = self.pf_binance_profile_var.get().strip() or None
        if not profile:
            messagebox.showinfo("Auto-size", "Select a Binance profile first.")
            return
        try:
            buy_pct = max(0.0, min(100.0, float((self.pf_auto_buy_pct_var.get() or "10").strip())))
        except Exception:
            buy_pct = 10.0
        try:
            sell_pct = max(0.0, min(100.0, float((self.pf_auto_sell_pct_var.get() or "100").strip())))
        except Exception:
            sell_pct = 100.0

        if not self.latest_portfolio_snapshot or not bool(self.latest_portfolio_snapshot.get("ok")):
            res = self.bridge.fetch_binance_portfolio(profile_name=profile)
            if not res.get("ok"):
                messagebox.showerror("Auto-size", str(res.get("error", "Failed to fetch Binance portfolio.")))
                return
            self.latest_portfolio_snapshot = res
            self._finish_refresh_portfolio(res, task_id=None)

        balances = self.latest_portfolio_snapshot.get("balances", []) or []
        free_by_asset: Dict[str, float] = {}
        for b in balances:
            if not isinstance(b, dict):
                continue
            a = str(b.get("asset", "")).strip().upper()
            if not a:
                continue
            try:
                free_by_asset[a] = float(b.get("free", 0.0) or 0.0)
            except Exception:
                free_by_asset[a] = 0.0

        updated = 0
        blocked = 0
        for rid in ids:
            rec = next((r for r in self.pending_recommendations if int(r.get("id", -1)) == int(rid)), None)
            if not rec:
                continue
            symbol = str(rec.get("symbol", "")).strip().upper()
            side = str(rec.get("side", "")).strip().upper()
            otype = str(rec.get("order_type", "MARKET")).strip().upper()
            if not symbol or side not in ("BUY", "SELL"):
                blocked += 1
                rec["status"] = "BLOCKED"
                rec["reason"] = "Missing symbol/side for auto-size."
                continue

            base = self._base_asset_from_symbol(symbol)
            quote = self._quote_asset_from_symbol(symbol)
            mult = self._confidence_multiplier(rec.get("confidence", ""))

            qty_guess = 0.0
            if side == "BUY":
                free_quote = float(free_by_asset.get(quote, 0.0) or 0.0)
                spend = free_quote * (buy_pct / 100.0) * mult
                if spend <= 0:
                    blocked += 1
                    rec["status"] = "BLOCKED"
                    rec["reason"] = f"No available {quote} balance to auto-size BUY."
                    continue
                p = self.bridge.get_binance_last_price(symbol=symbol, profile_name=profile)
                if not p.get("ok"):
                    blocked += 1
                    rec["status"] = "BLOCKED"
                    rec["reason"] = str(p.get("error", "Failed to fetch last price."))
                    continue
                px = float(p.get("price", 0.0) or 0.0)
                if px <= 0:
                    blocked += 1
                    rec["status"] = "BLOCKED"
                    rec["reason"] = "Invalid market price for auto-size."
                    continue
                qty_guess = spend / px
            else:
                free_base = float(free_by_asset.get(base, 0.0) or 0.0)
                qty_guess = free_base * (sell_pct / 100.0) * mult
                if qty_guess <= 0:
                    blocked += 1
                    rec["status"] = "BLOCKED"
                    rec["reason"] = f"No available {base} balance to auto-size SELL."
                    continue

            v = self.bridge.validate_binance_order(
                symbol=symbol,
                side=side,
                order_type=otype,
                quantity=qty_guess,
                profile_name=profile,
            )
            if not v.get("ok"):
                min_n = self._parse_min_notional_from_error(str(v.get("error", "")))
                if min_n is not None:
                    adj_qty, adj_note = self._attempt_min_notional_qty_adjustment(
                        symbol=symbol,
                        side=side,
                        order_type=otype,
                        min_notional=min_n,
                        profile=profile,
                        free_by_asset=free_by_asset,
                    )
                    if adj_qty is not None and adj_qty > 0:
                        rec["quantity"] = adj_qty
                        rec["status"] = "PENDING"
                        rec["reason"] = adj_note
                        updated += 1
                        continue
                blocked += 1
                rec["status"] = "BLOCKED"
                rec["reason"] = str(v.get("error", "Auto-size failed validation."))
                continue

            nqty = float(v.get("normalized_quantity", 0.0) or 0.0)
            if nqty <= 0:
                blocked += 1
                rec["status"] = "BLOCKED"
                rec["reason"] = "Auto-size normalized qty is zero."
                continue
            rec["quantity"] = nqty
            rec["status"] = "PENDING"
            rec["reason"] = f"Auto-sized ({side}) using available balance and Binance filters."
            updated += 1

        self._refresh_pending_recommendations_view()
        self._append_task_terminal(
            f"Auto-size completed -> updated={updated}, blocked={blocked}, "
            f"buy_pct={buy_pct:.1f}%, sell_pct={sell_pct:.1f}%, confidence_weight={bool(self.pf_auto_confidence_var.get())}"
        )
        messagebox.showinfo(
            "Auto-size complete",
            f"Updated: {updated}\nBlocked: {blocked}\n\n"
            f"BUY sizing: {buy_pct:.1f}% of available quote balance\n"
            f"SELL sizing: {sell_pct:.1f}% of available base balance",
        )

    def _submit_selected_pending_orders(self, ids_override: Optional[List[int]] = None, require_confirm: bool = True) -> None:
        ids = list(ids_override) if isinstance(ids_override, list) and ids_override else self._selected_pending_ids()
        if not ids:
            messagebox.showinfo("Pending Orders", "Select one or more rows to submit.")
            return
        mode = self.pf_exec_mode_var.get().strip().lower()
        if mode == "manual":
            messagebox.showinfo("Execution Mode", "Current mode is MANUAL. Switch to semi_auto/full_auto to submit.")
            return
        profile = self.pf_binance_profile_var.get().strip() or None
        if not profile:
            messagebox.showinfo("Pending Orders", "Select a Binance profile first.")
            return
        if require_confirm:
            ok = messagebox.askyesno("Submit Orders", f"Submit {len(ids)} selected order(s) to Binance?")
            if not ok:
                return
        try:
            cooldown = max(1, int((self.pf_cooldown_min_var.get() or "240").strip()))
        except Exception:
            cooldown = 240
        submitted = 0
        blocked = 0
        failed = 0
        free_by_asset = self._balances_free_map()
        if not free_by_asset:
            # Best effort refresh for balance-aware retry logic.
            fres = self.bridge.fetch_binance_portfolio(profile_name=profile)
            if fres.get("ok"):
                self.latest_portfolio_snapshot = fres
                free_by_asset = self._balances_free_map()
        for rid in ids:
            rec = next((r for r in self.pending_recommendations if int(r.get("id", -1)) == int(rid)), None)
            if not rec:
                continue
            if str(rec.get("status", "")).upper() not in ("PENDING", "FAILED"):
                continue
            symbol = str(rec.get("symbol", "")).strip().upper()
            side = str(rec.get("side", "")).strip().upper()
            order_type = str(rec.get("order_type", "MARKET")).strip().upper()
            try:
                qty = float(rec.get("quantity", 0.0) or 0.0)
            except Exception:
                qty = 0.0
            if qty <= 0:
                rec["status"] = "BLOCKED"
                rec["reason"] = "Quantity is 0. Edit quantity before submit."
                self._vlog(f"Submit blocked: {symbol} {side} qty={qty}")
                blocked += 1
                continue
            # Pre-submit balance sanity check to reduce exchange rejects.
            base = self._base_asset_from_symbol(symbol)
            quote = self._quote_asset_from_symbol(symbol)
            if side == "SELL":
                free_base = float(free_by_asset.get(base, 0.0) or 0.0)
                if free_base > 0 and qty > free_base:
                    rec["status"] = "BLOCKED"
                    rec["reason"] = f"Insufficient {base} free balance ({free_base:.8f}) for qty {qty:.8f}."
                    self._vlog(f"Submit blocked pre-check: {symbol} SELL qty={qty} free_{base}={free_base}")
                    blocked += 1
                    continue
            elif side == "BUY":
                free_quote = float(free_by_asset.get(quote, 0.0) or 0.0)
                p = self.bridge.get_binance_last_price(symbol=symbol, profile_name=profile)
                try:
                    px = float(p.get("price", 0.0) or 0.0) if p.get("ok") else 0.0
                except Exception:
                    px = 0.0
                if free_quote > 0 and px > 0:
                    est_cost = qty * px * 1.003  # small fee/slippage safety
                    if est_cost > free_quote:
                        rec["status"] = "BLOCKED"
                        rec["reason"] = (
                            f"Insufficient {quote} free balance ({free_quote:.6f}) for est cost {est_cost:.6f}. "
                            f"Use Auto-size or lower qty."
                        )
                        self._vlog(
                            f"Submit blocked pre-check: {symbol} BUY qty={qty} px={px} est_cost={est_cost} free_{quote}={free_quote}"
                        )
                        blocked += 1
                        continue
            self._vlog(f"Submitting order: symbol={symbol} side={side} type={order_type} qty={qty}")
            price_arg = None
            stop_arg = None
            if order_type == "STOP_LOSS_LIMIT":
                try:
                    stop_arg = float(rec.get("stop_loss_price", 0.0) or 0.0)
                except Exception:
                    stop_arg = 0.0
                try:
                    price_arg = float(rec.get("limit_price", 0.0) or 0.0)
                except Exception:
                    price_arg = 0.0
                if stop_arg <= 0:
                    rec["status"] = "BLOCKED"
                    rec["reason"] = "STOP_LOSS_LIMIT requires stop_loss_price > 0."
                    blocked += 1
                    continue
                if not price_arg or price_arg <= 0:
                    price_arg = float(stop_arg) * 0.995

                if side == "SELL":
                    # Tighten-only guard: never move a protective stop backwards for long positions.
                    # If an existing protective stop is higher, block this update.
                    oo = self.bridge.list_open_binance_orders(profile_name=profile, symbol=symbol)
                    if not oo.get("ok"):
                        rec["status"] = "BLOCKED"
                        rec["reason"] = (
                            f"Could not verify existing protective stops ({oo.get('error', 'unknown')}). "
                            "Tighten-only guard blocked update."
                        )
                        blocked += 1
                        continue

                    protective_orders: List[Dict[str, Any]] = []
                    best_existing_stop = 0.0
                    for od in oo.get("orders", []) or []:
                        if not isinstance(od, dict):
                            continue
                        o_side = str(od.get("side", "")).upper()
                        o_type = str(od.get("type", "")).upper()
                        o_status = str(od.get("status", "")).upper()
                        if o_side != "SELL" or o_status not in ("NEW", "PARTIALLY_FILLED"):
                            continue
                        if o_type not in ("STOP_LOSS_LIMIT", "STOP_LOSS", "TAKE_PROFIT", "TAKE_PROFIT_LIMIT", "TRAILING_STOP_MARKET"):
                            continue
                        protective_orders.append(od)
                        try:
                            o_stop = float(od.get("stopPrice", 0.0) or 0.0)
                        except Exception:
                            o_stop = 0.0
                        if o_stop <= 0:
                            try:
                                o_stop = float(od.get("price", 0.0) or 0.0)
                            except Exception:
                                o_stop = 0.0
                        if o_stop > best_existing_stop:
                            best_existing_stop = o_stop

                    if best_existing_stop > 0 and float(stop_arg) + 1e-12 < float(best_existing_stop):
                        rec["status"] = "BLOCKED"
                        rec["reason"] = (
                            f"Tighten-only guard: new stop {float(stop_arg):.8f} is below "
                            f"existing protective stop {float(best_existing_stop):.8f}."
                        )
                        self._vlog(f"Submit blocked tighten-only: {symbol} new_stop={stop_arg} existing_stop={best_existing_stop}")
                        blocked += 1
                        continue

                    if bool(rec.get("replace_existing_stop", False)):
                        for od in protective_orders:
                            try:
                                oid = int(od.get("orderId", 0) or 0)
                            except Exception:
                                oid = 0
                            if oid <= 0:
                                continue
                            self.bridge.cancel_binance_order(symbol=symbol, order_id=oid, profile_name=profile)
                            self._vlog(f"Canceled existing protective order before replace: {symbol} oid={oid}")

            out = self.bridge.submit_binance_order(
                symbol=symbol,
                side=side,
                order_type=order_type,
                quantity=qty,
                profile_name=profile,
                price=price_arg,
                stop_price=stop_arg,
            )
            if not out.get("ok"):
                err_text = str(out.get("error", "submit failed"))
                min_n = self._parse_min_notional_from_error(err_text)
                if min_n is not None:
                    adj_qty, adj_note = self._attempt_min_notional_qty_adjustment(
                        symbol=symbol,
                        side=side,
                        order_type=order_type,
                        min_notional=min_n,
                        profile=profile,
                        free_by_asset=free_by_asset,
                    )
                    if adj_qty is not None and adj_qty > 0:
                        self._vlog(f"Retry submit with minNotional-adjusted qty: {adj_qty} ({symbol})")
                        out2 = self.bridge.submit_binance_order(
                            symbol=symbol,
                            side=side,
                            order_type=order_type,
                            quantity=adj_qty,
                            profile_name=profile,
                        )
                        if out2.get("ok"):
                            out = out2
                            qty = adj_qty
                            rec["reason"] = f"{rec.get('reason','')} | {adj_note}".strip(" |")
                        else:
                            err_text = f"{err_text} | retry failed: {out2.get('error', 'submit failed')}"
                if not out.get("ok"):
                    rec["status"] = "FAILED"
                    rec["reason"] = err_text
                    self._vlog(f"Submit failed: symbol={symbol} error={rec['reason']}")
                    failed += 1
                    continue
            rec["status"] = "SUBMITTED"
            nq = out.get("normalized_quantity", None)
            npv = out.get("normalized_price", None)
            if nq is not None:
                rec["quantity"] = float(nq)
            if npv is not None:
                rec["reason"] = f"{rec.get('reason','')} | normalized px={npv}"
            submitted += 1
            self._vlog(
                f"Submit ok: symbol={symbol} side={side} normalized_qty={out.get('normalized_quantity')} normalized_px={out.get('normalized_price')}"
            )
            if side == "BUY":
                try:
                    sl = float(rec.get("stop_loss_price", 0.0) or 0.0)
                except Exception:
                    sl = 0.0
                if sl > 0:
                    # Tighten-only guard for auto protective placement after BUY:
                    # if a stronger (higher) protective stop already exists, keep it.
                    try:
                        oo = self.bridge.list_open_binance_orders(profile_name=profile, symbol=symbol)
                    except Exception:
                        oo = {"ok": False}
                    if oo.get("ok"):
                        best_existing_stop = 0.0
                        for od in oo.get("orders", []) or []:
                            if not isinstance(od, dict):
                                continue
                            o_side = str(od.get("side", "")).upper()
                            o_type = str(od.get("type", "")).upper()
                            o_status = str(od.get("status", "")).upper()
                            if o_side != "SELL" or o_status not in ("NEW", "PARTIALLY_FILLED"):
                                continue
                            if o_type not in ("STOP_LOSS_LIMIT", "STOP_LOSS", "TAKE_PROFIT", "TAKE_PROFIT_LIMIT", "TRAILING_STOP_MARKET"):
                                continue
                            try:
                                o_stop = float(od.get("stopPrice", 0.0) or 0.0)
                            except Exception:
                                o_stop = 0.0
                            if o_stop <= 0:
                                try:
                                    o_stop = float(od.get("price", 0.0) or 0.0)
                                except Exception:
                                    o_stop = 0.0
                            if o_stop > best_existing_stop:
                                best_existing_stop = o_stop
                        if best_existing_stop > 0 and sl < best_existing_stop:
                            self._vlog(
                                f"Tighten-only guard adjusted BUY protective stop for {symbol}: "
                                f"{sl:.8f} -> {best_existing_stop:.8f}"
                            )
                            sl = float(best_existing_stop)
                    stop_limit = sl * 0.995
                    stop_out = self.bridge.submit_binance_order(
                        symbol=symbol,
                        side="SELL",
                        order_type="STOP_LOSS_LIMIT",
                        quantity=float(out.get("normalized_quantity", qty) or qty),
                        profile_name=profile,
                        price=stop_limit,
                        stop_price=sl,
                    )
                    if stop_out.get("ok"):
                        rec["reason"] = (str(rec.get("reason", "") or "") + f" | protective stop set @{sl:.8f}").strip(" |")
                        self._vlog(f"Protective stop placed: {symbol} stop={sl:.8f} limit={stop_limit:.8f}")
                    else:
                        rec["reason"] = (str(rec.get("reason", "") or "") + f" | stop set failed: {stop_out.get('error', 'unknown')}").strip(" |")
                        self._vlog(f"Protective stop failed: {symbol} err={stop_out.get('error', 'unknown')}")
            lg = self.bridge.record_signal_event(
                {
                    "market": "crypto",
                    "timeframe": "spot",
                    "panel": "ai_trade_queue",
                    "asset": self._base_asset_from_symbol(symbol),
                    "action": side,
                    "qty": qty,
                    "price": float(out.get("normalized_price", 0.0) or 0.0),
                    "quote_currency": self._quote_asset_from_symbol(symbol),
                    "display_currency": self.display_currency_var.get().strip() or "USD",
                    "note": f"Submitted to Binance ({symbol})",
                    "is_execution": True,
                    "is_placeholder": True,
                    "exchange_symbol": symbol,
                    "exchange_order_id": int((out.get("data", {}) or {}).get("orderId", 0) or 0),
                },
                cooldown_minutes=cooldown,
                # Execution events must be recorded even if a recent signal exists.
                allow_duplicate=True,
                guard_hold_signals=bool(self.pf_guard_hold_var.get()),
            )
            if not lg.get("ok"):
                self._vlog(f"Execution ledger write failed for {symbol}: {lg.get('error') or lg.get('reason') or 'unknown'}")
        self._refresh_pending_recommendations_view()
        self._refresh_ledger_view()
        self._append_task_terminal(
            f"Submitted pending orders -> submitted={submitted}, blocked={blocked}, failed={failed}"
        )
        messagebox.showinfo(
            "Order Submission",
            f"Submitted: {submitted}\nBlocked: {blocked}\nFailed: {failed}",
        )

    def _refresh_open_orders(self) -> None:
        profile = self.pf_binance_profile_var.get().strip() or None
        symbol_filter = self.pf_open_symbol_filter_var.get().strip().upper()
        out = self.bridge.list_open_binance_orders(profile_name=profile, symbol=symbol_filter)
        if not hasattr(self, "open_orders_tree"):
            return
        self.open_orders_tree.delete(*self.open_orders_tree.get_children())
        if not out.get("ok"):
            self._append_task_terminal(f"Open orders refresh failed: {out.get('error', 'unknown')}")
            messagebox.showerror("Open Orders", str(out.get("error", "Failed to fetch open orders.")))
            return
        for o in out.get("orders", []) or []:
            self.open_orders_tree.insert(
                "",
                "end",
                values=(
                    o.get("symbol", ""),
                    o.get("orderId", ""),
                    o.get("side", ""),
                    o.get("type", ""),
                    o.get("status", ""),
                    o.get("price", ""),
                    o.get("origQty", ""),
                    o.get("executedQty", ""),
                ),
            )
        self._append_task_terminal(f"Open orders refreshed ({len(out.get('orders', []) or [])} rows).")

    def _reconcile_binance_fills(self) -> None:
        profile = self.pf_binance_profile_var.get().strip() or None
        if not profile:
            messagebox.showinfo("Reconcile Fills", "Select a Binance profile first.")
            return

        symbols: set[str] = set()
        # Include pending/recent symbols first.
        for r in self.pending_recommendations:
            if not isinstance(r, dict):
                continue
            s = str(r.get("symbol", "")).strip().upper()
            if s:
                symbols.add(s)

        # Include current holdings mapped to configured quote.
        if not self.latest_portfolio_snapshot or not bool(self.latest_portfolio_snapshot.get("ok")):
            fres = self.bridge.fetch_binance_portfolio(profile_name=profile)
            if fres.get("ok"):
                self.latest_portfolio_snapshot = fres
        rows = self.latest_portfolio_snapshot.get("balances", []) if isinstance(self.latest_portfolio_snapshot, dict) else []
        quote = self._primary_quote_asset() if self._is_primary_quote_locked() else (self.pf_quote_var.get().strip().upper() or "USDT")
        stables = {"USDT", "USDC", "BUSD", "FDUSD", "TUSD", "USDP", "DAI", "USD"}
        if isinstance(rows, list):
            for b in rows:
                if not isinstance(b, dict):
                    continue
                a = str(b.get("asset", "")).strip().upper()
                if not a or a in stables:
                    continue
                try:
                    total = float(b.get("total", 0.0) or 0.0)
                except Exception:
                    total = 0.0
                if total > 0:
                    symbols.add(f"{a}{quote}")

        if not symbols:
            messagebox.showinfo("Reconcile Fills", "No symbols found to reconcile yet.")
            return

        self._append_task_terminal(f"START Reconcile Fills ({len(symbols)} symbols)")
        out = self.bridge.reconcile_binance_fills(
            profile_name=profile,
            symbols=sorted(symbols),
            max_trades_per_symbol=200,
            display_currency=self.display_currency_var.get().strip() or "USD",
        )
        if not out.get("ok"):
            self._append_task_terminal(f"DONE Reconcile Fills (error: {out.get('error', 'unknown')})")
            messagebox.showerror("Reconcile Fills", str(out.get("error", "Failed to reconcile fills.")))
            return
        self._refresh_ledger_view()
        added = int(out.get("added_entries", 0) or 0)
        dup = int(out.get("duplicates_skipped", 0) or 0)
        fetched = int(out.get("fetched_trades", 0) or 0)
        errs = out.get("errors", []) or []
        self._append_task_terminal(
            f"DONE Reconcile Fills -> fetched={fetched}, added={added}, duplicates={dup}, errors={len(errs)}"
        )
        if errs:
            self._vlog("Reconcile errors: " + " | ".join([str(e) for e in errs[:5]]))
        msg = f"Fetched trades: {fetched}\nAdded ledger entries: {added}\nDuplicates skipped: {dup}\nErrors: {len(errs)}"
        if errs:
            msg += f"\n\nFirst error:\n{errs[0]}"
        messagebox.showinfo("Reconcile Fills", msg)

    def _extract_protection_plan_from_ai_text(self, text: str) -> List[Dict[str, Any]]:
        raw = str(text or "")
        if not raw.strip():
            return []
        m = re.search(
            r"BEGIN_STRATA_PROTECTION_PLAN_JSON\s*(\{[\s\S]*?\})\s*END_STRATA_PROTECTION_PLAN_JSON",
            raw,
            flags=re.IGNORECASE,
        )
        blob = ""
        if m:
            blob = m.group(1).strip()
        else:
            m2 = re.search(r"(\{[\s\S]*\})", raw)
            if m2:
                blob = m2.group(1).strip()
        if not blob:
            return []
        try:
            obj = json.loads(blob)
        except Exception:
            return []
        if not isinstance(obj, dict):
            return []
        arr = obj.get("protections", [])
        if not isinstance(arr, list):
            return []
        return [x for x in arr if isinstance(x, dict) and str(x.get("symbol", "")).strip()]

    def _build_protection_ai_prompt(self, position_rows: List[Dict[str, Any]], bt_context: Dict[str, Dict[str, Any]]) -> str:
        payload = {
            "positions": position_rows,
            "targeted_backtests": bt_context,
            "constraints": {
                "allowed_actions": ["SET_STOP", "SET_TRAILING", "HOLD"],
                "stop_pct_bounds": [1.0, 20.0],
                "trailing_pct_bounds": [0.5, 15.0],
            },
        }
        return (
            "You are a risk manager for open spot positions.\n"
            "Use positions and targeted backtest context to recommend protection.\n"
            "Return ONLY JSON between markers.\n\n"
            "BEGIN_STRATA_PROTECTION_PLAN_JSON\n"
            "{\n"
            "  \"protections\": [\n"
            "    {\n"
            "      \"symbol\": \"BTCUSDT\",\n"
            "      \"action\": \"SET_STOP|SET_TRAILING|HOLD\",\n"
            "      \"stop_pct\": 5.0,\n"
            "      \"trailing_pct\": 2.0,\n"
            "      \"confidence\": 75,\n"
            "      \"reason\": \"short reason\"\n"
            "    }\n"
            "  ]\n"
            "}\n"
            "END_STRATA_PROTECTION_PLAN_JSON\n\n"
            "Data:\n"
            + json.dumps(payload, indent=2)
        )

    def _run_protect_open_positions_ai(self, auto_submit: bool = False, source: str = "manual") -> None:
        task_name = "Protect Open Positions"
        starter = lambda: self._start_protect_open_positions_ai(auto_submit=auto_submit, source=source)
        if self._queue_if_busy(task_name, starter):
            return
        starter()

    def _start_protect_open_positions_ai(self, auto_submit: bool = False, source: str = "manual") -> None:
        profile = self.pf_binance_profile_var.get().strip() or None
        if not profile:
            messagebox.showinfo("Protection", "Select a Binance profile first.")
            return
        task_name = "Protect Open Positions"
        task_id = self._set_busy(True, task_name)
        self._append_task_terminal(f"START Protect Open Positions ({source})")
        cfg = {
            "profile": profile,
            "display_currency": self.display_currency_var.get().strip() or "USD",
            "bt": {
                "months": int(self.bt_months.get()) if hasattr(self, "bt_months") else 12,
                "country": parse_country_code(self.bt_country.get(), self.bt_country_manual.get()) if hasattr(self, "bt_country") else "2",
                "initial_capital": float(self.bt_initial.get()) if hasattr(self, "bt_initial") else 10000.0,
                "stop_loss_pct": float(self.bt_stop_loss.get()) if hasattr(self, "bt_stop_loss") else 8.0,
                "take_profit_pct": float(self.bt_take_profit.get()) if hasattr(self, "bt_take_profit") else 20.0,
                "max_hold_days": int(self.bt_max_hold_days.get()) if hasattr(self, "bt_max_hold_days") else 45,
                "min_hold_bars": int(self.bt_min_hold_bars.get()) if hasattr(self, "bt_min_hold_bars") else 2,
                "cooldown_bars": int(self.bt_cooldown_bars.get()) if hasattr(self, "bt_cooldown_bars") else 1,
                "same_asset_cooldown_bars": int(self.bt_same_asset_cooldown.get()) if hasattr(self, "bt_same_asset_cooldown") else 3,
                "max_consecutive_same_asset_entries": int(self.bt_max_same_asset_entries.get()) if hasattr(self, "bt_max_same_asset_entries") else 3,
                "fee_pct": float(self.bt_fee_pct.get()) if hasattr(self, "bt_fee_pct") else 0.10,
                "slippage_pct": float(self.bt_slippage_pct.get()) if hasattr(self, "bt_slippage_pct") else 0.05,
                "position_size": max(0.01, min(1.0, (float(self.bt_position_size_pct.get()) if hasattr(self, "bt_position_size_pct") else 30.0) / 100.0)),
                "atr_multiplier": float(self.bt_atr_mult.get()) if hasattr(self, "bt_atr_mult") else 2.2,
                "adx_threshold": float(self.bt_adx_threshold.get()) if hasattr(self, "bt_adx_threshold") else 25.0,
                "cmf_threshold": float(self.bt_cmf_threshold.get()) if hasattr(self, "bt_cmf_threshold") else 0.02,
                "obv_slope_threshold": float(self.bt_obv_threshold.get()) if hasattr(self, "bt_obv_threshold") else 0.0,
                "max_drawdown_limit_pct": float(self.bt_max_dd_target_pct.get()) if hasattr(self, "bt_max_dd_target_pct") else 35.0,
                "max_exposure_pct": max(0.01, min(1.0, (float(self.bt_max_exposure_pct.get()) if hasattr(self, "bt_max_exposure_pct") else 40.0) / 100.0)),
                "cache_workers": int(self.bt_cache_workers.get()) if hasattr(self, "bt_cache_workers") else 4,
                "buy_threshold": int(self.bt_buy_threshold.get()) if hasattr(self, "bt_buy_threshold") else 2,
                "sell_threshold": int(self.bt_sell_threshold.get()) if hasattr(self, "bt_sell_threshold") else -2,
            },
            "auto_submit": bool(auto_submit),
            "source": source,
        }

        def worker():
            bridge = self._bridge_for_task()
            try:
                res = self._compute_protection_recommendations(bridge, cfg)
            except Exception as exc:
                res = {"ok": False, "error": f"{type(exc).__name__}: {exc}", "source": source}
            self.root.after(0, lambda: self._finish_protect_open_positions_ai(res, task_id))

        threading.Thread(target=worker, daemon=True).start()

    def _compute_protection_recommendations(self, bridge: EngineBridge, cfg: Dict[str, Any]) -> Dict[str, Any]:
        profile = str(cfg.get("profile", "")).strip() or None
        out = bridge.get_trade_ledger()
        if not out.get("ok"):
            return {"ok": False, "error": str(out.get("error", "Failed to load ledger."))}
        ledger = out.get("ledger", {}) if isinstance(out.get("ledger"), dict) else {}
        open_positions = ledger.get("open_positions", {}) if isinstance(ledger, dict) else {}
        if not isinstance(open_positions, dict) or not open_positions:
            return {"ok": True, "staged_recs": [], "positions": 0, "ai_ok": False, "bt_ctx": 0, "note": "No open positions in ledger."}

        display_ccy = str(cfg.get("display_currency", "USD") or "USD")
        bt_opts = cfg.get("bt", {}) if isinstance(cfg.get("bt"), dict) else {}
        pos_rows: List[Dict[str, Any]] = []
        bt_context: Dict[str, Dict[str, Any]] = {}
        for _, pos in open_positions.items():
            if not isinstance(pos, dict):
                continue
            asset = str(pos.get("asset", "")).strip().upper()
            if not asset:
                continue
            quote = str(pos.get("quote_currency", self._effective_crypto_quote("USDT")) or self._effective_crypto_quote("USDT")).strip().upper()
            symbol = str(pos.get("symbol", "")).strip().upper() or f"{asset}{quote}"
            timeframe = str(pos.get("timeframe", "4h") or "4h").strip().lower()
            try:
                qty = float(pos.get("qty", 0.0) or 0.0)
            except Exception:
                qty = 0.0
            try:
                entry_price = float(pos.get("entry_price", 0.0) or 0.0)
            except Exception:
                entry_price = 0.0
            if qty <= 0:
                continue
            lp = bridge.get_binance_last_price(symbol=symbol, profile_name=profile)
            last_price = float(lp.get("price", 0.0) or 0.0) if lp.get("ok") else 0.0
            pnl_pct = ((last_price - entry_price) / entry_price * 100.0) if (entry_price > 0 and last_price > 0) else 0.0
            pos_rows.append(
                {
                    "asset": asset,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "qty": round(qty, 8),
                    "entry_price": round(entry_price, 8),
                    "last_price": round(last_price, 8),
                    "pnl_pct": round(pnl_pct, 4),
                }
            )
            try:
                bt_cfg = dict(bt_opts)
                bt_cfg.update(
                    {
                        "market": "crypto",
                        "timeframe": timeframe if timeframe in ("1d", "4h", "8h", "12h") else "4h",
                        "top_n": 1,
                        "quote_currency": quote,
                        "display_currency": display_ccy,
                        "tickers": [f"{asset}-USD" if quote == "USD" else f"{asset}-{quote}"],
                    }
                )
                bt = bridge.run_backtest(bt_cfg)
                bt_context[symbol] = {"ok": bool(bt.get("ok")), "summary_text": str(bt.get("summary_text", "") or "")[:3500]}
            except Exception as exc:
                bt_context[symbol] = {"ok": False, "summary_text": f"Backtest context error: {type(exc).__name__}"}

        if not pos_rows:
            return {"ok": True, "staged_recs": [], "positions": 0, "ai_ok": False, "bt_ctx": 0, "note": "No valid open positions found."}

        prompt = self._build_protection_ai_prompt(pos_rows, bt_context)
        dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ai_res = bridge.run_ai_analysis(
            dashboard_text=json.dumps({"positions": pos_rows}),
            datetime_context=dt,
            prompt_override=prompt,
            system_prompt_override=(
                "You are a strict JSON risk assistant. "
                "Output only the protection plan JSON between required markers."
            ),
        )
        plans = self._extract_protection_plan_from_ai_text(ai_res.get("response", "")) if ai_res.get("ok") else []
        if not plans:
            fallback_stop = float(bt_opts.get("stop_loss_pct", 8.0) or 8.0)
            plans = [
                {
                    "symbol": p.get("symbol", ""),
                    "action": "SET_STOP",
                    "stop_pct": fallback_stop,
                    "trailing_pct": 0.0,
                    "confidence": 50,
                    "reason": "Fallback protection (AI unavailable/empty).",
                }
                for p in pos_rows
            ]

        staged_recs: List[Dict[str, Any]] = []
        for plan in plans:
            symbol = str(plan.get("symbol", "")).strip().upper()
            action = str(plan.get("action", "SET_STOP")).strip().upper()
            if not symbol or action == "HOLD":
                continue
            pos = next((x for x in pos_rows if str(x.get("symbol", "")).upper() == symbol), None)
            if not pos:
                continue
            qty = float(pos.get("qty", 0.0) or 0.0)
            last_price = float(pos.get("last_price", 0.0) or 0.0)
            if qty <= 0 or last_price <= 0:
                continue
            try:
                stop_pct = max(1.0, min(20.0, float(plan.get("stop_pct", 0.0) or 0.0)))
            except Exception:
                stop_pct = float(bt_opts.get("stop_loss_pct", 8.0) or 8.0)
            try:
                trailing_pct = max(0.0, min(15.0, float(plan.get("trailing_pct", 0.0) or 0.0)))
            except Exception:
                trailing_pct = 0.0
            stop_price = last_price * (1.0 - (stop_pct / 100.0))
            limit_price = stop_price * 0.995
            reason = str(plan.get("reason", "Protection recommendation") or "Protection recommendation").strip()
            if trailing_pct > 0:
                reason += f" | trailing suggested {trailing_pct:.2f}% (implemented as fixed stop for compatibility)"
            staged_recs.append(
                {
                    "symbol": symbol,
                    "asset": self._base_asset_from_symbol(symbol),
                    "side": "SELL",
                    "order_type": "STOP_LOSS_LIMIT",
                    "quantity": qty,
                    "stop_loss_price": round(stop_price, 8),
                    "limit_price": round(limit_price, 8),
                    "timeframe": str(pos.get("timeframe", "4h")),
                    "confidence": str(plan.get("confidence", "") or ""),
                    "status": "PENDING",
                    "reason": reason,
                    "replace_existing_stop": True,
                    "protection_mode": action,
                }
            )
        return {
            "ok": True,
            "staged_recs": staged_recs,
            "positions": len(pos_rows),
            "ai_ok": bool(ai_res.get("ok")),
            "bt_ctx": len(bt_context),
            "auto_submit": bool(cfg.get("auto_submit", False)),
            "source": str(cfg.get("source", "manual")),
        }

    def _finish_protect_open_positions_ai(self, res: Dict[str, Any], task_id: Optional[int]) -> None:
        source = str(res.get("source", "manual") or "manual")
        if not res.get("ok"):
            self._append_task_terminal(f"DONE Protect Open Positions ({source}, error: {res.get('error', 'unknown')})")
            messagebox.showerror("Protection", str(res.get("error", "Failed to generate protection recommendations.")))
            self._finish_task(task_id, task_name="Protect Open Positions")
            return
        staged_recs = res.get("staged_recs", []) if isinstance(res.get("staged_recs"), list) else []
        if not staged_recs:
            note = str(res.get("note", "No protective recommendations generated.") or "No protective recommendations generated.")
            self._append_task_terminal(f"DONE Protect Open Positions ({source}, {note})")
            if source == "manual":
                messagebox.showinfo("Protection", note)
            self._finish_task(task_id, task_name="Protect Open Positions")
            return
        new_ids: List[int] = []
        for r in staged_recs:
            if not isinstance(r, dict):
                continue
            self._pending_rec_seq += 1
            rr = dict(r)
            rr["id"] = self._pending_rec_seq
            self.pending_recommendations.append(rr)
            new_ids.append(int(self._pending_rec_seq))
        self._refresh_pending_recommendations_view()
        self._append_task_terminal(
            f"DONE Protect Open Positions ({source}) -> staged={len(new_ids)}, positions={int(res.get('positions', 0) or 0)}, ai_ok={bool(res.get('ai_ok'))}, bt_ctx={int(res.get('bt_ctx', 0) or 0)}"
        )
        if bool(res.get("auto_submit", False)) and new_ids:
            self._submit_selected_pending_orders(ids_override=new_ids, require_confirm=False)
        elif source == "manual":
            messagebox.showinfo(
                "Protection Recommendations",
                f"Staged {len(new_ids)} protective order recommendation(s).\nReview and submit from Pending Recommendations.",
            )
        self._finish_task(task_id, task_name="Protect Open Positions")

    def _protection_monitor_tick(self) -> None:
        if not self.protection_monitor_job:
            return
        self._run_protect_open_positions_ai(
            auto_submit=bool(self.pf_protect_auto_send_var.get()),
            source="monitor",
        )
        try:
            mins = max(1, int((self.pf_protect_interval_min_var.get() or "30").strip()))
        except Exception:
            mins = 30
        self.protection_monitor_job = self.root.after(mins * 60 * 1000, self._protection_monitor_tick)

    def _start_protection_monitor(self) -> None:
        self._stop_protection_monitor()
        try:
            mins = max(1, int((self.pf_protect_interval_min_var.get() or "30").strip()))
        except Exception:
            mins = 30
            self.pf_protect_interval_min_var.set("30")
        self._append_task_terminal(
            f"Started protection monitor ({mins} min interval, auto_send={bool(self.pf_protect_auto_send_var.get())})."
        )
        self.protection_monitor_job = self.root.after(1000, self._protection_monitor_tick)

    def _stop_protection_monitor(self) -> None:
        if self.protection_monitor_job:
            try:
                self.root.after_cancel(self.protection_monitor_job)
            except Exception:
                pass
            self.protection_monitor_job = None
            self._append_task_terminal("Stopped protection monitor.")

    def _is_portfolio_tab_selected(self) -> bool:
        try:
            cur = self.nb.select()
            return bool(cur) and (self.nb.tab(cur, "text") == "Portfolio & Ledger")
        except Exception:
            return False

    def _refresh_portfolio_suite(self) -> None:
        # Keep local views fresh instantly.
        self._refresh_ledger_view()
        # Network-backed views.
        self._refresh_portfolio()
        try:
            self.root.after(120, self._refresh_open_orders)
        except Exception:
            pass

    def _start_portfolio_auto_refresh(self) -> None:
        self._stop_portfolio_auto_refresh()
        if not hasattr(self, "pf_auto_refresh_var") or not bool(self.pf_auto_refresh_var.get()):
            return
        self.portfolio_auto_refresh_running = True
        self._schedule_portfolio_auto_refresh_tick(initial=True)

    def _stop_portfolio_auto_refresh(self) -> None:
        self.portfolio_auto_refresh_running = False
        if self.portfolio_auto_refresh_job:
            try:
                self.root.after_cancel(self.portfolio_auto_refresh_job)
            except Exception:
                pass
            self.portfolio_auto_refresh_job = None

    def _schedule_portfolio_auto_refresh_tick(self, initial: bool = False) -> None:
        if not self.portfolio_auto_refresh_running:
            return
        if not self._is_portfolio_tab_selected():
            self._stop_portfolio_auto_refresh()
            return
        try:
            secs = max(10, int((self.pf_auto_refresh_secs_var.get() or "45").strip()))
        except Exception:
            secs = 45
        if initial:
            self._append_task_terminal(f"Portfolio auto-refresh started ({secs}s).")
        self._refresh_portfolio_suite()
        self.portfolio_auto_refresh_job = self.root.after(
            secs * 1000,
            lambda: self._schedule_portfolio_auto_refresh_tick(initial=False),
        )

    def _on_notebook_tab_changed(self, _event=None) -> None:
        if self._is_portfolio_tab_selected():
            self._refresh_portfolio_suite()
            if hasattr(self, "pf_auto_refresh_var") and bool(self.pf_auto_refresh_var.get()):
                self._start_portfolio_auto_refresh()
        else:
            self._stop_portfolio_auto_refresh()

    def _review_open_positions_mtf(self) -> None:
        profile = self.pf_binance_profile_var.get().strip() or None
        if not profile:
            messagebox.showinfo("Open Position Review", "Select a Binance profile first.")
            return
        task_name = "Open Position Review (MTF)"
        if self._queue_if_busy(task_name, self._start_review_open_positions_mtf):
            return
        self._start_review_open_positions_mtf()

    def _start_review_open_positions_mtf(self) -> None:
        profile = self.pf_binance_profile_var.get().strip() or None
        if not profile:
            messagebox.showinfo("Open Position Review", "Select a Binance profile first.")
            return
        task_name = "Open Position Review (MTF)"
        task_id = self._set_busy(True, task_name)
        self._append_task_terminal("START Open Position Review (4h/8h/12h/1d)")
        display_ccy = self.display_currency_var.get().strip() or "USD"

        def worker():
            bridge = self._bridge_for_task()
            try:
                res = bridge.analyze_open_positions_multi_tf(
                    profile_name=profile,
                    timeframes=["4h", "8h", "12h", "1d"],
                    display_currency=display_ccy,
                )
            except Exception as exc:
                res = {"ok": False, "error": str(exc)}
            self.root.after(0, lambda: self._finish_review_open_positions_mtf(res, task_id))

        threading.Thread(target=worker, daemon=True).start()

    def _finish_review_open_positions_mtf(self, out: Dict[str, Any], task_id: Optional[int]) -> None:
        task_name = "Open Position Review (MTF)"
        if not out.get("ok"):
            self._append_task_terminal(f"DONE Open Position Review (error: {out.get('error', 'unknown')})")
            messagebox.showerror("Open Position Review", str(out.get("error", "Failed to analyze open positions.")))
            self._finish_task(task_id, task_name=task_name)
            return
        rows = out.get("rows", []) or []
        if not rows:
            note = str(out.get("note", "") or "No open positions to analyze.")
            self._append_task_terminal(f"DONE Open Position Review ({note})")
            messagebox.showinfo("Open Position Review", note)
            self._finish_task(task_id, task_name=task_name)
            return
        lines = []
        lines.append("OPEN POSITION MTF REVIEW")
        lines.append("=" * 90)
        for r in rows:
            actions = r.get("actions", {}) if isinstance(r.get("actions"), dict) else {}
            lines.append(
                f"{r.get('asset','')} ({r.get('symbol','')}) qty={r.get('qty',0)} stance={r.get('stance','HOLD')} "
                f"| BUY/HOLD/SELL votes={r.get('buy_votes',0)}/{r.get('hold_votes',0)}/{r.get('sell_votes',0)}"
            )
            lines.append(
                f"  4h:{actions.get('4h','N/A')}  8h:{actions.get('8h','N/A')}  12h:{actions.get('12h','N/A')}  1d:{actions.get('1d','N/A')}"
            )
        blob = "\n".join(lines)
        self._append_task_terminal(blob)
        self._append_task_terminal(f"DONE Open Position Review ({len(rows)} positions)")
        messagebox.showinfo("Open Position Review", f"Reviewed {len(rows)} open position(s).\nSee Task Terminal for detailed breakdown.")
        self._finish_task(task_id, task_name=task_name)

    def _cancel_selected_open_orders(self) -> None:
        if not hasattr(self, "open_orders_tree"):
            return
        sels = list(self.open_orders_tree.selection())
        if not sels:
            messagebox.showinfo("Cancel Orders", "Select one or more open orders first.")
            return
        ok = messagebox.askyesno("Cancel Orders", f"Cancel {len(sels)} selected order(s)?")
        if not ok:
            return
        profile = self.pf_binance_profile_var.get().strip() or None
        done = 0
        fail = 0
        for iid in sels:
            vals = self.open_orders_tree.item(iid, "values")
            if not vals or len(vals) < 2:
                continue
            symbol = str(vals[0]).strip().upper()
            try:
                oid = int(vals[1])
            except Exception:
                continue
            out = self.bridge.cancel_binance_order(symbol=symbol, order_id=oid, profile_name=profile)
            if out.get("ok"):
                done += 1
            else:
                fail += 1
        self._refresh_open_orders()
        self._append_task_terminal(f"Cancel orders -> canceled={done}, failed={fail}")
        messagebox.showinfo("Cancel Orders", f"Canceled: {done}\nFailed: {fail}")

    def _refresh_portfolio(self) -> None:
        task_name = "Binance Portfolio Refresh"
        if self._queue_if_busy(task_name, self._start_refresh_portfolio):
            return
        self._start_refresh_portfolio()

    def _start_refresh_portfolio(self) -> None:
        task_name = "Binance Portfolio Refresh"
        task_id = self._set_busy(True, task_name)
        self._append_task_terminal("START Binance Portfolio Refresh")
        self.pf_portfolio_text.delete("1.0", tk.END)
        profile_name = self.pf_binance_profile_var.get().strip() or None

        def worker():
            bridge = self._bridge_for_task()
            res = bridge.fetch_binance_portfolio(profile_name=profile_name)
            self.root.after(0, lambda: self._finish_refresh_portfolio(res, task_id))

        threading.Thread(target=worker, daemon=True).start()

    def _finish_refresh_portfolio(self, res: Dict[str, Any], task_id: Optional[int]) -> None:
        if not res.get("ok"):
            self.pf_portfolio_text.insert("1.0", f"ERROR: {res.get('error', 'unknown')}\n")
            self._append_task_terminal(f"DONE Binance Portfolio Refresh (error: {res.get('error', 'unknown')})")
            self._finish_task(task_id, task_name="Binance Portfolio Refresh")
            return
        self.latest_portfolio_snapshot = res
        profile = str(res.get("profile", "") or "")
        total = float(res.get("total_est_usd", 0.0) or 0.0)
        rows = res.get("balances", []) or []
        lines = [
            f"Profile: {profile}",
            f"Estimated Total (USD): ${total:,.2f}",
            "",
        ]
        if rows:
            lines.append(pd.DataFrame(rows).to_string(index=False))
        else:
            lines.append("No non-zero balances returned.")
        self.pf_portfolio_text.insert("1.0", "\n".join(lines))
        self._apply_color_tags(self.pf_portfolio_text)
        self._append_task_terminal("DONE Binance Portfolio Refresh")
        self._finish_task(task_id, task_name="Binance Portfolio Refresh")

    def _extract_signals_from_live_text(self, text: str) -> List[Dict[str, str]]:
        signals: List[Dict[str, str]] = []
        if not text.strip():
            return signals
        current_panel = "live"
        current_market = "crypto"
        current_tf = "1d"
        header_re = re.compile(r"^===\s*(.+?)\s*\((Crypto|Traditional)\s+([^)]+)\)\s*===", re.IGNORECASE)
        row_re = re.compile(r"^\s*\d+\s+([A-Z0-9\-\.\^]+)\s+(?:🟢|🟠|🔴|\?\?)?\s*(BUY|HOLD|SELL)\b", re.IGNORECASE)
        for raw in text.splitlines():
            line = raw.strip()
            h = header_re.match(line)
            if h:
                current_panel = h.group(1).strip()
                current_market = h.group(2).strip().lower()
                current_tf = h.group(3).strip().lower()
                continue
            m = row_re.match(raw)
            if m:
                asset = m.group(1).strip().upper()
                action = m.group(2).strip().upper()
                signals.append(
                    {
                        "panel": current_panel,
                        "market": current_market,
                        "timeframe": current_tf,
                        "asset": asset,
                        "action": action,
                    }
                )
        return signals

    def _import_signals_from_live(self) -> None:
        text = (self.latest_live_output_text or "").strip()
        if not text:
            path = self.repo_root / "experiments" / "live_snapshots" / "latest_live_dashboard.txt"
            if path.exists():
                text = path.read_text(encoding="utf-8")
        if not text.strip():
            messagebox.showinfo("Import Signals", "No live dashboard output is available yet.")
            return
        signals = self._extract_signals_from_live_text(text)
        if not signals:
            messagebox.showinfo("Import Signals", "No BUY/HOLD/SELL signal rows were found.")
            return
        track_hold = bool(self.pf_track_hold_var.get())
        try:
            cooldown = max(1, int((self.pf_cooldown_min_var.get() or "240").strip()))
        except Exception:
            cooldown = 240
        accepted = 0
        blocked = 0
        skipped = 0
        for s in signals:
            action = str(s.get("action", "")).upper()
            if (not track_hold) and action == "HOLD":
                skipped += 1
                continue
            out = self.bridge.record_signal_event(
                {
                    "market": s.get("market", "crypto"),
                    "timeframe": s.get("timeframe", "1d"),
                    "panel": s.get("panel", "live"),
                    "asset": s.get("asset", ""),
                    "action": action,
                    "quote_currency": self._effective_crypto_quote("USDT"),
                    "display_currency": self.display_currency_var.get().strip() or "USD",
                    "note": "Imported from latest live dashboard",
                },
                cooldown_minutes=cooldown,
                allow_duplicate=False,
                guard_hold_signals=bool(self.pf_guard_hold_var.get()),
            )
            if out.get("ok"):
                accepted += 1
            elif out.get("blocked"):
                blocked += 1
        self._refresh_ledger_view()
        self._append_task_terminal(
            f"Imported signals -> accepted={accepted}, blocked={blocked}, skipped={skipped}, cooldown={cooldown}m"
        )
        messagebox.showinfo(
            "Signal Import Complete",
            f"Accepted: {accepted}\nBlocked (duplicate-guard): {blocked}\nSkipped: {skipped}",
        )

    def _add_manual_ledger_event(self) -> None:
        asset = self.pf_manual_asset_var.get().strip().upper()
        action = self.pf_manual_action_var.get().strip().upper()
        tf = self.pf_manual_tf_var.get().strip().lower() or "1d"
        note = self.pf_manual_note_var.get().strip()
        try:
            price = float((self.pf_manual_price_var.get() or "0").strip())
        except Exception:
            price = 0.0
        try:
            qty = float((self.pf_manual_qty_var.get() or "0").strip())
        except Exception:
            qty = 0.0
        try:
            cooldown = max(1, int((self.pf_cooldown_min_var.get() or "240").strip()))
        except Exception:
            cooldown = 240
        if not asset:
            messagebox.showinfo("Manual Event", "Asset is required.")
            return
        out = self.bridge.record_signal_event(
            {
                "market": "crypto",
                "timeframe": tf,
                "panel": "manual",
                "asset": asset,
                "action": action,
                "price": price,
                "qty": qty,
                "quote_currency": self._effective_crypto_quote("USDT"),
                "display_currency": self.display_currency_var.get().strip() or "USD",
                "note": note,
                "is_execution": bool(qty > 0),
            },
            cooldown_minutes=cooldown,
            allow_duplicate=False,
            guard_hold_signals=bool(self.pf_guard_hold_var.get()),
        )
        if out.get("ok"):
            self._append_task_terminal(f"Manual ledger event added: {action} {asset} ({tf})")
            self.pf_manual_note_var.set("")
            self._refresh_ledger_view()
            return
        if out.get("blocked"):
            messagebox.showinfo("Manual Event Blocked", str(out.get("reason", "Duplicate signal blocked.")))
            return
        messagebox.showerror("Manual Event Failed", str(out.get("error", "Unknown error")))

    def _refresh_ledger_view(self) -> None:
        out = self.bridge.get_trade_ledger()
        if not out.get("ok"):
            if hasattr(self, "pf_ledger_text"):
                self.pf_ledger_text.delete("1.0", tk.END)
                self.pf_ledger_text.insert("1.0", f"ERROR: {out.get('error', 'unknown')}")
            return
        ledger = out.get("ledger", {}) if isinstance(out.get("ledger"), dict) else {}
        entries = ledger.get("entries", []) if isinstance(ledger, dict) else []
        signal_entries = out.get("signal_entries", [])
        execution_entries = out.get("execution_entries", [])
        if not isinstance(signal_entries, list):
            signal_entries = []
        if not isinstance(execution_entries, list):
            execution_entries = []
        visible_execution_entries = [e for e in execution_entries if not bool((e or {}).get("is_placeholder", False))]
        hidden_placeholders = max(0, len(execution_entries) - len(visible_execution_entries))
        open_positions = ledger.get("open_positions", {}) if isinstance(ledger, dict) else {}
        guard = ledger.get("activity_guard", {}) if isinstance(ledger, dict) else {}
        profile_name = self.pf_binance_profile_var.get().strip() or None
        if hasattr(self, "display_currency_var"):
            try:
                display_ccy = (self.display_currency_var.get().strip() or "USD").upper()
            except Exception:
                display_ccy = str(self.state.get("display_currency", "USD") or "USD").strip().upper()
        else:
            display_ccy = str(self.state.get("display_currency", "USD") or "USD").strip().upper()
        usd_like = {"USD", "USDT", "USDC", "BUSD", "FDUSD", "TUSD", "USDP", "DAI"}
        try:
            fx_usd_to_disp = float(self.bridge.mod.get_usd_to_currency_rate(display_ccy))
            if fx_usd_to_disp <= 0:
                fx_usd_to_disp = 1.0
        except Exception:
            fx_usd_to_disp = 1.0

        unrealized_quote: Dict[str, float] = {}
        unrealized_display: Dict[str, float] = {}
        op_rows: List[Dict[str, Any]] = []
        protective_orders_by_symbol: Dict[str, List[Dict[str, Any]]] = {}
        if profile_name:
            try:
                oout = self.bridge.list_open_binance_orders(profile_name=profile_name, symbol="")
                if oout.get("ok"):
                    for o in (oout.get("orders", []) or []):
                        if not isinstance(o, dict):
                            continue
                        sym = str(o.get("symbol", "") or "").strip().upper()
                        side = str(o.get("side", "") or "").strip().upper()
                        otype = str(o.get("type", "") or "").strip().upper()
                        # Treat sell-side stop / trailing / take-profit orders as protective overlays.
                        is_protective = side == "SELL" and (
                            ("STOP" in otype)
                            or ("TAKE_PROFIT" in otype)
                            or ("TRAILING" in otype)
                        )
                        if not sym or not is_protective:
                            continue
                        protective_orders_by_symbol.setdefault(sym, []).append(o)
            except Exception:
                protective_orders_by_symbol = {}

        if isinstance(open_positions, dict):
            for _, pos in open_positions.items():
                if not isinstance(pos, dict):
                    continue
                row = dict(pos)
                asset = str(row.get("asset", "") or "").strip().upper()
                quote_ccy = str(row.get("quote_currency", self._effective_crypto_quote("USDT")) or self._effective_crypto_quote("USDT")).strip().upper()
                symbol = str(row.get("symbol", "") or "").strip().upper()
                if (not symbol) and asset and quote_ccy:
                    symbol = f"{asset}{quote_ccy}"
                try:
                    entry_px = float(row.get("entry_price", 0.0) or 0.0)
                except Exception:
                    entry_px = 0.0
                try:
                    qty = float(row.get("qty", 0.0) or 0.0)
                except Exception:
                    qty = 0.0
                last_px = 0.0
                if symbol and profile_name:
                    try:
                        lpx = self.bridge.get_binance_last_price(symbol=symbol, profile_name=profile_name)
                        if lpx.get("ok"):
                            last_px = float(lpx.get("price", 0.0) or 0.0)
                    except Exception:
                        last_px = 0.0
                upnl_q = 0.0
                upnl_pct = 0.0
                if entry_px > 0 and qty > 0 and last_px > 0:
                    upnl_q = (last_px - entry_px) * qty
                    upnl_pct = ((last_px - entry_px) / entry_px) * 100.0
                upnl_d = 0.0
                if abs(upnl_q) > 0:
                    if quote_ccy in usd_like:
                        upnl_d = upnl_q * fx_usd_to_disp
                    elif quote_ccy == display_ccy:
                        upnl_d = upnl_q
                    else:
                        upnl_d = 0.0
                if abs(upnl_q) > 0:
                    unrealized_quote[quote_ccy] = unrealized_quote.get(quote_ccy, 0.0) + upnl_q
                if abs(upnl_d) > 0:
                    unrealized_display[display_ccy] = unrealized_display.get(display_ccy, 0.0) + upnl_d

                # Protection status:
                # - PROTECTED: has at least one protective open order
                # - STALE_PROTECTED: protective order exists but latest update/time > 24h old
                # - UNPROTECTED: no protective open order detected
                prot_state = "UNPROTECTED"
                prot_detail = ""
                now_utc = pd.Timestamp.now(tz="UTC")
                prot_orders = protective_orders_by_symbol.get(symbol, [])
                if prot_orders:
                    newest_ts = None
                    otypes: List[str] = []
                    for po in prot_orders:
                        otypes.append(str(po.get("type", "") or "").strip().upper())
                        try:
                            t_ms = int(po.get("updateTime", 0) or po.get("time", 0) or 0)
                        except Exception:
                            t_ms = 0
                        if t_ms > 0:
                            ts = pd.to_datetime(t_ms, unit="ms", utc=True, errors="coerce")
                            if pd.notna(ts):
                                if newest_ts is None or ts > newest_ts:
                                    newest_ts = ts
                    if newest_ts is not None:
                        age_h = (now_utc - newest_ts).total_seconds() / 3600.0
                        prot_state = "STALE_PROTECTED" if age_h > 24.0 else "PROTECTED"
                        prot_detail = f"{','.join(sorted(set(otypes)))} @ {newest_ts.strftime('%Y-%m-%d %H:%MZ')}"
                    else:
                        prot_state = "PROTECTED"
                        prot_detail = ",".join(sorted(set(otypes)))

                row["symbol"] = symbol
                row["quote_currency"] = quote_ccy
                row["last_price"] = round(last_px, 8) if last_px > 0 else ""
                row["unreal_pnl_quote"] = round(upnl_q, 8)
                row["unreal_pnl_pct"] = round(upnl_pct, 4)
                row["unreal_pnl_display"] = round(upnl_d, 8)
                row["protection_status"] = prot_state
                row["protection_detail"] = prot_detail
                op_rows.append(row)

        if hasattr(self, "pf_open_positions_text"):
            self.pf_open_positions_text.delete("1.0", tk.END)
            if op_rows:
                op_df = pd.DataFrame(op_rows)
                op_df = self._humanize_df_timestamps(op_df, ["entry_ts"])
                preferred = [
                    "entry_id", "entry_ts", "symbol", "asset", "timeframe", "qty", "entry_price",
                    "last_price", "unreal_pnl_pct", "unreal_pnl_quote", "unreal_pnl_display",
                    "quote_currency", "display_currency", "protection_status", "protection_detail", "panel",
                ]
                cols = [c for c in preferred if c in op_df.columns] + [c for c in op_df.columns if c not in preferred]
                op_df = op_df[cols]
                self.pf_open_positions_text.insert("1.0", op_df.to_string(index=False))
            else:
                self.pf_open_positions_text.insert("1.0", "No open positions tracked.")
            self._apply_color_tags(self.pf_open_positions_text)

        if hasattr(self, "pf_ledger_text"):
            self.pf_ledger_text.delete("1.0", tk.END)
            lines = []
            lines.append(f"Entries: {len(entries) if isinstance(entries, list) else 0}")
            lines.append(f"Signal Journal: {len(signal_entries)}")
            lines.append(f"Execution Ledger: {len(visible_execution_entries)}")
            if hidden_placeholders > 0:
                lines.append(f"Execution placeholders hidden: {hidden_placeholders}")
            lines.append(f"Guard Keys: {len(guard) if isinstance(guard, dict) else 0}")
            # Realized PnL summary in quote/display currencies (execution rows only).
            realized_quote: Dict[str, float] = {}
            realized_display: Dict[str, float] = {}
            for e in visible_execution_entries:
                if not isinstance(e, dict):
                    continue
                try:
                    pq = float(e.get("pnl_quote", 0.0) or 0.0)
                except Exception:
                    pq = 0.0
                if abs(pq) > 0:
                    qc = str(e.get("quote_currency", "USD") or "USD").strip().upper()
                    realized_quote[qc] = realized_quote.get(qc, 0.0) + pq
                try:
                    pdv = float(e.get("pnl_display", 0.0) or 0.0)
                except Exception:
                    pdv = 0.0
                if abs(pdv) > 0:
                    dc = str(e.get("display_currency", display_ccy) or display_ccy).strip().upper()
                    realized_display[dc] = realized_display.get(dc, 0.0) + pdv
            if realized_quote:
                qtxt = ", ".join([f"{k} {v:+,.4f}" for k, v in sorted(realized_quote.items())])
                lines.append(f"Realized PnL (Quote): {qtxt}")
            if realized_display:
                dtxt = ", ".join([f"{k} {v:+,.4f}" for k, v in sorted(realized_display.items())])
                lines.append(f"Realized PnL (Display): {dtxt}")
            if unrealized_quote:
                uqtxt = ", ".join([f"{k} {v:+,.4f}" for k, v in sorted(unrealized_quote.items())])
                lines.append(f"Unrealized PnL (Quote): {uqtxt}")
            if unrealized_display:
                udtxt = ", ".join([f"{k} {v:+,.4f}" for k, v in sorted(unrealized_display.items())])
                lines.append(f"Unrealized PnL (Display): {udtxt}")
            lines.append("")
            if isinstance(entries, list) and entries:
                display_entries = []
                for e in entries:
                    if not isinstance(e, dict):
                        continue
                    if bool(e.get("is_execution", False)) and bool(e.get("is_placeholder", False)):
                        continue
                    display_entries.append(e)
                if display_entries:
                    df = pd.DataFrame(display_entries[-200:])
                    df = self._humanize_df_timestamps(df, ["ts", "entry_ts", "exit_ts"])
                    lines.append(df.to_string(index=False))
                else:
                    lines.append("No non-placeholder ledger entries yet.")
            else:
                lines.append("No ledger entries yet.")
            self.pf_ledger_text.insert("1.0", "\n".join(lines))
            self._apply_color_tags(self.pf_ledger_text)

        if hasattr(self, "pf_signal_text"):
            self.pf_signal_text.delete("1.0", tk.END)
            if signal_entries:
                sig_df = pd.DataFrame(signal_entries[-200:])
                sig_df = self._humanize_df_timestamps(sig_df, ["ts", "entry_ts", "exit_ts"])
                self.pf_signal_text.insert("1.0", sig_df.to_string(index=False))
            else:
                self.pf_signal_text.insert("1.0", "No signal-only entries.")
            self._apply_color_tags(self.pf_signal_text)

        if hasattr(self, "pf_execution_text"):
            self.pf_execution_text.delete("1.0", tk.END)
            if visible_execution_entries:
                exe_df = pd.DataFrame(visible_execution_entries[-200:])
                exe_df = self._humanize_df_timestamps(exe_df, ["ts", "entry_ts", "exit_ts"])
                for col in ["pnl_pct", "pnl_quote", "pnl_display"]:
                    if col not in exe_df.columns:
                        exe_df[col] = ""
                preferred = [
                    "id", "ts", "market", "timeframe", "panel", "asset", "action",
                    "price", "qty", "pnl_pct", "pnl_quote", "pnl_display",
                    "quote_currency", "display_currency", "note", "is_execution",
                ]
                cols = [c for c in preferred if c in exe_df.columns] + [c for c in exe_df.columns if c not in preferred]
                exe_df = exe_df[cols]
                self.pf_execution_text.insert("1.0", exe_df.to_string(index=False))
            else:
                self.pf_execution_text.insert("1.0", "No execution entries.")
            self._apply_color_tags(self.pf_execution_text)

    def _prune_signal_history(self) -> None:
        keep = 0
        if not messagebox.askyesno(
            "Prune Signal History",
            "Remove signal-only history from ledger?\n\n"
            "- Execution entries are kept.\n"
            "- Open positions with qty <= 0 are cleaned.\n"
            "- Activity guard remains intact.",
        ):
            return
        out = self.bridge.prune_signal_only_history(keep_last_signals=keep)
        if not out.get("ok"):
            messagebox.showerror("Prune Failed", str(out.get("error", "Unknown error")))
            return
        removed = int(out.get("removed_signal_entries", 0) or 0)
        kept = int(out.get("kept_signal_entries", 0) or 0)
        exe = int(out.get("execution_entries", 0) or 0)
        total = int(out.get("total_entries", 0) or 0)
        self._refresh_ledger_view()
        self._append_task_terminal(
            f"Ledger prune complete: removed_signal={removed}, kept_signal={kept}, "
            f"execution={exe}, total={total}"
        )
        messagebox.showinfo(
            "Prune Complete",
            f"Removed signal entries: {removed}\n"
            f"Kept signal entries: {kept}\n"
            f"Execution entries kept: {exe}\n"
            f"Total ledger entries now: {total}",
        )

    def _run_standard_research(self) -> None:
        self._run_research_job(standard=True)

    def _run_comprehensive_research(self) -> None:
        self._run_research_job(standard=False)

    def _run_research_job(self, standard: bool) -> None:
        task_name = "Auto-Research (Standard)" if standard else "Auto-Research (Comprehensive)"
        if self._queue_if_busy(task_name, lambda s=standard: self._start_run_research_job(s)):
            return
        self._start_run_research_job(standard)

    def _start_run_research_job(self, standard: bool) -> None:
        task_name = "Auto-Research (Standard)" if standard else "Auto-Research (Comprehensive)"
        task_id = self._set_busy(True, task_name)
        self._append_task_terminal(f"START {task_name}")
        self.rs_output.delete("1.0", tk.END)

        def worker():
            if standard:
                bridge = self._bridge_for_task()
                out = bridge.run_standard_research()
            else:
                scenarios = self._build_comprehensive_scenarios_from_form()
                bridge = self._bridge_for_task()
                out = bridge.run_comprehensive_research(
                    scenarios=scenarios,
                    optuna_trials=max(1, int(self.rs_trials.get() or "10")),
                    optuna_jobs=max(1, int(self.rs_jobs.get() or "4")),
                )
            stdlog = (out.get("stdout", "") or "").strip()
            errlog = (out.get("stderr", "") or "").strip()
            if stdlog:
                self._append_task_terminal_from_worker(f"LOG [{task_name}] {stdlog[:6000]}")
            if errlog:
                self._append_task_terminal_from_worker(f"ERR [{task_name}] {errlog[:6000]}")
            self.root.after(0, lambda: self._finish_research_output(out, task_name, task_id))

        threading.Thread(target=worker, daemon=True).start()

    def _build_comprehensive_scenarios_from_form(self) -> List[Dict[str, Any]]:
        scope = self.rs_market_scope.get().strip().lower()
        include_crypto = scope in ("crypto", "both")
        include_trad = scope in ("traditional", "both")
        quote = self.rs_quote.get().strip().upper() or "USD"
        country = parse_country_code(self.rs_country.get(), self.rs_country_manual.get())
        tfs = ["1d", "4h", "8h", "12h"]
        months = [1, 3, 6, 12, 18, 24]
        scenarios: List[Dict[str, Any]] = []
        if include_crypto:
            for tf in tfs:
                for m in months:
                    for n in [10, 20, 50, 100]:
                        scenarios.append(
                            {
                                "id": f"gui_crypto_{tf}_{m}m_top{n}_{quote}",
                                "market": "crypto",
                                "timeframe": tf,
                                "months": m,
                                "top_n": n,
                                "quote_currency": quote,
                            }
                        )
        if include_trad:
            for tf in tfs:
                for m in months:
                    for n in [10, 20, 50, 100]:
                        scenarios.append(
                            {
                                "id": f"gui_trad_c{country}_{tf}_{m}m_top{n}",
                                "market": "traditional",
                                "country": country,
                                "timeframe": tf,
                                "months": m,
                                "top_n": n,
                            }
                        )
        return scenarios

    def _finish_research_output(self, out: Dict[str, Any], task_name: str = "Auto-Research", task_id: Optional[int] = None) -> None:
        lines = [f"Command: {out.get('cmd', '')}", f"Return code: {out.get('returncode', '')}", ""]
        if out.get("stdout"):
            lines += ["STDOUT:", out["stdout"], ""]
        if out.get("stderr"):
            lines += ["STDERR:", out["stderr"], ""]
        self.rs_output.insert("1.0", "\n".join(lines))
        if out.get("ok"):
            self._append_task_terminal("DONE Auto-Research")
        else:
            self._append_task_terminal("DONE Auto-Research (error)")
        self._finish_task(task_id, task_name=task_name)

    def _save_dashboard_profile(self) -> None:
        name = self.dashboard_name_var.get().strip() or "default"
        dashboards = self.state.get("saved_dashboards", {})
        if not isinstance(dashboards, dict):
            dashboards = {}
        dashboards[name] = [asdict(p) for p in self.live_panels]
        self.state["saved_dashboards"] = dashboards
        self._persist_state()
        self._append_settings(f"Saved dashboard profile: {name}")

    def _load_dashboard_profile(self) -> None:
        name = self.dashboard_name_var.get().strip() or "default"
        dashboards = self.state.get("saved_dashboards", {})
        if not isinstance(dashboards, dict) or name not in dashboards:
            self._append_settings(f"Profile not found: {name}")
            return
        panels = dashboards[name]
        if not isinstance(panels, list) or not panels:
            self._append_settings(f"Profile empty: {name}")
            return
        self.live_panels = [LivePanelConfig(**p) for p in panels if isinstance(p, dict)]
        self._refresh_panel_list()
        self._persist_state()
        self._append_settings(f"Loaded dashboard profile: {name}")

    def _delete_dashboard_profile(self) -> None:
        name = self.dashboard_name_var.get().strip() or "default"
        dashboards = self.state.get("saved_dashboards", {})
        if isinstance(dashboards, dict) and name in dashboards:
            del dashboards[name]
            self.state["saved_dashboards"] = dashboards
            self._persist_state()
            self._append_settings(f"Deleted dashboard profile: {name}")
        else:
            self._append_settings(f"Profile not found: {name}")

    def _refresh_ai_profiles(self) -> None:
        res = self.bridge.list_ai_profiles()
        if not res.get("ok"):
            self._append_settings(f"AI refresh failed: {res.get('error', 'unknown')}")
            return
        profiles = res.get("profiles", []) or []
        self._ai_profiles_cache = {p.get("name", ""): p for p in profiles if isinstance(p, dict)}
        names = list(self._ai_profiles_cache.keys())
        self.ai_profile_combo["values"] = names
        active = str(res.get("active_profile", "") or "")
        if active and active in self._ai_profiles_cache:
            self.ai_profile_var.set(active)
        elif names:
            self.ai_profile_var.set(names[0])
        else:
            self.ai_profile_var.set("")
        self._load_ai_profile_into_form()
        self._append_settings(f"AI profiles loaded: {len(names)} (active: {self.ai_profile_var.get() or 'none'})")

    def _load_ai_profile_into_form(self) -> None:
        name = self.ai_profile_var.get().strip()
        prof = getattr(self, "_ai_profiles_cache", {}).get(name, {})
        if not prof:
            return
        self.ai_provider_var.set(str(prof.get("provider", "xai") or "xai"))
        self.ai_model_var.set(str(prof.get("model", "") or ""))
        self.ai_endpoint_var.set(str(prof.get("endpoint", "") or ""))
        self.ai_internet_var.set(bool(prof.get("internet_access", True)))
        self.ai_temp_var.set(str(prof.get("temperature", 0.2)))
        key_state = "set" if bool(prof.get("api_key_set", False)) else "missing"
        key_source = str(prof.get("api_key_source", "") or "")
        self._append_settings(f"Profile `{name}` loaded. API key: {key_state} ({key_source})")

    def _save_ai_profile_from_form(self) -> None:
        name = self.ai_profile_var.get().strip()
        if not name:
            messagebox.showinfo("AI Profiles", "Enter/select a profile name first.")
            return
        try:
            temp = float(self.ai_temp_var.get().strip() or "0.2")
        except Exception:
            temp = 0.2
        res = self.bridge.upsert_ai_profile(
            name=name,
            provider=self.ai_provider_var.get().strip().lower(),
            model=self.ai_model_var.get().strip(),
            endpoint=self.ai_endpoint_var.get().strip(),
            internet_access=bool(self.ai_internet_var.get()),
            temperature=temp,
            activate=False,
        )
        if not res.get("ok"):
            self._append_settings(f"Save AI profile failed: {res.get('error', 'unknown')}")
            return
        self._append_settings(f"Saved AI profile: {name}")
        self._refresh_ai_profiles()
        self.ai_profile_var.set(name)
        self._load_ai_profile_into_form()

    def _set_active_ai_profile_from_form(self) -> None:
        name = self.ai_profile_var.get().strip()
        if not name:
            return
        res = self.bridge.set_active_ai_profile(name)
        if not res.get("ok"):
            self._append_settings(f"Set active failed: {res.get('error', 'unknown')}")
            return
        self._append_settings(f"Active AI profile set: {name}")
        self._refresh_ai_profiles()

    def _delete_ai_profile_from_form(self) -> None:
        name = self.ai_profile_var.get().strip()
        if not name:
            return
        ok = messagebox.askyesno("Delete AI Profile", f"Delete AI profile '{name}'?")
        if not ok:
            return
        res = self.bridge.delete_ai_profile(name)
        if not res.get("ok"):
            self._append_settings(f"Delete AI profile failed: {res.get('error', 'unknown')}")
            return
        self._append_settings(f"Deleted AI profile: {name}")
        self._refresh_ai_profiles()

    def _set_ai_key_from_form(self) -> None:
        name = self.ai_profile_var.get().strip()
        key = self.ai_key_var.get().strip()
        if not name or not key:
            messagebox.showinfo("AI API Key", "Select profile and enter API key.")
            return
        res = self.bridge.set_ai_profile_key(name, key)
        if not res.get("ok"):
            self._append_settings(f"Set API key failed: {res.get('error', 'unknown')}")
            return
        self.ai_key_var.set("")
        self._append_settings(f"Stored API key for profile: {name}")
        self._refresh_ai_profiles()

    def _remove_ai_key_from_form(self) -> None:
        name = self.ai_profile_var.get().strip()
        if not name:
            return
        res = self.bridge.remove_ai_profile_key(name)
        if not res.get("ok"):
            self._append_settings(f"Remove API key failed: {res.get('error', 'unknown')}")
            return
        self._append_settings(f"Removed stored API key for profile: {name}")
        self._refresh_ai_profiles()

    def _test_ai_profile_from_form(self) -> None:
        name = self.ai_profile_var.get().strip()
        if not name:
            return
        self._append_settings(f"Testing AI profile: {name} ...")
        res = self.bridge.test_ai_profile(name)
        if res.get("ok"):
            snippet = (res.get("response", "") or "").strip()[:200]
            self._append_settings(f"AI test OK ({name}): {snippet}")
        else:
            err = res.get("error", "unknown")
            status = res.get("status", "")
            self._append_settings(f"AI test failed ({name}) status={status}: {err}")

    def _refresh_binance_profiles_from_settings(self) -> None:
        self._refresh_binance_profiles()
        names = list(self._binance_profiles_cache.keys())
        self.bn_profile_combo["values"] = names
        active = self.pf_binance_profile_var.get().strip()
        if active and active in self._binance_profiles_cache:
            self.bn_profile_var.set(active)
        elif names:
            self.bn_profile_var.set(names[0])
        else:
            self.bn_profile_var.set("")
        self._load_binance_profile_into_form()

    def _load_binance_profile_into_form(self) -> None:
        name = self.bn_profile_var.get().strip()
        prof = self._binance_profiles_cache.get(name, {})
        if not prof:
            return
        self.bn_endpoint_var.set(str(prof.get("endpoint", "https://api.binance.com") or "https://api.binance.com"))
        self.bn_key_env_var.set(str(prof.get("api_key_env", "BINANCE_API_KEY") or "BINANCE_API_KEY"))
        self.bn_secret_env_var.set(str(prof.get("api_secret_env", "BINANCE_API_SECRET") or "BINANCE_API_SECRET"))
        key_state = "set" if bool(prof.get("api_key_set", False)) else "missing"
        sec_state = "set" if bool(prof.get("api_secret_set", False)) else "missing"
        self._append_settings(
            f"Binance profile `{name}` loaded. key={key_state} ({prof.get('api_key_source','')}) secret={sec_state} ({prof.get('api_secret_source','')})"
        )

    def _save_binance_profile_from_form(self) -> None:
        name = self.bn_profile_var.get().strip()
        if not name:
            messagebox.showinfo("Binance Profiles", "Enter/select a profile name first.")
            return
        res = self.bridge.upsert_binance_profile(
            name=name,
            endpoint=self.bn_endpoint_var.get().strip(),
            api_key_env=self.bn_key_env_var.get().strip(),
            api_secret_env=self.bn_secret_env_var.get().strip(),
            activate=False,
        )
        if not res.get("ok"):
            self._append_settings(f"Save Binance profile failed: {res.get('error', 'unknown')}")
            return
        self._append_settings(f"Saved Binance profile: {name}")
        self._refresh_binance_profiles_from_settings()
        self.bn_profile_var.set(name)
        self.pf_binance_profile_var.set(name)
        self._load_binance_profile_into_form()

    def _set_active_binance_profile_from_form(self) -> None:
        name = self.bn_profile_var.get().strip()
        if not name:
            return
        res = self.bridge.set_active_binance_profile(name)
        if not res.get("ok"):
            self._append_settings(f"Set active Binance profile failed: {res.get('error', 'unknown')}")
            return
        self._append_settings(f"Active Binance profile set: {name}")
        self._refresh_binance_profiles_from_settings()
        self.pf_binance_profile_var.set(name)

    def _delete_binance_profile_from_form(self) -> None:
        name = self.bn_profile_var.get().strip()
        if not name:
            return
        ok = messagebox.askyesno("Delete Binance Profile", f"Delete Binance profile '{name}'?")
        if not ok:
            return
        res = self.bridge.delete_binance_profile(name)
        if not res.get("ok"):
            self._append_settings(f"Delete Binance profile failed: {res.get('error', 'unknown')}")
            return
        self._append_settings(f"Deleted Binance profile: {name}")
        self._refresh_binance_profiles_from_settings()

    def _set_binance_keys_from_form(self) -> None:
        name = self.bn_profile_var.get().strip()
        api_key = self.bn_key_var.get().strip()
        api_secret = self.bn_secret_var.get().strip()
        if not name or not api_key or not api_secret:
            messagebox.showinfo("Binance Keys", "Select a profile and provide both API key and API secret.")
            return
        res = self.bridge.set_binance_profile_keys(name, api_key, api_secret)
        if not res.get("ok"):
            self._append_settings(f"Set Binance keys failed: {res.get('error', 'unknown')}")
            return
        self.bn_key_var.set("")
        self.bn_secret_var.set("")
        self._append_settings(f"Stored Binance keys for profile: {name}")
        self._refresh_binance_profiles_from_settings()

    def _remove_binance_keys_from_form(self) -> None:
        name = self.bn_profile_var.get().strip()
        if not name:
            return
        res = self.bridge.remove_binance_profile_keys(name)
        if not res.get("ok"):
            self._append_settings(f"Remove Binance keys failed: {res.get('error', 'unknown')}")
            return
        self._append_settings(f"Removed stored Binance keys for profile: {name}")
        self._refresh_binance_profiles_from_settings()

    def _test_binance_profile_from_form(self) -> None:
        name = self.bn_profile_var.get().strip()
        if not name:
            return
        self._append_settings(f"Testing Binance profile: {name} ...")
        res = self.bridge.test_binance_profile(name)
        if res.get("ok"):
            self._append_settings(
                f"Binance test OK ({name}) canTrade={res.get('can_trade')} buyerCommission={res.get('buyer_commission_bps')}"
            )
            return
        self._append_settings(
            f"Binance test failed ({name}) status={res.get('status','')}: {res.get('error','unknown')}"
        )

    def _show_ai_settings_hint(self) -> None:
        messagebox.showinfo(
            "AI Provider Settings",
            "AI provider profiles/keys are managed via nightly CLI menu option:\n\n"
            "5. AI Provider Settings\n\n"
            "This GUI uses the active profile from that configuration.",
        )

    def _append_settings(self, text: str) -> None:
        if not hasattr(self, "settings_output") or self.settings_output is None:
            return
        self.settings_output.insert("end", text + "\n")
        self.settings_output.see("end")

    def _persist_state(self) -> None:
        self.state["display_currency"] = self.display_currency_var.get().strip() or "USD"
        self.state["primary_quote_asset"] = self._primary_quote_asset()
        self.state["lock_primary_quote"] = self._is_primary_quote_locked()
        if isinstance(getattr(self, "agent_context", None), dict):
            self.state["agent_context"] = dict(self.agent_context)
        if hasattr(self, "agent_ai_fallback_var"):
            self.state["agent_ai_fallback_enabled"] = bool(self.agent_ai_fallback_var.get())
        if hasattr(self, "agent_guard_enabled_var"):
            self.state["agent_guard_enabled"] = bool(self.agent_guard_enabled_var.get())
        if hasattr(self, "agent_guard_require_stop_var"):
            self.state["agent_guard_require_stop"] = bool(self.agent_guard_require_stop_var.get())
        if hasattr(self, "agent_guard_max_daily_loss_var"):
            try:
                self.state["agent_guard_max_daily_loss_pct"] = float(
                    (self.agent_guard_max_daily_loss_var.get() or "5").strip()
                )
            except Exception:
                self.state["agent_guard_max_daily_loss_pct"] = 5.0
        if hasattr(self, "agent_guard_max_trades_var"):
            try:
                self.state["agent_guard_max_trades_per_day"] = max(
                    1, int((self.agent_guard_max_trades_var.get() or "8").strip())
                )
            except Exception:
                self.state["agent_guard_max_trades_per_day"] = 8
        if hasattr(self, "agent_guard_max_exposure_var"):
            try:
                self.state["agent_guard_max_exposure_pct"] = max(
                    0.0, min(100.0, float((self.agent_guard_max_exposure_var.get() or "40").strip()))
                )
            except Exception:
                self.state["agent_guard_max_exposure_pct"] = 40.0
        if hasattr(self, "pf_auto_refresh_var"):
            self.state["portfolio_auto_refresh_enabled"] = bool(self.pf_auto_refresh_var.get())
        if hasattr(self, "pf_auto_refresh_secs_var"):
            try:
                self.state["portfolio_auto_refresh_seconds"] = max(
                    10, int((self.pf_auto_refresh_secs_var.get() or "45").strip())
                )
            except Exception:
                self.state["portfolio_auto_refresh_seconds"] = 45
        self.state["auto_refresh_seconds"] = int(self.refresh_secs_var.get().strip() or "120")
        mode_var = getattr(self, "parallel_mode_var", None)
        jobs_var = getattr(self, "parallel_jobs_var", None)
        self.state["parallel_mode_enabled"] = bool(mode_var.get()) if mode_var is not None else False
        self.state["verbose_terminal_logging"] = bool(self.verbose_logging_var.get()) if hasattr(self, "verbose_logging_var") else True
        try:
            raw_jobs = str(jobs_var.get()).strip() if jobs_var is not None else "2"
            self.state["parallel_max_jobs"] = max(1, int(raw_jobs or "2"))
        except Exception:
            self.state["parallel_max_jobs"] = 2
        self.state["live_panels"] = [asdict(p) for p in self.live_panels]
        save_state(self.state)

def run_gui() -> None:
    root = tk.Tk()
    repo_root = Path(__file__).resolve().parents[1]
    app = StrataGuiApp(root, repo_root=repo_root)

    def _shutdown():
        app._stop_pipeline_scheduler()
        app._stop_protection_monitor()
        app._stop_portfolio_auto_refresh()
        app._close_task_monitor()
        if app.task_tab_job:
            try:
                root.after_cancel(app.task_tab_job)
            except Exception:
                pass
            app.task_tab_job = None
        app._persist_state()
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", _shutdown)
    root.mainloop()


if __name__ == "__main__":
    run_gui()
