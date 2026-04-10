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
        self.portfolio_tab = self._create_scrollable_tab("Portfolio & Ledger", "portfolio")
        self.research_tab = self._create_scrollable_tab("Auto-Research", "research")
        self.task_tab = self._create_scrollable_tab("Task Monitor", "task")
        self.settings_tab = self._create_scrollable_tab("Settings", "settings")

        self._build_live_tab()
        self._build_backtest_tab()
        self._build_ai_tab()
        self._build_portfolio_tab()
        self._build_research_tab()
        self._build_task_tab()
        self._build_settings_tab()
        self._build_status_bar()

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
        left = ttk.Frame(self.live_tab, padding=8)
        right = ttk.Frame(self.live_tab, padding=8)
        left.pack(side="left", fill="y")
        right.pack(side="left", fill="both", expand=True)

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

    def _build_backtest_tab(self) -> None:
        top = ttk.Frame(self.backtest_tab, padding=8)
        top.pack(fill="x")
        out = ttk.Frame(self.backtest_tab, padding=8)
        out.pack(fill="both", expand=True)

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
        bt_trades_frame, self.bt_trades = self._create_scrolled_text(out, wrap="none")
        bt_trades_frame.pack(fill="both", expand=True, pady=(8, 0))
        self._configure_dashboard_tags(self.bt_trades)

    def _build_ai_tab(self) -> None:
        top = ttk.Frame(self.ai_tab, padding=8)
        top.pack(fill="x")
        body = ttk.Frame(self.ai_tab, padding=8)
        body.pack(fill="both", expand=True)

        self.ai_source = tk.StringVar(value="live")
        self.ai_datetime = tk.StringVar(value=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        self.ai_prompt_mode = tk.StringVar(value="preset_dashboard")
        self.ai_require_confirm = tk.BooleanVar(value=True)
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
        ttk.Button(top, text="Preview Prompt", command=self._preview_ai_prompt).pack(side="left")
        ttk.Button(top, text="Stage Recommendations", command=self._stage_ai_recommendations).pack(side="left", padx=6)
        ttk.Button(top, text="Clear Pending", command=self._clear_pending_recommendations).pack(side="left")
        ttk.Label(top, text="Pipeline min").pack(side="left", padx=(10, 0))
        ttk.Entry(top, textvariable=self.pipeline_interval_min_var, width=6).pack(side="left", padx=4)
        ttk.Button(top, text="Run Live->AI Pipeline", command=self._run_live_ai_pipeline_now).pack(side="left", padx=4)
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
        self.pf_manual_asset_var = tk.StringVar(value="")
        self.pf_manual_action_var = tk.StringVar(value="BUY")
        self.pf_manual_price_var = tk.StringVar(value="")
        self.pf_manual_qty_var = tk.StringVar(value="")
        self.pf_manual_tf_var = tk.StringVar(value="1d")
        self.pf_manual_note_var = tk.StringVar(value="")
        self.pf_pending_qty_var = tk.StringVar(value="0")
        self.pf_pending_type_var = tk.StringVar(value="MARKET")

        ttk.Label(top, text="Binance Profile").pack(side="left")
        self.pf_binance_profile_combo = ttk.Combobox(top, textvariable=self.pf_binance_profile_var, values=[], width=22, state="readonly")
        self.pf_binance_profile_combo.pack(side="left", padx=4)
        ttk.Label(top, text="Mode").pack(side="left")
        ttk.Combobox(top, textvariable=self.pf_exec_mode_var, values=["manual", "semi_auto", "full_auto"], width=10, state="readonly").pack(side="left", padx=4)
        ttk.Label(top, text="Quote").pack(side="left")
        ttk.Combobox(top, textvariable=self.pf_quote_var, values=["USDT", "USD", "BTC", "ETH", "BNB"], width=8, state="readonly").pack(side="left", padx=4)
        ttk.Button(top, text="Refresh Profiles", command=self._refresh_binance_profiles).pack(side="left", padx=(4, 8))
        ttk.Button(top, text="Refresh Portfolio", command=self._refresh_portfolio).pack(side="left")
        ttk.Label(top, text="Signal Cooldown (min)").pack(side="left", padx=(12, 0))
        ttk.Entry(top, textvariable=self.pf_cooldown_min_var, width=8).pack(side="left", padx=4)
        ttk.Checkbutton(top, text="Track HOLD signals", variable=self.pf_track_hold_var).pack(side="left", padx=(8, 0))
        ttk.Button(top, text="Import Signals from Live", command=self._import_signals_from_live).pack(side="left", padx=8)
        ttk.Button(top, text="Refresh Ledger", command=self._refresh_ledger_view).pack(side="left")

        pending_frame = ttk.LabelFrame(self.portfolio_tab, text="Pending Recommendations (Review & Approve)", padding=8)
        pending_frame.pack(fill="both", expand=True, padx=8, pady=(0, 8))
        pending_tree_frame, self.pending_tree = self._create_scrolled_tree(
            pending_frame,
            columns=("id", "symbol", "side", "type", "qty", "tf", "conf", "status", "reason"),
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
        ttk.Combobox(pending_btns_row1, textvariable=self.pf_pending_type_var, values=["MARKET", "LIMIT"], width=8, state="readonly").pack(side="left", padx=4)
        ttk.Button(pending_btns_row1, text="Apply to Selected", command=self._apply_pending_edit_to_selected).pack(side="left", padx=6)
        pending_btns_row2 = ttk.Frame(pending_frame)
        pending_btns_row2.pack(fill="x")
        ttk.Button(pending_btns_row2, text="Submit Selected", command=self._submit_selected_pending_orders).pack(side="left")
        ttk.Button(pending_btns_row2, text="Remove Selected", command=self._remove_selected_pending_orders).pack(side="left", padx=6)
        ttk.Button(pending_btns_row2, text="Clear All", command=self._clear_pending_recommendations).pack(side="left")

        manual = ttk.LabelFrame(self.portfolio_tab, text="Manual Ledger Event", padding=8)
        manual.pack(fill="x", padx=8, pady=(0, 8))
        self._labeled_entry(manual, "Asset", self.pf_manual_asset_var)
        self._labeled_combo(manual, "Action", self.pf_manual_action_var, ["BUY", "SELL", "HOLD"], state="readonly")
        self._labeled_combo(manual, "Timeframe", self.pf_manual_tf_var, ["1d", "4h", "8h", "12h"], state="readonly")
        self._labeled_entry(manual, "Price", self.pf_manual_price_var)
        self._labeled_entry(manual, "Qty", self.pf_manual_qty_var)
        self._labeled_entry(manual, "Note", self.pf_manual_note_var)
        ttk.Button(manual, text="Add Manual Event", command=self._add_manual_ledger_event).pack(fill="x", pady=(6, 0))

        orders = ttk.LabelFrame(self.portfolio_tab, text="Open Binance Orders (Cancel via GUI)", padding=8)
        orders.pack(fill="both", expand=True, padx=8, pady=(0, 8))
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
        cols.pack(fill="both", expand=True)
        left = ttk.LabelFrame(cols, text="Current Portfolio (Binance)", padding=6)
        right = ttk.LabelFrame(cols, text="Open Positions (Ledger)", padding=6)
        left.pack(side="left", fill="both", expand=True, padx=(0, 6))
        right.pack(side="left", fill="both", expand=True)

        pf_portfolio_frame, self.pf_portfolio_text = self._create_scrolled_text(left, wrap="none")
        pf_portfolio_frame.pack(fill="both", expand=True)
        self._configure_dashboard_tags(self.pf_portfolio_text)

        pf_open_frame, self.pf_open_positions_text = self._create_scrolled_text(right, wrap="none")
        pf_open_frame.pack(fill="both", expand=True)
        self._configure_dashboard_tags(self.pf_open_positions_text)

        bottom = ttk.LabelFrame(body, text="Trade Ledger (History + Activity Guard)", padding=6)
        bottom.pack(fill="both", expand=True, pady=(8, 0))
        pf_ledger_frame, self.pf_ledger_text = self._create_scrolled_text(bottom, wrap="none")
        pf_ledger_frame.pack(fill="both", expand=True)
        self._configure_dashboard_tags(self.pf_ledger_text)

        self._refresh_binance_profiles()
        self._refresh_ledger_view()
        self._refresh_pending_recommendations_view()

    def _build_research_tab(self) -> None:
        top = ttk.Frame(self.research_tab, padding=8)
        top.pack(fill="x")
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

        rs_frame, self.rs_output = self._create_scrolled_text(self.research_tab, wrap="none")
        rs_frame.pack(fill="both", expand=True, padx=8, pady=8)

    def _build_task_tab(self) -> None:
        top = ttk.Frame(self.task_tab, padding=8)
        top.pack(fill="x")
        body = ttk.Frame(self.task_tab, padding=8)
        body.pack(fill="both", expand=True)

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
        left.pack(side="left", fill="both", expand=True, padx=(0, 6))
        right.pack(side="left", fill="both", expand=True)

        self.running_list = tk.Listbox(left, height=12)
        self.running_list.pack(fill="both", expand=True)
        self.queued_list = tk.Listbox(right, height=12)
        self.queued_list.pack(fill="both", expand=True)

        task_out_frame, self.task_tab_output = self._create_scrolled_text(body, height=4, wrap="none")
        task_out_frame.pack(fill="x", pady=(8, 0))
        task_term_frame, self.task_terminal = self._create_scrolled_text(
            body,
            height=12,
            wrap="none",
            bg="#101315",
            fg="#9CF5C6",
            insertbackground="#9CF5C6",
        )
        task_term_frame.pack(fill="both", expand=True, pady=(6, 0))
        self.task_terminal.insert("1.0", "STRATA Task Terminal\n")
        self._refresh_task_tab()
        self._schedule_task_tab_refresh()

    def _build_settings_tab(self) -> None:
        frame = ttk.Frame(self.settings_tab, padding=8)
        frame.pack(fill="both", expand=True)

        self.display_currency_var = tk.StringVar(value=self.state.get("display_currency", "USD"))
        self._labeled_combo(frame, "Display Currency", self.display_currency_var, ["USD", "AUD", "EUR", "GBP", "CAD", "JPY", "NZD", "SGD", "HKD", "CHF"])
        self.parallel_mode_var = tk.BooleanVar(value=bool(self.state.get("parallel_mode_enabled", False)))
        self.parallel_jobs_var = tk.StringVar(value=str(self.state.get("parallel_max_jobs", 2)))
        ttk.Checkbutton(
            frame,
            text="Advanced Mode: Parallel jobs (experimental)",
            variable=self.parallel_mode_var,
            command=lambda: self._update_run_controls_and_status(),
        ).pack(anchor="w", pady=(8, 2))
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

        settings_frame, self.settings_output = self._create_scrolled_text(frame, height=8, wrap="none")
        settings_frame.pack(fill="both", expand=True, pady=(8, 0))
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
        return frame, tree

    def _configure_dashboard_tags(self, widget: tk.Text) -> None:
        # Section/header emphasis
        widget.tag_configure("hdr", foreground="#7FDBFF")
        widget.tag_configure("subhdr", foreground="#FFD166")
        # Action emphasis
        widget.tag_configure("buy", foreground="#2ECC71")
        widget.tag_configure("hold", foreground="#F39C12")
        widget.tag_configure("sell", foreground="#E74C3C")
        # Result emphasis
        widget.tag_configure("okline", foreground="#2ECC71")
        widget.tag_configure("errline", foreground="#FF6B6B")

    def _apply_color_tags(self, widget: tk.Text) -> None:
        if widget is None:
            return
        # Clear prior highlights
        for tag in ("hdr", "subhdr", "buy", "hold", "sell", "okline", "errline"):
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
        return p

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
                res = bridge.run_live_panel(asdict(p))
                log = (res.get("log", "") or "").strip()
                if log:
                    self._append_task_terminal_from_worker(f"LOG [{p.name}] {log[:4000]}")
                if not res.get("ok"):
                    chunks.append(f"=== {p.name} ===\nERROR: {res.get('error', 'unknown')}\n")
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
            res = bridge.run_live_panel(asdict(panel))
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

        cfg = {
            "market": self.bt_market.get(),
            "timeframe": self.bt_tf.get(),
            "months": _to_int(self.bt_months.get(), 12),
            "top_n": _to_int(self.bt_topn.get(), 20),
            "quote_currency": self.bt_quote.get(),
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

    def _start_run_ai_analysis(self) -> None:
        self._start_run_ai_analysis_internal(source_override=None, force_no_confirm=False)

    def _start_run_ai_analysis_internal(self, source_override: Optional[str], force_no_confirm: bool) -> None:
        task_name = "AI Analysis"
        task_id = self._set_busy(True, task_name)
        self._append_task_terminal("START AI Analysis")
        self.ai_output.delete("1.0", tk.END)
        original_source = self.ai_source.get().strip()
        if source_override:
            self.ai_source.set(source_override)
        text = self._resolve_ai_source_text()
        if source_override:
            self.ai_source.set(original_source)
        if not text.strip():
            self.ai_output.insert("1.0", "No source text available.")
            self._append_task_terminal("DONE AI Analysis (no source text)")
            self._finish_task(task_id, task_name=task_name)
            return
        self.ai_last_source_text = text
        dt = self.ai_datetime.get().strip() or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        prompt = self._build_ai_prompt(text, dt)
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
            self.ai_output.insert("1.0", "AI analysis failed or returned empty response.\n\nPrompt preview:\n\n")
            self.ai_output.insert("end", (res.get("prompt", "") or "")[:4000])
            self._append_task_terminal("DONE AI Analysis (failed/empty)")
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
            return
        dt = self.ai_datetime.get().strip() or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        prompt = self._build_ai_prompt(source_text, dt)
        self.ai_output.delete("1.0", tk.END)
        self.ai_output.insert("1.0", "PROMPT PREVIEW\n" + ("=" * 60) + "\n" + prompt)

    def _extract_trade_recommendations_from_ai_text(self, text: str) -> List[Dict[str, Any]]:
        recs: List[Dict[str, Any]] = []
        if not text.strip():
            return recs
        # Preferred path: structured JSON payload at bottom of AI response.
        structured = self._extract_structured_trade_plan_from_ai_text(text)
        if structured:
            return structured
        quote_default = "USDT"
        try:
            quote_default = (self.quote_var.get().strip().upper() or "USDT")
        except Exception:
            pass
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

        quote_default = "USDT"
        try:
            quote_default = (self.quote_var.get().strip().upper() or "USDT")
        except Exception:
            pass
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
            qty = 0.0
            for qk in ("quantity", "qty", "size_qty"):
                try:
                    qv = float(tr.get(qk, 0.0) or 0.0)
                except Exception:
                    qv = 0.0
                if qv > 0:
                    qty = qv
                    break

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
        self.pending_recommendations.extend(recs)
        self._refresh_pending_recommendations_view()
        self._append_task_terminal(f"Staged {len(recs)} AI recommendation(s) into pending orders.")
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
                        "note": "AI interpretation signal",
                    },
                    cooldown_minutes=cooldown,
                    allow_duplicate=False,
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
            messagebox.showinfo("AI Recommendations", f"Staged {len(recs)} recommendation(s).")
        return len(recs)

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

    def _submit_selected_pending_orders(self) -> None:
        ids = self._selected_pending_ids()
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
                blocked += 1
                continue
            out = self.bridge.submit_binance_order(
                symbol=symbol,
                side=side,
                order_type=order_type,
                quantity=qty,
                profile_name=profile,
            )
            if not out.get("ok"):
                rec["status"] = "FAILED"
                rec["reason"] = str(out.get("error", "submit failed"))
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
            self.bridge.record_signal_event(
                {
                    "market": "crypto",
                    "timeframe": str(rec.get("timeframe", "1d")),
                    "panel": "ai_trade_queue",
                    "asset": self._base_asset_from_symbol(symbol),
                    "action": side,
                    "qty": qty,
                    "note": f"Submitted to Binance ({symbol})",
                    "is_execution": True,
                },
                cooldown_minutes=cooldown,
                allow_duplicate=False,
            )
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
                    "note": "Imported from latest live dashboard",
                },
                cooldown_minutes=cooldown,
                allow_duplicate=False,
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
                "note": note,
                "is_execution": bool(qty > 0),
            },
            cooldown_minutes=cooldown,
            allow_duplicate=False,
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
        open_positions = ledger.get("open_positions", {}) if isinstance(ledger, dict) else {}
        guard = ledger.get("activity_guard", {}) if isinstance(ledger, dict) else {}

        if hasattr(self, "pf_open_positions_text"):
            self.pf_open_positions_text.delete("1.0", tk.END)
            if isinstance(open_positions, dict) and open_positions:
                op_df = pd.DataFrame(list(open_positions.values()))
                self.pf_open_positions_text.insert("1.0", op_df.to_string(index=False))
            else:
                self.pf_open_positions_text.insert("1.0", "No open positions tracked.")
            self._apply_color_tags(self.pf_open_positions_text)

        if hasattr(self, "pf_ledger_text"):
            self.pf_ledger_text.delete("1.0", tk.END)
            lines = []
            lines.append(f"Entries: {len(entries) if isinstance(entries, list) else 0}")
            lines.append(f"Guard Keys: {len(guard) if isinstance(guard, dict) else 0}")
            lines.append("")
            if isinstance(entries, list) and entries:
                df = pd.DataFrame(entries[-200:])
                lines.append(df.to_string(index=False))
            else:
                lines.append("No ledger entries yet.")
            self.pf_ledger_text.insert("1.0", "\n".join(lines))
            self._apply_color_tags(self.pf_ledger_text)

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
        self.state["auto_refresh_seconds"] = int(self.refresh_secs_var.get().strip() or "120")
        mode_var = getattr(self, "parallel_mode_var", None)
        jobs_var = getattr(self, "parallel_jobs_var", None)
        self.state["parallel_mode_enabled"] = bool(mode_var.get()) if mode_var is not None else False
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
