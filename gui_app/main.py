import threading
import tkinter as tk
from collections import deque
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
import re
from tkinter import ttk, messagebox, filedialog
from typing import Any, Dict, List, Optional

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


class CTMTGuiApp:
    def __init__(self, root: tk.Tk, repo_root: Path) -> None:
        self.root = root
        self.repo_root = repo_root
        self.root.title("CTMT GUI (gui-nightly)")
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
        self.task_monitor_window: Optional[tk.Toplevel] = None
        self.task_monitor_text: Optional[tk.Text] = None
        self.task_monitor_job: Optional[str] = None
        self.task_tab_job: Optional[str] = None

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
        topbar = ttk.Frame(self.root, padding=(8, 4))
        topbar.pack(side="top", fill="x")
        ttk.Button(topbar, text="Tasks", command=self._open_task_monitor_tab).pack(side="right")

        self.nb = ttk.Notebook(self.root)
        self.nb.pack(fill="both", expand=True)

        self.live_tab = ttk.Frame(self.nb)
        self.backtest_tab = ttk.Frame(self.nb)
        self.ai_tab = ttk.Frame(self.nb)
        self.research_tab = ttk.Frame(self.nb)
        self.task_tab = ttk.Frame(self.nb)
        self.settings_tab = ttk.Frame(self.nb)

        self.nb.add(self.live_tab, text="Live Dashboard")
        self.nb.add(self.backtest_tab, text="Backtest")
        self.nb.add(self.ai_tab, text="AI Analysis")
        self.nb.add(self.research_tab, text="Auto-Research")
        self.nb.add(self.task_tab, text="Task Monitor")
        self.nb.add(self.settings_tab, text="Settings")

        self._build_live_tab()
        self._build_backtest_tab()
        self._build_ai_tab()
        self._build_research_tab()
        self._build_task_tab()
        self._build_settings_tab()
        self._build_status_bar()

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
        self.panel_list = tk.Listbox(left, width=40, height=18)
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

        self.live_output = tk.Text(right, wrap="none")
        self.live_output.pack(fill="both", expand=True)
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

        self.bt_summary = tk.Text(out, height=10, wrap="word")
        self.bt_summary.pack(fill="x")
        self._configure_dashboard_tags(self.bt_summary)
        self.bt_trades = tk.Text(out, wrap="none")
        self.bt_trades.pack(fill="both", expand=True, pady=(8, 0))
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
        self.ai_backtest_path = tk.StringVar(
            value=str(self.repo_root / "experiments" / "backtest_snapshots" / "latest_backtest.txt")
        )
        ttk.Label(top, text="Date/Time Context").pack(side="left")
        ttk.Entry(top, textvariable=self.ai_datetime, width=24).pack(side="left", padx=4)
        ttk.Label(top, text="Source").pack(side="left", padx=(12, 0))
        ttk.Combobox(
            top,
            textvariable=self.ai_source,
            values=["live", "backtest_latest", "backtest_file", "paste"],
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
        self.btn_run_ai = ttk.Button(top, text="Run AI Analysis", command=self._run_ai_analysis)
        self.btn_run_ai.pack(side="left", padx=8)
        ttk.Button(top, text="Preview Prompt", command=self._preview_ai_prompt).pack(side="left")

        self.ai_input = tk.Text(body, height=10, wrap="word")
        self.ai_input.pack(fill="x")
        ai_file_row = ttk.Frame(body)
        ai_file_row.pack(fill="x", pady=(6, 0))
        ttk.Label(ai_file_row, text="Backtest file").pack(side="left")
        ttk.Entry(ai_file_row, textvariable=self.ai_backtest_path, width=90).pack(side="left", padx=6, fill="x", expand=True)
        ttk.Button(ai_file_row, text="Browse...", command=self._browse_ai_backtest_file).pack(side="left")
        ttk.Label(body, text="Custom prompt (used when Prompt=custom_prompt)").pack(anchor="w", pady=(8, 0))
        self.ai_custom_prompt = tk.Text(body, height=8, wrap="word")
        self.ai_custom_prompt.pack(fill="x")
        follow_row = ttk.Frame(body)
        follow_row.pack(fill="x", pady=(8, 0))
        self.ai_followup_var = tk.StringVar(value="")
        ttk.Label(follow_row, text="Follow-up").pack(side="left")
        ttk.Entry(follow_row, textvariable=self.ai_followup_var, width=100).pack(side="left", padx=6, fill="x", expand=True)
        ttk.Button(follow_row, text="Send Follow-up", command=self._run_ai_followup).pack(side="left")
        self.ai_output = tk.Text(body, wrap="word")
        self.ai_output.pack(fill="both", expand=True, pady=(8, 0))

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

        self.rs_output = tk.Text(self.research_tab, wrap="word")
        self.rs_output.pack(fill="both", expand=True, padx=8, pady=8)

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

        self.task_tab_output = tk.Text(body, height=4, wrap="word")
        self.task_tab_output.pack(fill="x", pady=(8, 0))
        self.task_terminal = tk.Text(
            body,
            height=12,
            wrap="none",
            bg="#101315",
            fg="#9CF5C6",
            insertbackground="#9CF5C6",
        )
        self.task_terminal.pack(fill="both", expand=True, pady=(6, 0))
        self.task_terminal.insert("1.0", "CTMT Task Terminal\n")
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

        dash_frame = ttk.LabelFrame(frame, text="User Dashboard Profiles", padding=8)
        dash_frame.pack(fill="x", pady=8)
        self.dashboard_name_var = tk.StringVar(value="default")
        self._labeled_entry(dash_frame, "Profile Name", self.dashboard_name_var)
        ttk.Button(dash_frame, text="Save Current Live Panels", command=self._save_dashboard_profile).pack(fill="x", pady=2)
        ttk.Button(dash_frame, text="Load Profile", command=self._load_dashboard_profile).pack(fill="x", pady=2)
        ttk.Button(dash_frame, text="Delete Profile", command=self._delete_dashboard_profile).pack(fill="x", pady=2)

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

        ttk.Button(frame, text="Save Settings", command=self._persist_state).pack(fill="x")

        self.settings_output = tk.Text(frame, height=8, wrap="word")
        self.settings_output.pack(fill="both", expand=True, pady=(8, 0))
        self._append_settings("Settings are stored under %USERPROFILE%\\.ctmt\\gui\\gui_state.json")
        self.ai_profile_combo.bind("<<ComboboxSelected>>", lambda _e: self._load_ai_profile_into_form())
        self._refresh_ai_profiles()

    def _labeled_entry(self, parent, label: str, var: tk.StringVar) -> None:
        row = ttk.Frame(parent)
        row.pack(fill="x", pady=2)
        ttk.Label(row, text=label, width=16).pack(side="left")
        ttk.Entry(row, textvariable=var, width=18).pack(side="left")

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
        text = tk.Text(win, wrap="word")
        text.pack(fill="both", expand=True, padx=8, pady=8)
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
        self.live_output.delete("1.0", tk.END)

        def worker():
            bridge = self._bridge_for_task()
            chunks = []
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
                chunks.append("\n".join(text) + "\n")

            out = "\n".join(chunks)
            self.root.after(0, lambda: self._finish_live_output(out, task_name, task_id))

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
        if selected_only:
            self.live_output.delete("1.0", tk.END)

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

    def _finish_live_output(self, text: str, task_name: str = "Live Dashboard", task_id: Optional[int] = None) -> None:
        self.live_output.delete("1.0", tk.END)
        self.live_output.insert("1.0", text)
        self._apply_color_tags(self.live_output)
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
        task_name = "AI Analysis"
        task_id = self._set_busy(True, task_name)
        self._append_task_terminal("START AI Analysis")
        self.ai_output.delete("1.0", tk.END)
        text = self._resolve_ai_source_text()
        if not text.strip():
            self.ai_output.insert("1.0", "No source text available.")
            self._append_task_terminal("DONE AI Analysis (no source text)")
            self._finish_task(task_id, task_name=task_name)
            return
        self.ai_last_source_text = text
        dt = self.ai_datetime.get().strip() or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        prompt = self._build_ai_prompt(text, dt)
        if self.ai_require_confirm.get():
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

    def _show_ai_settings_hint(self) -> None:
        messagebox.showinfo(
            "AI Provider Settings",
            "AI provider profiles/keys are managed via nightly CLI menu option:\n\n"
            "5. AI Provider Settings\n\n"
            "This GUI uses the active profile from that configuration.",
        )

    def _append_settings(self, text: str) -> None:
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
    app = CTMTGuiApp(root, repo_root=repo_root)
    def _shutdown():
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
