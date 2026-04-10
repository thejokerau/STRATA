import threading
import tkinter as tk
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from tkinter import ttk, messagebox
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

        self.live_panels: List[LivePanelConfig] = []
        for p in self.state.get("live_panels", []):
            if isinstance(p, dict):
                self.live_panels.append(LivePanelConfig(**{k: p.get(k) for k in asdict(LivePanelConfig("x")).keys()}))
        if not self.live_panels:
            self.live_panels = [LivePanelConfig(name="Crypto 1d", market="crypto", timeframe="1d", quote_currency="USD", top_n=20)]

        self._build_ui()
        self._refresh_panel_list()

    def _build_ui(self) -> None:
        self.nb = ttk.Notebook(self.root)
        self.nb.pack(fill="both", expand=True)

        self.live_tab = ttk.Frame(self.nb)
        self.backtest_tab = ttk.Frame(self.nb)
        self.ai_tab = ttk.Frame(self.nb)
        self.research_tab = ttk.Frame(self.nb)
        self.settings_tab = ttk.Frame(self.nb)

        self.nb.add(self.live_tab, text="Live Dashboard")
        self.nb.add(self.backtest_tab, text="Backtest")
        self.nb.add(self.ai_tab, text="AI Analysis")
        self.nb.add(self.research_tab, text="Auto-Research")
        self.nb.add(self.settings_tab, text="Settings")

        self._build_live_tab()
        self._build_backtest_tab()
        self._build_ai_tab()
        self._build_research_tab()
        self._build_settings_tab()

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
        self.country_var = tk.StringVar(value="2")
        self.panel_name_var = tk.StringVar(value="Panel")

        form = ttk.LabelFrame(left, text="Panel Config", padding=8)
        form.pack(fill="x")
        self._labeled_entry(form, "Name", self.panel_name_var)
        self._labeled_combo(form, "Market", self.market_var, ["crypto", "traditional"])
        self._labeled_combo(form, "Timeframe", self.timeframe_var, ["1d", "4h", "8h", "12h"])
        self._labeled_combo(form, "Quote (crypto)", self.quote_var, ["USD", "USDT", "BTC", "ETH", "BNB"])
        self._labeled_combo(form, "Top N", self.topn_var, ["10", "20", "50", "100"])
        self._labeled_combo(form, "Country (trad)", self.country_var, ["1", "2", "3", "4", "5"])

        btns = ttk.Frame(left)
        btns.pack(fill="x", pady=8)
        ttk.Button(btns, text="Add Panel", command=self._add_panel).pack(fill="x")
        ttk.Button(btns, text="Update Panel", command=self._update_panel).pack(fill="x", pady=2)
        ttk.Button(btns, text="Remove Panel", command=self._remove_panel).pack(fill="x")
        ttk.Button(btns, text="Run Selected", command=self._run_selected_panel).pack(fill="x", pady=(8, 2))
        ttk.Button(btns, text="Run All Panels", command=self._run_all_panels).pack(fill="x")

        auto = ttk.LabelFrame(left, text="Auto Refresh", padding=8)
        auto.pack(fill="x", pady=8)
        self.refresh_secs_var = tk.StringVar(value=str(self.state.get("auto_refresh_seconds", 120)))
        self._labeled_entry(auto, "Seconds", self.refresh_secs_var)
        ttk.Button(auto, text="Start Auto", command=self._start_auto_refresh).pack(fill="x")
        ttk.Button(auto, text="Stop Auto", command=self._stop_auto_refresh).pack(fill="x", pady=2)

        self.live_output = tk.Text(right, wrap="none")
        self.live_output.pack(fill="both", expand=True)

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
        self.bt_country = tk.StringVar(value="2")
        self.bt_initial = tk.StringVar(value="10000")

        self._labeled_combo(top, "Market", self.bt_market, ["crypto", "traditional"])
        self._labeled_combo(top, "Timeframe", self.bt_tf, ["1d", "4h", "8h", "12h"])
        self._labeled_combo(top, "Months", self.bt_months, ["1", "3", "6", "12", "18", "24"])
        self._labeled_combo(top, "Top N", self.bt_topn, ["10", "20", "50", "100"])
        self._labeled_combo(top, "Quote (crypto)", self.bt_quote, ["USD", "USDT", "BTC", "ETH", "BNB"])
        self._labeled_combo(top, "Country (trad)", self.bt_country, ["1", "2", "3", "4", "5"])
        self._labeled_entry(top, "Initial USD", self.bt_initial)
        ttk.Button(top, text="Run Backtest", command=self._run_backtest).pack(side="left", padx=8)

        self.bt_summary = tk.Text(out, height=10, wrap="word")
        self.bt_summary.pack(fill="x")
        self.bt_trades = tk.Text(out, wrap="none")
        self.bt_trades.pack(fill="both", expand=True, pady=(8, 0))

    def _build_ai_tab(self) -> None:
        top = ttk.Frame(self.ai_tab, padding=8)
        top.pack(fill="x")
        body = ttk.Frame(self.ai_tab, padding=8)
        body.pack(fill="both", expand=True)

        self.ai_source = tk.StringVar(value="live")
        self.ai_datetime = tk.StringVar(value=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        ttk.Label(top, text="Date/Time Context").pack(side="left")
        ttk.Entry(top, textvariable=self.ai_datetime, width=24).pack(side="left", padx=4)
        ttk.Label(top, text="Source").pack(side="left", padx=(12, 0))
        ttk.Combobox(top, textvariable=self.ai_source, values=["live", "backtest", "paste"], width=12, state="readonly").pack(side="left", padx=4)
        ttk.Button(top, text="Run AI Analysis", command=self._run_ai_analysis).pack(side="left", padx=8)

        self.ai_input = tk.Text(body, height=14, wrap="word")
        self.ai_input.pack(fill="x")
        self.ai_output = tk.Text(body, wrap="word")
        self.ai_output.pack(fill="both", expand=True, pady=(8, 0))

    def _build_research_tab(self) -> None:
        top = ttk.Frame(self.research_tab, padding=8)
        top.pack(fill="x")
        self.rs_market_scope = tk.StringVar(value="both")
        self.rs_quote = tk.StringVar(value="USD")
        self.rs_country = tk.StringVar(value="2")
        self.rs_trials = tk.StringVar(value="10")
        self.rs_jobs = tk.StringVar(value="4")

        self._labeled_combo(top, "Scope", self.rs_market_scope, ["crypto", "traditional", "both"])
        self._labeled_combo(top, "Quote (crypto)", self.rs_quote, ["USD", "USDT", "BTC", "ETH", "BNB"])
        self._labeled_combo(top, "Country (trad)", self.rs_country, ["1", "2", "3", "4", "5"])
        self._labeled_entry(top, "Trials", self.rs_trials)
        self._labeled_entry(top, "Jobs", self.rs_jobs)
        ttk.Button(top, text="Run Standard", command=self._run_standard_research).pack(side="left", padx=8)
        ttk.Button(top, text="Run Comprehensive", command=self._run_comprehensive_research).pack(side="left")

        self.rs_output = tk.Text(self.research_tab, wrap="word")
        self.rs_output.pack(fill="both", expand=True, padx=8, pady=8)

    def _build_settings_tab(self) -> None:
        frame = ttk.Frame(self.settings_tab, padding=8)
        frame.pack(fill="both", expand=True)

        self.display_currency_var = tk.StringVar(value=self.state.get("display_currency", "USD"))
        self._labeled_combo(frame, "Display Currency", self.display_currency_var, ["USD", "AUD", "EUR", "GBP", "CAD", "JPY", "NZD", "SGD", "HKD", "CHF"])

        dash_frame = ttk.LabelFrame(frame, text="User Dashboard Profiles", padding=8)
        dash_frame.pack(fill="x", pady=8)
        self.dashboard_name_var = tk.StringVar(value="default")
        self._labeled_entry(dash_frame, "Profile Name", self.dashboard_name_var)
        ttk.Button(dash_frame, text="Save Current Live Panels", command=self._save_dashboard_profile).pack(fill="x", pady=2)
        ttk.Button(dash_frame, text="Load Profile", command=self._load_dashboard_profile).pack(fill="x", pady=2)
        ttk.Button(dash_frame, text="Delete Profile", command=self._delete_dashboard_profile).pack(fill="x", pady=2)

        ttk.Button(frame, text="Open AI Provider Settings (CLI)", command=self._show_ai_settings_hint).pack(fill="x", pady=4)
        ttk.Button(frame, text="Save Settings", command=self._persist_state).pack(fill="x")

        self.settings_output = tk.Text(frame, height=8, wrap="word")
        self.settings_output.pack(fill="both", expand=True, pady=(8, 0))
        self._append_settings("Settings are stored under %USERPROFILE%\\.ctmt\\gui\\gui_state.json")

    def _labeled_entry(self, parent, label: str, var: tk.StringVar) -> None:
        row = ttk.Frame(parent)
        row.pack(fill="x", pady=2)
        ttk.Label(row, text=label, width=16).pack(side="left")
        ttk.Entry(row, textvariable=var, width=18).pack(side="left")

    def _labeled_combo(self, parent, label: str, var: tk.StringVar, values: List[str]) -> None:
        row = ttk.Frame(parent)
        row.pack(fill="x", pady=2)
        ttk.Label(row, text=label, width=16).pack(side="left")
        ttk.Combobox(row, textvariable=var, values=values, width=16, state="readonly").pack(side="left")

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
        self.country_var.set(p.country)

    def _panel_from_form(self) -> LivePanelConfig:
        return LivePanelConfig(
            name=self.panel_name_var.get().strip() or "Panel",
            market=self.market_var.get().strip() or "crypto",
            timeframe=self.timeframe_var.get().strip() or "1d",
            quote_currency=self.quote_var.get().strip() or "USD",
            top_n=max(1, int(self.topn_var.get().strip() or "20")),
            country=self.country_var.get().strip() or "2",
            display_currency=self.display_currency_var.get().strip() or "USD",
        )

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
        if self.busy:
            return
        self.busy = True
        self.live_output.delete("1.0", tk.END)

        def worker():
            chunks = []
            for p in self.live_panels:
                res = self.bridge.run_live_panel(asdict(p))
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
            self.root.after(0, lambda: self._finish_live_output(out))

        threading.Thread(target=worker, daemon=True).start()

    def _run_live_job(self, panel: LivePanelConfig, selected_only: bool = False) -> None:
        if self.busy:
            return
        self.busy = True
        if selected_only:
            self.live_output.delete("1.0", tk.END)

        def worker():
            res = self.bridge.run_live_panel(asdict(panel))
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
            self.root.after(0, lambda: self._finish_live_output(out))

        threading.Thread(target=worker, daemon=True).start()

    def _finish_live_output(self, text: str) -> None:
        self.live_output.delete("1.0", tk.END)
        self.live_output.insert("1.0", text)
        self.busy = False
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
        if self.busy:
            return
        self.busy = True
        self.bt_summary.delete("1.0", tk.END)
        self.bt_trades.delete("1.0", tk.END)

        cfg = {
            "market": self.bt_market.get(),
            "timeframe": self.bt_tf.get(),
            "months": int(self.bt_months.get() or "12"),
            "top_n": int(self.bt_topn.get() or "20"),
            "quote_currency": self.bt_quote.get(),
            "country": self.bt_country.get(),
            "initial_capital": float(self.bt_initial.get() or "10000"),
            "display_currency": self.display_currency_var.get() or "USD",
        }

        def worker():
            res = self.bridge.run_backtest(cfg)
            self.root.after(0, lambda: self._finish_backtest_output(res))

        threading.Thread(target=worker, daemon=True).start()

    def _finish_backtest_output(self, res: Dict[str, Any]) -> None:
        if not res.get("ok"):
            self.bt_summary.insert("1.0", f"ERROR: {res.get('error', 'unknown')}")
        else:
            self.bt_summary.insert("1.0", res.get("summary_text", ""))
            self.bt_trades.insert("1.0", res.get("trades_text", ""))
        self.busy = False

    def _run_ai_analysis(self) -> None:
        if self.busy:
            return
        self.busy = True
        self.ai_output.delete("1.0", tk.END)
        source = self.ai_source.get().strip().lower()
        if source == "live":
            path = self.repo_root / "experiments" / "live_snapshots" / "latest_live_dashboard.txt"
            text = path.read_text(encoding="utf-8") if path.exists() else ""
        elif source == "backtest":
            path = self.repo_root / "experiments" / "backtest_snapshots" / "latest_backtest.txt"
            text = path.read_text(encoding="utf-8") if path.exists() else ""
        else:
            text = self.ai_input.get("1.0", tk.END).strip()
        if not text.strip():
            self.ai_output.insert("1.0", "No source text available.")
            self.busy = False
            return
        dt = self.ai_datetime.get().strip() or datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        def worker():
            res = self.bridge.run_ai_analysis(text, dt)
            self.root.after(0, lambda: self._finish_ai_output(res))

        threading.Thread(target=worker, daemon=True).start()

    def _finish_ai_output(self, res: Dict[str, Any]) -> None:
        if not res.get("ok"):
            self.ai_output.insert("1.0", "AI analysis failed or returned empty response.\n\nPrompt preview:\n\n")
            self.ai_output.insert("end", (res.get("prompt", "") or "")[:4000])
        else:
            self.ai_output.insert("1.0", res.get("response", ""))
        self.busy = False

    def _run_standard_research(self) -> None:
        self._run_research_job(standard=True)

    def _run_comprehensive_research(self) -> None:
        self._run_research_job(standard=False)

    def _run_research_job(self, standard: bool) -> None:
        if self.busy:
            return
        self.busy = True
        self.rs_output.delete("1.0", tk.END)

        def worker():
            if standard:
                out = self.bridge.run_standard_research()
            else:
                scenarios = self._build_comprehensive_scenarios_from_form()
                out = self.bridge.run_comprehensive_research(
                    scenarios=scenarios,
                    optuna_trials=max(1, int(self.rs_trials.get() or "10")),
                    optuna_jobs=max(1, int(self.rs_jobs.get() or "4")),
                )
            self.root.after(0, lambda: self._finish_research_output(out))

        threading.Thread(target=worker, daemon=True).start()

    def _build_comprehensive_scenarios_from_form(self) -> List[Dict[str, Any]]:
        scope = self.rs_market_scope.get().strip().lower()
        include_crypto = scope in ("crypto", "both")
        include_trad = scope in ("traditional", "both")
        quote = self.rs_quote.get().strip().upper() or "USD"
        country = self.rs_country.get().strip() or "2"
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

    def _finish_research_output(self, out: Dict[str, Any]) -> None:
        lines = [f"Command: {out.get('cmd', '')}", f"Return code: {out.get('returncode', '')}", ""]
        if out.get("stdout"):
            lines += ["STDOUT:", out["stdout"], ""]
        if out.get("stderr"):
            lines += ["STDERR:", out["stderr"], ""]
        self.rs_output.insert("1.0", "\n".join(lines))
        self.busy = False

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
        self.state["live_panels"] = [asdict(p) for p in self.live_panels]
        save_state(self.state)


def run_gui() -> None:
    root = tk.Tk()
    repo_root = Path(__file__).resolve().parents[1]
    app = CTMTGuiApp(root, repo_root=repo_root)
    root.protocol("WM_DELETE_WINDOW", lambda: (app._persist_state(), root.destroy()))
    root.mainloop()


if __name__ == "__main__":
    run_gui()

