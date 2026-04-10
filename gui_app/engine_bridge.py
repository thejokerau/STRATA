import importlib.util
import io
import json
import hashlib
import hmac
import os
import subprocess
import sys
import threading
import time
import urllib.parse
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

from .binance_store import (
    default_trade_ledger,
    load_binance_preferences,
    load_binance_secrets,
    load_trade_ledger,
    save_binance_preferences,
    save_binance_secrets,
    save_trade_ledger,
)


class EngineBridge:
    def __init__(self, repo_root: Path) -> None:
        self.repo_root = repo_root
        self.nightly_path = repo_root / "nightly" / "BTC-beta.py"
        self.scripts_dir = repo_root / "scripts"
        self._lock = threading.Lock()
        self.mod = self._load_nightly_module()
        self.mod.ensure_table()

    def _load_nightly_module(self):
        spec = importlib.util.spec_from_file_location("ctmt_nightly_gui", str(self.nightly_path))
        if spec is None or spec.loader is None:
            raise RuntimeError(f"Failed to load nightly module from {self.nightly_path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def _assets_for_live(self, cfg: Dict[str, Any]) -> List[str]:
        market = str(cfg.get("market", "crypto")).lower()
        top_n = int(cfg.get("top_n", 20))
        if market == "crypto":
            quote = str(cfg.get("quote_currency", "USD")).upper()
            tickers = self.mod.fetch_top_coins(top_n, quote_currency=quote)
            return self.mod.filter_crypto_tickers_by_binance(tickers)
        country = str(cfg.get("country", "2"))
        universe = list(self.mod.TRADITIONAL_TOP.get(country, self.mod.TRADITIONAL_TOP["2"]))
        return universe[:max(1, top_n)]

    def run_live_panel(self, cfg: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            stream = io.StringIO()
            with redirect_stdout(stream), redirect_stderr(stream):
                out = self._run_live_panel_impl(cfg)
            out["log"] = stream.getvalue()
            return out

    def _run_live_panel_impl(self, cfg: Dict[str, Any]) -> Dict[str, Any]:
        is_crypto = str(cfg.get("market", "crypto")).lower() == "crypto"
        timeframe = str(cfg.get("timeframe", "1d"))
        display_currency = str(cfg.get("display_currency", "USD")).upper()
        tickers = self._assets_for_live(cfg)
        fetch_years = 2.2
        raw_data: Dict[str, pd.DataFrame] = {}
        for t in tickers:
            df = self.mod.fetch_with_cache(t, timeframe, is_crypto=is_crypto, years=fetch_years)
            if df is not None and len(df) >= 60:
                raw_data[t] = df
        if not raw_data:
            return {"ok": False, "error": "No usable data returned."}

        tuned = self.mod.TunedParams()
        enriched = self.mod.build_indicator_cache(raw_data, timeframe, max_workers=1, verbose=False)
        if not enriched:
            return {"ok": False, "error": "No indicator-ready data."}

        table, risk = self.mod.build_live_tables(enriched, is_crypto, tuned, timeframe)
        if table.empty:
            return {"ok": False, "error": "No scored assets."}
        notes = self.mod.build_portfolio_insights(enriched, table)

        table_print = table.drop(columns=["_Ticker"], errors="ignore").copy()
        fx = self.mod.get_usd_to_currency_rate(display_currency)
        if display_currency != "USD":
            table_print[f"Price ({display_currency})"] = (pd.to_numeric(table_print["Price"], errors="coerce") * fx).round(4)
            table_print[f"Fib Support ({display_currency})"] = (pd.to_numeric(table_print["Fib Support"], errors="coerce") * fx).round(4)
            table_print[f"Fib Resistance ({display_currency})"] = (pd.to_numeric(table_print["Fib Resistance"], errors="coerce") * fx).round(4)

        return {
            "ok": True,
            "table_text": table_print.to_string(),
            "risk_text": risk.to_string(),
            "notes": notes,
            "loaded_assets": len(raw_data),
            "requested_assets": len(tickers),
            "timeframe": timeframe,
            "market": "Crypto" if is_crypto else "Traditional",
        }

    def run_backtest(self, cfg: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            stream = io.StringIO()
            with redirect_stdout(stream), redirect_stderr(stream):
                out = self._run_backtest_impl(cfg)
            out["log"] = stream.getvalue()
            return out

    def _run_backtest_impl(self, cfg: Dict[str, Any]) -> Dict[str, Any]:
        is_crypto = str(cfg.get("market", "crypto")).lower() == "crypto"
        timeframe = str(cfg.get("timeframe", "1d"))
        months = int(cfg.get("months", 12))
        top_n = int(cfg.get("top_n", 20))
        quote = str(cfg.get("quote_currency", "USD")).upper()
        display_currency = str(cfg.get("display_currency", "USD")).upper()
        country = str(cfg.get("country", "2"))

        if is_crypto:
            tickers = self.mod.filter_crypto_tickers_by_binance(self.mod.fetch_top_coins(top_n, quote_currency=quote))
        else:
            tickers = list(self.mod.TRADITIONAL_TOP.get(country, self.mod.TRADITIONAL_TOP["2"]))[:max(1, top_n)]

        fetch_years = max(2.2, (months / 12.0) + 0.25)
        raw_data: Dict[str, pd.DataFrame] = {}
        for t in tickers:
            df = self.mod.fetch_with_cache(t, timeframe, is_crypto=is_crypto, years=fetch_years)
            if df is not None and len(df) >= 60:
                raw_data[t] = df
        if not raw_data:
            return {"ok": False, "error": "No usable data."}

        tuned = self.mod.TunedParams(
            position_size=float(cfg.get("position_size", 0.30)),
            atr_multiplier=float(cfg.get("atr_multiplier", 2.2)),
            adx_threshold=float(cfg.get("adx_threshold", 25.0)),
            cmf_threshold=float(cfg.get("cmf_threshold", 0.02)),
            obv_slope_threshold=float(cfg.get("obv_slope_threshold", 0.0)),
            buy_threshold=int(cfg.get("buy_threshold", 2)),
            sell_threshold=int(cfg.get("sell_threshold", -2)),
        )
        bt_cfg = self.mod.BacktestConfig(
            initial_capital=float(cfg.get("initial_capital", 10000.0)),
            stop_loss_pct=float(cfg.get("stop_loss_pct", 8.0)),
            take_profit_pct=float(cfg.get("take_profit_pct", 20.0)),
            max_hold_days=int(cfg.get("max_hold_days", 45)),
            fee_pct=float(cfg.get("fee_pct", 0.10)),
            slippage_pct=float(cfg.get("slippage_pct", 0.05)),
            min_hold_bars=int(cfg.get("min_hold_bars", 2)),
            cooldown_bars=int(cfg.get("cooldown_bars", 1)),
            same_asset_cooldown_bars=int(cfg.get("same_asset_cooldown_bars", 3)),
            max_consecutive_same_asset_entries=int(cfg.get("max_consecutive_same_asset_entries", 3)),
            max_drawdown_limit_pct=float(cfg.get("max_drawdown_limit_pct", 35.0)),
            max_exposure_pct=float(cfg.get("max_exposure_pct", 0.40)),
            tuned=tuned,
        )

        indicator_cache = self.mod.build_indicator_cache(raw_data, timeframe, max_workers=int(cfg.get("cache_workers", 4)), verbose=False)
        if not indicator_cache:
            return {"ok": False, "error": "No indicator-ready data for backtest."}

        anchor = next(iter(raw_data.keys()))
        end_date = raw_data[anchor].index.max()
        backtest_days = int(round(months * 365.25 / 12.0))
        start_date = end_date - pd.Timedelta(days=backtest_days)

        result = self.mod.simulate_backtest(
            raw_data,
            timeframe,
            bt_cfg,
            is_crypto,
            start_date=start_date,
            end_date=end_date,
            indicator_cache=indicator_cache,
            verbose=False,
        )
        eq = result.get("equity", [])
        if not eq:
            return {"ok": False, "error": "Backtest returned no results."}
        metrics = result.get("metrics", {}) or {}
        trades = result.get("trades", []) or []
        fx = self.mod.get_usd_to_currency_rate(display_currency)

        summary = [
            f"Final Value (USD): ${float(result.get('final', 0.0)):,.2f}",
            f"Total Return: {float(result.get('return_pct', 0.0)):+.2f}%",
            f"Trades closed: {len(trades)}",
            f"Max Drawdown: {float(metrics.get('max_drawdown_pct', 0.0)):.2f}%",
            f"Sharpe/Sortino: {float(metrics.get('sharpe', 0.0)):.2f} / {float(metrics.get('sortino', 0.0)):.2f}",
        ]
        if display_currency != "USD":
            summary.append(
                f"Final Value ({display_currency}): {display_currency} {(float(result.get('final', 0.0)) * fx):,.2f}  (FX USD->{display_currency} {fx:.4f})"
            )

        trade_df = pd.DataFrame(trades).head(200) if trades else pd.DataFrame()
        return {
            "ok": True,
            "summary_text": "\n".join(summary),
            "trades_text": trade_df.to_string(index=False) if not trade_df.empty else "No closed trades.",
            "metrics": metrics,
        }

    def build_dashboard_prompt(self, dashboard_text: str, datetime_context: str) -> str:
        with self._lock:
            return self.mod.build_grok_prompt(dashboard_text, datetime_context)

    def _default_system_prompt_for_active_profile(self) -> str:
        try:
            prefs = self.mod.load_ai_preferences()
            profile_name, profile = self.mod.get_active_ai_profile(prefs)
            provider = str(profile.get("provider", "")).strip().lower()
            internet_access = bool(
                profile.get(
                    "internet_access",
                    self.mod.provider_default_internet_access(provider),
                )
            )
            return self.mod.ONLINE_SYSTEM_PROMPT if internet_access else self.mod.OFFLINE_SYSTEM_PROMPT
        except Exception:
            return "You are Grok, built by xAI."

    def list_ai_profiles(self) -> Dict[str, Any]:
        with self._lock:
            prefs = self.mod.load_ai_preferences()
            secrets = self.mod.load_ai_secrets()
            active_name, _ = self.mod.get_active_ai_profile(prefs)
            profiles = prefs.get("profiles", {}) if isinstance(prefs, dict) else {}
            out: List[Dict[str, Any]] = []
            if isinstance(profiles, dict):
                for name, profile in profiles.items():
                    if not isinstance(name, str) or not isinstance(profile, dict):
                        continue
                    provider = str(profile.get("provider", "")).strip().lower()
                    model = str(profile.get("model", self.mod.provider_default_model(provider))).strip()
                    endpoint = str(profile.get("endpoint", self.mod.provider_default_endpoint(provider))).strip() or self.mod.provider_default_endpoint(provider)
                    internet_access = bool(profile.get("internet_access", self.mod.provider_default_internet_access(provider)))
                    key, key_source = self.mod.resolve_profile_api_key(profile, secrets)
                    out.append(
                        {
                            "name": name,
                            "provider": provider,
                            "model": model,
                            "endpoint": endpoint,
                            "internet_access": internet_access,
                            "temperature": float(profile.get("temperature", 0.2)),
                            "api_key_env": str(profile.get("api_key_env", "") or ""),
                            "api_key_name": str(profile.get("api_key_name", "") or ""),
                            "api_key_set": bool(key),
                            "api_key_source": key_source,
                            "active": name == active_name,
                        }
                    )
            return {"ok": True, "active_profile": active_name, "profiles": out}

    def upsert_ai_profile(
        self,
        name: str,
        provider: str,
        model: str,
        endpoint: str,
        internet_access: bool,
        temperature: float = 0.2,
        api_key_env: Optional[str] = None,
        activate: bool = False,
    ) -> Dict[str, Any]:
        with self._lock:
            pname = str(name).strip()
            if not pname:
                return {"ok": False, "error": "Profile name is required."}
            pvd = str(provider).strip().lower()
            if not pvd:
                return {"ok": False, "error": "Provider is required."}
            prefs = self.mod.load_ai_preferences()
            if not isinstance(prefs, dict):
                prefs = self.mod.default_ai_preferences()
            profiles = prefs.get("profiles", {})
            if not isinstance(profiles, dict):
                profiles = {}
            profiles[pname] = {
                "provider": pvd,
                "model": str(model).strip() or self.mod.provider_default_model(pvd),
                "endpoint": str(endpoint).strip() or self.mod.provider_default_endpoint(pvd),
                "api_key_env": str(api_key_env or "").strip() or self.mod.provider_default_envvar(pvd),
                "api_key_name": f"{pname}_api_key",
                "internet_access": bool(internet_access),
                "temperature": float(temperature),
            }
            prefs["profiles"] = profiles
            if activate:
                prefs["active_profile"] = pname
            self.mod.save_ai_preferences(prefs)
            return {"ok": True}

    def set_active_ai_profile(self, name: str) -> Dict[str, Any]:
        with self._lock:
            prefs = self.mod.load_ai_preferences()
            profiles = prefs.get("profiles", {}) if isinstance(prefs, dict) else {}
            pname = str(name).strip()
            if not isinstance(profiles, dict) or pname not in profiles:
                return {"ok": False, "error": f"Profile not found: {pname}"}
            prefs["active_profile"] = pname
            self.mod.save_ai_preferences(prefs)
            return {"ok": True}

    def delete_ai_profile(self, name: str) -> Dict[str, Any]:
        with self._lock:
            pname = str(name).strip()
            prefs = self.mod.load_ai_preferences()
            secrets = self.mod.load_ai_secrets()
            profiles = prefs.get("profiles", {}) if isinstance(prefs, dict) else {}
            if not isinstance(profiles, dict) or pname not in profiles:
                return {"ok": False, "error": f"Profile not found: {pname}"}
            prof = profiles.pop(pname)
            key_name = str((prof or {}).get("api_key_name", "") or "").strip()
            if key_name and key_name in secrets:
                del secrets[key_name]
            if prefs.get("active_profile") == pname:
                prefs["active_profile"] = next(iter(profiles.keys()), "")
            prefs["profiles"] = profiles
            self.mod.save_ai_preferences(prefs)
            self.mod.save_ai_secrets(secrets)
            return {"ok": True}

    def set_ai_profile_key(self, name: str, api_key: str) -> Dict[str, Any]:
        with self._lock:
            pname = str(name).strip()
            key = str(api_key).strip()
            if not pname or not key:
                return {"ok": False, "error": "Profile and API key are required."}
            prefs = self.mod.load_ai_preferences()
            secrets = self.mod.load_ai_secrets()
            profiles = prefs.get("profiles", {}) if isinstance(prefs, dict) else {}
            if not isinstance(profiles, dict) or pname not in profiles or not isinstance(profiles[pname], dict):
                return {"ok": False, "error": f"Profile not found: {pname}"}
            prof = profiles[pname]
            key_name = str(prof.get("api_key_name", "") or "").strip() or f"{pname}_api_key"
            prof["api_key_name"] = key_name
            profiles[pname] = prof
            prefs["profiles"] = profiles
            secrets[key_name] = key
            self.mod.save_ai_preferences(prefs)
            self.mod.save_ai_secrets(secrets)
            return {"ok": True}

    def remove_ai_profile_key(self, name: str) -> Dict[str, Any]:
        with self._lock:
            pname = str(name).strip()
            prefs = self.mod.load_ai_preferences()
            secrets = self.mod.load_ai_secrets()
            profiles = prefs.get("profiles", {}) if isinstance(prefs, dict) else {}
            if not isinstance(profiles, dict) or pname not in profiles or not isinstance(profiles[pname], dict):
                return {"ok": False, "error": f"Profile not found: {pname}"}
            key_name = str(profiles[pname].get("api_key_name", "") or "").strip()
            if key_name and key_name in secrets:
                del secrets[key_name]
                self.mod.save_ai_secrets(secrets)
            return {"ok": True}

    def test_ai_profile(self, name: str, timeout_prompt: str = "Reply with OK.") -> Dict[str, Any]:
        with self._lock:
            prefs = self.mod.load_ai_preferences()
            secrets = self.mod.load_ai_secrets()
            profiles = prefs.get("profiles", {}) if isinstance(prefs, dict) else {}
            pname = str(name).strip()
            if not isinstance(profiles, dict) or pname not in profiles or not isinstance(profiles[pname], dict):
                return {"ok": False, "error": f"Profile not found: {pname}"}
            prof = profiles[pname]
            provider = str(prof.get("provider", "")).strip().lower()
            model = str(prof.get("model", self.mod.provider_default_model(provider))).strip()
            endpoint = str(prof.get("endpoint", self.mod.provider_default_endpoint(provider))).strip() or self.mod.provider_default_endpoint(provider)
            temperature = float(prof.get("temperature", 0.0))
            internet_access = bool(prof.get("internet_access", self.mod.provider_default_internet_access(provider)))
            system_prompt = self.mod.ONLINE_SYSTEM_PROMPT if internet_access else self.mod.OFFLINE_SYSTEM_PROMPT
            key, key_source = self.mod.resolve_profile_api_key(prof, secrets)

            if self.mod.provider_needs_api_key(provider) and not key:
                return {"ok": False, "error": f"Missing API key ({key_source})."}

            if provider in ("xai", "openai", "openai_compatible", "openclaw"):
                text, status, err = self.mod.call_openai_style_chat(
                    endpoint, model, timeout_prompt, system_prompt, temperature, api_key=key
                )
                return {"ok": bool(text), "response": text or "", "status": status, "error": err}
            if provider == "anthropic":
                text, status, err = self.mod.call_anthropic_chat(
                    endpoint, model, timeout_prompt, system_prompt, temperature, api_key=key
                )
                return {"ok": bool(text), "response": text or "", "status": status, "error": err}
            if provider == "ollama":
                text, status, err = self.mod.call_ollama_chat(
                    endpoint, model, timeout_prompt, system_prompt, temperature
                )
                return {"ok": bool(text), "response": text or "", "status": status, "error": err}
            return {"ok": False, "error": f"Unsupported provider: {provider}"}

    def run_ai_analysis(
        self,
        dashboard_text: str,
        datetime_context: str,
        prompt_override: Optional[str] = None,
        system_prompt_override: Optional[str] = None,
    ) -> Dict[str, Any]:
        with self._lock:
            stream = io.StringIO()
            with redirect_stdout(stream), redirect_stderr(stream):
                prompt = prompt_override if (prompt_override and str(prompt_override).strip()) else self.mod.build_grok_prompt(dashboard_text, datetime_context)
                system_prompt = (
                    system_prompt_override
                    if (system_prompt_override and str(system_prompt_override).strip())
                    else self._default_system_prompt_for_active_profile()
                )
                response = self.mod.call_active_ai_provider(prompt, system_prompt=system_prompt)
            return {
                "ok": bool(response),
                "prompt": prompt,
                "response": response or "",
                "system_prompt": system_prompt,
                "log": stream.getvalue(),
            }

    def _default_binance_endpoint(self) -> str:
        return "https://api.binance.com"

    def _resolve_binance_credentials(self, profile: Dict[str, Any], secrets: Dict[str, str]) -> Dict[str, str]:
        key_env = str(profile.get("api_key_env", "") or "").strip()
        secret_env = str(profile.get("api_secret_env", "") or "").strip()
        key_name = str(profile.get("api_key_name", "") or "").strip()
        secret_name = str(profile.get("api_secret_name", "") or "").strip()

        api_key = ""
        api_secret = ""
        key_source = "missing"
        secret_source = "missing"

        if key_env:
            env_val = (str(os.getenv(key_env, "")) or "").strip()
            if env_val:
                api_key = env_val
                key_source = f"env:{key_env}"
        if not api_key and key_name and key_name in secrets:
            v = str(secrets.get(key_name, "")).strip()
            if v:
                api_key = v
                key_source = f"prefs:{key_name}"

        if secret_env:
            env_val = (str(os.getenv(secret_env, "")) or "").strip()
            if env_val:
                api_secret = env_val
                secret_source = f"env:{secret_env}"
        if not api_secret and secret_name and secret_name in secrets:
            v = str(secrets.get(secret_name, "")).strip()
            if v:
                api_secret = v
                secret_source = f"prefs:{secret_name}"

        return {
            "api_key": api_key,
            "api_secret": api_secret,
            "key_source": key_source,
            "secret_source": secret_source,
        }

    def _signed_binance_get(self, endpoint: str, path: str, api_key: str, api_secret: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        q = dict(params or {})
        q["timestamp"] = int(time.time() * 1000)
        q.setdefault("recvWindow", 5000)
        query = urllib.parse.urlencode(q, doseq=True)
        sig = hmac.new(api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
        url = endpoint.rstrip("/") + path + "?" + query + "&signature=" + sig
        headers = {"X-MBX-APIKEY": api_key}
        try:
            r = requests.get(url, headers=headers, timeout=30)
            if not (200 <= r.status_code < 300):
                return {"ok": False, "status": r.status_code, "error": (r.text or "")[:1200]}
            return {"ok": True, "status": r.status_code, "data": r.json()}
        except Exception as e:
            return {"ok": False, "status": -1, "error": str(e)}

    def list_binance_profiles(self) -> Dict[str, Any]:
        with self._lock:
            prefs = load_binance_preferences()
            secrets = load_binance_secrets()
            profiles = prefs.get("profiles", {}) if isinstance(prefs, dict) else {}
            active = str(prefs.get("active_profile", "") or "")
            out: List[Dict[str, Any]] = []
            if isinstance(profiles, dict):
                for name, profile in profiles.items():
                    if not isinstance(name, str) or not isinstance(profile, dict):
                        continue
                    endpoint = str(profile.get("endpoint", self._default_binance_endpoint()) or self._default_binance_endpoint()).strip()
                    creds = self._resolve_binance_credentials(profile, secrets)
                    out.append(
                        {
                            "name": name,
                            "endpoint": endpoint,
                            "api_key_env": str(profile.get("api_key_env", "") or ""),
                            "api_secret_env": str(profile.get("api_secret_env", "") or ""),
                            "api_key_set": bool(creds["api_key"]),
                            "api_secret_set": bool(creds["api_secret"]),
                            "api_key_source": creds["key_source"],
                            "api_secret_source": creds["secret_source"],
                            "active": name == active,
                        }
                    )
            return {"ok": True, "active_profile": active, "profiles": out}

    def upsert_binance_profile(
        self,
        name: str,
        endpoint: str,
        api_key_env: Optional[str] = None,
        api_secret_env: Optional[str] = None,
        activate: bool = False,
    ) -> Dict[str, Any]:
        with self._lock:
            pname = str(name).strip()
            if not pname:
                return {"ok": False, "error": "Profile name is required."}
            ep = str(endpoint).strip() or self._default_binance_endpoint()
            prefs = load_binance_preferences()
            profiles = prefs.get("profiles", {}) if isinstance(prefs, dict) else {}
            if not isinstance(profiles, dict):
                profiles = {}
            profiles[pname] = {
                "endpoint": ep,
                "api_key_env": str(api_key_env or "BINANCE_API_KEY").strip() or "BINANCE_API_KEY",
                "api_secret_env": str(api_secret_env or "BINANCE_API_SECRET").strip() or "BINANCE_API_SECRET",
                "api_key_name": f"{pname}_api_key",
                "api_secret_name": f"{pname}_api_secret",
            }
            prefs["profiles"] = profiles
            if activate:
                prefs["active_profile"] = pname
            save_binance_preferences(prefs)
            return {"ok": True}

    def set_active_binance_profile(self, name: str) -> Dict[str, Any]:
        with self._lock:
            pname = str(name).strip()
            prefs = load_binance_preferences()
            profiles = prefs.get("profiles", {}) if isinstance(prefs, dict) else {}
            if not isinstance(profiles, dict) or pname not in profiles:
                return {"ok": False, "error": f"Profile not found: {pname}"}
            prefs["active_profile"] = pname
            save_binance_preferences(prefs)
            return {"ok": True}

    def delete_binance_profile(self, name: str) -> Dict[str, Any]:
        with self._lock:
            pname = str(name).strip()
            prefs = load_binance_preferences()
            secrets = load_binance_secrets()
            profiles = prefs.get("profiles", {}) if isinstance(prefs, dict) else {}
            if not isinstance(profiles, dict) or pname not in profiles:
                return {"ok": False, "error": f"Profile not found: {pname}"}
            prof = profiles.pop(pname) or {}
            for k in [str(prof.get("api_key_name", "") or ""), str(prof.get("api_secret_name", "") or "")]:
                if k and k in secrets:
                    del secrets[k]
            if prefs.get("active_profile") == pname:
                prefs["active_profile"] = next(iter(profiles.keys()), "")
            prefs["profiles"] = profiles
            save_binance_preferences(prefs)
            save_binance_secrets(secrets)
            return {"ok": True}

    def set_binance_profile_keys(self, name: str, api_key: str, api_secret: str) -> Dict[str, Any]:
        with self._lock:
            pname = str(name).strip()
            if not pname:
                return {"ok": False, "error": "Profile name is required."}
            key = str(api_key).strip()
            sec = str(api_secret).strip()
            if not key or not sec:
                return {"ok": False, "error": "API key and secret are required."}
            prefs = load_binance_preferences()
            secrets = load_binance_secrets()
            profiles = prefs.get("profiles", {}) if isinstance(prefs, dict) else {}
            if not isinstance(profiles, dict) or pname not in profiles or not isinstance(profiles[pname], dict):
                return {"ok": False, "error": f"Profile not found: {pname}"}
            prof = profiles[pname]
            key_name = str(prof.get("api_key_name", "") or "").strip() or f"{pname}_api_key"
            secret_name = str(prof.get("api_secret_name", "") or "").strip() or f"{pname}_api_secret"
            prof["api_key_name"] = key_name
            prof["api_secret_name"] = secret_name
            profiles[pname] = prof
            prefs["profiles"] = profiles
            secrets[key_name] = key
            secrets[secret_name] = sec
            save_binance_preferences(prefs)
            save_binance_secrets(secrets)
            return {"ok": True}

    def remove_binance_profile_keys(self, name: str) -> Dict[str, Any]:
        with self._lock:
            pname = str(name).strip()
            prefs = load_binance_preferences()
            secrets = load_binance_secrets()
            profiles = prefs.get("profiles", {}) if isinstance(prefs, dict) else {}
            if not isinstance(profiles, dict) or pname not in profiles or not isinstance(profiles[pname], dict):
                return {"ok": False, "error": f"Profile not found: {pname}"}
            prof = profiles[pname]
            for key_name in [str(prof.get("api_key_name", "") or ""), str(prof.get("api_secret_name", "") or "")]:
                if key_name and key_name in secrets:
                    del secrets[key_name]
            save_binance_secrets(secrets)
            return {"ok": True}

    def test_binance_profile(self, name: str) -> Dict[str, Any]:
        with self._lock:
            prefs = load_binance_preferences()
            secrets = load_binance_secrets()
            profiles = prefs.get("profiles", {}) if isinstance(prefs, dict) else {}
            pname = str(name).strip()
            if not isinstance(profiles, dict) or pname not in profiles or not isinstance(profiles[pname], dict):
                return {"ok": False, "error": f"Profile not found: {pname}"}
            profile = profiles[pname]
            creds = self._resolve_binance_credentials(profile, secrets)
            if not creds["api_key"] or not creds["api_secret"]:
                return {"ok": False, "error": f"Missing API key/secret ({creds['key_source']}, {creds['secret_source']})."}
            endpoint = str(profile.get("endpoint", self._default_binance_endpoint()) or self._default_binance_endpoint())
            out = self._signed_binance_get(endpoint, "/api/v3/account", creds["api_key"], creds["api_secret"], params={})
            if not out.get("ok"):
                return {"ok": False, "status": out.get("status"), "error": out.get("error", "request failed")}
            data = out.get("data", {}) if isinstance(out.get("data"), dict) else {}
            can_trade = bool(data.get("canTrade", False))
            buyer_comm = data.get("buyerCommission", "")
            return {
                "ok": True,
                "status": out.get("status"),
                "can_trade": can_trade,
                "buyer_commission_bps": buyer_comm,
            }

    def fetch_binance_portfolio(self, profile_name: Optional[str] = None) -> Dict[str, Any]:
        with self._lock:
            prefs = load_binance_preferences()
            secrets = load_binance_secrets()
            profiles = prefs.get("profiles", {}) if isinstance(prefs, dict) else {}
            if not isinstance(profiles, dict) or not profiles:
                return {"ok": False, "error": "No Binance profiles configured."}
            active = str(profile_name or prefs.get("active_profile", "") or "").strip()
            if not active or active not in profiles:
                active = next(iter(profiles.keys()))
            profile = profiles.get(active, {})
            if not isinstance(profile, dict):
                return {"ok": False, "error": f"Profile not found: {active}"}
            creds = self._resolve_binance_credentials(profile, secrets)
            if not creds["api_key"] or not creds["api_secret"]:
                return {"ok": False, "error": f"Missing API key/secret ({creds['key_source']}, {creds['secret_source']})."}
            endpoint = str(profile.get("endpoint", self._default_binance_endpoint()) or self._default_binance_endpoint())
            acct = self._signed_binance_get(endpoint, "/api/v3/account", creds["api_key"], creds["api_secret"], params={})
            if not acct.get("ok"):
                return {"ok": False, "status": acct.get("status"), "error": acct.get("error", "account request failed")}

            try:
                tick = requests.get(endpoint.rstrip("/") + "/api/v3/ticker/price", timeout=20)
                tick.raise_for_status()
                prices = tick.json()
            except Exception as e:
                return {"ok": False, "error": f"Failed to fetch prices: {e}"}

            price_map: Dict[str, float] = {}
            if isinstance(prices, list):
                for item in prices:
                    if not isinstance(item, dict):
                        continue
                    s = str(item.get("symbol", "") or "").strip().upper()
                    try:
                        p = float(item.get("price", 0.0) or 0.0)
                    except Exception:
                        p = 0.0
                    if s and p > 0:
                        price_map[s] = p

            account = acct.get("data", {}) if isinstance(acct.get("data"), dict) else {}
            raw_bal = account.get("balances", []) if isinstance(account, dict) else []
            rows: List[Dict[str, Any]] = []
            total_usd = 0.0
            stable = {"USDT", "USDC", "BUSD", "FDUSD", "TUSD", "USDP", "DAI"}
            for b in raw_bal:
                if not isinstance(b, dict):
                    continue
                asset = str(b.get("asset", "") or "").strip().upper()
                try:
                    free = float(b.get("free", 0.0) or 0.0)
                    locked = float(b.get("locked", 0.0) or 0.0)
                except Exception:
                    continue
                total = free + locked
                if total <= 0:
                    continue
                usd_val = 0.0
                if asset in stable:
                    usd_val = total
                else:
                    pair = f"{asset}USDT"
                    if pair in price_map:
                        usd_val = total * float(price_map[pair])
                total_usd += usd_val
                rows.append(
                    {
                        "asset": asset,
                        "free": round(free, 8),
                        "locked": round(locked, 8),
                        "total": round(total, 8),
                        "est_usd": round(usd_val, 2),
                    }
                )
            rows = sorted(rows, key=lambda r: float(r.get("est_usd", 0.0)), reverse=True)
            return {
                "ok": True,
                "profile": active,
                "total_est_usd": round(total_usd, 2),
                "balances": rows,
            }

    def get_trade_ledger(self) -> Dict[str, Any]:
        with self._lock:
            return {"ok": True, "ledger": load_trade_ledger()}

    def record_signal_event(self, event: Dict[str, Any], cooldown_minutes: int = 240, allow_duplicate: bool = False) -> Dict[str, Any]:
        with self._lock:
            ledger = load_trade_ledger()
            if not isinstance(ledger, dict):
                ledger = default_trade_ledger()
            entries = ledger.get("entries", [])
            open_positions = ledger.get("open_positions", {})
            activity_guard = ledger.get("activity_guard", {})
            if not isinstance(entries, list):
                entries = []
            if not isinstance(open_positions, dict):
                open_positions = {}
            if not isinstance(activity_guard, dict):
                activity_guard = {}

            market = str(event.get("market", "crypto")).strip().lower()
            timeframe = str(event.get("timeframe", "1d")).strip().lower()
            asset = str(event.get("asset", "")).strip().upper()
            action = str(event.get("action", "")).strip().upper()
            panel = str(event.get("panel", "live")).strip()
            note = str(event.get("note", "")).strip()
            score = str(event.get("score", "")).strip()
            try:
                price = float(event.get("price", 0.0) or 0.0)
            except Exception:
                price = 0.0
            try:
                qty = float(event.get("qty", 0.0) or 0.0)
            except Exception:
                qty = 0.0

            if not asset or action not in ("BUY", "SELL", "HOLD"):
                return {"ok": False, "error": "Event must include valid asset and action (BUY/SELL/HOLD)."}

            now = pd.Timestamp.utcnow()
            guard_key = f"{market}|{timeframe}|{asset}|{action}"
            blocked = False
            blocked_reason = ""
            if not allow_duplicate:
                last = activity_guard.get(guard_key)
                if isinstance(last, dict):
                    try:
                        last_ts = pd.to_datetime(last.get("ts", ""), utc=True)
                        delta_min = (now - last_ts).total_seconds() / 60.0
                    except Exception:
                        delta_min = 10e9
                    if delta_min < max(1, int(cooldown_minutes)):
                        blocked = True
                        blocked_reason = f"Duplicate signal within cooldown ({delta_min:.1f}m < {cooldown_minutes}m)."

            if blocked:
                return {"ok": False, "blocked": True, "reason": blocked_reason}

            entry_id = len(entries) + 1
            rec = {
                "id": entry_id,
                "ts": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "market": market,
                "timeframe": timeframe,
                "panel": panel,
                "asset": asset,
                "action": action,
                "price": price,
                "qty": qty,
                "score": score,
                "note": note,
            }
            entries.append(rec)
            activity_guard[guard_key] = {"ts": rec["ts"], "id": entry_id}

            pos_key = f"{market}|{timeframe}|{asset}"
            open_pos = open_positions.get(pos_key)
            if action == "BUY":
                open_positions[pos_key] = {
                    "entry_id": entry_id,
                    "entry_ts": rec["ts"],
                    "entry_price": price,
                    "qty": qty,
                    "asset": asset,
                    "market": market,
                    "timeframe": timeframe,
                    "panel": panel,
                }
            elif action == "SELL":
                if isinstance(open_pos, dict):
                    rec["closed_entry_id"] = int(open_pos.get("entry_id", 0) or 0)
                    try:
                        entry_price = float(open_pos.get("entry_price", 0.0) or 0.0)
                    except Exception:
                        entry_price = 0.0
                    if entry_price > 0 and price > 0:
                        rec["pnl_pct"] = round(((price - entry_price) / entry_price) * 100.0, 4)
                    open_positions.pop(pos_key, None)

            ledger["entries"] = entries
            ledger["open_positions"] = open_positions
            ledger["activity_guard"] = activity_guard
            save_trade_ledger(ledger)
            return {"ok": True, "entry": rec}

    def run_standard_research(self) -> Dict[str, Any]:
        cmd = [sys.executable, str(self.scripts_dir / "auto_research_cycle.py")]
        return self._run_subprocess(cmd)

    def run_comprehensive_research(self, scenarios: List[Dict[str, Any]], optuna_trials: int = 10, optuna_jobs: int = 4) -> Dict[str, Any]:
        runs_dir = self.repo_root / "experiments" / "runs"
        runs_dir.mkdir(parents=True, exist_ok=True)
        stamp = pd.Timestamp.utcnow().strftime("%Y%m%dT%H%M%SZ")
        scen_path = runs_dir / f"gui_scenarios_comprehensive_{stamp}.json"
        scen_path.write_text(json.dumps(scenarios, indent=2), encoding="utf-8")
        cmd = [
            sys.executable,
            str(self.scripts_dir / "run_experiments.py"),
            "--scenarios",
            str(scen_path),
            "--enable-optuna",
            "--optuna-trials",
            str(max(1, int(optuna_trials))),
            "--optuna-jobs",
            str(max(1, int(optuna_jobs))),
        ]
        out = self._run_subprocess(cmd)
        if not out["ok"]:
            return out
        step2 = self._run_subprocess([sys.executable, str(self.scripts_dir / "promote_champion.py")])
        step3 = self._run_subprocess([sys.executable, str(self.scripts_dir / "update_handoff.py")])
        out["stdout"] += "\n\n=== promote_champion ===\n" + step2.get("stdout", "")
        out["stdout"] += "\n\n=== update_handoff ===\n" + step3.get("stdout", "")
        out["ok"] = out["ok"] and step2["ok"] and step3["ok"]
        return out

    def _run_subprocess(self, cmd: List[str]) -> Dict[str, Any]:
        try:
            proc = subprocess.run(
                cmd,
                cwd=str(self.repo_root),
                check=False,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
            return {
                "ok": proc.returncode == 0,
                "returncode": proc.returncode,
                "stdout": (proc.stdout or "").strip(),
                "stderr": (proc.stderr or "").strip(),
                "cmd": " ".join(cmd),
            }
        except Exception as e:
            return {"ok": False, "returncode": -1, "stdout": "", "stderr": str(e), "cmd": " ".join(cmd)}
