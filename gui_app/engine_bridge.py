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
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

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
        self._binance_exchange_info_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
        self._result_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
        self._raw_data_cache: Dict[str, Tuple[float, Dict[str, pd.DataFrame]]] = {}
        self._indicator_cache_store: Dict[str, Tuple[float, Dict[str, pd.DataFrame]]] = {}
        self._cache_ttl_live_sec = 20.0
        self._cache_ttl_backtest_sec = 90.0
        self._cache_ttl_raw_sec = 45.0
        self._cache_ttl_indicator_sec = 60.0
        self.mod = self._load_nightly_module()
        self.mod.ensure_table()

    @staticmethod
    def _now() -> float:
        return time.time()

    @staticmethod
    def _stable_cache_key(prefix: str, payload: Dict[str, Any]) -> str:
        blob = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
        return f"{prefix}:{hashlib.sha1(blob.encode('utf-8')).hexdigest()}"

    @staticmethod
    def _cache_get(cache: Dict[str, Tuple[float, Any]], key: str, ttl_sec: float) -> Optional[Any]:
        rec = cache.get(key)
        if not rec:
            return None
        ts, val = rec
        if (time.time() - float(ts)) > float(ttl_sec):
            try:
                del cache[key]
            except Exception:
                pass
            return None
        return val

    @staticmethod
    def _cache_put(cache: Dict[str, Tuple[float, Any]], key: str, val: Any) -> None:
        cache[key] = (time.time(), val)

    def _resolve_tickers(
        self,
        is_crypto: bool,
        top_n: int,
        quote: str,
        country: str,
        explicit: Optional[List[str]] = None,
    ) -> List[str]:
        if isinstance(explicit, list) and explicit:
            return [str(x).strip() for x in explicit if str(x).strip()]
        if is_crypto:
            return self.mod.filter_crypto_tickers_by_binance(self.mod.fetch_top_coins(top_n, quote_currency=quote))
        return list(self.mod.TRADITIONAL_TOP.get(country, self.mod.TRADITIONAL_TOP["2"]))[: max(1, top_n)]

    def _load_raw_data(
        self,
        tickers: List[str],
        timeframe: str,
        is_crypto: bool,
        fetch_years: float,
        min_bars: int = 60,
        fetch_workers: int = 1,
    ) -> Tuple[Dict[str, pd.DataFrame], Dict[str, Any]]:
        perf: Dict[str, Any] = {"tickers": len(tickers), "fetch_workers": int(max(1, fetch_workers))}
        raw_key = self._stable_cache_key(
            "raw",
            {
                "tickers": sorted([str(t).upper() for t in tickers]),
                "timeframe": str(timeframe),
                "is_crypto": bool(is_crypto),
                "years": round(float(fetch_years), 4),
                "min_bars": int(min_bars),
            },
        )
        t0 = self._now()
        cached = self._cache_get(self._raw_data_cache, raw_key, self._cache_ttl_raw_sec)
        if isinstance(cached, dict) and cached:
            perf["raw_cache_hit"] = True
            perf["raw_load_sec"] = round(self._now() - t0, 4)
            return cached, perf

        raw_data: Dict[str, pd.DataFrame] = {}
        fetch_workers = max(1, int(fetch_workers))

        def _fetch_one(t: str) -> Tuple[str, Optional[pd.DataFrame]]:
            df = self.mod.fetch_with_cache(t, timeframe, is_crypto=is_crypto, years=fetch_years)
            if df is not None and len(df) >= min_bars:
                return t, df
            return t, None

        t_fetch = self._now()
        if fetch_workers <= 1 or len(tickers) <= 1:
            for t in tickers:
                sym, df = _fetch_one(t)
                if df is not None:
                    raw_data[sym] = df
        else:
            with ThreadPoolExecutor(max_workers=fetch_workers) as ex:
                futs = [ex.submit(_fetch_one, t) for t in tickers]
                for fut in as_completed(futs):
                    try:
                        sym, df = fut.result()
                    except Exception:
                        continue
                    if df is not None:
                        raw_data[sym] = df
        perf["fetch_sec"] = round(self._now() - t_fetch, 4)
        perf["raw_cache_hit"] = False
        perf["loaded_assets"] = len(raw_data)
        perf["raw_load_sec"] = round(self._now() - t0, 4)
        if raw_data:
            self._cache_put(self._raw_data_cache, raw_key, raw_data)
        return raw_data, perf

    def _load_indicator_cache(
        self,
        raw_data: Dict[str, pd.DataFrame],
        timeframe: str,
        workers: int,
    ) -> Tuple[Dict[str, pd.DataFrame], Dict[str, Any]]:
        perf: Dict[str, Any] = {}
        ind_key = self._stable_cache_key(
            "ind",
            {
                "keys": sorted([str(k).upper() for k in raw_data.keys()]),
                "timeframe": str(timeframe),
            },
        )
        t0 = self._now()
        cached = self._cache_get(self._indicator_cache_store, ind_key, self._cache_ttl_indicator_sec)
        if isinstance(cached, dict) and cached:
            perf["indicator_cache_hit"] = True
            perf["indicator_sec"] = round(self._now() - t0, 4)
            return cached, perf
        workers = max(1, int(workers))
        t_ind = self._now()
        enriched = self.mod.build_indicator_cache(raw_data, timeframe, max_workers=workers, verbose=False)
        perf["indicator_cache_hit"] = False
        perf["indicator_sec"] = round(self._now() - t_ind, 4)
        if enriched:
            self._cache_put(self._indicator_cache_store, ind_key, enriched)
        return enriched, perf

    def _load_nightly_module(self):
        # Runtime safety for environments where numba cache locators fail
        # (observed in Streamlit + dynamic module loading on Python 3.13).
        try:
            os.environ.setdefault("NUMBA_DISABLE_JIT", "1")
            os.environ.setdefault("NUMBA_CACHE_DIR", str(Path.home() / ".numba_cache"))
            import numba  # type: ignore

            if not bool(getattr(numba, "_ctmt_cache_patch", False)):
                orig_njit = getattr(numba, "njit", None)
                orig_jit = getattr(numba, "jit", None)

                def _wrap_no_cache(orig_fn):
                    if not callable(orig_fn):
                        return orig_fn

                    def _wrapped(*args, **kwargs):
                        if isinstance(kwargs, dict) and "cache" in kwargs:
                            kwargs = dict(kwargs)
                            kwargs["cache"] = False
                        return orig_fn(*args, **kwargs)

                    return _wrapped

                if callable(orig_njit):
                    numba.njit = _wrap_no_cache(orig_njit)  # type: ignore[attr-defined]
                if callable(orig_jit):
                    numba.jit = _wrap_no_cache(orig_jit)  # type: ignore[attr-defined]
                setattr(numba, "_ctmt_cache_patch", True)
        except Exception:
            # Non-fatal: continue with default behavior if numba patching is unavailable.
            pass

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
        ck = self._stable_cache_key(
            "live_result",
            {
                "market": str(cfg.get("market", "crypto")),
                "timeframe": str(cfg.get("timeframe", "1d")),
                "top_n": int(cfg.get("top_n", 20)),
                "quote_currency": str(cfg.get("quote_currency", "USD")).upper(),
                "country": str(cfg.get("country", "2")),
                "display_currency": str(cfg.get("display_currency", "USD")).upper(),
                "name": str(cfg.get("name", "")),
            },
        )
        cached = self._cache_get(self._result_cache, ck, self._cache_ttl_live_sec)
        if isinstance(cached, dict):
            out = dict(cached)
            p = out.get("perf", {}) if isinstance(out.get("perf"), dict) else {}
            p["result_cache_hit"] = True
            out["perf"] = p
            out["log"] = ""
            return out

        stream = io.StringIO()
        t0 = self._now()
        with self._lock:
            with redirect_stdout(stream), redirect_stderr(stream):
                out = self._run_live_panel_impl(cfg)
        out["log"] = stream.getvalue()
        perf = out.get("perf", {}) if isinstance(out.get("perf"), dict) else {}
        perf["total_sec"] = round(self._now() - t0, 4)
        perf["result_cache_hit"] = False
        out["perf"] = perf
        if out.get("ok"):
            self._cache_put(self._result_cache, ck, dict(out))
        return out

    def _run_live_panel_impl(self, cfg: Dict[str, Any]) -> Dict[str, Any]:
        is_crypto = str(cfg.get("market", "crypto")).lower() == "crypto"
        timeframe = str(cfg.get("timeframe", "1d"))
        display_currency = str(cfg.get("display_currency", "USD")).upper()
        tickers = self._assets_for_live(cfg)
        fetch_years = 2.2
        fetch_workers = max(1, int(cfg.get("fetch_workers", 1) or 1))
        raw_data, perf_raw = self._load_raw_data(
            tickers=tickers,
            timeframe=timeframe,
            is_crypto=is_crypto,
            fetch_years=fetch_years,
            min_bars=60,
            fetch_workers=fetch_workers,
        )
        if not raw_data:
            return {"ok": False, "error": "No usable data returned."}

        tuned = self.mod.TunedParams()
        indicator_workers = max(1, int(cfg.get("cache_workers", 1) or 1))
        enriched, perf_ind = self._load_indicator_cache(raw_data, timeframe, workers=indicator_workers)
        if not enriched:
            return {"ok": False, "error": "No indicator-ready data."}

        t_table = self._now()
        table, risk = self.mod.build_live_tables(enriched, is_crypto, tuned, timeframe)
        table_sec = round(self._now() - t_table, 4)
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
            "table_rows": table_print.reset_index().to_dict(orient="records"),
            "notes": notes,
            "loaded_assets": len(raw_data),
            "requested_assets": len(tickers),
            "timeframe": timeframe,
            "market": "Crypto" if is_crypto else "Traditional",
            "perf": {
                **perf_raw,
                **perf_ind,
                "table_sec": table_sec,
            },
        }

    def run_backtest(self, cfg: Dict[str, Any]) -> Dict[str, Any]:
        ck = self._stable_cache_key(
            "backtest_result",
            {
                "market": str(cfg.get("market", "crypto")).lower(),
                "timeframe": str(cfg.get("timeframe", "1d")),
                "months": int(cfg.get("months", 12)),
                "top_n": int(cfg.get("top_n", 20)),
                "quote_currency": str(cfg.get("quote_currency", "USD")).upper(),
                "country": str(cfg.get("country", "2")),
                "tickers": list(cfg.get("tickers", []) or []),
                "risk": {
                    "sl": float(cfg.get("stop_loss_pct", 8.0)),
                    "tp": float(cfg.get("take_profit_pct", 20.0)),
                    "hold": int(cfg.get("max_hold_days", 45)),
                    "fee": float(cfg.get("fee_pct", 0.10)),
                    "slip": float(cfg.get("slippage_pct", 0.05)),
                },
                "tuned": {
                    "position_size": float(cfg.get("position_size", 0.30)),
                    "atr": float(cfg.get("atr_multiplier", 2.2)),
                    "adx": float(cfg.get("adx_threshold", 25.0)),
                    "cmf": float(cfg.get("cmf_threshold", 0.02)),
                    "obv": float(cfg.get("obv_slope_threshold", 0.0)),
                    "buy_t": int(cfg.get("buy_threshold", 2)),
                    "sell_t": int(cfg.get("sell_threshold", -2)),
                },
            },
        )
        cached = self._cache_get(self._result_cache, ck, self._cache_ttl_backtest_sec)
        if isinstance(cached, dict):
            out = dict(cached)
            p = out.get("perf", {}) if isinstance(out.get("perf"), dict) else {}
            p["result_cache_hit"] = True
            out["perf"] = p
            out["log"] = ""
            return out

        stream = io.StringIO()
        t0 = self._now()
        with self._lock:
            with redirect_stdout(stream), redirect_stderr(stream):
                out = self._run_backtest_impl(cfg)
        out["log"] = stream.getvalue()
        perf = out.get("perf", {}) if isinstance(out.get("perf"), dict) else {}
        perf["total_sec"] = round(self._now() - t0, 4)
        perf["result_cache_hit"] = False
        out["perf"] = perf
        if out.get("ok"):
            self._cache_put(self._result_cache, ck, dict(out))
        return out

    def _run_backtest_impl(self, cfg: Dict[str, Any]) -> Dict[str, Any]:
        is_crypto = str(cfg.get("market", "crypto")).lower() == "crypto"
        timeframe = str(cfg.get("timeframe", "1d"))
        months = int(cfg.get("months", 12))
        top_n = int(cfg.get("top_n", 20))
        quote = str(cfg.get("quote_currency", "USD")).upper()
        display_currency = str(cfg.get("display_currency", "USD")).upper()
        country = str(cfg.get("country", "2"))

        explicit = cfg.get("tickers", None)
        tickers = self._resolve_tickers(is_crypto, top_n, quote, country, explicit=explicit if isinstance(explicit, list) else None)

        fetch_years = max(2.2, (months / 12.0) + 0.25)
        fetch_workers = max(1, int(cfg.get("fetch_workers", 1) or 1))
        raw_data, perf_raw = self._load_raw_data(
            tickers=tickers,
            timeframe=timeframe,
            is_crypto=is_crypto,
            fetch_years=fetch_years,
            min_bars=60,
            fetch_workers=fetch_workers,
        )
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

        indicator_cache, perf_ind = self._load_indicator_cache(
            raw_data=raw_data,
            timeframe=timeframe,
            workers=max(1, int(cfg.get("cache_workers", 4) or 4)),
        )
        if not indicator_cache:
            return {"ok": False, "error": "No indicator-ready data for backtest."}

        anchor = next(iter(raw_data.keys()))
        end_date = raw_data[anchor].index.max()
        backtest_days = int(round(months * 365.25 / 12.0))
        start_date = end_date - pd.Timedelta(days=backtest_days)

        t_sim = self._now()
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
        sim_sec = round(self._now() - t_sim, 4)
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
            "perf": {
                **perf_raw,
                **perf_ind,
                "simulate_sec": sim_sec,
            },
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
            prompt = ""
            system_prompt = ""
            try:
                with redirect_stdout(stream), redirect_stderr(stream):
                    prompt = prompt_override if (prompt_override and str(prompt_override).strip()) else self.mod.build_grok_prompt(dashboard_text, datetime_context)
                    system_prompt = (
                        system_prompt_override
                        if (system_prompt_override and str(system_prompt_override).strip())
                        else self._default_system_prompt_for_active_profile()
                    )
                    response = self.mod.call_active_ai_provider(prompt, system_prompt=system_prompt)
            except Exception as exc:
                return {
                    "ok": False,
                    "prompt": prompt,
                    "response": "",
                    "system_prompt": system_prompt,
                    "error": f"{type(exc).__name__}: {exc}",
                    "log": stream.getvalue(),
                }
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

    def _signed_binance_request(
        self,
        method: str,
        endpoint: str,
        path: str,
        api_key: str,
        api_secret: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        q = dict(params or {})
        q["timestamp"] = int(time.time() * 1000)
        q.setdefault("recvWindow", 5000)
        query = urllib.parse.urlencode(q, doseq=True)
        sig = hmac.new(api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
        url = endpoint.rstrip("/") + path + "?" + query + "&signature=" + sig
        headers = {"X-MBX-APIKEY": api_key}

        def _do_req(disable_proxy: bool = False):
            m = method.strip().upper()
            kwargs: Dict[str, Any] = {"headers": headers, "timeout": 30}
            if disable_proxy:
                kwargs["proxies"] = {"http": None, "https": None}
            if m == "POST":
                return requests.post(url, **kwargs)
            if m == "DELETE":
                return requests.delete(url, **kwargs)
            return requests.get(url, **kwargs)

        try:
            r = _do_req(disable_proxy=False)
            if not (200 <= r.status_code < 300):
                return {"ok": False, "status": r.status_code, "error": (r.text or "")[:1200]}
            return {"ok": True, "status": r.status_code, "data": r.json()}
        except requests.exceptions.ProxyError:
            # Retry once with proxies disabled to handle bad local proxy defaults.
            try:
                r = _do_req(disable_proxy=True)
                if not (200 <= r.status_code < 300):
                    return {"ok": False, "status": r.status_code, "error": (r.text or "")[:1200]}
                return {"ok": True, "status": r.status_code, "data": r.json()}
            except Exception as e2:
                return {"ok": False, "status": -1, "error": str(e2)}
        except Exception as e:
            return {"ok": False, "status": -1, "error": str(e)}

    def _signed_binance_get(self, endpoint: str, path: str, api_key: str, api_secret: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self._signed_binance_request("GET", endpoint, path, api_key, api_secret, params=params)

    @staticmethod
    def _to_decimal(v: Any, default: str = "0") -> Decimal:
        try:
            return Decimal(str(v))
        except (InvalidOperation, ValueError, TypeError):
            return Decimal(default)

    @staticmethod
    def _round_to_step(value: Decimal, step: Decimal) -> Decimal:
        if step <= Decimal("0"):
            return value
        # Floor to allowed increment.
        units = (value / step).to_integral_value(rounding=ROUND_DOWN)
        return units * step

    def _get_symbol_exchange_info(self, endpoint: str, symbol: str, ttl_seconds: int = 300) -> Optional[Dict[str, Any]]:
        sym = str(symbol).strip().upper()
        if not sym:
            return None
        key = f"{endpoint.rstrip('/')}|{sym}"
        now = time.time()
        cached = self._binance_exchange_info_cache.get(key)
        if cached:
            ts, payload = cached
            if now - float(ts) <= max(5, int(ttl_seconds)):
                return payload
        try:
            url = endpoint.rstrip("/") + "/api/v3/exchangeInfo"
            r = requests.get(url, params={"symbol": sym}, timeout=20)
            r.raise_for_status()
            data = r.json()
            symbols = data.get("symbols", []) if isinstance(data, dict) else []
            if not isinstance(symbols, list) or not symbols:
                return None
            payload = symbols[0] if isinstance(symbols[0], dict) else None
            if payload:
                self._binance_exchange_info_cache[key] = (now, payload)
            return payload
        except Exception:
            return None

    def _get_last_price(self, endpoint: str, symbol: str) -> Optional[Decimal]:
        sym = str(symbol).strip().upper()
        if not sym:
            return None
        try:
            url = endpoint.rstrip("/") + "/api/v3/ticker/price"
            r = requests.get(url, params={"symbol": sym}, timeout=15)
            r.raise_for_status()
            data = r.json()
            if not isinstance(data, dict):
                return None
            px = self._to_decimal(data.get("price", "0"))
            if px > Decimal("0"):
                return px
            return None
        except Exception:
            return None

    @staticmethod
    def _split_symbol_quote(symbol: str) -> Tuple[str, str]:
        s = str(symbol).strip().upper()
        for q in ["USDT", "USDC", "BUSD", "FDUSD", "USDP", "TUSD", "DAI", "USD", "BTC", "ETH", "BNB"]:
            if s.endswith(q) and len(s) > len(q):
                return s[: -len(q)], q
        return s, "USD"

    def validate_binance_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        quantity: float,
        profile_name: Optional[str] = None,
        price: Optional[float] = None,
        stop_price: Optional[float] = None,
    ) -> Dict[str, Any]:
        sym = str(symbol).strip().upper()
        s = str(side).strip().upper()
        t = str(order_type).strip().upper()
        if not sym or s not in ("BUY", "SELL"):
            return {"ok": False, "error": "Valid symbol and side (BUY/SELL) are required."}
        if t not in ("MARKET", "LIMIT", "STOP_LOSS_LIMIT", "TAKE_PROFIT_LIMIT"):
            return {"ok": False, "error": "order_type must be MARKET, LIMIT, STOP_LOSS_LIMIT, or TAKE_PROFIT_LIMIT."}

        qty = self._to_decimal(quantity)
        if qty <= Decimal("0"):
            return {"ok": False, "error": "Quantity must be > 0."}

        _, profile, _, err = self._resolve_active_binance_profile(profile_name)
        if err:
            return {"ok": False, "error": err}
        endpoint = str((profile or {}).get("endpoint", self._default_binance_endpoint()) or self._default_binance_endpoint())

        info = self._get_symbol_exchange_info(endpoint, sym)
        if not info:
            return {"ok": False, "error": f"Unable to load exchange info for {sym}."}
        filters = info.get("filters", []) if isinstance(info, dict) else []
        if not isinstance(filters, list):
            filters = []

        f_map: Dict[str, Dict[str, Any]] = {}
        for f in filters:
            if isinstance(f, dict):
                k = str(f.get("filterType", "")).strip()
                if k:
                    f_map[k] = f

        lot = f_map.get("LOT_SIZE", {})
        market_lot = f_map.get("MARKET_LOT_SIZE", {})
        price_f = f_map.get("PRICE_FILTER", {})
        min_notional_f = f_map.get("MIN_NOTIONAL", {})
        notional_f = f_map.get("NOTIONAL", {})

        # Binance still enforces LOT_SIZE for quantity stepping; MARKET_LOT_SIZE may add tighter bounds.
        lot_min = self._to_decimal(lot.get("minQty", "0"))
        lot_max = self._to_decimal(lot.get("maxQty", "0"))
        lot_step = self._to_decimal(lot.get("stepSize", "0"))
        mkt_min = self._to_decimal(market_lot.get("minQty", "0")) if isinstance(market_lot, dict) and market_lot else Decimal("0")
        mkt_max = self._to_decimal(market_lot.get("maxQty", "0")) if isinstance(market_lot, dict) and market_lot else Decimal("0")
        mkt_step = self._to_decimal(market_lot.get("stepSize", "0")) if isinstance(market_lot, dict) and market_lot else Decimal("0")

        step_qty = lot_step if lot_step > Decimal("0") else mkt_step
        norm_qty = self._round_to_step(qty, step_qty) if step_qty > Decimal("0") else qty
        if norm_qty <= Decimal("0"):
            return {"ok": False, "error": f"Quantity too small after step-size normalization (step {step_qty})."}

        min_qty = lot_min
        max_qty = lot_max
        if t == "MARKET":
            if mkt_min > Decimal("0"):
                min_qty = max(min_qty, mkt_min) if min_qty > Decimal("0") else mkt_min
            if mkt_max > Decimal("0"):
                max_qty = min(max_qty, mkt_max) if max_qty > Decimal("0") else mkt_max

        if min_qty > Decimal("0") and norm_qty < min_qty:
            return {"ok": False, "error": f"Quantity {norm_qty} below minQty {min_qty}."}
        if max_qty > Decimal("0") and norm_qty > max_qty:
            return {"ok": False, "error": f"Quantity {norm_qty} above maxQty {max_qty}."}

        norm_price: Optional[Decimal] = None
        norm_stop: Optional[Decimal] = None
        if t in ("LIMIT", "STOP_LOSS_LIMIT", "TAKE_PROFIT_LIMIT"):
            p = self._to_decimal(price or 0)
            if p <= Decimal("0"):
                return {"ok": False, "error": f"{t} orders require positive price."}
            tick = self._to_decimal(price_f.get("tickSize", "0"))
            min_p = self._to_decimal(price_f.get("minPrice", "0"))
            max_p = self._to_decimal(price_f.get("maxPrice", "0"))
            norm_price = self._round_to_step(p, tick) if tick > Decimal("0") else p
            if min_p > Decimal("0") and norm_price < min_p:
                return {"ok": False, "error": f"Price {norm_price} below minPrice {min_p}."}
            if max_p > Decimal("0") and norm_price > max_p:
                return {"ok": False, "error": f"Price {norm_price} above maxPrice {max_p}."}
            if t in ("STOP_LOSS_LIMIT", "TAKE_PROFIT_LIMIT"):
                sp = self._to_decimal(stop_price or 0)
                if sp <= Decimal("0"):
                    return {"ok": False, "error": f"{t} orders require positive stop_price."}
                norm_stop = self._round_to_step(sp, tick) if tick > Decimal("0") else sp
                if min_p > Decimal("0") and norm_stop < min_p:
                    return {"ok": False, "error": f"Stop price {norm_stop} below minPrice {min_p}."}
                if max_p > Decimal("0") and norm_stop > max_p:
                    return {"ok": False, "error": f"Stop price {norm_stop} above maxPrice {max_p}."}

        # Notional checks (order value).
        check_price = norm_price
        if check_price is None:
            check_price = self._get_last_price(endpoint, sym)
        if check_price is not None and check_price > Decimal("0"):
            notion = norm_qty * check_price
            min_notional = Decimal("0")
            max_notional = Decimal("0")
            apply_min = True
            apply_max = False

            if isinstance(min_notional_f, dict) and min_notional_f:
                min_notional = self._to_decimal(min_notional_f.get("minNotional", "0"))
                if t == "MARKET":
                    apply_min = bool(min_notional_f.get("applyToMarket", True))
            if isinstance(notional_f, dict) and notional_f:
                mn = self._to_decimal(notional_f.get("minNotional", "0"))
                mx = self._to_decimal(notional_f.get("maxNotional", "0"))
                if mn > Decimal("0"):
                    min_notional = mn
                if mx > Decimal("0"):
                    max_notional = mx
                if t == "MARKET":
                    apply_min = bool(notional_f.get("applyMinToMarket", True))
                    apply_max = bool(notional_f.get("applyMaxToMarket", False))
                else:
                    apply_min = True
                    apply_max = max_notional > Decimal("0")

            if apply_min and min_notional > Decimal("0") and notion < min_notional:
                return {
                    "ok": False,
                    "error": f"Order notional {notion:.8f} below minNotional {min_notional}. Increase quantity.",
                }
            if apply_max and max_notional > Decimal("0") and notion > max_notional:
                return {
                    "ok": False,
                    "error": f"Order notional {notion:.8f} above maxNotional {max_notional}. Reduce quantity.",
                }

        return {
            "ok": True,
            "symbol": sym,
            "side": s,
            "order_type": t,
            "normalized_quantity": float(norm_qty),
            "normalized_price": (float(norm_price) if norm_price is not None else None),
            "normalized_stop_price": (float(norm_stop) if norm_stop is not None else None),
            "note": "Validated against Binance symbol filters.",
        }

    def get_binance_last_price(self, symbol: str, profile_name: Optional[str] = None) -> Dict[str, Any]:
        with self._lock:
            sym = str(symbol).strip().upper()
            if not sym:
                return {"ok": False, "error": "Symbol is required."}
            _, profile, _, err = self._resolve_active_binance_profile(profile_name)
            if err:
                return {"ok": False, "error": err}
            endpoint = str((profile or {}).get("endpoint", self._default_binance_endpoint()) or self._default_binance_endpoint())
            px = self._get_last_price(endpoint, sym)
            if px is None or px <= Decimal("0"):
                return {"ok": False, "error": f"Unable to fetch last price for {sym}."}
            return {"ok": True, "symbol": sym, "price": float(px)}

    def get_binance_symbol_filters(self, symbol: str, profile_name: Optional[str] = None) -> Dict[str, Any]:
        with self._lock:
            sym = str(symbol).strip().upper()
            if not sym:
                return {"ok": False, "error": "Symbol is required."}
            _, profile, _, err = self._resolve_active_binance_profile(profile_name)
            if err:
                return {"ok": False, "error": err}
            endpoint = str((profile or {}).get("endpoint", self._default_binance_endpoint()) or self._default_binance_endpoint())
            info = self._get_symbol_exchange_info(endpoint, sym)
            if not info:
                return {"ok": False, "error": f"Unable to load exchange info for {sym}."}
            filters = info.get("filters", []) if isinstance(info, dict) else []
            if not isinstance(filters, list):
                filters = []
            f_map: Dict[str, Dict[str, Any]] = {}
            for f in filters:
                if isinstance(f, dict):
                    k = str(f.get("filterType", "")).strip()
                    if k:
                        f_map[k] = f
            min_notional = 0.0
            mn = f_map.get("MIN_NOTIONAL", {})
            nt = f_map.get("NOTIONAL", {})
            try:
                if isinstance(mn, dict):
                    min_notional = max(min_notional, float(mn.get("minNotional", 0.0) or 0.0))
            except Exception:
                pass
            try:
                if isinstance(nt, dict):
                    min_notional = max(min_notional, float(nt.get("minNotional", 0.0) or 0.0))
            except Exception:
                pass
            return {"ok": True, "symbol": sym, "min_notional": float(min_notional)}

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

    def _resolve_active_binance_profile(self, profile_name: Optional[str] = None) -> Tuple[Optional[str], Optional[Dict[str, Any]], Optional[Dict[str, str]], Optional[str]]:
        prefs = load_binance_preferences()
        secrets = load_binance_secrets()
        profiles = prefs.get("profiles", {}) if isinstance(prefs, dict) else {}
        if not isinstance(profiles, dict) or not profiles:
            return None, None, None, "No Binance profiles configured."
        active = str(profile_name or prefs.get("active_profile", "") or "").strip()
        if not active or active not in profiles:
            active = next(iter(profiles.keys()))
        profile = profiles.get(active, {})
        if not isinstance(profile, dict):
            return None, None, None, f"Profile not found: {active}"
        creds = self._resolve_binance_credentials(profile, secrets)
        if not creds["api_key"] or not creds["api_secret"]:
            return None, None, None, f"Missing API key/secret ({creds['key_source']}, {creds['secret_source']})."
        return active, profile, creds, None

    def list_open_binance_orders(self, profile_name: Optional[str] = None, symbol: str = "") -> Dict[str, Any]:
        with self._lock:
            active, profile, creds, err = self._resolve_active_binance_profile(profile_name)
            if err:
                return {"ok": False, "error": err}
            endpoint = str((profile or {}).get("endpoint", self._default_binance_endpoint()) or self._default_binance_endpoint())
            params: Dict[str, Any] = {}
            sym = str(symbol).strip().upper()
            if sym:
                params["symbol"] = sym
            out = self._signed_binance_get(endpoint, "/api/v3/openOrders", str(creds["api_key"]), str(creds["api_secret"]), params=params)
            if not out.get("ok"):
                return {"ok": False, "status": out.get("status"), "error": out.get("error", "openOrders request failed")}
            data = out.get("data", [])
            rows: List[Dict[str, Any]] = []
            if isinstance(data, list):
                for o in data:
                    if not isinstance(o, dict):
                        continue
                    rows.append(
                        {
                            "symbol": str(o.get("symbol", "") or ""),
                            "orderId": int(o.get("orderId", 0) or 0),
                            "side": str(o.get("side", "") or ""),
                            "type": str(o.get("type", "") or ""),
                            "status": str(o.get("status", "") or ""),
                            "price": str(o.get("price", "") or ""),
                            "stopPrice": str(o.get("stopPrice", "") or ""),
                            "origQty": str(o.get("origQty", "") or ""),
                            "executedQty": str(o.get("executedQty", "") or ""),
                            "time": int(o.get("time", 0) or 0),
                            "updateTime": int(o.get("updateTime", 0) or 0),
                        }
                    )
            return {"ok": True, "profile": active, "orders": rows}

    def cancel_binance_order(self, symbol: str, order_id: int, profile_name: Optional[str] = None) -> Dict[str, Any]:
        with self._lock:
            sym = str(symbol).strip().upper()
            if not sym:
                return {"ok": False, "error": "Symbol is required."}
            oid = int(order_id or 0)
            if oid <= 0:
                return {"ok": False, "error": "order_id must be > 0."}
            active, profile, creds, err = self._resolve_active_binance_profile(profile_name)
            if err:
                return {"ok": False, "error": err}
            endpoint = str((profile or {}).get("endpoint", self._default_binance_endpoint()) or self._default_binance_endpoint())
            out = self._signed_binance_request(
                "DELETE",
                endpoint,
                "/api/v3/order",
                str(creds["api_key"]),
                str(creds["api_secret"]),
                params={"symbol": sym, "orderId": oid},
            )
            if not out.get("ok"):
                return {"ok": False, "status": out.get("status"), "error": out.get("error", "cancel request failed")}
            return {"ok": True, "profile": active, "data": out.get("data", {})}

    def submit_binance_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        quantity: float,
        profile_name: Optional[str] = None,
        price: Optional[float] = None,
        stop_price: Optional[float] = None,
        time_in_force: str = "GTC",
    ) -> Dict[str, Any]:
        with self._lock:
            v = self.validate_binance_order(
                symbol=symbol,
                side=side,
                order_type=order_type,
                quantity=quantity,
                profile_name=profile_name,
                price=price,
                stop_price=stop_price,
            )
            if not v.get("ok"):
                return {"ok": False, "error": v.get("error", "Binance order validation failed.")}
            sym = str(v.get("symbol", "")).upper()
            s = str(v.get("side", "")).upper()
            t = str(v.get("order_type", "")).upper()
            qty = float(v.get("normalized_quantity", 0.0) or 0.0)
            px_norm = v.get("normalized_price", None)
            sp_norm = v.get("normalized_stop_price", None)
            active, profile, creds, err = self._resolve_active_binance_profile(profile_name)
            if err:
                return {"ok": False, "error": err}
            endpoint = str((profile or {}).get("endpoint", self._default_binance_endpoint()) or self._default_binance_endpoint())
            params: Dict[str, Any] = {
                "symbol": sym,
                "side": s,
                "type": t,
                "quantity": f"{qty:.8f}".rstrip("0").rstrip("."),
            }
            if t in ("LIMIT", "STOP_LOSS_LIMIT", "TAKE_PROFIT_LIMIT"):
                try:
                    px = float(px_norm if px_norm is not None else (price or 0.0))
                except Exception:
                    px = 0.0
                if px <= 0:
                    return {"ok": False, "error": f"{t} orders require positive price."}
                params["price"] = f"{px:.8f}".rstrip("0").rstrip(".")
                params["timeInForce"] = str(time_in_force or "GTC").upper()
                if t in ("STOP_LOSS_LIMIT", "TAKE_PROFIT_LIMIT"):
                    try:
                        sp = float(sp_norm if sp_norm is not None else (stop_price or 0.0))
                    except Exception:
                        sp = 0.0
                    if sp <= 0:
                        return {"ok": False, "error": f"{t} orders require positive stop_price."}
                    params["stopPrice"] = f"{sp:.8f}".rstrip("0").rstrip(".")
            out = self._signed_binance_request(
                "POST",
                endpoint,
                "/api/v3/order",
                str(creds["api_key"]),
                str(creds["api_secret"]),
                params=params,
            )
            if not out.get("ok"):
                return {"ok": False, "status": out.get("status"), "error": out.get("error", "order submit failed")}
            return {
                "ok": True,
                "profile": active,
                "data": out.get("data", {}),
                "normalized_quantity": qty,
                "normalized_price": px_norm,
                "normalized_stop_price": sp_norm,
            }

    def submit_binance_oco_sell(
        self,
        symbol: str,
        quantity: float,
        take_profit_price: float,
        stop_price: float,
        stop_limit_price: Optional[float] = None,
        profile_name: Optional[str] = None,
        stop_limit_time_in_force: str = "GTC",
    ) -> Dict[str, Any]:
        with self._lock:
            sym = str(symbol).strip().upper()
            if not sym:
                return {"ok": False, "error": "Symbol is required."}
            try:
                tp = float(take_profit_price or 0.0)
                sp = float(stop_price or 0.0)
                slp = float(stop_limit_price if stop_limit_price is not None else (sp * 0.995))
                qty = float(quantity or 0.0)
            except Exception:
                return {"ok": False, "error": "Invalid OCO numeric inputs."}
            if qty <= 0 or tp <= 0 or sp <= 0 or slp <= 0:
                return {"ok": False, "error": "OCO requires qty>0, take_profit_price>0, stop_price>0, stop_limit_price>0."}

            # Normalize and validate quantity + prices using existing filters.
            v_lim = self.validate_binance_order(
                symbol=sym,
                side="SELL",
                order_type="LIMIT",
                quantity=qty,
                profile_name=profile_name,
                price=tp,
            )
            if not v_lim.get("ok"):
                return {"ok": False, "error": f"OCO limit leg invalid: {v_lim.get('error', 'unknown')}"}

            v_stop = self.validate_binance_order(
                symbol=sym,
                side="SELL",
                order_type="STOP_LOSS_LIMIT",
                quantity=float(v_lim.get("normalized_quantity", qty) or qty),
                profile_name=profile_name,
                price=slp,
                stop_price=sp,
            )
            if not v_stop.get("ok"):
                return {"ok": False, "error": f"OCO stop leg invalid: {v_stop.get('error', 'unknown')}"}

            nq = float(v_lim.get("normalized_quantity", qty) or qty)
            tp_norm = float(v_lim.get("normalized_price", tp) or tp)
            sp_norm = float(v_stop.get("normalized_stop_price", sp) or sp)
            slp_norm = float(v_stop.get("normalized_price", slp) or slp)

            active, profile, creds, err = self._resolve_active_binance_profile(profile_name)
            if err:
                return {"ok": False, "error": err}
            endpoint = str((profile or {}).get("endpoint", self._default_binance_endpoint()) or self._default_binance_endpoint())
            params: Dict[str, Any] = {
                "symbol": sym,
                "side": "SELL",
                "quantity": f"{nq:.8f}".rstrip("0").rstrip("."),
                "price": f"{tp_norm:.8f}".rstrip("0").rstrip("."),
                "stopPrice": f"{sp_norm:.8f}".rstrip("0").rstrip("."),
                "stopLimitPrice": f"{slp_norm:.8f}".rstrip("0").rstrip("."),
                "stopLimitTimeInForce": str(stop_limit_time_in_force or "GTC").upper(),
            }
            out = self._signed_binance_request(
                "POST",
                endpoint,
                "/api/v3/order/oco",
                str(creds["api_key"]),
                str(creds["api_secret"]),
                params=params,
            )
            if not out.get("ok"):
                return {"ok": False, "status": out.get("status"), "error": out.get("error", "OCO submit failed")}
            return {
                "ok": True,
                "profile": active,
                "data": out.get("data", {}),
                "normalized_quantity": nq,
                "normalized_take_profit_price": tp_norm,
                "normalized_stop_price": sp_norm,
                "normalized_stop_limit_price": slp_norm,
            }

    def analyze_open_positions_multi_tf(
        self,
        profile_name: Optional[str] = None,
        timeframes: Optional[List[str]] = None,
        display_currency: str = "USD",
    ) -> Dict[str, Any]:
        with self._lock:
            tf_list = [str(x).strip().lower() for x in (timeframes or ["4h", "8h", "12h", "1d"]) if str(x).strip()]
            if not tf_list:
                tf_list = ["4h", "8h", "12h", "1d"]

            ledger = load_trade_ledger()
            if not isinstance(ledger, dict):
                ledger = default_trade_ledger()
            open_positions = ledger.get("open_positions", {})
            if not isinstance(open_positions, dict) or not open_positions:
                return {"ok": True, "rows": [], "note": "No open positions in ledger."}

            _, profile, _, err = self._resolve_active_binance_profile(profile_name)
            if err:
                return {"ok": False, "error": err}
            endpoint = str((profile or {}).get("endpoint", self._default_binance_endpoint()) or self._default_binance_endpoint())

            rows: List[Dict[str, Any]] = []
            for _, pos in open_positions.items():
                if not isinstance(pos, dict):
                    continue
                base = str(pos.get("asset", "")).strip().upper()
                quote = str(pos.get("quote_currency", "USDT")).strip().upper() or "USDT"
                if not base:
                    continue
                symbol = f"{base}{quote}"
                info = self._get_symbol_exchange_info(endpoint, symbol)
                if not info:
                    # skip non-Binance spot symbols in this monitor.
                    continue
                tf_actions: Dict[str, str] = {}
                tf_scores: Dict[str, str] = {}
                for tf in tf_list:
                    tkr = f"{base}-{quote}"
                    df = self.mod.fetch_with_cache(tkr, tf, is_crypto=True, years=2.2)
                    if df is None or len(df) < 60:
                        tf_actions[tf] = "N/A"
                        tf_scores[tf] = "N/A"
                        continue
                    enriched = self.mod.build_indicator_cache({tkr: df}, tf, max_workers=1, verbose=False)
                    if not enriched:
                        tf_actions[tf] = "N/A"
                        tf_scores[tf] = "N/A"
                        continue
                    table, _ = self.mod.build_live_tables(enriched, True, self.mod.TunedParams(), tf)
                    if table is None or table.empty:
                        tf_actions[tf] = "N/A"
                        tf_scores[tf] = "N/A"
                        continue
                    row = table.iloc[0]
                    tf_actions[tf] = str(row.get("Action", "N/A"))
                    tf_scores[tf] = str(row.get("Score", "N/A"))

                sell_votes = sum([1 for v in tf_actions.values() if "SELL" in str(v).upper()])
                hold_votes = sum([1 for v in tf_actions.values() if "HOLD" in str(v).upper()])
                buy_votes = sum([1 for v in tf_actions.values() if "BUY" in str(v).upper()])
                stance = "HOLD"
                if sell_votes >= 3:
                    stance = "REDUCE/EXIT"
                elif buy_votes >= 3:
                    stance = "HOLD/ADD"
                rows.append(
                    {
                        "asset": base,
                        "symbol": symbol,
                        "qty": float(pos.get("qty", 0.0) or 0.0),
                        "entry_price": float(pos.get("entry_price", 0.0) or 0.0),
                        "stance": stance,
                        "buy_votes": buy_votes,
                        "hold_votes": hold_votes,
                        "sell_votes": sell_votes,
                        "actions": tf_actions,
                        "scores": tf_scores,
                        "display_currency": str(display_currency or "USD").strip().upper() or "USD",
                    }
                )

            rows = sorted(rows, key=lambda r: int(r.get("sell_votes", 0)), reverse=True)
            return {"ok": True, "rows": rows}

    def evaluate_agent_policy(
        self,
        profile_name: Optional[str],
        display_currency: str,
        max_daily_loss_pct: float,
        max_trades_per_day: int,
        max_exposure_pct: float,
        pending_buy_count: int = 0,
    ) -> Dict[str, Any]:
        with self._lock:
            dcur = str(display_currency or "USD").strip().upper() or "USD"
            daily_loss_limit = max(0.0, float(max_daily_loss_pct))
            trades_limit = max(1, int(max_trades_per_day))
            exposure_limit = max(0.0, min(100.0, float(max_exposure_pct)))
            usd_like = {"USD", "USDT", "USDC", "BUSD", "FDUSD", "TUSD", "USDP", "DAI"}

            port = self.fetch_binance_portfolio(profile_name=profile_name)
            if not port.get("ok"):
                return {"ok": False, "error": str(port.get("error", "Failed to fetch portfolio for policy checks."))}
            total_usd = float(port.get("total_est_usd", 0.0) or 0.0)
            try:
                fx = float(self.mod.get_usd_to_currency_rate(dcur))
            except Exception:
                fx = 1.0
            total_display = total_usd * fx if dcur != "USD" else total_usd
            if total_display <= 0:
                return {"ok": False, "error": "Portfolio value is zero; cannot evaluate policy limits."}

            ledger = load_trade_ledger()
            if not isinstance(ledger, dict):
                ledger = default_trade_ledger()
            entries = ledger.get("entries", [])
            if not isinstance(entries, list):
                entries = []
            open_positions = ledger.get("open_positions", {})
            if not isinstance(open_positions, dict):
                open_positions = {}

            today = pd.Timestamp.utcnow().date()
            trades_today = 0
            realized_today = 0.0
            for e in entries:
                if not isinstance(e, dict):
                    continue
                if not bool(e.get("is_execution", False)):
                    continue
                ts = str(e.get("ts", "") or "").strip()
                try:
                    d = pd.to_datetime(ts, utc=True).date()
                except Exception:
                    continue
                if d != today:
                    continue
                trades_today += 1
                if str(e.get("action", "")).strip().upper() == "SELL":
                    try:
                        pdv = float(e.get("pnl_display", 0.0) or 0.0)
                    except Exception:
                        pdv = 0.0
                    if pdv == 0.0:
                        try:
                            pq = float(e.get("pnl_quote", 0.0) or 0.0)
                        except Exception:
                            pq = 0.0
                        qc = str(e.get("quote_currency", "USD") or "USD").strip().upper()
                        if qc == dcur:
                            pdv = pq
                        elif qc in usd_like:
                            pdv = pq * fx
                    realized_today += pdv

            day_loss_pct = 0.0
            if realized_today < 0:
                day_loss_pct = abs(realized_today) / total_display * 100.0

            active, profile, _, err = self._resolve_active_binance_profile(profile_name)
            if err:
                return {"ok": False, "error": err}
            endpoint = str((profile or {}).get("endpoint", self._default_binance_endpoint()) or self._default_binance_endpoint())
            open_exposure_display = 0.0
            for _, pos in open_positions.items():
                if not isinstance(pos, dict):
                    continue
                asset = str(pos.get("asset", "")).strip().upper()
                quote = str(pos.get("quote_currency", "USDT")).strip().upper() or "USDT"
                try:
                    qty = float(pos.get("qty", 0.0) or 0.0)
                except Exception:
                    qty = 0.0
                if not asset or qty <= 0:
                    continue
                sym = f"{asset}{quote}"
                px = self._get_last_price(endpoint, sym)
                if px is None or px <= Decimal("0"):
                    continue
                val_quote = qty * float(px)
                if quote == dcur:
                    open_exposure_display += val_quote
                elif quote in usd_like:
                    open_exposure_display += val_quote * fx

            exposure_pct = (open_exposure_display / total_display * 100.0) if total_display > 0 else 0.0

            reasons: List[str] = []
            if trades_today >= trades_limit:
                reasons.append(f"Trades today {trades_today} reached limit {trades_limit}.")
            if day_loss_pct >= daily_loss_limit > 0:
                reasons.append(f"Daily realized loss {day_loss_pct:.2f}% reached limit {daily_loss_limit:.2f}%.")
            if pending_buy_count > 0 and exposure_pct >= exposure_limit > 0:
                reasons.append(f"Current exposure {exposure_pct:.2f}% reached limit {exposure_limit:.2f}%; blocking new BUYs.")

            return {
                "ok": len(reasons) == 0,
                "profile": active,
                "display_currency": dcur,
                "total_value_display": total_display,
                "trades_today": trades_today,
                "realized_today_display": realized_today,
                "day_loss_pct": day_loss_pct,
                "open_exposure_display": open_exposure_display,
                "exposure_pct": exposure_pct,
                "reasons": reasons,
            }

    def reconcile_binance_fills(
        self,
        profile_name: Optional[str] = None,
        symbols: Optional[List[str]] = None,
        max_trades_per_symbol: int = 200,
        display_currency: str = "USD",
    ) -> Dict[str, Any]:
        with self._lock:
            active, profile, creds, err = self._resolve_active_binance_profile(profile_name)
            if err:
                return {"ok": False, "error": err}
            endpoint = str((profile or {}).get("endpoint", self._default_binance_endpoint()) or self._default_binance_endpoint())
            syms_in = symbols or []
            syms: List[str] = []
            seen_syms: set = set()
            for s in syms_in:
                sym = str(s).strip().upper()
                if not sym or sym in seen_syms:
                    continue
                seen_syms.add(sym)
                syms.append(sym)
            if not syms:
                return {"ok": False, "error": "No symbols provided for reconciliation."}

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

            existing_trade_keys: set = set()
            for e in entries:
                if not isinstance(e, dict):
                    continue
                k = str(e.get("exchange_trade_id", "") or "").strip()
                if k:
                    existing_trade_keys.add(k)

            fetched = 0
            duplicates = 0
            added = 0
            errors: List[str] = []
            fill_events: List[Dict[str, Any]] = []

            lim = max(1, min(1000, int(max_trades_per_symbol or 200)))
            for sym in syms:
                out = self._signed_binance_get(
                    endpoint,
                    "/api/v3/myTrades",
                    str(creds["api_key"]),
                    str(creds["api_secret"]),
                    params={"symbol": sym, "limit": lim},
                )
                if not out.get("ok"):
                    errors.append(f"{sym}: {out.get('error', 'myTrades request failed')}")
                    continue
                data = out.get("data", [])
                if not isinstance(data, list):
                    continue
                fetched += len(data)
                for tr in data:
                    if not isinstance(tr, dict):
                        continue
                    tid = str(tr.get("id", "") or "").strip()
                    if not tid:
                        continue
                    trade_key = f"{sym}:{tid}"
                    if trade_key in existing_trade_keys:
                        duplicates += 1
                        continue
                    try:
                        qty = float(tr.get("qty", 0.0) or 0.0)
                        px = float(tr.get("price", 0.0) or 0.0)
                        tms = int(tr.get("time", 0) or 0)
                    except Exception:
                        continue
                    if qty <= 0 or px <= 0 or tms <= 0:
                        continue
                    side = "BUY" if bool(tr.get("isBuyer", False)) else "SELL"
                    base, quote = self._split_symbol_quote(sym)
                    fill_events.append(
                        {
                            "trade_key": trade_key,
                            "order_id": int(tr.get("orderId", 0) or 0),
                            "symbol": sym,
                            "asset": base,
                            "quote": quote,
                            "side": side,
                            "qty": qty,
                            "price": px,
                            "ts_ms": tms,
                            "ts": pd.to_datetime(tms, unit="ms", utc=True).strftime("%Y-%m-%dT%H:%M:%SZ"),
                        }
                    )

            fill_events.sort(key=lambda x: int(x.get("ts_ms", 0) or 0))
            dcur = str(display_currency or "USD").strip().upper() or "USD"
            usd_like = {"USD", "USDT", "USDC", "BUSD", "FDUSD", "TUSD", "USDP", "DAI"}

            for ev in fill_events:
                trade_key = str(ev.get("trade_key", "") or "").strip()
                if not trade_key or trade_key in existing_trade_keys:
                    continue
                matched_idx: Optional[int] = None
                target_order_id = int(ev.get("order_id", 0) or 0)
                for idx in range(len(entries) - 1, -1, -1):
                    e = entries[idx]
                    if not isinstance(e, dict):
                        continue
                    if not bool(e.get("is_execution", False)):
                        continue
                    if str(e.get("action", "")).upper() != str(ev.get("side", "")).upper():
                        continue
                    if int(e.get("exchange_order_id", 0) or 0) != target_order_id:
                        continue
                    if str(e.get("exchange_symbol", "")).strip().upper() not in ("", str(ev.get("symbol", "")).upper()):
                        continue
                    is_placeholder = bool(e.get("is_placeholder", False))
                    try:
                        cur_px = float(e.get("price", 0.0) or 0.0)
                    except Exception:
                        cur_px = 0.0
                    if is_placeholder or cur_px <= 0:
                        matched_idx = idx
                        break

                if matched_idx is not None:
                    rec = dict(entries[matched_idx]) if isinstance(entries[matched_idx], dict) else {}
                    entry_id = int(rec.get("id", 0) or 0) or (matched_idx + 1)
                    rec.update(
                        {
                            "id": entry_id,
                            "ts": ev.get("ts", ""),
                            "market": "crypto",
                            "timeframe": "spot",
                            "panel": "binance_reconcile",
                            "asset": str(ev.get("asset", "")).upper(),
                            "action": str(ev.get("side", "")).upper(),
                            "price": float(ev.get("price", 0.0) or 0.0),
                            "qty": float(ev.get("qty", 0.0) or 0.0),
                            "score": "",
                            "note": f"Reconciled Binance fill ({ev.get('symbol', '')})",
                            "is_execution": True,
                            "quote_currency": str(ev.get("quote", "USD")).upper(),
                            "display_currency": dcur,
                            "exchange_symbol": str(ev.get("symbol", "")).upper(),
                            "exchange_trade_id": trade_key,
                            "exchange_order_id": int(ev.get("order_id", 0) or 0),
                            "is_placeholder": False,
                        }
                    )
                    entries[matched_idx] = rec
                else:
                    entry_id = len(entries) + 1
                    rec = {
                        "id": entry_id,
                        "ts": ev.get("ts", ""),
                        "market": "crypto",
                        "timeframe": "spot",
                        "panel": "binance_reconcile",
                        "asset": str(ev.get("asset", "")).upper(),
                        "action": str(ev.get("side", "")).upper(),
                        "price": float(ev.get("price", 0.0) or 0.0),
                        "qty": float(ev.get("qty", 0.0) or 0.0),
                        "score": "",
                        "note": f"Reconciled Binance fill ({ev.get('symbol', '')})",
                        "is_execution": True,
                        "quote_currency": str(ev.get("quote", "USD")).upper(),
                        "display_currency": dcur,
                        "exchange_symbol": str(ev.get("symbol", "")).upper(),
                        "exchange_trade_id": trade_key,
                        "exchange_order_id": int(ev.get("order_id", 0) or 0),
                        "is_placeholder": False,
                    }
                    entries.append(rec)
                    added += 1

                existing_trade_keys.add(trade_key)

                guard_key = f"crypto|spot|{rec['asset']}|{rec['action']}"
                activity_guard[guard_key] = {"ts": rec["ts"], "id": entry_id}

                pos_key = f"crypto|spot|{rec['asset']}"
                open_pos = open_positions.get(pos_key)
                if rec["action"] == "BUY":
                    open_positions[pos_key] = {
                        "entry_id": entry_id,
                        "entry_ts": rec["ts"],
                        "entry_price": rec["price"],
                        "qty": rec["qty"],
                        "quote_currency": rec["quote_currency"],
                        "display_currency": rec["display_currency"],
                        "asset": rec["asset"],
                        "market": "crypto",
                        "timeframe": "spot",
                        "panel": "binance_reconcile",
                    }
                elif rec["action"] == "SELL":
                    if isinstance(open_pos, dict):
                        rec["closed_entry_id"] = int(open_pos.get("entry_id", 0) or 0)
                        try:
                            entry_price = float(open_pos.get("entry_price", 0.0) or 0.0)
                        except Exception:
                            entry_price = 0.0
                        if entry_price <= 0:
                            # Fallback: best-effort link to most recent BUY execution with non-zero price.
                            for prev in reversed(entries):
                                if not isinstance(prev, dict):
                                    continue
                                if int(prev.get("id", 0) or 0) == int(entry_id):
                                    continue
                                if not bool(prev.get("is_execution", False)):
                                    continue
                                if str(prev.get("action", "")).upper() != "BUY":
                                    continue
                                if str(prev.get("asset", "")).upper() != str(rec.get("asset", "")).upper():
                                    continue
                                try:
                                    prev_px = float(prev.get("price", 0.0) or 0.0)
                                except Exception:
                                    prev_px = 0.0
                                if prev_px > 0:
                                    entry_price = prev_px
                                    rec["closed_entry_id"] = int(prev.get("id", rec.get("closed_entry_id", 0)) or 0)
                                    break
                        if entry_price > 0 and rec["price"] > 0:
                            rec["pnl_pct"] = round(((rec["price"] - entry_price) / entry_price) * 100.0, 4)
                            pnl_quote = (rec["price"] - entry_price) * max(0.0, float(rec["qty"]))
                            rec["pnl_quote"] = round(float(pnl_quote), 8)
                            qc = str(rec.get("quote_currency", "USD")).upper()
                            if qc == dcur:
                                rec["pnl_display"] = round(float(pnl_quote), 8)
                            elif qc in usd_like:
                                try:
                                    fx = float(self.mod.get_usd_to_currency_rate(dcur))
                                except Exception:
                                    fx = 1.0
                                rec["fx_usd_to_display"] = fx
                                rec["pnl_display"] = round(float(pnl_quote) * fx, 8)
                        open_positions.pop(pos_key, None)

            ledger["entries"] = entries
            ledger["open_positions"] = open_positions
            ledger["activity_guard"] = activity_guard
            save_trade_ledger(ledger)
            return {
                "ok": True,
                "profile": active,
                "symbols": syms,
                "fetched_trades": fetched,
                "duplicates_skipped": duplicates,
                "added_entries": added,
                "errors": errors[:20],
            }

    def get_trade_ledger(self) -> Dict[str, Any]:
        with self._lock:
            ledger = load_trade_ledger()
            if not isinstance(ledger, dict):
                ledger = default_trade_ledger()
            entries = ledger.get("entries", [])
            if not isinstance(entries, list):
                entries = []
            open_positions = ledger.get("open_positions", {})
            if not isinstance(open_positions, dict):
                open_positions = {}
            activity_guard = ledger.get("activity_guard", {})
            if not isinstance(activity_guard, dict):
                activity_guard = {}

            # Backfill older rows where is_execution was missing.
            entries_changed = False
            fixed_entries: List[Dict[str, Any]] = []
            for e in entries:
                if not isinstance(e, dict):
                    continue
                ee = dict(e)
                if "is_execution" not in ee:
                    ee["is_execution"] = False
                    entries_changed = True
                else:
                    ee["is_execution"] = bool(ee.get("is_execution", False))
                fixed_entries.append(ee)

            cleaned: Dict[str, Any] = {}
            changed = False
            for k, v in open_positions.items():
                if not isinstance(k, str) or not isinstance(v, dict):
                    changed = True
                    continue
                try:
                    qty = float(v.get("qty", 0.0) or 0.0)
                except Exception:
                    qty = 0.0
                if qty <= 0:
                    changed = True
                    continue
                cleaned[k] = v
            if changed or entries_changed:
                ledger["entries"] = fixed_entries
                ledger["open_positions"] = cleaned
                ledger["activity_guard"] = activity_guard
                save_trade_ledger(ledger)
            signal_entries = [x for x in fixed_entries if not bool(x.get("is_execution", False))]
            execution_entries = [x for x in fixed_entries if bool(x.get("is_execution", False))]
            return {
                "ok": True,
                "ledger": ledger,
                "signal_entries": signal_entries,
                "execution_entries": execution_entries,
            }

    def prune_signal_only_history(self, keep_last_signals: int = 0) -> Dict[str, Any]:
        with self._lock:
            ledger = load_trade_ledger()
            if not isinstance(ledger, dict):
                ledger = default_trade_ledger()
            entries = ledger.get("entries", [])
            if not isinstance(entries, list):
                entries = []
            execution_entries: List[Dict[str, Any]] = []
            signal_entries: List[Dict[str, Any]] = []
            for e in entries:
                if not isinstance(e, dict):
                    continue
                if bool(e.get("is_execution", False)):
                    execution_entries.append(e)
                else:
                    signal_entries.append(e)
            keep_n = max(0, int(keep_last_signals))
            if keep_n > 0:
                signal_keep = signal_entries[-keep_n:]
            else:
                signal_keep = []
            new_entries = execution_entries + signal_keep
            # Rebuild ids sequentially.
            for i, e in enumerate(new_entries, 1):
                if isinstance(e, dict):
                    e["id"] = i
            ledger["entries"] = new_entries
            # Also clean open positions to execution-only sane rows.
            open_positions = ledger.get("open_positions", {})
            if not isinstance(open_positions, dict):
                open_positions = {}
            clean_open = {}
            for k, v in open_positions.items():
                if not isinstance(k, str) or not isinstance(v, dict):
                    continue
                try:
                    q = float(v.get("qty", 0.0) or 0.0)
                except Exception:
                    q = 0.0
                if q > 0:
                    clean_open[k] = v
            ledger["open_positions"] = clean_open
            save_trade_ledger(ledger)
            return {
                "ok": True,
                "removed_signal_entries": max(0, len(signal_entries) - len(signal_keep)),
                "kept_signal_entries": len(signal_keep),
                "execution_entries": len(execution_entries),
                "total_entries": len(new_entries),
            }

    def record_signal_event(
        self,
        event: Dict[str, Any],
        cooldown_minutes: int = 240,
        allow_duplicate: bool = False,
        guard_hold_signals: bool = True,
    ) -> Dict[str, Any]:
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
            is_execution = bool(event.get("is_execution", False))
            quote_currency = str(event.get("quote_currency", "USD")).strip().upper() or "USD"
            display_currency = str(event.get("display_currency", "USD")).strip().upper() or "USD"
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
                if action != "HOLD" or guard_hold_signals:
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
                "is_execution": is_execution,
                "quote_currency": quote_currency,
                "display_currency": display_currency,
            }
            # Optional passthrough metadata for reconciliation/traceability.
            for extra_key in ("exchange_symbol", "exchange_trade_id", "exchange_order_id", "is_placeholder"):
                if extra_key in event:
                    rec[extra_key] = event.get(extra_key)
            entries.append(rec)
            if action != "HOLD" or guard_hold_signals:
                activity_guard[guard_key] = {"ts": rec["ts"], "id": entry_id}

            pos_key = f"{market}|{timeframe}|{asset}"
            open_pos = open_positions.get(pos_key)
            if action == "BUY" and is_execution and qty > 0:
                open_positions[pos_key] = {
                    "entry_id": entry_id,
                    "entry_ts": rec["ts"],
                    "entry_price": price,
                    "qty": qty,
                    "quote_currency": quote_currency,
                    "display_currency": display_currency,
                    "asset": asset,
                    "market": market,
                    "timeframe": timeframe,
                    "panel": panel,
                }
            elif action == "SELL" and is_execution:
                if isinstance(open_pos, dict):
                    rec["closed_entry_id"] = int(open_pos.get("entry_id", 0) or 0)
                    try:
                        entry_price = float(open_pos.get("entry_price", 0.0) or 0.0)
                    except Exception:
                        entry_price = 0.0
                    if entry_price <= 0:
                        # Fallback: try to link to most recent BUY execution with non-zero price.
                        for prev in reversed(entries[:-1]):
                            if not isinstance(prev, dict):
                                continue
                            if not bool(prev.get("is_execution", False)):
                                continue
                            if str(prev.get("action", "")).upper() != "BUY":
                                continue
                            if str(prev.get("asset", "")).upper() != asset:
                                continue
                            try:
                                prev_px = float(prev.get("price", 0.0) or 0.0)
                            except Exception:
                                prev_px = 0.0
                            if prev_px > 0:
                                entry_price = prev_px
                                rec["closed_entry_id"] = int(prev.get("id", rec.get("closed_entry_id", 0)) or 0)
                                break
                    if entry_price > 0 and price > 0:
                        rec["pnl_pct"] = round(((price - entry_price) / entry_price) * 100.0, 4)
                        pnl_quote = (price - entry_price) * max(0.0, qty)
                        rec["pnl_quote"] = round(float(pnl_quote), 8)
                        rec["quote_currency"] = str(open_pos.get("quote_currency", quote_currency) or quote_currency).strip().upper() or quote_currency
                        # Convert realized quote PnL into user's preferred display currency when quote is USD-like.
                        usd_like = {"USD", "USDT", "USDC", "BUSD", "FDUSD", "TUSD", "USDP", "DAI"}
                        dc = str(display_currency or "USD").strip().upper() or "USD"
                        qc = str(rec.get("quote_currency", quote_currency)).strip().upper() or quote_currency
                        rec["display_currency"] = dc
                        if qc == dc:
                            rec["pnl_display"] = round(float(pnl_quote), 8)
                        elif qc in usd_like:
                            try:
                                fx = float(self.mod.get_usd_to_currency_rate(dc))
                            except Exception:
                                fx = 1.0
                            rec["fx_usd_to_display"] = fx
                            rec["pnl_display"] = round(float(pnl_quote) * fx, 8)
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
