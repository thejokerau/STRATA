import argparse
import importlib.util
import json
import os
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
NIGHTLY_PATH = ROOT / "nightly" / "BTC-beta.py"


def resolve_data_root() -> Path:
    raw = str(os.environ.get("CTMT_DATA_ROOT", "") or "").strip()
    if raw:
        p = Path(raw).expanduser()
        if not p.is_absolute():
            p = (ROOT / p).resolve()
        else:
            p = p.resolve()
    else:
        p = ROOT
    return p


DATA_ROOT = resolve_data_root()
EXPERIMENTS_DIR = DATA_ROOT / "experiments"
RUNS_DIR = EXPERIMENTS_DIR / "runs"
CANDIDATES_PATH = EXPERIMENTS_DIR / "candidates.jsonl"
SCENARIOS_PATH = EXPERIMENTS_DIR / "scenarios.json"


def assert_writable_dir(path: Path, label: str) -> None:
    try:
        path.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        raise SystemExit(
            f"{label} is not accessible: {path}\n"
            f"Reason: {e}\n"
            "Set CTMT_DATA_ROOT to a writable location and retry."
        )
    probe = path / ".ctmt_write_probe.tmp"
    try:
        probe.write_text("ok", encoding="utf-8")
        probe.unlink(missing_ok=True)
    except Exception as e:
        raise SystemExit(
            f"{label} is not writable: {path}\n"
            f"Reason: {e}\n"
            "Set CTMT_DATA_ROOT to a writable location and retry."
        )


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_dirs() -> None:
    assert_writable_dir(DATA_ROOT, "CTMT data root")
    assert_writable_dir(EXPERIMENTS_DIR, "Experiments directory")
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    EXPERIMENTS_DIR.mkdir(parents=True, exist_ok=True)


def load_nightly_module(path: Path):
    spec = importlib.util.spec_from_file_location("btc_nightly", str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load module from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def serialize_cfg(cfg: Any) -> Dict[str, Any]:
    d = asdict(cfg)
    tuned = d.get("tuned", {})
    return {
        **{k: v for k, v in d.items() if k != "tuned"},
        "tuned": tuned,
    }


def scenario_assets(mod: Any, sc: Dict[str, Any]) -> Tuple[List[str], bool]:
    market = str(sc.get("market", "crypto")).strip().lower()
    if market == "crypto":
        top_n = int(sc.get("top_n", 20))
        quote_currency = str(sc.get("quote_currency", "USD")).strip().upper() or "USD"
        tickers = mod.filter_crypto_tickers_by_binance(mod.fetch_top_coins(top_n, quote_currency=quote_currency))
        return tickers, True

    country = str(sc.get("country", "2")).strip()
    top_n = int(sc.get("top_n", 20))
    tickers = list(mod.TRADITIONAL_TOP.get(country, mod.TRADITIONAL_TOP["2"]))[:max(1, top_n)]
    return tickers, False


def score_candidate(result: Dict[str, Any], max_dd_target: float) -> float:
    metrics = result.get("metrics", {}) or {}
    ret = safe_float(result.get("return_pct", 0.0))
    dd = safe_float(metrics.get("max_drawdown_pct", 100.0), 100.0)
    sharpe = safe_float(metrics.get("sharpe", 0.0))
    turnover = safe_float(metrics.get("turnover_per_year", 0.0))
    dd_over = max(0.0, dd - max_dd_target)
    churn_penalty = max(0.0, turnover - 40.0) * 0.05
    return ret + 2.0 * sharpe - 1.25 * dd_over - churn_penalty


def run_single_scenario(
    mod: Any,
    sc: Dict[str, Any],
    args: argparse.Namespace,
) -> Dict[str, Any]:
    scenario_id = str(sc.get("id", "scenario")).strip()
    timeframe = str(sc.get("timeframe", "1d")).strip()
    months = int(sc.get("months", 12))

    tickers, is_crypto = scenario_assets(mod, sc)
    if not tickers:
        raise RuntimeError(f"{scenario_id}: no tickers selected")

    fetch_years = max(2.2, (months / 12.0) + 0.25)
    raw_data: Dict[str, pd.DataFrame] = {}
    for t in tickers:
        df = mod.fetch_with_cache(t, timeframe, is_crypto=is_crypto, years=fetch_years)
        if df is not None and len(df) >= 60:
            raw_data[t] = df

    if not raw_data:
        raise RuntimeError(f"{scenario_id}: no usable data")

    indicator_cache = mod.build_indicator_cache(
        raw_data,
        timeframe,
        max_workers=max(1, int(args.cache_workers)),
        verbose=True,
    )
    if not indicator_cache:
        raise RuntimeError(f"{scenario_id}: no indicator-ready data")

    tuned = mod.TunedParams(
        position_size=args.position_size / 100.0,
        atr_multiplier=args.atr_multiplier,
        adx_threshold=args.adx_threshold,
        cmf_threshold=args.cmf_threshold,
        obv_slope_threshold=args.obv_slope_threshold,
        buy_threshold=args.buy_threshold,
        sell_threshold=args.sell_threshold,
    )
    cfg_base = mod.BacktestConfig(
        initial_capital=args.initial_capital,
        stop_loss_pct=args.stop_loss_pct,
        take_profit_pct=args.take_profit_pct,
        max_hold_days=args.max_hold_days,
        min_hold_bars=args.min_hold_bars,
        cooldown_bars=args.cooldown_bars,
        same_asset_cooldown_bars=args.same_asset_cooldown_bars,
        max_consecutive_same_asset_entries=args.max_consecutive_same_asset_entries,
        max_drawdown_limit_pct=args.max_drawdown_target_pct,
        max_exposure_pct=args.max_exposure_pct / 100.0,
        fee_pct=args.fee_pct,
        slippage_pct=args.slippage_pct,
        tuned=tuned,
    )

    anchor = next(iter(raw_data.keys()))
    end_date = raw_data[anchor].index.max()
    backtest_days = int(round(months * 365.25 / 12.0))
    start_date = end_date - pd.Timedelta(days=backtest_days)

    baseline_result = mod.simulate_backtest(
        raw_data,
        timeframe,
        cfg_base,
        is_crypto,
        start_date=start_date,
        end_date=end_date,
        indicator_cache=indicator_cache,
        verbose=False,
    )

    cfg_tuned = cfg_base
    holdout_result = None
    tuned_result = baseline_result
    if args.enable_optuna:
        cfg_tuned, holdout_result = mod.run_walk_forward_optuna(
            raw_data,
            timeframe,
            is_crypto,
            cfg_base,
            n_trials=max(1, int(args.optuna_trials)),
            n_jobs=max(1, int(args.optuna_jobs)),
            indicator_cache=indicator_cache,
        )
        tuned_result = mod.simulate_backtest(
            raw_data,
            timeframe,
            cfg_tuned,
            is_crypto,
            start_date=start_date,
            end_date=end_date,
            indicator_cache=indicator_cache,
            verbose=False,
        )

    baseline_score = score_candidate(baseline_result, cfg_base.max_drawdown_limit_pct)
    tuned_score = score_candidate(tuned_result, cfg_tuned.max_drawdown_limit_pct)

    selected_name = "tuned" if tuned_score > baseline_score else "baseline"
    selected_cfg = cfg_tuned if selected_name == "tuned" else cfg_base
    selected_result = tuned_result if selected_name == "tuned" else baseline_result

    out = {
        "run_ts_utc": utc_now_iso(),
        "scenario": {
            "id": scenario_id,
            "market": "crypto" if is_crypto else "traditional",
            "timeframe": timeframe,
            "months": months,
            "requested_assets": len(tickers),
            "loaded_assets": len(raw_data),
            "tickers": sorted(list(raw_data.keys())),
        },
        "baseline": {
            "config": serialize_cfg(cfg_base),
            "score": baseline_score,
            "result": {
                "final": safe_float(baseline_result.get("final", 0.0)),
                "return_pct": safe_float(baseline_result.get("return_pct", 0.0)),
                "trades_closed": len(baseline_result.get("trades", [])),
                "metrics": baseline_result.get("metrics", {}),
            },
        },
        "tuned": {
            "config": serialize_cfg(cfg_tuned),
            "score": tuned_score,
            "result": {
                "final": safe_float(tuned_result.get("final", 0.0)),
                "return_pct": safe_float(tuned_result.get("return_pct", 0.0)),
                "trades_closed": len(tuned_result.get("trades", [])),
                "metrics": tuned_result.get("metrics", {}),
            },
            "holdout": None
            if holdout_result is None
            else {
                "final": safe_float(holdout_result.get("final", 0.0)),
                "return_pct": safe_float(holdout_result.get("return_pct", 0.0)),
                "trades_closed": len(holdout_result.get("trades", [])),
                "metrics": holdout_result.get("metrics", {}),
            },
        },
        "selected": {
            "name": selected_name,
            "config": serialize_cfg(selected_cfg),
            "result": {
                "final": safe_float(selected_result.get("final", 0.0)),
                "return_pct": safe_float(selected_result.get("return_pct", 0.0)),
                "trades_closed": len(selected_result.get("trades", [])),
                "metrics": selected_result.get("metrics", {}),
            },
        },
    }
    return out


def write_artifacts(results: List[Dict[str, Any]]) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_path = RUNS_DIR / f"run_{stamp}.json"
    run_path.write_text(json.dumps(results, indent=2), encoding="utf-8")
    try:
        run_file_ref = str(run_path.relative_to(ROOT)).replace("\\", "/")
    except Exception:
        run_file_ref = str(run_path)

    with CANDIDATES_PATH.open("a", encoding="utf-8") as f:
        for rec in results:
            row = {
                "logged_at_utc": utc_now_iso(),
                "scenario_id": rec["scenario"]["id"],
                "run_file": run_file_ref,
                "selected": rec["selected"],
                "tuned": rec["tuned"],
                "baseline": rec["baseline"],
            }
            f.write(json.dumps(row) + "\n")

    return run_path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run scheduled CTMT experiments and log artifacts.")
    p.add_argument("--scenarios", type=str, default=str(SCENARIOS_PATH), help="Path to scenarios JSON.")
    p.add_argument("--cache-workers", type=int, default=8)
    p.add_argument("--enable-optuna", action="store_true", default=True)
    p.add_argument("--disable-optuna", action="store_true", default=False)
    p.add_argument("--optuna-trials", type=int, default=30)
    p.add_argument("--optuna-jobs", type=int, default=8)

    p.add_argument("--initial-capital", type=float, default=10000.0)
    p.add_argument("--stop-loss-pct", type=float, default=8.0)
    p.add_argument("--take-profit-pct", type=float, default=20.0)
    p.add_argument("--max-hold-days", type=int, default=45)
    p.add_argument("--min-hold-bars", type=int, default=2)
    p.add_argument("--cooldown-bars", type=int, default=1)
    p.add_argument("--same-asset-cooldown-bars", type=int, default=3)
    p.add_argument("--max-consecutive-same-asset-entries", type=int, default=3)
    p.add_argument("--max-drawdown-target-pct", type=float, default=35.0)
    p.add_argument("--max-exposure-pct", type=float, default=40.0)
    p.add_argument("--fee-pct", type=float, default=0.10)
    p.add_argument("--slippage-pct", type=float, default=0.05)

    p.add_argument("--position-size", type=float, default=30.0, help="Percent (20-40 recommended).")
    p.add_argument("--atr-multiplier", type=float, default=2.2)
    p.add_argument("--adx-threshold", type=float, default=25.0)
    p.add_argument("--cmf-threshold", type=float, default=0.02)
    p.add_argument("--obv-slope-threshold", type=float, default=0.0)
    p.add_argument("--buy-threshold", type=int, default=2)
    p.add_argument("--sell-threshold", type=int, default=-2)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if args.disable_optuna:
        args.enable_optuna = False
    ensure_dirs()

    scenarios = read_json(Path(args.scenarios), [])
    if not isinstance(scenarios, list) or not scenarios:
        raise SystemExit(f"No scenarios found in {args.scenarios}")

    mod = load_nightly_module(NIGHTLY_PATH)
    mod.ensure_table()

    results: List[Dict[str, Any]] = []
    failures: List[Dict[str, str]] = []

    for sc in scenarios:
        scenario_id = str(sc.get("id", "scenario")).strip()
        print("=" * 100)
        print(f"Running scenario: {scenario_id}")
        print("=" * 100)
        try:
            rec = run_single_scenario(mod, sc, args)
            results.append(rec)
            sel = rec["selected"]
            ret = safe_float(sel["result"].get("return_pct", 0.0))
            dd = safe_float((sel["result"].get("metrics", {}) or {}).get("max_drawdown_pct", 0.0))
            print(f"Selected: {sel['name']} | Return {ret:+.2f}% | MaxDD {dd:.2f}%")
        except Exception as e:
            failures.append({"scenario_id": scenario_id, "error": str(e)})
            print(f"FAILED {scenario_id}: {e}")

    if not results:
        raise SystemExit(f"All scenarios failed: {failures}")

    run_path = write_artifacts(results)
    print(f"Wrote experiment run: {run_path}")

    if failures:
        print("Failures:")
        for f in failures:
            print(f"- {f['scenario_id']}: {f['error']}")


if __name__ == "__main__":
    main()
