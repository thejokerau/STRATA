# Project Handoff

## Date
- 2026-04-08

## Branch
- `main`

## Objective
- Evolve `nightly/BTC-beta.py` into a robust multi-timeframe quant workflow with safer Fibonacci logic, faster backtesting, and tunable optimization.

## Completed
- Added Python runtime policy for nightly:
  - enforce `>=3.13`
  - advisory when not running `3.13.x`
- Implemented timeframe selection (`1d`, `4h`, `8h`, `12h`) and backtest lookback selection (`1/3/6/12/18/24 months`) using `365.25`-day math.
- Implemented robust Fibonacci swing logic with support/resistance guards.
- Added/expanded indicators via `pandas_ta` (RSI, MACD, BB, ATR, ADX, OBV, CMF, Doji).
- Added Optuna walk-forward tuning with CPU parallel trial support (`n_jobs`).
- Added CUDA detection (`cupy`) with safe CPU fallback.
- Added indicator cache to avoid repeated recomputation in backtests.
- Added CPU-parallel indicator cache build (worker prompt).
- Fixed timeframe hold behavior:
  - `Days Held` now based on actual elapsed days.
  - max hold now respects day-based intent on intraday bars.
- Added anti-churn controls:
  - `min_hold_bars`
  - `cooldown_bars`
- Added same-asset concentration control:
  - `same_asset_cooldown_bars` (prevents immediate re-entry into the same ticker)
- Added Binance symbol pre-filtering (`exchangeInfo`) for crypto top lists.
- Updated `README.md` with nightly dependencies/features and runtime notes.
- Added risk/robustness controls into optimizer objective:
  - drawdown penalty (`max_drawdown_limit_pct`)
  - exposure penalty (`max_exposure_pct`)
  - turnover penalty
- Upgraded optimizer risk controls to hard constraints:
  - validation-fold drawdown breaches are now pruned (`optuna.TrialPruned`)
  - position-size search upper bound now respects configured max exposure
- Added explicit train/validate/test split for auto-tune flow:
  - walk-forward optimization on pre-holdout history
  - final holdout summary on most recent window
- Added post-backtest risk report:
  - max drawdown, Sharpe, Sortino, turnover/year, avg hold days
  - top per-asset PnL contribution

## Current Files of Interest
- `nightly/BTC-beta.py`
- `README.md`

## Known Issues / Risks
- Some CoinGecko symbols still may not be usable across all data providers; filtering now favors Binance tradability for crypto.
- yfinance intraday data quality/availability can vary by symbol and window.
- CUDA path currently accelerates only a small numeric hotspot; most workload remains CPU/pandas-bound.
- Walk-forward objective uses a no-trade penalty heuristic; may need tuning depending on strategy goals.
- Current ranking remains single-position top-asset rotation; portfolio-level multi-asset allocation is not yet implemented.
- Hard constraints are now active for optimization folds; final full-window backtest can still exceed constraints if market regime shifts after tuning.

## Run / Validate
- Syntax check:
  - `python -m py_compile .\\nightly\\BTC-beta.py`
- Run nightly:
  - `python .\\nightly\\BTC-beta.py`

## Recommended Next Tasks
1. Add deterministic benchmark mode (fixed ticker set + timeframe + seed-like controls) to compare optimization changes across commits.
2. Add a compact diagnostics report after each run:
   - assets requested/loaded/dropped + reasons
   - trades count by exit type
   - average hold days and bars
3. Add optional export of backtest trades/equity/metrics to CSV for external analysis.

## Process Rule (Required)
- For every future code change in this project, update this file in the same change set:
  - refresh **Completed**
  - update **Known Issues / Risks**
  - keep **Recommended Next Tasks** current
