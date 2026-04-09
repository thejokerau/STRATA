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
- Added richer live-dashboard intelligence:
  - regime classification (`Trending`, `Choppy`, `High-Vol`, `Transitional`)
  - setup reliability model (expectancy, win rate, false-signal %, risk cone, post-entry DD, avg hold)
  - adjusted live score from raw score + reliability
- Added portfolio-aware live context notes:
  - concentration warning from rolling cross-asset correlations
  - lower-correlation alternatives vs top-ranked asset
- Added anti-concentration entry streak limiter:
  - `max_consecutive_same_asset_entries`
- Wired timeframe into backtest prompt defaults:
  - 8h defaults now use stricter anti-churn values
- Added auto-tune robustness/fail-safe:
  - handles all-pruned Optuna runs by safely reverting to base config
- Added post-tune drawdown guard in main flow:
  - if final-window DD breaches target, runs one constrained re-tune attempt and adopts it when risk/return improves
- Fixed live action label rendering artifact:
  - replaced hard-coded `??` labels with Unicode-escaped circle markers (`🟢/🟠/🔴`) to avoid source-encoding corruption
- Added terminal display fallback for action labels:
  - auto-detect emoji and ANSI-color capability at runtime
  - colorized labels when supported, plain text fallback otherwise
  - env overrides: `CTMT_DISABLE_EMOJI`, `NO_COLOR`, `FORCE_COLOR`
- Added automated continuous-research pipeline:
  - `scripts/run_experiments.py` for scenario backtests + Optuna tuning and artifact logging
  - `scripts/promote_champion.py` for champion/challenger promotion with risk gates
  - `scripts/update_handoff.py` for automatic handoff status block refresh
  - `scripts/auto_research_cycle.py` orchestrator for end-to-end unattended cycles
  - `scripts/register_research_task.ps1` helper for Windows Task Scheduler registration
- Added experiment registry/config assets:
  - `experiments/scenarios.json` default scenario set
  - `experiments/registry/champions.json` champion parameter registry
  - `.gitignore` updates for generated experiment artifacts (`runs`, `reports`, candidates log)
- Updated `README.md` with automation workflow commands and promotion gate defaults
- Added runtime champion auto-suggestion in nightly:
  - loads `experiments/registry/champions.json`
  - matches best champion by market/timeframe/lookback/asset-count context
  - prompts to apply champion tuned params in live mode
  - prompts to replace backtest config with champion settings in backtest mode
- Added Grok analysis capability to nightly menu:
  - new main-menu option `Grok Dashboard Analysis`
  - accepts pasted raw dashboard text and builds a strict Grok-ready analysis prompt
  - optionally sends prompt to xAI chat-completions API when `XAI_API_KEY` is configured
  - stores prompt/response artifacts in `experiments/grok/`
- Added Grok auto-feed from latest live snapshot:
  - each Live Dashboard run now saves snapshot text to `experiments/live_snapshots/`
  - Grok mode can consume `latest_live_dashboard.txt` without manual paste
- Updated `README.md` with Grok mode workflow and env-var configuration (`XAI_API_KEY`, `CTMT_GROK_MODEL`, `XAI_API_URL`)

## Current Files of Interest
- `nightly/BTC-beta.py`
- `README.md`
- `PROJECT_HANDOFF.md`
- `scripts/run_experiments.py`
- `scripts/promote_champion.py`
- `scripts/update_handoff.py`
- `scripts/auto_research_cycle.py`
- `experiments/scenarios.json`
- `experiments/registry/champions.json`

## Known Issues / Risks
- Some CoinGecko symbols still may not be usable across all data providers; filtering now favors Binance tradability for crypto.
- yfinance intraday data quality/availability can vary by symbol and window.
- CUDA path currently accelerates only a small numeric hotspot; most workload remains CPU/pandas-bound.
- Walk-forward objective uses a no-trade penalty heuristic; may need tuning depending on strategy goals.
- Current ranking remains single-position top-asset rotation; portfolio-level multi-asset allocation is not yet implemented.
- Hard constraints are now active for optimization folds; final full-window backtest can still exceed constraints if market regime shifts after tuning.
- Live setup reliability statistics are heuristic and sample-size-sensitive for thin-history assets/timeframes.
- Correlation-based live diversification notes use recent rolling history and may change quickly across volatile regimes.
- Some terminals/fonts may still not render emoji markers even when source encoding is correct.
- Automated experiment runs depend on live data provider availability and rate limits; failed data pulls can skip scenarios.
- Champion registry updates are gate-based but still require human review before promoting to stable release behavior.
- Champion matching is heuristic-based; when multiple close candidates exist, manual confirmation prompt remains the final safeguard.
- Direct xAI API execution in Grok mode depends on network availability, endpoint compatibility, and valid API credentials.
- Live snapshot and Grok artifact directories are generated outputs and are now git-ignored.

## Run / Validate
- Syntax check:
  - `python -m py_compile .\\nightly\\BTC-beta.py`
- Run nightly:
  - `python .\\nightly\\BTC-beta.py`

## Recommended Next Tasks
1. Add deterministic benchmark mode (fixed ticker set + timeframe + seed-like controls) to compare optimization changes across commits.
2. Extend automation outputs with per-scenario CSV exports (trades/equity/metrics) for offline analysis.
3. Add champion confidence scoring and tie-break transparency in runtime prompt (why this champion was chosen).
   - assets requested/loaded/dropped + reasons
   - trades count by exit type
   - average hold days and bars

## Process Rule (Required)
- For every future code change in this project, update this file in the same change set:
  - refresh **Completed**
  - update **Known Issues / Risks**
  - keep **Recommended Next Tasks** current

<!-- AUTO_HANDBACK_START -->
## Automated Research Status
- Last update UTC: 2026-04-09T08:50:42+00:00
- Latest experiment artifact: `experiments/runs/run_20260408T131249Z.json`
- Champion scenarios tracked: 1
- Latest run summary:
- `crypto_1d_12m_top20`: selected `tuned`, return +43.09%, maxDD 36.11%, sharpe 0.94
- `crypto_8h_12m_top20`: selected `tuned`, return +70.51%, maxDD 36.83%, sharpe 1.19
- `crypto_8h_24m_top10`: selected `tuned`, return +39.92%, maxDD 36.49%, sharpe 0.87
- `crypto_12h_24m_top10`: selected `tuned`, return +90.47%, maxDD 30.12%, sharpe 1.12
<!-- AUTO_HANDBACK_END -->

