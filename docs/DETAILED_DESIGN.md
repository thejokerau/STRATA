# STRATA Detailed Design

## 1. Scope

This document describes detailed design for:

- natural-language agent command execution,
- AI fallback intent routing,
- portfolio/ledger execution pipeline,
- open-position protection (AI + targeted backtest),
- scheduled protection monitor.

## 2. Module Responsibilities

## 2.1 `gui_app/main.py`

- UI composition and tab orchestration.
- Task queue and busy-state management.
- User command parsing and action dispatch.
- Recommendation staging table and submit lifecycle.
- Protection workflows and monitor scheduler.

## 2.2 `gui_app/engine_bridge.py`

- Thread-safe bridge between GUI and strategy runtime.
- Data acquisition for live/backtest.
- AI provider calls via active profile.
- Binance profile, portfolio, order, and reconcile operations.
- Ledger read/write normalization helpers.

## 2.3 `nightly/BTC-beta.py`

- Indicator computation, scoring, backtest simulation.
- AI provider adapters and prompt builders.
- Utility functions for market data, pricing, and formatting.

## 3. Agent Command Design

## 3.1 Deterministic Parser

Primary parser resolves high-confidence intents:

- `buy`
- `scan_allocate`
- `portfolio_balance`
- `portfolio_pnl`
- `scheduler` (`start/stop`)
- `set_context` (`exclude/clear exclusions`)
- `execute_staged`
- `open_view`
- `status`

Context memory persists:

- timeframe
- top_n
- quote asset
- stop %
- exclusion list

## 3.2 AI Fallback Parser

When deterministic parse yields `unknown` and AI fallback is enabled:

1. GUI builds strict structured-intent prompt.
2. Active AI profile returns JSON between markers.
3. JSON is validated against allowed intent set.
4. Valid intent is executed through the same guarded pipeline.

Safety note:
- AI never bypasses Binance validation or policy guardrails.

## 4. Recommendation Staging Model

Pending recommendation fields include:

- `id`, `symbol`, `asset`, `side`, `order_type`
- `quantity`, optional `limit_price`, `stop_loss_price`
- `timeframe`, `confidence`, `status`, `reason`
- optional flags (e.g., `replace_existing_stop`, `protection_mode`)

Status transitions:

- `PENDING` -> `SUBMITTED`
- `PENDING` -> `BLOCKED` / `FAILED`

## 5. Submit Pipeline

Submit path (`_submit_selected_pending_orders`) performs:

1. execution mode and profile checks,
2. pre-balance sanity checks,
3. Binance validation and normalization,
4. optional min-notional adjustment retry,
5. order submission,
6. optional protective stop/take-profit placement on BUY,
7. ledger execution logging.

For `STOP_LOSS_LIMIT` and `TAKE_PROFIT_LIMIT` pending orders:

- requires trigger price (`stop_loss_price` for stop, `take_profit_price` for take-profit)
- uses `limit_price` when provided; otherwise derives from trigger
- can cancel existing open protective stop orders before replacement

For `OCO_BRACKET` pending orders:

- requires both `stop_loss_price` and `take_profit_price`
- submits Binance `/api/v3/order/oco` as linked protective bracket
- replacement mode cancels prior protective SELL orders before OCO placement

## 6. Protection Workflow Design

## 6.1 Manual Protection Run

`Protect Open Positions (AI+BT)`:

1. reads open positions from ledger,
2. fetches last market price per symbol,
3. computes current unrealized context,
4. runs targeted backtest per symbol/timeframe,
5. calls AI for protection plan,
6. parses plan and stages protective recommendations.

Supported AI actions:

- `SET_STOP`
- `SET_TRAILING`
- `SET_TAKE_PROFIT`
- `SET_BOTH`
- `HOLD`

Current execution mapping:

- `SET_TRAILING` is mapped to fixed stop recommendation with explicit annotation.
- `SET_TAKE_PROFIT` creates `TAKE_PROFIT_LIMIT` protective orders.
- `SET_BOTH` stages both stop-loss and take-profit protective orders.

## 6.2 Background Execution Model

Protection run executes in background thread via task queue:

- avoids UI freeze / "Not Responding" perception.

## 7. Performance Architecture

### 7.1 Shared Engine Caches

`EngineBridge` now uses short-lived in-memory caches to reduce repeated work:

- raw data cache (ticker/timeframe/lookback key),
- indicator cache (symbol-set + timeframe key),
- live/backtest result cache (request signature key).

This improves repeated refresh/pipeline responsiveness while preserving bounded staleness with TTL.

### 7.2 Incremental UI Updates

Pending recommendations and open-order tables now use row upsert/sync (update changed rows, remove stale rows) instead of full clear/reinsert each refresh.

### 7.3 Runtime Profiling

Task runtime profiling is captured and exposed in GUI:

- per-task start time and completion duration,
- recent-duration rolling average in Task Monitor,
- stage-level `PERF ...` logs in task terminal for Live/Backtest/AI calls.

### 7.4 SQLite Tuning

Nightly data cache DB setup now applies:

- `WAL` journal mode,
- `synchronous=NORMAL`,
- memory temp store,
- cache-size hint and busy timeout,
- supporting indexes on `(timeframe, date)` and `(ticker, date)`.
- emits task terminal progress and completion logs.

## 6.3 Protection Monitor

Scheduler controls:

- interval minutes (`Protect every (min)`)
- start/stop monitor
- optional auto-send

Tick behavior:

1. run protection workflow,
2. stage recommendations,
3. optional direct submit (without modal confirmation),
4. re-arm next tick.

## 7. Ledger and PnL Design

Ledger domains:

- signal journal (`is_execution=false`)
- execution ledger (`is_execution=true`)
- open positions map

Realized PnL:

- computed on sell execution close-outs.
- stored in quote and display currencies where possible.

Unrealized PnL:

- computed at refresh from open positions using latest market prices.
- shown in open-positions view and summary lines.

Reconcile/placeholder behavior:

- submit-time market execution rows may be logged as placeholders (`is_placeholder=true`) when exact fill price is not yet known.
- fill reconciliation now upserts those placeholders (matching `exchange_order_id`) with real trade price/qty/time.
- execution ledger views hide placeholders by default so users see reconciled executions first.
- close-out PnL includes a fallback link to the latest valid BUY execution price when a direct open-position entry price is missing/zero.

## 8. Concurrency Model

- Single/parallel task limits controlled by settings.
- Queue-based launch for non-blocking operations.
- All UI updates marshaled back onto Tk main thread.
- Long workflows (AI/backtest/protection) run in worker threads.

## 9. Error Handling Strategy

- Explicit user-visible messages for blocking prerequisites.
- Fallback behavior for AI parse failure (deterministic/default protections).
- Partial-failure tolerance:
  - one symbol failure does not abort full batch.
- Diagnostic visibility:
  - task terminal logs + status labels.

## 10. Security and Data Handling

- API keys stored in user-local secure settings or env vars.
- Runtime artifacts and secrets excluded from git.
- Exchange actions only through explicit mode/guardrail gates.

## 11. Extension Points

- Native trailing-stop order support per exchange capability.
- Position-level strategy profiles (swing/intraday/defensive).
- Reinforcement loop using realized outcomes to tune protection policy.
- Cross-market protection orchestration (crypto + traditional holdings).

## 12. Implementation Recommendations (Near-Term)

1. Add trailing-order capability map:
   - pre-load per-symbol support for trailing-like order types to remove runtime ambiguity.
2. Introduce protection recommendation confidence gates:
   - auto-submit only above threshold and only when risk policy passes.
3. Add protection action idempotency keys:
   - prevent duplicate replacement actions in monitor loops.
4. Persist monitor run summaries:
   - write per-run protection report (inputs, decisions, submit outcome, errors).
