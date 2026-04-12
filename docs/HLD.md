# STRATA High-Level Design (HLD)

## 1. Purpose

STRATA is an AI-assisted, risk-aware trading platform for crypto and traditional markets. It combines:

- live signal generation,
- historical backtesting and optimization,
- AI interpretation and recommendations,
- guarded execution workflows,
- portfolio/ledger reconciliation and monitoring.

Primary user experience is GUI-first with increasing natural-language control via Agent Console.

## 2. Product Goals

- Deliver actionable trading insights from multi-timeframe quantitative signals.
- Keep execution safe via explicit guardrails and exchange validation.
- Support human-in-the-loop and automated operation modes.
- Continuously improve recommendations using live, backtest, and execution outcomes.

## 3. System Context

External systems:

- Binance APIs (market/account/orders/trades)
- yfinance and Binance market data
- LLM providers (xAI, OpenAI, Anthropic, Ollama, OpenClaw, OpenAI-compatible)

Local systems:

- GUI app (`gui_app/main.py`)
- Core engine bridge (`gui_app/engine_bridge.py`)
- Strategy runtime (`nightly/BTC-beta.py`)
- Local state + user secrets (outside git)
- Trade ledger and activity guard data

## 4. Core Functional Domains

- Signal & Analytics:
  - live dashboard generation, regime/risk scoring, setup metrics.
- Backtesting:
  - configurable historical simulation, walk-forward tuning context.
- AI Analysis:
  - dashboard/backtest interpretation, structured output parsing.
- Agentic Control:
  - natural-language intents, deterministic parser, optional AI fallback.
- Portfolio & Execution:
  - staged recommendations, autosizing, validation, submit/reconcile.
- Protection:
  - open-position review, AI+backtest-informed stop recommendations, monitor loop.

## 5. Runtime Modes

- Manual:
  - insights + staging, no auto execution.
- Semi-auto:
  - assistant prepares and sizes, user confirms submit.
- Auto-execute:
  - policy-gated submit path.
- Continuous monitor:
  - scheduled pipeline/protection loops with optional auto-send.

## 6. High-Level Component View

- Presentation Layer:
  - GUI tabs: Live, Backtest, AI, Agent Console, Portfolio/Ledger, Task Monitor, Settings.
- Orchestration Layer:
  - task queue, scheduler loops, run-state monitor, command router.
- Domain Layer:
  - signal extraction, backtest execution, AI prompt/parse, protection planner.
- Integration Layer:
  - Binance account/order adapters, AI provider adapters, market data fetchers.
- Persistence Layer:
  - local state, ledger entries/open positions/activity guard, snapshots.

Visual:

- High-level architecture diagram: [HLD_Diagram.svg](../HLD_Diagram.svg)

![STRATA HLD](../HLD_Diagram.svg)

## 7. Key Data Flows

1. Live -> AI:
   - live panels generate signal/risk context -> AI produces interpretation/trade plan -> staged recommendations.
2. Live -> Backtest -> AI:
   - live signals identify target assets -> targeted backtests -> AI receives combined context.
3. Portfolio Protection:
   - open positions + latest prices + targeted backtests -> AI protection plan -> staged protective orders.
4. Execution Lifecycle:
   - staged order -> exchange validation -> submit -> optional protective stop -> reconcile -> ledger update.

## 8. Safety and Controls

- Position/risk guardrails:
  - max daily loss, max trades/day, max exposure, stop-required policy.
- Exchange filter compliance:
  - step size, tick size, notional, min/max quantity.
- Duplicate control:
  - activity guard and cooldown for repeated signals/actions.
- Execution gating:
  - mode-based submit permissions and confirmation flows.

## 9. Non-Functional Priorities

- Responsiveness:
  - long-running workflows offloaded to background threads.
- Reliability:
  - graceful fallback paths (AI unavailable, parse failure, exchange errors).
- Observability:
  - task monitor + terminal logs + explicit status.
- Security:
  - API keys in user-local secure files/env, excluded from git.

## 10. Current Evolution Focus

- Increase natural-language coverage for intent-driven workflows.
- Tighten protection automation for open positions.
- Expand continuous unattended operation with strong guardrails.

## 11. Near-Term Recommendations

1. Exchange-native trailing support:
   - add native trailing-stop order path where exchange capabilities allow.
2. Protection policy profiles:
   - define conservative/balanced/aggressive protection templates per symbol group.
3. Protection drift control:
   - avoid rapid stop churn by requiring minimum delta/time between replacements.
4. Outcome feedback:
   - track whether protection actions improved drawdown and downside containment.
