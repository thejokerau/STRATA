# STRATA Automation Guide

## Goal

Run repeatable research cycles that collect evidence, tune parameters, and promote only risk-compliant winners.

## Components

- `scripts/run_experiments.py`
  - Loads scenarios from `experiments/scenarios.json`
  - Fetches data, builds indicator cache, runs baseline + optional Optuna tuning
  - Writes artifacts:
    - `experiments/runs/run_*.json`
    - `experiments/candidates.jsonl`

- `scripts/promote_champion.py`
  - Reads latest candidates per scenario
  - Applies promotion gates
  - Updates `experiments/registry/champions.json`
  - Writes report to `experiments/reports/promotion_*.md`

- `scripts/update_handoff.py`
  - Refreshes the managed automation block in `PROJECT_HANDOFF.md`

- `scripts/auto_research_cycle.py`
  - Runs all of the above in sequence

## Default Promotion Gates

- Minimum return improvement: `+1.0%`
- Maximum drawdown increase vs current champion: `+1.5%`
- Maximum Sharpe drop vs current champion: `-0.10`

Override gates:

```bash
python scripts/promote_champion.py --min-return-delta 1.5 --max-dd-increase 1.0 --max-sharpe-drop 0.05
```

## Manual Runs

```bash
python scripts/run_experiments.py --enable-optuna
python scripts/promote_champion.py
python scripts/update_handoff.py
```

One-shot:

```bash
python scripts/auto_research_cycle.py
```

## Scheduling (Windows)

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\register_research_task.ps1 -TaskName STRATA_Nightly_Research -Time 02:00 -PythonExe python
```

## Notes

- Generated files under `experiments/runs/`, `experiments/reports/`, and `experiments/candidates.jsonl` are ignored in git.
- Champion registry (`experiments/registry/champions.json`) is tracked so learned parameters can be reviewed and versioned.
