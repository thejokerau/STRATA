# CTMT Bug Scorecard

Build-by-build bug tracking plus aggregate summary.

## Overall Summary (Latest Status Per Issue)

- Total unique issues tracked: 17
- Fixed: 15
- Partial: 2
- Open: 0

## Build-by-Build Summary

| Build | Branch | Date | Total Items | Fixed | Partial | Open |
|---|---|---:|---:|---:|---:|---:|
| gui-nightly-20260410A | gui-nightly | 2026-04-10 | 17 | 15 | 2 | 0 |

## Latest Issue Register

| ID | Title | Latest Status | First Seen Build | Latest Build |
|---|---|---|---|---|
| B001 | run_gui.py import path issue (ModuleNotFoundError: gui_app) | Fixed | gui-nightly-20260410A | gui-nightly-20260410A |
| B002 | SQLite cross-thread error in GUI | Fixed | gui-nightly-20260410A | gui-nightly-20260410A |
| B003 | Mixed cache date parse crash (date-only vs datetime) | Fixed | gui-nightly-20260410A | gui-nightly-20260410A |
| B004 | AI call not running due to missing env/API config visibility | Fixed | gui-nightly-20260410A | gui-nightly-20260410A |
| B005 | xAI 400 failures from model/endpoint mismatch | Fixed | gui-nightly-20260410A | gui-nightly-20260410A |
| B006 | Generated test/report files polluting git | Fixed | gui-nightly-20260410A | gui-nightly-20260410A |
| B007 | Live action label artifact (??) | Fixed | gui-nightly-20260410A | gui-nightly-20260410A |
| B008 | Task Monitor stale until manual click | Fixed | gui-nightly-20260410A | gui-nightly-20260410A |
| B009 | Task Monitor removed wrong task under overlap | Fixed | gui-nightly-20260410A | gui-nightly-20260410A |
| B010 | Tasks button visibility issue | Fixed | gui-nightly-20260410A | gui-nightly-20260410A |
| B011 | Task Monitor lacked queue operations | Fixed | gui-nightly-20260410A | gui-nightly-20260410A |
| B012 | Task Monitor lacked CLI-style debug output | Fixed | gui-nightly-20260410A | gui-nightly-20260410A |
| B013 | GUI backtest options behind CLI options | Fixed | gui-nightly-20260410A | gui-nightly-20260410A |
| B014 | AI GUI lacked prompt acceptance and follow-up conversation | Fixed | gui-nightly-20260410A | gui-nightly-20260410A |
| B015 | AI GUI lacked in-app provider/model/key management | Fixed | gui-nightly-20260410A | gui-nightly-20260410A |
| B016 | Stop/cancel currently running task from Task Monitor | Partial | gui-nightly-20260410A | gui-nightly-20260410A |
| B017 | Parallel mode high-load robustness (transient DB lock risk) | Partial | gui-nightly-20260410A | gui-nightly-20260410A |

## Build Details

### gui-nightly-20260410A

- Branch: `gui-nightly`
- Date: 2026-04-10
- Notes: Initial consolidated scoreboard snapshot from user-reported issues.

| ID | Issue | Status |
|---|---|---|
| B001 | run_gui.py import path issue (ModuleNotFoundError: gui_app) | Fixed |
| B002 | SQLite cross-thread error in GUI | Fixed |
| B003 | Mixed cache date parse crash (date-only vs datetime) | Fixed |
| B004 | AI call not running due to missing env/API config visibility | Fixed |
| B005 | xAI 400 failures from model/endpoint mismatch | Fixed |
| B006 | Generated test/report files polluting git | Fixed |
| B007 | Live action label artifact (??) | Fixed |
| B008 | Task Monitor stale until manual click | Fixed |
| B009 | Task Monitor removed wrong task under overlap | Fixed |
| B010 | Tasks button visibility issue | Fixed |
| B011 | Task Monitor lacked queue operations | Fixed |
| B012 | Task Monitor lacked CLI-style debug output | Fixed |
| B013 | GUI backtest options behind CLI options | Fixed |
| B014 | AI GUI lacked prompt acceptance and follow-up conversation | Fixed |
| B015 | AI GUI lacked in-app provider/model/key management | Fixed |
| B016 | Stop/cancel currently running task from Task Monitor | Partial |
| B017 | Parallel mode high-load robustness (transient DB lock risk) | Partial |

## Maintenance

1. Update `docs/BUG_SCORECARD_DATA.json` for each build update.
2. Run `python scripts/update_bug_scorecard.py`.
3. Commit `docs/BUG_SCORECARD.md` and `docs/BUG_SCORECARD_DATA.json` together.

