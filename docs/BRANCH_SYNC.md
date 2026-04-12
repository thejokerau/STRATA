# Branch Strategy and Promotion Policy (AI-Nightly -> Main)

## Current Branch Model

- `main`: stable, primary STRATA product branch.
- `ai-nightly`: active development branch for newest GUI/AI/risk/automation work.
- `archive/*`: historical snapshot branches from prior workflows (read-only by convention).

## Primary Promotion Flow

Use fast-forward promotion from `ai-nightly` to `main` after smoke testing:

```bash
git switch main
git merge --ff-only ai-nightly
git push origin main
```

This keeps release history linear and easy to support.

## Archive Workflow (Legacy Branch Cleanup)

When retiring old long-lived branches:

1. Create archive branch at the old branch tip (for traceability).
2. Push archive branch to origin.
3. Delete old branch (local and optionally remote).

Example:

```bash
git branch archive/gui-nightly-2026Q2 gui-nightly
git push origin archive/gui-nightly-2026Q2
git branch -D gui-nightly
git push origin --delete gui-nightly
```

Repeat for any branch being retired (for example `gui-stable`).

## Documentation Rule

Any branch-strategy or promotion-model change should update:

- `README.md` (user-facing branch/run guidance)
- `docs/BRANCH_SYNC.md` (operational source of truth)

