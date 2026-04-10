# Branch Sync Policy (GUI Nightly -> Nightly)

When work is done on `gui-nightly`, only required core-script edits must be propagated into `nightly`.

## Core Script Scope

Required core sync scope is currently:

- `nightly/BTC-beta.py`

GUI-only files (for example `gui_app/*`) should not be copied into `nightly` unless explicitly intended.

## Quick Check

From `gui-nightly`, run:

```bash
python scripts/sync_core_to_nightly.py --base nightly
```

If there are required edits, the script lists the exact core paths to sync and returns a non-zero code.

## Patch-Assisted Sync

To generate a minimal patch for required core changes:

```bash
python scripts/sync_core_to_nightly.py --base nightly --create-patch --patch-path experiments/reports/core_sync.patch
```

Then apply on `nightly`:

```bash
git switch nightly
git apply experiments/reports/core_sync.patch
git add nightly/BTC-beta.py
git commit -m "sync: required core script edits from gui-nightly"
git switch gui-nightly
```

## Documentation Rule

Any required core-script sync should include documentation updates in the same change set where relevant:

- `README.md` (user-facing behavior/commands)
- `PROJECT_HANDOFF.md` (completed work, risks, next tasks)

