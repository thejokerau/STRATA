import argparse
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


CORE_PATHS = [
    "nightly/BTC-beta.py",
]


def run_git(args):
    proc = subprocess.run(
        ["git", *args],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    return proc.returncode, (proc.stdout or "").strip(), (proc.stderr or "").strip()


def ref_exists(ref: str) -> bool:
    rc, _, _ = run_git(["rev-parse", "--verify", f"{ref}^{{commit}}"])
    return rc == 0


def resolve_base_ref(base_branch: str) -> str:
    if ref_exists(base_branch):
        return base_branch
    remote_ref = f"origin/{base_branch}"
    if ref_exists(remote_ref):
        return remote_ref
    raise RuntimeError(
        f"Could not resolve base ref `{base_branch}` (or `{remote_ref}`). "
        "Fetch remote branches or create the local branch first."
    )


def changed_core_paths(base_branch: str):
    base_ref = resolve_base_ref(base_branch)
    rc, out, err = run_git(["diff", "--name-only", f"{base_ref}...HEAD", "--", *CORE_PATHS])
    if rc != 0:
        raise RuntimeError(err or "git diff failed")
    return [line.strip() for line in out.splitlines() if line.strip()]


def write_patch(base_branch: str, patch_path: Path):
    base_ref = resolve_base_ref(base_branch)
    rc, out, err = run_git(["diff", "--binary", f"{base_ref}...HEAD", "--", *CORE_PATHS])
    if rc != 0:
        raise RuntimeError(err or "git diff --binary failed")
    patch_path.parent.mkdir(parents=True, exist_ok=True)
    patch_path.write_text(out, encoding="utf-8")
    return patch_path


def main():
    parser = argparse.ArgumentParser(
        description="Check and package required core-script sync edits from gui-nightly to nightly."
    )
    parser.add_argument("--base", default="nightly", help="Target branch to compare against (default: nightly)")
    parser.add_argument(
        "--create-patch",
        action="store_true",
        help="Create a patch file containing only required core-script edits.",
    )
    parser.add_argument(
        "--patch-path",
        default="experiments/reports/core_sync.patch",
        help="Patch output path when --create-patch is set.",
    )
    args = parser.parse_args()

    try:
        changed = changed_core_paths(args.base)
    except Exception as e:
        print(f"[ERROR] {e}")
        return 2

    if not changed:
        print(f"No required core-script sync edits relative to `{args.base}`.")
        return 0

    print("Required core-script edits to sync:")
    for p in changed:
        print(f"- {p}")

    patch_path = None
    if args.create_patch:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        pp = Path(args.patch_path)
        if pp.suffix.lower() != ".patch":
            pp = pp / f"core_sync_{ts}.patch"
        try:
            patch_path = write_patch(args.base, pp)
            print(f"\nWrote patch: {patch_path}")
        except Exception as e:
            print(f"[ERROR] Failed to write patch: {e}")
            return 2

    print("\nSuggested sync flow:")
    print("1. git switch nightly")
    if patch_path:
        print(f"2. git apply \"{patch_path}\"")
        print("3. git add nightly/BTC-beta.py")
    else:
        print("2. Cherry-pick or manually copy only the listed core edits.")
        print("3. git add nightly/BTC-beta.py")
    print("4. git commit -m \"sync: required core script edits from gui-nightly\"")
    print("5. git switch gui-nightly")
    return 1


if __name__ == "__main__":
    sys.exit(main())
