import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


ROOT = Path(__file__).resolve().parents[1]
HANDOFF = ROOT / "PROJECT_HANDOFF.md"


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
RUNS_DIR = DATA_ROOT / "experiments" / "runs"
CHAMPIONS = DATA_ROOT / "experiments" / "registry" / "champions.json"

START = "<!-- AUTO_HANDBACK_START -->"
END = "<!-- AUTO_HANDBACK_END -->"


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


def read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def latest_run_file() -> Optional[Path]:
    files = sorted(RUNS_DIR.glob("run_*.json"))
    return files[-1] if files else None


def scenario_summary(latest: List[Dict[str, Any]]) -> List[str]:
    lines: List[str] = []
    for rec in latest:
        sc = rec.get("scenario", {})
        sid = str(sc.get("id", "scenario"))
        sel = rec.get("selected", {})
        rs = (sel.get("result", {}) or {})
        m = (rs.get("metrics", {}) or {})
        lines.append(
            f"- `{sid}`: selected `{sel.get('name', 'n/a')}`, "
            f"return {float(rs.get('return_pct', 0.0)):+.2f}%, "
            f"maxDD {float(m.get('max_drawdown_pct', 0.0)):.2f}%, "
            f"sharpe {float(m.get('sharpe', 0.0)):.2f}"
        )
    return lines


def build_block() -> str:
    run_file = latest_run_file()
    has_run = (run_file is not None) and run_file.exists()
    run_data = read_json(run_file, []) if has_run else []
    champions = read_json(CHAMPIONS, {})
    promoted_count = len(champions) if isinstance(champions, dict) else 0

    lines: List[str] = []
    lines.append(START)
    lines.append("## Automated Research Status")
    lines.append(f"- Last update UTC: {utc_now_iso()}")
    if has_run:
        try:
            rel = run_file.relative_to(ROOT).as_posix()
        except Exception:
            rel = str(run_file)
        lines.append(f"- Latest experiment artifact: `{rel}`")
    else:
        lines.append("- Latest experiment artifact: none")
    lines.append(f"- Champion scenarios tracked: {promoted_count}")

    if isinstance(run_data, list) and run_data:
        lines.append("- Latest run summary:")
        lines.extend(scenario_summary(run_data))
    else:
        lines.append("- Latest run summary: no data")
    lines.append(END)
    return "\n".join(lines)


def main() -> None:
    assert_writable_dir(DATA_ROOT, "CTMT data root")
    assert_writable_dir(DATA_ROOT / "experiments", "Experiments directory")
    if not HANDOFF.exists():
        raise SystemExit(f"Missing {HANDOFF}")

    content = HANDOFF.read_text(encoding="utf-8")
    block = build_block()
    if START in content and END in content:
        pre = content.split(START)[0].rstrip()
        post = content.split(END)[1].lstrip()
        updated = f"{pre}\n\n{block}\n\n{post}"
    else:
        updated = content.rstrip() + "\n\n" + block + "\n"
    HANDOFF.write_text(updated, encoding="utf-8")
    print(f"Updated handoff automation block in {HANDOFF}")


if __name__ == "__main__":
    main()
