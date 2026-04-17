import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple


ROOT = Path(__file__).resolve().parents[1]


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
EXPERIMENTS_DIR = DATA_ROOT / "experiments"
CANDIDATES_PATH = EXPERIMENTS_DIR / "candidates.jsonl"
REGISTRY_DIR = EXPERIMENTS_DIR / "registry"
CHAMPIONS_PATH = REGISTRY_DIR / "champions.json"
REPORTS_DIR = EXPERIMENTS_DIR / "reports"


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


def read_champions() -> Dict[str, Any]:
    if not CHAMPIONS_PATH.exists():
        return {}
    try:
        return json.loads(CHAMPIONS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def read_candidates(limit: int = 200) -> List[Dict[str, Any]]:
    if not CANDIDATES_PATH.exists():
        return []
    rows: List[Dict[str, Any]] = []
    with CANDIDATES_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                continue
    return rows[-limit:]


def pick_latest_per_scenario(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        sid = str(r.get("scenario_id", "")).strip()
        if not sid:
            continue
        prev = out.get(sid)
        if prev is None or str(r.get("logged_at_utc", "")) > str(prev.get("logged_at_utc", "")):
            out[sid] = r
    return out


def sf(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def candidate_metrics(row: Dict[str, Any]) -> Dict[str, float]:
    selected = row.get("selected", {}) or {}
    result = selected.get("result", {}) or {}
    metrics = result.get("metrics", {}) or {}
    tuned = row.get("tuned", {}) or {}
    holdout = tuned.get("holdout", {}) or {}
    holdout_metrics = holdout.get("metrics", {}) or {}
    return {
        "return_pct": sf(result.get("return_pct", 0.0)),
        "max_drawdown_pct": sf(metrics.get("max_drawdown_pct", 100.0), 100.0),
        "sharpe": sf(metrics.get("sharpe", 0.0)),
        "sortino": sf(metrics.get("sortino", 0.0)),
        "turnover_per_year": sf(metrics.get("turnover_per_year", 0.0)),
        "holdout_return_pct": sf(holdout.get("return_pct", 0.0)),
        "holdout_max_drawdown_pct": sf(holdout_metrics.get("max_drawdown_pct", 0.0)),
        "holdout_sharpe": sf(holdout_metrics.get("sharpe", 0.0)),
    }


def promote_decision(
    candidate: Dict[str, Any],
    champion: Dict[str, Any],
    min_return_delta: float,
    max_dd_increase: float,
    max_sharpe_drop: float,
) -> Tuple[bool, str]:
    c = candidate_metrics(candidate)
    selected_cfg = (candidate.get("selected", {}) or {}).get("config", {}) or {}
    dd_target = sf(selected_cfg.get("max_drawdown_limit_pct", 35.0), 35.0)

    if c["max_drawdown_pct"] > dd_target:
        return False, f"rejected: max drawdown {c['max_drawdown_pct']:.2f}% > target {dd_target:.2f}%"

    if not champion:
        return True, "accepted: no existing champion"

    ch = champion.get("metrics", {}) or {}
    ch_ret = sf(ch.get("return_pct", -1e9), -1e9)
    ch_dd = sf(ch.get("max_drawdown_pct", 100.0), 100.0)
    ch_sharpe = sf(ch.get("sharpe", -1e9), -1e9)

    if c["return_pct"] < (ch_ret + min_return_delta):
        return False, f"rejected: return delta {c['return_pct'] - ch_ret:+.2f}% < required +{min_return_delta:.2f}%"
    if c["max_drawdown_pct"] > (ch_dd + max_dd_increase):
        return False, f"rejected: drawdown increase {c['max_drawdown_pct'] - ch_dd:+.2f}% > allowed +{max_dd_increase:.2f}%"
    if c["sharpe"] < (ch_sharpe - max_sharpe_drop):
        return False, f"rejected: sharpe drop {c['sharpe'] - ch_sharpe:+.2f} < allowed -{max_sharpe_drop:.2f}"
    return True, "accepted: beats champion under promotion gates"


def write_report(lines: List[str]) -> Path:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = REPORTS_DIR / f"promotion_{stamp}.md"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Promote experiment candidates into champion registry.")
    p.add_argument("--min-return-delta", type=float, default=1.0)
    p.add_argument("--max-dd-increase", type=float, default=1.5)
    p.add_argument("--max-sharpe-drop", type=float, default=0.10)
    p.add_argument("--limit", type=int, default=300)
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    assert_writable_dir(DATA_ROOT, "CTMT data root")
    assert_writable_dir(EXPERIMENTS_DIR, "Experiments directory")
    REGISTRY_DIR.mkdir(parents=True, exist_ok=True)

    champions = read_champions()
    rows = read_candidates(limit=max(1, int(args.limit)))
    latest = pick_latest_per_scenario(rows)
    if not latest:
        raise SystemExit("No candidates found.")

    report_lines: List[str] = []
    report_lines.append("# Champion Promotion Report")
    report_lines.append(f"- Generated UTC: {utc_now_iso()}")
    report_lines.append("")

    promotions = 0
    for sid, cand in sorted(latest.items()):
        champ = champions.get(sid, {})
        ok, reason = promote_decision(
            cand,
            champ,
            min_return_delta=float(args.min_return_delta),
            max_dd_increase=float(args.max_dd_increase),
            max_sharpe_drop=float(args.max_sharpe_drop),
        )
        cm = candidate_metrics(cand)
        report_lines.append(f"## {sid}")
        report_lines.append(f"- Candidate return/maxDD/sharpe: {cm['return_pct']:+.2f}% / {cm['max_drawdown_pct']:.2f}% / {cm['sharpe']:.2f}")
        report_lines.append(f"- Decision: {reason}")

        if ok:
            promotions += 1
            champions[sid] = {
                "promoted_at_utc": utc_now_iso(),
                "source_run_file": cand.get("run_file"),
                "metrics": {
                    "return_pct": cm["return_pct"],
                    "max_drawdown_pct": cm["max_drawdown_pct"],
                    "sharpe": cm["sharpe"],
                    "sortino": cm["sortino"],
                    "turnover_per_year": cm["turnover_per_year"],
                    "holdout_return_pct": cm["holdout_return_pct"],
                    "holdout_max_drawdown_pct": cm["holdout_max_drawdown_pct"],
                    "holdout_sharpe": cm["holdout_sharpe"],
                },
                "selected": cand.get("selected", {}),
                "tuned": cand.get("tuned", {}),
                "baseline": cand.get("baseline", {}),
            }
            report_lines.append("- Action: promoted")
        else:
            report_lines.append("- Action: kept existing champion")
        report_lines.append("")

    report_path = write_report(report_lines)
    print(f"Wrote report: {report_path}")

    if args.dry_run:
        print("Dry run enabled: champions registry not updated.")
        return

    CHAMPIONS_PATH.write_text(json.dumps(champions, indent=2), encoding="utf-8")
    print(f"Updated champions registry: {CHAMPIONS_PATH}")
    print(f"Promotions applied: {promotions}")


if __name__ == "__main__":
    main()
