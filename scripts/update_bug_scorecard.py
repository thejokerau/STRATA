import json
from collections import Counter, defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DATA_PATH = ROOT / "docs" / "BUG_SCORECARD_DATA.json"
OUT_PATH = ROOT / "docs" / "BUG_SCORECARD.md"


def load_data() -> dict:
    if not DATA_PATH.exists():
        return {"builds": []}
    return json.loads(DATA_PATH.read_text(encoding="utf-8"))


def status_norm(v: str) -> str:
    s = str(v or "").strip().lower()
    if s == "fixed":
        return "Fixed"
    if s == "partial":
        return "Partial"
    return "Open"


def render_markdown(data: dict) -> str:
    builds = data.get("builds", [])
    if not isinstance(builds, list):
        builds = []

    overall_counter = Counter()
    issue_latest = {}
    issue_first_seen = {}
    issue_counts_by_build = []

    for b in builds:
        items = b.get("items", []) if isinstance(b, dict) else []
        if not isinstance(items, list):
            items = []
        c = Counter()
        for it in items:
            if not isinstance(it, dict):
                continue
            iid = str(it.get("id", "")).strip()
            st = status_norm(str(it.get("status", "")))
            c[st] += 1
            overall_counter[st] += 1
            if iid:
                issue_latest[iid] = {
                    "status": st,
                    "title": str(it.get("title", "")).strip(),
                    "build_id": str(b.get("build_id", "")).strip(),
                }
                issue_first_seen.setdefault(iid, str(b.get("build_id", "")).strip())
        issue_counts_by_build.append((b, c, len(items)))

    latest_status_counter = Counter(v["status"] for v in issue_latest.values())

    lines = []
    lines.append("# CTMT Bug Scorecard")
    lines.append("")
    lines.append("Build-by-build bug tracking plus aggregate summary.")
    lines.append("")
    lines.append("## Overall Summary (Latest Status Per Issue)")
    lines.append("")
    lines.append(f"- Total unique issues tracked: {len(issue_latest)}")
    lines.append(f"- Fixed: {latest_status_counter.get('Fixed', 0)}")
    lines.append(f"- Partial: {latest_status_counter.get('Partial', 0)}")
    lines.append(f"- Open: {latest_status_counter.get('Open', 0)}")
    lines.append("")
    lines.append("## Build-by-Build Summary")
    lines.append("")
    lines.append("| Build | Branch | Date | Total Items | Fixed | Partial | Open |")
    lines.append("|---|---|---:|---:|---:|---:|---:|")
    for b, c, total in issue_counts_by_build:
        lines.append(
            f"| {str(b.get('build_id','')).strip()} | {str(b.get('branch','')).strip()} | {str(b.get('date','')).strip()} | {total} | {c.get('Fixed',0)} | {c.get('Partial',0)} | {c.get('Open',0)} |"
        )
    lines.append("")
    lines.append("## Latest Issue Register")
    lines.append("")
    lines.append("| ID | Title | Latest Status | First Seen Build | Latest Build |")
    lines.append("|---|---|---|---|---|")
    for iid in sorted(issue_latest.keys()):
        rec = issue_latest[iid]
        lines.append(
            f"| {iid} | {rec['title']} | {rec['status']} | {issue_first_seen.get(iid, '')} | {rec['build_id']} |"
        )
    lines.append("")
    lines.append("## Build Details")
    lines.append("")
    for b in builds:
        bid = str(b.get("build_id", "")).strip()
        branch = str(b.get("branch", "")).strip()
        date = str(b.get("date", "")).strip()
        notes = str(b.get("notes", "")).strip()
        lines.append(f"### {bid}")
        lines.append("")
        lines.append(f"- Branch: `{branch}`")
        lines.append(f"- Date: {date}")
        if notes:
            lines.append(f"- Notes: {notes}")
        lines.append("")
        lines.append("| ID | Issue | Status |")
        lines.append("|---|---|---|")
        items = b.get("items", [])
        if isinstance(items, list):
            for it in items:
                if not isinstance(it, dict):
                    continue
                iid = str(it.get("id", "")).strip()
                title = str(it.get("title", "")).strip()
                st = status_norm(str(it.get("status", "")))
                lines.append(f"| {iid} | {title} | {st} |")
        lines.append("")

    lines.append("## Maintenance")
    lines.append("")
    lines.append("1. Update `docs/BUG_SCORECARD_DATA.json` for each build update.")
    lines.append("2. Run `python scripts/update_bug_scorecard.py`.")
    lines.append("3. Commit `docs/BUG_SCORECARD.md` and `docs/BUG_SCORECARD_DATA.json` together.")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    data = load_data()
    md = render_markdown(data)
    OUT_PATH.write_text(md + "\n", encoding="utf-8")
    print(f"Updated {OUT_PATH}")


if __name__ == "__main__":
    main()

