import json
import os
from pathlib import Path
from typing import Any, Dict


CTMT_GUI_DIR = Path.home() / ".ctmt" / "gui"
STATE_PATH = CTMT_GUI_DIR / "gui_state.json"


def _default_state() -> Dict[str, Any]:
    return {
        "display_currency": "USD",
        "live_panels": [],
        "saved_dashboards": {},
        "auto_refresh_seconds": 120,
    }


def load_state() -> Dict[str, Any]:
    if not STATE_PATH.exists():
        return _default_state()
    try:
        data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return _default_state()
    if not isinstance(data, dict):
        return _default_state()
    base = _default_state()
    base.update(data)
    return base


def save_state(state: Dict[str, Any]) -> None:
    CTMT_GUI_DIR.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(CTMT_GUI_DIR, 0o700)
    except Exception:
        pass
    STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")
    try:
        os.chmod(STATE_PATH, 0o600)
    except Exception:
        pass

