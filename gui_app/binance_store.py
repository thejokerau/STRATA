import json
import os
from pathlib import Path
from typing import Any, Dict


CTMT_USER_DIR = Path.home() / ".ctmt"
BINANCE_PREFS_PATH = CTMT_USER_DIR / "binance_profiles.json"
BINANCE_SECRETS_PATH = CTMT_USER_DIR / "binance_secrets.json"
TRADE_LEDGER_PATH = CTMT_USER_DIR / "trade_ledger.json"


def _ensure_user_dir() -> None:
    CTMT_USER_DIR.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(CTMT_USER_DIR, 0o700)
    except Exception:
        pass


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _save_json(path: Path, data: Any, mode: int = 0o600) -> None:
    _ensure_user_dir()
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    try:
        os.chmod(path, mode)
    except Exception:
        pass


def default_binance_preferences() -> Dict[str, Any]:
    return {
        "active_profile": "",
        "profiles": {},
    }


def load_binance_preferences() -> Dict[str, Any]:
    raw = _load_json(BINANCE_PREFS_PATH, default_binance_preferences())
    if not isinstance(raw, dict):
        return default_binance_preferences()
    profiles = raw.get("profiles", {})
    if not isinstance(profiles, dict):
        profiles = {}
    active = str(raw.get("active_profile", "") or "")
    if active and active not in profiles:
        active = ""
    return {"active_profile": active, "profiles": profiles}


def save_binance_preferences(prefs: Dict[str, Any]) -> None:
    _save_json(BINANCE_PREFS_PATH, prefs, mode=0o600)


def load_binance_secrets() -> Dict[str, str]:
    raw = _load_json(BINANCE_SECRETS_PATH, {})
    if not isinstance(raw, dict):
        return {}
    out: Dict[str, str] = {}
    for k, v in raw.items():
        if isinstance(k, str) and isinstance(v, str):
            out[k] = v
    return out


def save_binance_secrets(secrets: Dict[str, str]) -> None:
    _save_json(BINANCE_SECRETS_PATH, secrets, mode=0o600)


def default_trade_ledger() -> Dict[str, Any]:
    return {
        "entries": [],
        "open_positions": {},
        "activity_guard": {},
    }


def load_trade_ledger() -> Dict[str, Any]:
    raw = _load_json(TRADE_LEDGER_PATH, default_trade_ledger())
    if not isinstance(raw, dict):
        return default_trade_ledger()
    entries = raw.get("entries", [])
    open_positions = raw.get("open_positions", {})
    activity_guard = raw.get("activity_guard", {})
    if not isinstance(entries, list):
        entries = []
    if not isinstance(open_positions, dict):
        open_positions = {}
    if not isinstance(activity_guard, dict):
        activity_guard = {}
    return {
        "entries": entries,
        "open_positions": open_positions,
        "activity_guard": activity_guard,
    }


def save_trade_ledger(ledger: Dict[str, Any]) -> None:
    _save_json(TRADE_LEDGER_PATH, ledger, mode=0o600)
