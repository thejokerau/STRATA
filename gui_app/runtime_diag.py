from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class RuntimeDiagLogger:
    """
    Lightweight thread-safe JSONL runtime logger used across Tk + Streamlit + engine.
    """

    def __init__(self, repo_root: Path) -> None:
        self.repo_root = Path(repo_root)
        self.enabled = str(os.environ.get("CTMT_RUNTIME_DIAG", "1")).strip().lower() not in ("0", "false", "off", "no")
        explicit = str(os.environ.get("CTMT_RUNTIME_LOG_PATH", "") or "").strip()
        self.path = Path(explicit) if explicit else self._default_path()
        self._lock = threading.Lock()

    def _default_path(self) -> Path:
        data_root = str(os.environ.get("CTMT_DATA_ROOT", "") or "").strip()
        if data_root:
            return Path(data_root) / "logs" / "runtime_diag.jsonl"
        return self.repo_root / "logs" / "runtime_diag.jsonl"

    def log(self, component: str, event: str, level: str = "INFO", run_id: Optional[str] = None, **fields: Any) -> None:
        if not self.enabled:
            return
        rec: Dict[str, Any] = {
            "ts": _utc_now_iso(),
            "component": str(component or "").strip() or "unknown",
            "event": str(event or "").strip() or "event",
            "level": str(level or "INFO").upper(),
        }
        rid = str(run_id or "").strip()
        if rid:
            rec["run_id"] = rid
        for k, v in fields.items():
            if v is None:
                continue
            rec[str(k)] = v
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            payload = json.dumps(rec, ensure_ascii=False, default=str)
            with self._lock:
                with self.path.open("a", encoding="utf-8") as f:
                    f.write(payload + "\n")
        except Exception:
            # Diagnostics must never break application flow.
            return

