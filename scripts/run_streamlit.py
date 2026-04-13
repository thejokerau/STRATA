from pathlib import Path
import subprocess
import sys
import os


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    app_path = repo_root / "streamlit_app" / "app.py"
    env = os.environ.copy()
    env.setdefault("NUMBA_DISABLE_JIT", "1")
    env.setdefault("NUMBA_CACHE_DIR", str(Path.home() / ".numba_cache"))
    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(app_path),
        "--server.headless",
        "true",
        "--server.port",
        "8501",
    ]
    return subprocess.call(cmd, cwd=str(repo_root), env=env)


if __name__ == "__main__":
    raise SystemExit(main())
