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
    env.setdefault("NO_PROXY", "api.binance.com,localhost,127.0.0.1")
    env.setdefault("no_proxy", env.get("NO_PROXY", "api.binance.com,localhost,127.0.0.1"))
    # If a broken local proxy is configured (common: 127.0.0.1:9), neutralize for this process.
    for k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
        v = str(env.get(k, "") or "").strip().lower()
        if ("127.0.0.1:9" in v) or ("localhost:9" in v):
            env[k] = ""
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
