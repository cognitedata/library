"""Load .env credentials for local ETL runs."""

from __future__ import annotations

import os
from pathlib import Path

# ``transform/local_runner/env.py`` → ``cdf_discovery/`` module root → repo root
_DISCOVERY_ROOT = Path(__file__).resolve().parent.parent.parent
_REPO_ROOT = _DISCOVERY_ROOT.parent.parent.parent.parent


def load_env(module_root: Path | None = None) -> None:
    """Load environment variables from repo-root ``.env`` when present."""
    del module_root  # kept for call-site compatibility; repo root is derived from ``__file__``
    env_path = _REPO_ROOT / ".env"
    try:
        from dotenv import load_dotenv

        if env_path.is_file():
            load_dotenv(env_path)
        else:
            load_dotenv()
        return
    except Exception:
        pass
    if not env_path.is_file():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = val
