"""Load .env for local runs."""

import os
from pathlib import Path

from .paths import REPO_ROOT


def load_env() -> None:
    """Load environment variables from .env if present. Prefer repo root .env."""
    try:
        from dotenv import load_dotenv

        env_path = REPO_ROOT / ".env"
        if env_path.exists():
            load_dotenv(env_path)
        else:
            load_dotenv()
    except Exception:
        pass
