"""Load repository ``.env`` for local CDF runs."""

from __future__ import annotations

from pathlib import Path

from local_runner.paths import get_repo_root


def load_env() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    env_file = get_repo_root() / ".env"
    if env_file.is_file():
        load_dotenv(env_file, override=False)
