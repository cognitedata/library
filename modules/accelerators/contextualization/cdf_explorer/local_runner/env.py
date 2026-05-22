"""Load .env for local runs."""

from pathlib import Path

from .paths import REPO_ROOT


def load_env() -> None:
    """Load environment variables from repo-root ``.env`` when present."""
    try:
        from dotenv import load_dotenv

        env_path = REPO_ROOT / ".env"
        if env_path.exists():
            load_dotenv(env_path)
        else:
            load_dotenv()
    except Exception:
        pass
