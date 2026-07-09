"""Load .env for local runs."""

from __future__ import annotations

from pathlib import Path


def load_env(module_root: Path | None = None) -> None:
    """Load environment variables from repo or module .env."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        return

    root = module_root or Path(__file__).resolve().parent.parent
    repo_root = root.parent.parent.parent.parent
    for candidate in (repo_root / ".env", root / ".env"):
        if candidate.exists():
            load_dotenv(candidate)
            return
    load_dotenv()
