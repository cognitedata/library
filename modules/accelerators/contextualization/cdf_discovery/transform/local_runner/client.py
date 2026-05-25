"""Create CogniteClient from environment for local ETL runs."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Optional

_DISCOVERY_ROOT = Path(__file__).resolve().parent.parent.parent


def _discovery_auth():
    root = str(_DISCOVERY_ROOT)
    if root not in sys.path:
        sys.path.insert(0, root)
    from cdf_client_auth import auth_mode_from_env, create_cognite_client as _create

    return _create, auth_mode_from_env


def create_cognite_client() -> Optional[Any]:
    """Return a CogniteClient when credentials are configured, else ``None``."""
    try:
        create, _ = _discovery_auth()
        return create(client_name="cdf-discovery-etl-local")
    except RuntimeError:
        return None
    except ImportError:
        return None


def auth_mode_from_env() -> str:
    try:
        _, auth_mode = _discovery_auth()
        return auth_mode()
    except ImportError:
        return "oauth"
