"""Create CogniteClient from environment (API key or OAuth)."""

from cdf_client_auth import auth_mode_from_env, create_cognite_client

__all__ = ["auth_mode_from_env", "create_cognite_client"]
