"""Unit tests for cdf_client_auth credential selection."""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def _clear_cdf_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in list(os.environ):
        if key.startswith(
            (
                "COGNITE_",
                "CDF_",
                "IDP_",
                "LOGIN_",
                "PROJECT",
                "BASE_URL",
                "API_KEY",
                "CLIENT_",
                "TOKEN_",
                "TENANT_",
                "SCOPES",
            )
        ) or key in ("LOGIN_FLOW", "PROJECT", "BASE_URL"):
            monkeypatch.delenv(key, raising=False)


def test_auth_mode_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COGNITE_API_KEY", "secret")
    from cdf_client_auth import auth_mode_from_env

    assert auth_mode_from_env() == "api_key"


def test_auth_mode_interactive_without_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CDF_PROJECT", "my-project")
    monkeypatch.setenv("CDF_CLUSTER", "greenfield")
    monkeypatch.setenv("IDP_TENANT_ID", "tenant")
    monkeypatch.setenv("IDP_CLIENT_ID", "app-id")
    from cdf_client_auth import auth_mode_from_env

    assert auth_mode_from_env() == "interactive"


def test_create_client_uses_interactive_when_no_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CDF_PROJECT", "my-project")
    monkeypatch.setenv("CDF_CLUSTER", "greenfield")
    monkeypatch.setenv("IDP_TENANT_ID", "tenant")
    monkeypatch.setenv("IDP_CLIENT_ID", "app-id")
    monkeypatch.setenv("LOGIN_FLOW", "interactive")

    mock_credentials = MagicMock()
    mock_client = MagicMock()

    with (
        patch(
            "cognite.client.credentials.OAuthInteractive.default_for_azure_ad",
            return_value=mock_credentials,
        ) as default_for_azure,
        patch("cognite.client.CogniteClient", return_value=mock_client) as client_cls,
    ):
        from cdf_client_auth import create_cognite_client

        out = create_cognite_client(client_name="test")

    default_for_azure.assert_called_once()
    assert out is mock_client
    mock_client.iam.token.inspect.assert_called_once()
    client_cls.assert_called_once()
    cfg = client_cls.call_args.kwargs.get("config") or client_cls.call_args[0][0]
    assert cfg.client_name == "test"
    assert cfg.project == "my-project"


def test_missing_credentials_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    from cdf_client_auth import create_cognite_client

    with pytest.raises(RuntimeError, match="Missing CDF credentials"):
        create_cognite_client()
