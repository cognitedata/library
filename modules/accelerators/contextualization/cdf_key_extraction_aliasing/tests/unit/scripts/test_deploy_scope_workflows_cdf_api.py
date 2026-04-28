"""Tests for deploy_scope_workflows_cdf_api (CDF payload shaping)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_PKG = Path(__file__).resolve().parents[3]
_SCRIPTS = _PKG / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from deploy_scope_workflows_cdf_api import (
    _trigger_dict_for_sdk,
    _workflow_trigger_authentication_from_env,
    _workflow_version_body_for_cdf_api,
)


def test_workflow_version_body_strips_definition_input() -> None:
    raw = {
        "workflowExternalId": "w",
        "version": "v1",
        "workflowDefinition": {"description": "d", "input": {"x": 1}, "tasks": []},
    }
    out = _workflow_version_body_for_cdf_api(raw)
    assert "input" not in out["workflowDefinition"]
    assert out["workflowDefinition"]["tasks"] == []
    assert out["workflowDefinition"]["description"] == "d"


def test_workflow_version_body_identity_when_no_input() -> None:
    raw = {
        "workflowExternalId": "w",
        "version": "v1",
        "workflowDefinition": {"tasks": []},
    }
    assert _workflow_version_body_for_cdf_api(raw) is raw


def test_trigger_dict_strips_file_auth_and_injects_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("KEA_WORKFLOW_TRIGGER_CLIENT_ID", raising=False)
    monkeypatch.delenv("KEA_WORKFLOW_TRIGGER_CLIENT_SECRET", raising=False)
    monkeypatch.setenv("IDP_CLIENT_ID", "id-from-env")
    monkeypatch.setenv("IDP_CLIENT_SECRET", "secret-from-env")
    raw = {
        "externalId": "t1",
        "authentication": {"clientId": "bad", "clientSecret": "bad"},
        "input": {},
    }
    out = _trigger_dict_for_sdk(raw, require_deploy_credentials=False)
    assert out["authentication"] == {"clientId": "id-from-env", "clientSecret": "secret-from-env"}


def test_trigger_dict_requires_env_when_flag_set(monkeypatch: pytest.MonkeyPatch) -> None:
    for k in (
        "KEA_WORKFLOW_TRIGGER_CLIENT_ID",
        "KEA_WORKFLOW_TRIGGER_CLIENT_SECRET",
        "IDP_CLIENT_ID",
        "IDP_CLIENT_SECRET",
        "COGNITE_CLIENT_ID",
        "COGNITE_CLIENT_SECRET",
        "CLIENT_ID",
        "CLIENT_SECRET",
    ):
        monkeypatch.delenv(k, raising=False)
    raw = {"externalId": "t1", "input": {}}
    with pytest.raises(ValueError, match="WorkflowTrigger credentials"):
        _trigger_dict_for_sdk(raw, require_deploy_credentials=True)


def test_workflow_trigger_auth_prefers_kea_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KEA_WORKFLOW_TRIGGER_CLIENT_ID", "kea-id")
    monkeypatch.setenv("KEA_WORKFLOW_TRIGGER_CLIENT_SECRET", "kea-sec")
    monkeypatch.setenv("IDP_CLIENT_ID", "idp-id")
    monkeypatch.setenv("IDP_CLIENT_SECRET", "idp-sec")
    assert _workflow_trigger_authentication_from_env() == {
        "clientId": "kea-id",
        "clientSecret": "kea-sec",
    }
