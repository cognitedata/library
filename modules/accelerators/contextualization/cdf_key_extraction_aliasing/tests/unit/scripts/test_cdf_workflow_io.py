"""Tests for scripts/cdf_workflow_io.py (trigger input + version parsing)."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
import yaml

_PKG = Path(__file__).resolve().parents[3]
_SCRIPTS = _PKG / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from cdf_workflow_io import (
    assert_expected_workflow_input_keys,
    shallow_has_toolkit_placeholder,
    substitute_instance_space_placeholder,
    workflow_input_from_trigger_yaml,
    workflow_version_from_yaml,
)


def test_workflow_version_from_yaml(tmp_path: Path) -> None:
    p = tmp_path / "wv.yaml"
    p.write_text(
        yaml.dump(
            {"workflowExternalId": "key_extraction_aliasing.x", "version": "v5", "workflowDefinition": {}}
        ),
        encoding="utf-8",
    )
    assert workflow_version_from_yaml(p) == "v5"


def test_workflow_input_from_trigger_yaml(tmp_path: Path) -> None:
    p = tmp_path / "tr.yaml"
    body = {
        "externalId": "key_extraction_aliasing.x",
        "input": {
            "run_all": False,
            "run_id": "",
            "configuration": {"source_views": []},
            "compiled_workflow": {"schemaVersion": 1, "tasks": []},
        },
    }
    p.write_text(yaml.dump(body), encoding="utf-8")
    inp = workflow_input_from_trigger_yaml(p)
    assert_expected_workflow_input_keys(inp)
    assert inp["run_all"] is False
    assert "configuration" in inp and "compiled_workflow" in inp


def test_assert_expected_workflow_input_keys_missing() -> None:
    with pytest.raises(ValueError, match="configuration"):
        assert_expected_workflow_input_keys({"compiled_workflow": {}})


def test_shallow_has_toolkit_placeholder() -> None:
    assert shallow_has_toolkit_placeholder("{{instance_space}}") is True
    assert shallow_has_toolkit_placeholder({"a": {"b": "{{x}}"}}) is True
    assert shallow_has_toolkit_placeholder({"a": "ok"}) is False


def test_substitute_instance_space_placeholder() -> None:
    raw = {
        "configuration": {
            "source_views": [
                {"filters": [{"values": ["{{instance_space}}", "other"]}]},
            ]
        },
        "compiled_workflow": {},
        "run_all": False,
        "run_id": "",
    }
    assert_expected_workflow_input_keys(raw)
    out = substitute_instance_space_placeholder(raw, "sp_demo")
    assert out["configuration"]["source_views"][0]["filters"][0]["values"][0] == "sp_demo"
    assert out["configuration"]["source_views"][0]["filters"][0]["values"][1] == "other"
    assert shallow_has_toolkit_placeholder(out) is False


def test_substitute_instance_space_empty_space_is_noop() -> None:
    assert substitute_instance_space_placeholder({"a": "{{instance_space}}"}, "") == {"a": "{{instance_space}}"}


def test_workflow_input_from_json_roundtrip(tmp_path: Path) -> None:
    from cdf_workflow_io import workflow_input_from_json

    p = tmp_path / "in.json"
    payload = {"configuration": {}, "compiled_workflow": {}}
    p.write_text(json.dumps(payload), encoding="utf-8")
    assert workflow_input_from_json(p) == payload
