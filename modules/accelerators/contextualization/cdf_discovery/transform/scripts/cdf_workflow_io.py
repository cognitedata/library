"""Parse generated Toolkit workflow YAML for SDK deploy and remote runs."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Mapping

import yaml

_PLACEHOLDER = re.compile(r"\{\{[^}]+\}\}")


def workflow_external_id_from_yaml(path: Path) -> str:
    if not path.is_file():
        raise FileNotFoundError(str(path))
    with open(path, encoding="utf-8") as f:
        doc = yaml.safe_load(f)
    if not isinstance(doc, dict):
        raise ValueError(f"Workflow root must be a mapping: {path}")
    ext = doc.get("externalId")
    if not ext or not str(ext).strip():
        raise ValueError(f"Workflow YAML missing externalId: {path}")
    return str(ext).strip()


def workflow_version_from_yaml(path: Path) -> str:
    if not path.is_file():
        raise FileNotFoundError(str(path))
    with open(path, encoding="utf-8") as f:
        doc = yaml.safe_load(f)
    if not isinstance(doc, dict):
        raise ValueError(f"WorkflowVersion root must be a mapping: {path}")
    v = doc.get("version")
    if not v or not str(v).strip():
        raise ValueError(f"Missing workflow version in {path}")
    return str(v).strip()


def workflow_input_from_trigger_yaml(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(str(path))
    with open(path, encoding="utf-8") as f:
        doc = yaml.safe_load(f)
    if not isinstance(doc, dict):
        raise ValueError(f"WorkflowTrigger root must be a mapping: {path}")
    inp = doc.get("input")
    if not isinstance(inp, dict):
        raise ValueError(f"WorkflowTrigger.input must be a mapping in {path}")
    return dict(inp)


def workflow_input_from_json(path: Path) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)
    if not isinstance(raw, dict):
        raise ValueError("--input-json root must be an object")
    return dict(raw)


_INSTANCE_SPACE_TOKEN = "{{instance_space}}"


def substitute_instance_space_placeholder(value: Any, space: str) -> Any:
    if not space:
        return value
    if isinstance(value, str):
        return value.replace(_INSTANCE_SPACE_TOKEN, space) if _INSTANCE_SPACE_TOKEN in value else value
    if isinstance(value, list):
        return [substitute_instance_space_placeholder(x, space) for x in value]
    if isinstance(value, dict):
        return {k: substitute_instance_space_placeholder(v, space) for k, v in value.items()}
    return value


def shallow_has_toolkit_placeholder(obj: Any) -> bool:
    if isinstance(obj, str):
        return bool(_PLACEHOLDER.search(obj))
    if isinstance(obj, Mapping):
        return any(shallow_has_toolkit_placeholder(v) for v in obj.values())
    if isinstance(obj, list):
        return any(shallow_has_toolkit_placeholder(x) for x in obj)
    return False


def assert_expected_workflow_input_keys(inp: Mapping[str, Any]) -> None:
    if "configuration" not in inp:
        raise ValueError("workflow input missing required key 'configuration'")
