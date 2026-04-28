"""Parse generated Toolkit workflow YAML for SDK smoke runs (no CDF calls)."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Mapping

import yaml

_PLACEHOLDER = re.compile(r"\{\{[^}]+\}\}")


def workflow_version_from_yaml(path: Path) -> str:
    """Return ``version`` from a ``*.WorkflowVersion.yaml`` document."""
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
    """Return the trigger ``input`` mapping (becomes ``workflow.input`` on the run)."""
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


def shallow_has_toolkit_placeholder(obj: Any) -> bool:
    """True if any string value contains ``{{ ... }}`` (unresolved Toolkit template)."""
    if isinstance(obj, str):
        return bool(_PLACEHOLDER.search(obj))
    if isinstance(obj, Mapping):
        return any(shallow_has_toolkit_placeholder(v) for v in obj.values())
    if isinstance(obj, list):
        return any(shallow_has_toolkit_placeholder(x) for x in obj)
    return False


def assert_expected_workflow_input_keys(inp: Mapping[str, Any]) -> None:
    """Minimal shape check for key-extraction-aliasing triggers."""
    for key in ("configuration", "compiled_workflow"):
        if key not in inp:
            raise ValueError(f"workflow input missing required key {key!r}")
