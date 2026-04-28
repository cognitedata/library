"""Load v1 scope YAML for ``build_scopes`` (same canvas sibling merge as ``module.py run``).

Sibling ``*.canvas.yaml`` with a non-empty graph replaces any inline ``canvas`` on the scope YAML so
built WorkflowTrigger configuration matches the flow editor export.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import yaml

from functions.cdf_fn_common.scope_canvas_merge import merge_sibling_canvas_yaml_into_scope


def load_scope_document_dict_for_build(scope_yaml_path: Path) -> Dict[str, Any]:
    """Parse scope YAML from *scope_yaml_path* and merge sibling ``*.canvas.yaml`` when needed."""
    if not scope_yaml_path.is_file():
        raise FileNotFoundError(f"Scope document not found: {scope_yaml_path}")
    raw = yaml.safe_load(scope_yaml_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"Scope YAML root must be a mapping: {scope_yaml_path}")
    merge_sibling_canvas_yaml_into_scope(raw, scope_yaml_path)
    return raw
