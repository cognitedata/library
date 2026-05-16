"""Shared scope YAML load/dump/compile validation (operator server, promote CLI, scope build)."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Mapping

import yaml

from cdf_fn_common.scope_canvas_merge import normalize_root_graph_into_canvas

SCOPE_DOCUMENT_DUMP_KWARGS: Dict[str, Any] = {
    "default_flow_style": False,
    "sort_keys": False,
    "allow_unicode": True,
}


def load_scope_document_dict_normalized(path: Path) -> Dict[str, Any]:
    """Parse scope YAML from *path*, require a mapping root, hoist root graph into ``canvas`` (in place)."""
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML: {e}") from e
    if not isinstance(raw, dict):
        raise ValueError("Scope YAML root must be a mapping")
    normalize_root_graph_into_canvas(raw)
    return raw


def dump_scope_document_yaml_roundtrip(doc: Mapping[str, Any]) -> str:
    """Dump *doc* with stable operator/build kwargs and verify parse round-trip."""
    text = yaml.safe_dump(doc, **SCOPE_DOCUMENT_DUMP_KWARGS)
    try:
        yaml.safe_load(text)
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML after dump: {e}") from e
    return text


def compile_validate_scope_document(doc: Dict[str, Any]) -> None:
    """Raise ``CanvasCompileError`` or ``ValueError`` if canvas does not compile."""
    from functions.cdf_fn_common.workflow_compile.canvas_dag import (  # noqa: PLC0415
        compiled_workflow_for_scope_document,
    )

    compiled_workflow_for_scope_document(doc)


def promote_unified_scope_file_to_template_config(*, source: Path, destination: Path) -> None:
    """Read unified scope from *source*, normalize, strip ``compiled_workflow``, dump, write *destination*."""
    doc = load_scope_document_dict_normalized(source)
    doc.pop("compiled_workflow", None)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(dump_scope_document_yaml_roundtrip(doc), encoding="utf-8")
