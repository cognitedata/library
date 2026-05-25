"""Load workflow definitions, templates, and registry."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

import yaml

DEFAULT_REGISTRY_REL = "workflow_definitions/registry.yaml"
INSTANCES_DIR_REL = "workflow_definitions/instances"
TEMPLATES_DIR_REL = "workflow_definitions/templates"


@dataclass
class WorkflowBuildSource:
    workflow_id: str
    source_kind: str  # instance | template
    document: Dict[str, Any]
    path: Path


def load_yaml(path: Path) -> dict:
    with path.open(encoding="utf-8") as f:
        doc = yaml.safe_load(f) or {}
    if not isinstance(doc, dict):
        raise ValueError(f"YAML must be a mapping: {path}")
    return doc


def definitions_dir(module_root: Path, config: Mapping[str, Any]) -> Path:
    block = config.get("workflow_definitions") if isinstance(config.get("workflow_definitions"), dict) else {}
    rel = str(block.get("instances_dir") or INSTANCES_DIR_REL)
    return (module_root / rel).parent if rel.endswith("/instances") else module_root / "workflow_definitions"


def instances_dir(module_root: Path, config: Mapping[str, Any]) -> Path:
    block = config.get("workflow_definitions") if isinstance(config.get("workflow_definitions"), dict) else {}
    rel = str(block.get("instances_dir") or INSTANCES_DIR_REL)
    return module_root / rel


def templates_dir(module_root: Path, config: Mapping[str, Any]) -> Path:
    block = config.get("workflow_definitions") if isinstance(config.get("workflow_definitions"), dict) else {}
    rel = str(block.get("templates_dir") or TEMPLATES_DIR_REL)
    return module_root / rel


def registry_path(module_root: Path, config: Mapping[str, Any]) -> Path:
    block = config.get("workflow_definitions") if isinstance(config.get("workflow_definitions"), dict) else {}
    rel = str(block.get("registry") or DEFAULT_REGISTRY_REL)
    return module_root / rel


def template_document_for_build(doc: dict, *, workflow_id: str) -> dict:
    out = dict(doc)
    out["id"] = workflow_id
    out.setdefault("label", str(doc.get("label") or workflow_id))
    return out


def load_instance(module_root: Path, workflow_id: str, config: Mapping[str, Any]) -> WorkflowBuildSource:
    path = instances_dir(module_root, config) / f"{workflow_id}.yaml"
    if not path.is_file():
        raise FileNotFoundError(f"Workflow instance not found: {path}")
    doc = load_yaml(path)
    wid = str(doc.get("id") or workflow_id)
    return WorkflowBuildSource(workflow_id=wid, source_kind="instance", document=doc, path=path)


def load_template(module_root: Path, workflow_id: str, config: Mapping[str, Any]) -> WorkflowBuildSource:
    path = templates_dir(module_root, config) / f"{workflow_id}.template.yaml"
    if not path.is_file():
        raise FileNotFoundError(f"Workflow template not found: {path}")
    raw = load_yaml(path)
    tid = str(raw.get("template_id") or workflow_id)
    doc = template_document_for_build(raw, workflow_id=tid)
    return WorkflowBuildSource(workflow_id=tid, source_kind="template", document=doc, path=path)


def load_registry_entries(module_root: Path, config: Mapping[str, Any]) -> List[Dict[str, Any]]:
    path = registry_path(module_root, config)
    if not path.is_file():
        return []
    doc = load_yaml(path)
    workflows = doc.get("workflows")
    if not isinstance(workflows, list):
        return []
    return [w for w in workflows if isinstance(w, dict) and w.get("id")]


def resolve_sources(
    *,
    module_root: Path,
    config: Mapping[str, Any],
    workflow_ids: Optional[List[str]] = None,
    template_ids: Optional[List[str]] = None,
) -> List[WorkflowBuildSource]:
    if template_ids:
        return [load_template(module_root, tid, config) for tid in template_ids]
    if workflow_ids:
        out: List[WorkflowBuildSource] = []
        for wid in workflow_ids:
            inst = instances_dir(module_root, config) / f"{wid}.yaml"
            tpl = templates_dir(module_root, config) / f"{wid}.template.yaml"
            if inst.is_file():
                out.append(load_instance(module_root, wid, config))
            elif tpl.is_file():
                out.append(load_template(module_root, wid, config))
            else:
                raise FileNotFoundError(f"No workflow definition for id={wid!r}")
        return out
    entries = load_registry_entries(module_root, config)
    if not entries:
        inst_root = instances_dir(module_root, config)
        if inst_root.is_dir():
            return [
                load_instance(module_root, p.stem, config)
                for p in sorted(inst_root.glob("*.yaml"))
            ]
        tpl_root = templates_dir(module_root, config)
        if tpl_root.is_dir():
            return [
                load_template(module_root, p.name[: -len(".template.yaml")], config)
                for p in sorted(tpl_root.glob("*.template.yaml"))
            ]
        return []
    out: List[WorkflowBuildSource] = []
    for entry in entries:
        wid = str(entry.get("id") or "").strip()
        if not wid:
            continue
        kind = str(entry.get("source") or "instance").strip()
        if kind == "template":
            out.append(load_template(module_root, wid, config))
        else:
            out.append(load_instance(module_root, wid, config))
    return out
