"""Validate compiled workflow IR against generated WorkflowVersion tasks."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence, Set

import yaml


def _task_ids_from_ir(compiled: Mapping[str, Any]) -> Set[str]:
    out: Set[str] = set()
    for t in compiled.get("tasks") or []:
        if isinstance(t, dict) and t.get("id"):
            out.add(str(t["id"]))
    return out


def _task_ids_from_wv(wv: Mapping[str, Any]) -> Set[str]:
    out: Set[str] = set()
    wf_def = wv.get("workflowDefinition") or {}
    for t in wf_def.get("tasks") or []:
        if isinstance(t, dict) and t.get("externalId"):
            out.add(str(t["externalId"]))
    return out


def _depends_from_ir(compiled: Mapping[str, Any]) -> Dict[str, Set[str]]:
    out: Dict[str, Set[str]] = {}
    for t in compiled.get("tasks") or []:
        if not isinstance(t, dict) or not t.get("id"):
            continue
        tid = str(t["id"])
        out[tid] = {str(d) for d in (t.get("depends_on") or []) if str(d).strip()}
    return out


def _depends_from_wv(wv: Mapping[str, Any]) -> Dict[str, Set[str]]:
    out: Dict[str, Set[str]] = {}
    wf_def = wv.get("workflowDefinition") or {}
    for t in wf_def.get("tasks") or []:
        if not isinstance(t, dict) or not t.get("externalId"):
            continue
        tid = str(t["externalId"])
        deps = set()
        for d in t.get("dependsOn") or []:
            if isinstance(d, dict) and d.get("externalId"):
                deps.add(str(d["externalId"]))
        out[tid] = deps
    return out


def validate_graph_match(*, compiled: Mapping[str, Any], workflow_version: Mapping[str, Any]) -> List[str]:
    errors: List[str] = []
    ir_ids = _task_ids_from_ir(compiled)
    wv_ids = _task_ids_from_wv(workflow_version)
    if ir_ids != wv_ids:
        missing = sorted(ir_ids - wv_ids)
        extra = sorted(wv_ids - ir_ids)
        if missing:
            errors.append(f"WorkflowVersion missing tasks: {missing}")
        if extra:
            errors.append(f"WorkflowVersion has extra tasks: {extra}")
    ir_deps = _depends_from_ir(compiled)
    wv_deps = _depends_from_wv(workflow_version)
    for tid in ir_ids:
        if ir_deps.get(tid, set()) != wv_deps.get(tid, set()):
            errors.append(
                f"dependsOn mismatch for {tid!r}: ir={sorted(ir_deps.get(tid, set()))} "
                f"wv={sorted(wv_deps.get(tid, set()))}"
            )
    return errors


def load_yaml(path: Path) -> Dict[str, Any]:
    with path.open(encoding="utf-8") as f:
        doc = yaml.safe_load(f) or {}
    if not isinstance(doc, dict):
        raise ValueError(f"Expected mapping in {path}")
    return doc


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate ETL pipeline workflow graph")
    parser.add_argument("--config", type=Path, required=True, help="Pipeline *.config.yaml")
    parser.add_argument("--workflow-version", type=Path, required=True, help="WorkflowVersion YAML")
    args = parser.parse_args(list(argv) if argv is not None else None)

    doc = load_yaml(args.config)
    wv = load_yaml(args.workflow_version)
    compiled = doc.get("compiled_workflow")
    if not isinstance(compiled, dict):
        print("Missing compiled_workflow in config", file=sys.stderr)
        return 1
    errors = validate_graph_match(compiled=compiled, workflow_version=wv)
    if errors:
        for err in errors:
            print(f"ERROR: {err}", file=sys.stderr)
        return 1
    print("OK: workflow graph matches compiled IR")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
