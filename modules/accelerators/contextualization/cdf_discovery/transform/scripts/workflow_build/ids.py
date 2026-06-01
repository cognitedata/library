"""Workflow and WorkflowTrigger external id pairing."""

from __future__ import annotations

from typing import Any, Dict, Mapping, MutableMapping


def workflow_base_from_config(config: Mapping[str, Any], workflow_id: str) -> str:
    del config
    # Build IDs should derive from the workflow being built unless explicitly
    # overridden on the start node (workflow_external_id / workflow_base).
    return f"wf_etl_{workflow_id}"


def workflow_external_id(*, workflow_base: str, scope_suffix: str = "") -> str:
    from workflow_build.paths import normalize_scope_suffix

    base = str(workflow_base).strip()
    suffix = normalize_scope_suffix(scope_suffix)
    if not suffix:
        return base
    return f"{base}_{suffix}"


def workflow_trigger_external_id(workflow_external_id: str) -> str:
    wf = str(workflow_external_id).strip()
    if wf.startswith("trg_"):
        return wf
    return f"trg_{wf}"


def _start_node(canvas: Mapping[str, Any]) -> MutableMapping[str, Any] | None:
    for n in canvas.get("nodes") or []:
        if isinstance(n, dict) and str(n.get("kind") or "").strip() == "start":
            return n
    return None


def read_workflow_base_override(canvas: Mapping[str, Any]) -> str | None:
    node = _start_node(canvas)
    if not node:
        return None
    data = node.get("data") if isinstance(node.get("data"), dict) else {}
    cfg = data.get("config") if isinstance(data.get("config"), dict) else {}
    # Prefer an explicit workflow external id override from start-node config.
    raw_external = str(cfg.get("workflow_external_id") or "").strip()
    if raw_external:
        return raw_external or None
    raw_base = str(cfg.get("workflow_base") or "").strip()
    return raw_base or None


def resolve_workflow_base(
    *,
    config: Mapping[str, Any],
    workflow_id: str,
    canvas: Mapping[str, Any] | None = None,
) -> str:
    if canvas is not None:
        override = read_workflow_base_override(canvas)
        if override:
            return override
    return workflow_base_from_config(config, workflow_id)


def resolve_workflow_base_for_build(
    *,
    source_kind: str,
    config: Mapping[str, Any],
    workflow_id: str,
    canvas: Mapping[str, Any] | None = None,
) -> str:
    if canvas is not None:
        override = read_workflow_base_override(canvas)
        if override:
            return override
    del source_kind
    return workflow_base_from_config(config, workflow_id)


def patch_start_node_workflow_pairing(
    canvas: MutableMapping[str, Any],
    *,
    workflow_base: str,
    scope_suffix: str = "",
    workflow_version: str = "1",
) -> Dict[str, str]:
    node = _start_node(canvas)
    if node is None:
        wf_ext = workflow_external_id(workflow_base=workflow_base, scope_suffix=scope_suffix)
        trg_ext = workflow_trigger_external_id(wf_ext)
        return {
            "workflow_base": workflow_base,
            "workflow_external_id": wf_ext,
            "trigger_external_id": trg_ext,
            "workflow_version": workflow_version,
        }

    data = node.setdefault("data", {})
    if not isinstance(data, dict):
        data = {}
        node["data"] = data
    cfg = data.setdefault("config", {})
    if not isinstance(cfg, dict):
        cfg = {}
        data["config"] = cfg

    wf_ext = workflow_external_id(workflow_base=workflow_base, scope_suffix=scope_suffix)
    trg_ext = workflow_trigger_external_id(wf_ext)
    cfg["workflow_base"] = workflow_base
    cfg["workflow_external_id"] = wf_ext
    cfg["trigger_external_id"] = trg_ext
    cfg["workflow_version"] = str(workflow_version or "1")
    cfg.pop("externalId", None)
    return {
        "workflow_base": workflow_base,
        "workflow_external_id": wf_ext,
        "trigger_external_id": trg_ext,
        "workflow_version": str(workflow_version or "1"),
    }


def list_build_pairings(
    *,
    workflow_base: str,
    scope_suffixes: list[str],
    workflow_version: str = "1",
) -> list[Dict[str, str]]:
    out: list[Dict[str, str]] = []
    for suffix in scope_suffixes:
        wf = workflow_external_id(workflow_base=workflow_base, scope_suffix=suffix)
        out.append(
            {
                "scope_suffix": suffix,
                "workflow_external_id": wf,
                "trigger_external_id": workflow_trigger_external_id(wf),
                "workflow_version": workflow_version,
            }
        )
    return out
