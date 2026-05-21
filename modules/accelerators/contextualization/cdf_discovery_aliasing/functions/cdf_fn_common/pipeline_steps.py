"""Shared execution + steps config for transform and validation discovery tasks."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Tuple

EXECUTION_ORDERED = "ordered"
EXECUTION_PARALLEL = "parallel"
EXECUTION_MODES = frozenset({EXECUTION_ORDERED, EXECUTION_PARALLEL})


def _as_dict(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def parse_execution_mode(cfg: Mapping[str, Any]) -> str:
    exec_block = cfg.get("execution")
    if isinstance(exec_block, dict):
        mode = str(exec_block.get("mode") or "").strip().lower()
        if mode in EXECUTION_MODES:
            return mode
    return EXECUTION_ORDERED


def parse_steps_list(cfg: Mapping[str, Any]) -> List[Dict[str, Any]]:
    raw = cfg.get("steps")
    if not isinstance(raw, list):
        return []
    out: List[Dict[str, Any]] = []
    for item in raw:
        if isinstance(item, dict):
            out.append(dict(item))
    return out


def has_explicit_steps(cfg: Mapping[str, Any]) -> bool:
    return bool(parse_steps_list(cfg))


def is_transform_step(step: Mapping[str, Any]) -> bool:
    return bool(str(step.get("handler_id") or step.get("handler") or "").strip())


def is_validation_step(step: Mapping[str, Any]) -> bool:
    if is_transform_step(step):
        return False
    if step.get("ref") is not None:
        return True
    return bool(
        str(step.get("name") or "").strip()
        or step.get("match") is not None
        or step.get("modifiers") is not None
    )


def materialize_transform_steps(cfg: Mapping[str, Any]) -> Tuple[str, List[Dict[str, Any]]]:
    """
    Return (execution_mode, steps). Legacy single-handler cfg becomes one implicit step.
    """
    steps = parse_steps_list(cfg)
    if steps:
        mode = parse_execution_mode(cfg)
        return mode, steps

    handler = str(cfg.get("handler_id") or cfg.get("handler") or "").strip()
    if not handler:
        return EXECUTION_ORDERED, []

    step = {k: v for k, v in cfg.items() if k not in ("execution", "steps", "field_policies")}
    if "handler_id" not in step and handler:
        step["handler_id"] = handler
    return EXECUTION_ORDERED, [step]


def materialize_validation_steps(cfg: Mapping[str, Any]) -> Tuple[str, List[Dict[str, Any]]]:
    """
    Return (execution_mode, steps). Migrates legacy validation_rules / validation_rule_definitions.
    """
    steps = parse_steps_list(cfg)
    if steps:
        return parse_execution_mode(cfg), steps

    migrated: List[Dict[str, Any]] = []
    defs = cfg.get("validation_rule_definitions")
    if isinstance(defs, dict):
        for rid, body in sorted(defs.items()):
            if isinstance(body, dict):
                step = dict(body)
                if not str(step.get("name") or "").strip():
                    step["name"] = str(rid).strip()
                migrated.append(step)

    from .confidence_match_rule_refs import validation_rules_list_get

    inline = validation_rules_list_get(cfg)
    if isinstance(inline, list):
        for item in inline:
            if isinstance(item, dict):
                migrated.append(dict(item))
            elif isinstance(item, str) and str(item).strip():
                migrated.append({"ref": str(item).strip()})
            elif item is not None:
                migrated.append({"ref": str(item)})

    return EXECUTION_ORDERED, migrated


def validation_steps_to_rules_raw(
    steps: List[Dict[str, Any]],
    execution_mode: str,
) -> List[Any]:
    """Compile steps to validation_rules structure for confidence_match_eval."""
    if not steps:
        return []
    if execution_mode == EXECUTION_PARALLEL:
        return [{"hierarchy": {"mode": "concurrent", "children": list(steps)}}]
    return list(steps)


def validate_execution_block(cfg: Mapping[str, Any], *, context: str = "config") -> None:
    exec_block = cfg.get("execution")
    if exec_block is None:
        return
    if not isinstance(exec_block, dict):
        raise ValueError(f"{context}.execution must be an object")
    mode = str(exec_block.get("mode") or "").strip().lower()
    if mode and mode not in EXECUTION_MODES:
        raise ValueError(
            f"{context}.execution.mode must be ordered or parallel; got {mode!r}"
        )
