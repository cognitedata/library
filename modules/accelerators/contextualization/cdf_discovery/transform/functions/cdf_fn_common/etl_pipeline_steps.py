"""Shared execution + steps config for ETL transform tasks."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Tuple

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


def materialize_transform_steps(cfg: Mapping[str, Any]) -> Tuple[str, List[Dict[str, Any]]]:
    """Return (execution_mode, steps). Legacy single-handler cfg becomes one implicit step."""
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
