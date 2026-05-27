"""Kuiper expressions and local input prep for diagram annotation jsonMapping nodes."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, MutableMapping, Sequence

from cdf_fn_common.etl_annotation_map.expand import (
    expand_cohort_rows_to_classic_rows,
    expand_cohort_rows_to_dm_rows,
)
from cdf_fn_common.etl_file_annotation.cohort_rows import predecessor_cohort_rows
from cdf_fn_common.kuiper_eval import evaluate_kuiper_expression, normalize_kuiper_json_value

_DIAGRAM_MAPPER_KINDS = frozenset({"diagram_detect_to_dm", "diagram_detect_to_classic"})


def is_diagram_mapper_kind(mapper_kind: str) -> bool:
    return str(mapper_kind or "").strip().lower() in _DIAGRAM_MAPPER_KINDS


def default_kuiper_expression(cfg: Mapping[str, Any]) -> str:
    """Default Kuiper body when the canvas config leaves expression empty."""
    mapper_kind = str(cfg.get("mapper_kind") or "custom").strip().lower()
    if mapper_kind in _DIAGRAM_MAPPER_KINDS:
        return "input.rows"
    return "input"


def resolve_json_mapping_expression(cfg: Mapping[str, Any]) -> str:
    expr = str(cfg.get("expression") or "").strip()
    return expr or default_kuiper_expression(cfg)


def enrich_json_mapping_config_for_compile(cfg: Mapping[str, Any]) -> Dict[str, Any]:
    """Ensure compiled IR carries the Kuiper expression CDF will execute."""
    out = {k: v for k, v in cfg.items() if k not in ("compile_as", "source_task_id")}
    out["expression"] = resolve_json_mapping_expression(cfg)
    return out


def _expand_diagram_rows(
    cohort_rows: List[Mapping[str, Any]],
    mapper_kind: str,
    cfg: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    if mapper_kind == "diagram_detect_to_dm":
        return expand_cohort_rows_to_dm_rows(
            cohort_rows,
            {
                "annotation_space": str(cfg.get("annotation_space") or "discovery-annotations"),
                "default_status": str(cfg.get("default_status") or "Suggested"),
            },
        )
    return expand_cohort_rows_to_classic_rows(cohort_rows, cfg)


def prepare_local_json_mapping_input(
    cfg: Mapping[str, Any],
    resolved_input: Mapping[str, Any],
    *,
    client: Any,
    data: Mapping[str, Any],
    source_task_id: str,
) -> Dict[str, Any]:
    """
    Build the ``input`` object passed to Kuiper locally.

    Diagram mappers load file-annotation cohort rows from the wired predecessor RAW
    table from the wired predecessor task, then expand to staging rows.
    """
    mapper_kind = str(cfg.get("mapper_kind") or "custom").strip().lower()
    inp = dict(resolved_input)

    if is_diagram_mapper_kind(mapper_kind):
        rows = inp.get("rows")
        if not isinstance(rows, list) or not rows:
            cohort = predecessor_cohort_rows(client, data, source_task_id)
            if not cohort:
                raise ValueError(
                    f"jsonMapping diagram mapper: no cohort rows from predecessor {source_task_id!r}"
                )
            inp["rows"] = _expand_diagram_rows(cohort, mapper_kind, cfg)
        return inp

    return inp


def run_json_mapping_kuiper(
    cfg: Mapping[str, Any],
    input_data: Mapping[str, Any],
) -> Any:
    expression = resolve_json_mapping_expression(cfg)
    result = evaluate_kuiper_expression(expression, input_data)
    return normalize_kuiper_json_value(result)
