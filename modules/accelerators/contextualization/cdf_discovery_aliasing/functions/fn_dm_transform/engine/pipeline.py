"""Config validation, dispatch, and row-level transform orchestration."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Tuple

from .handlers.base import AbstractTransformHandler, TransformResult
from .constants import (
    TRANSFORM_HANDLERS,
    _MULTI_VALUE_HANDLERS,
    _OUTPUT_MODES,
    _OUTPUT_MULTI_VALUE,
)
from .field_template import apply_output_template, extract_field_values
from .output_type import coerce_transform_output, validate_output_field_type
from .handlers.heuristic_sampler import (
    apply_heuristic_sampler,
    heuristic_sampler_multi_value,
    validate_heuristic_sampler_block,
)
from .handlers.split_join import validate_split_join_block
from .handlers.split_parts import validate_split_parts_block
from .registry import HANDLER_BY_ID


def _default_output_multi_value(handler_id: str) -> str:
    if handler_id in ("split_string", "heuristic_sampler"):
        return "array_json"
    return "explode_rows"


def resolve_handler_id(cfg: Mapping[str, Any]) -> str:
    return AbstractTransformHandler.first_nonempty(cfg.get("handler_id"), cfg.get("handler"))


def resolve_handler_block(cfg: Mapping[str, Any], handler_id: str) -> Dict[str, Any]:
    block = cfg.get(handler_id)
    if isinstance(block, dict):
        return dict(block)
    nested = cfg.get("config")
    if isinstance(nested, dict):
        return dict(nested)
    return {}


def validate_transform_config(cfg: Mapping[str, Any]) -> None:
    from cdf_fn_common.pipeline_steps import parse_steps_list

    if parse_steps_list(cfg):
        return
    handler_id = resolve_handler_id(cfg)
    if handler_id not in TRANSFORM_HANDLERS:
        raise ValueError(
            f"transform config handler_id must be one of {sorted(TRANSFORM_HANDLERS)}; got {handler_id!r}"
        )
    mode = AbstractTransformHandler.first_nonempty(cfg.get("output_mode"), "append")
    if mode not in _OUTPUT_MODES:
        raise ValueError(f"output_mode must be overwrite or append; got {mode!r}")
    if handler_id in _MULTI_VALUE_HANDLERS:
        if handler_id == "substitution_variants":
            block = resolve_handler_block(cfg, handler_id)
            variants = block.get("variants") or []
            normalized = [str(v).strip() for v in variants if str(v).strip()]
            if len(normalized) != len(set(normalized)):
                raise ValueError("substitution_variants: variants[] must not contain duplicates")
        omv = AbstractTransformHandler.first_nonempty(
            cfg.get("output_multi_value"), _default_output_multi_value(handler_id)
        )
        if omv not in _OUTPUT_MULTI_VALUE:
            raise ValueError(f"output_multi_value must be array_json or explode_rows; got {omv!r}")
    validate_output_field_type(cfg)
    if handler_id == "heuristic_sampler":
        hs_block = resolve_handler_block(cfg, handler_id)
        validate_heuristic_sampler_block(hs_block)
        if heuristic_sampler_multi_value(hs_block):
            omv = AbstractTransformHandler.first_nonempty(
                cfg.get("output_multi_value"), _default_output_multi_value(handler_id)
            )
            if omv not in _OUTPUT_MULTI_VALUE:
                raise ValueError(f"output_multi_value must be array_json or explode_rows; got {omv!r}")
    if handler_id in ("split_join", "split_string"):
        validate_split_parts_block(resolve_handler_block(cfg, handler_id))
    if handler_id == "split_join":
        validate_split_join_block(resolve_handler_block(cfg, handler_id))


def apply_transform_handler(
    handler_id: str,
    working: str,
    block: Mapping[str, Any],
    *,
    field_values: Optional[Mapping[str, str]] = None,
    props: Optional[Mapping[str, Any]] = None,
) -> Tuple[TransformResult, bool]:
    cls = HANDLER_BY_ID.get(handler_id)
    if cls is None:
        raise ValueError(f"Unknown transform handler_id: {handler_id!r}")
    result = cls.apply(working, block, field_values=field_values, props=props)
    multi = cls.multi_value
    if handler_id == "heuristic_sampler":
        multi = heuristic_sampler_multi_value(block)
    return result, multi


def _append_values_unique(existing: Any, coerced: Any) -> Any:
    """Append *coerced* to *existing* list field without duplicating equal tokens."""
    if existing is None or existing == "":
        return coerced
    to_add = coerced if isinstance(coerced, list) else [coerced]
    if isinstance(existing, list):
        out = list(existing)
        for item in to_add:
            if item not in out:
                out.append(item)
        return out
    out = [existing]
    for item in to_add:
        if item not in out:
            out.append(item)
    return out


def write_output_to_props(
    props: MutableMapping[str, Any],
    output_field: str,
    value: TransformResult,
    mode: str,
    *,
    output_field_type: str = "auto",
) -> None:
    if not output_field:
        return
    coerced = coerce_transform_output(value, output_field_type)
    if mode == "append":
        props[output_field] = _append_values_unique(props.get(output_field), coerced)
    else:
        props[output_field] = coerced


def transform_row_properties(
    props: Mapping[str, Any], cfg: Mapping[str, Any]
) -> List[Dict[str, Any]]:
    """Return one or more property dicts after applying the configured transform."""
    validate_transform_config(cfg)
    handler_id = resolve_handler_id(cfg)
    block = resolve_handler_block(cfg, handler_id)
    fields = cfg.get("fields") or []
    field_values = extract_field_values(props, fields if isinstance(fields, list) else [])
    template = AbstractTransformHandler.first_nonempty(cfg.get("output_template"))
    if template:
        working = apply_output_template(template, field_values)
    elif field_values:
        if len(field_values) == 1:
            working = next(iter(field_values.values()))
        else:
            # Comma-join unique values in rule-row order (dict insertion order from
            # extract_field_values / fields[] sort), not alphabetical by property name.
            working = ",".join(dict.fromkeys(field_values.values()))
    else:
        working = ""

    result, multi = apply_transform_handler(
        handler_id,
        working,
        block,
        field_values=field_values,
        props=props,
    )
    output_field = AbstractTransformHandler.first_nonempty(cfg.get("output_field"))
    output_mode = AbstractTransformHandler.first_nonempty(cfg.get("output_mode"), "append")
    output_multi = AbstractTransformHandler.first_nonempty(
        cfg.get("output_multi_value"), _default_output_multi_value(handler_id)
    )
    oft = AbstractTransformHandler.first_nonempty(cfg.get("output_field_type"), "auto")

    out_props = deepcopy(dict(props))
    if multi and isinstance(result, list):
        if output_multi == "array_json":
            write_output_to_props(out_props, output_field, result, output_mode, output_field_type=oft)
            return [out_props]
        rows: List[Dict[str, Any]] = []
        for idx, variant in enumerate(result):
            row_props = deepcopy(dict(props))
            write_output_to_props(row_props, output_field, variant, output_mode, output_field_type=oft)
            row_props["_variant_index"] = idx
            rows.append(row_props)
        return rows or [out_props]

    if result is None:
        write_output_to_props(out_props, output_field, "", output_mode, output_field_type=oft)
        return [out_props]
    if isinstance(result, (int, float, bool)):
        write_output_to_props(out_props, output_field, result, output_mode, output_field_type=oft)
        return [out_props]
    scalar = str(result)
    write_output_to_props(out_props, output_field, scalar, output_mode, output_field_type=oft)
    return [out_props]