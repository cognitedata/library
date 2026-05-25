"""Multi-step transform execution (ordered / parallel) with property merge."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Mapping, MutableMapping, Sequence

from cdf_fn_common.etl_pipeline_steps import (
    EXECUTION_ORDERED,
    EXECUTION_PARALLEL,
    materialize_transform_steps,
)
from cdf_fn_common.etl_property_merge import merge_property_dicts, parse_field_policies
from cdf_fn_common.etl_transform.transform_handlers import (
    transform_row_properties,
    validate_transform_config,
)

# Internal transform step outputs — not written to cohort PROPERTIES_JSON.
_TRANSFORM_SCRATCH_PROPS = frozenset({"_tagAliasDraft"})


def _drop_transform_scratch_props(props: MutableMapping[str, Any]) -> None:
    for key in _TRANSFORM_SCRATCH_PROPS:
        props.pop(key, None)


def validate_transform_pipeline_config(cfg: Mapping[str, Any]) -> None:
    from cdf_fn_common.etl_pipeline_steps import parse_steps_list, validate_execution_block

    validate_execution_block(cfg, context="transform config")
    mode, steps = materialize_transform_steps(cfg)
    if not steps:
        raise ValueError("transform config requires at least one step or handler_id")
    for i, step in enumerate(steps):
        validate_transform_config(step)
    if mode == EXECUTION_PARALLEL:
        parse_field_policies(cfg)  # validates field_policies list shape when present


def apply_transform_steps_to_props(
    props: Mapping[str, Any],
    cfg: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    """
    Apply all transform steps to one property dict.
    Returns one or more output property dicts (explode_rows may return multiple).
    """
    mode, steps = materialize_transform_steps(cfg)
    if not steps:
        return [dict(props)]

    if mode == EXECUTION_PARALLEL:
        branches: List[Dict[str, Any]] = []
        for step in steps:
            base = deepcopy(dict(props))
            outs = transform_row_properties(base, step)
            if outs:
                branches.append(outs[0])
            else:
                branches.append(base)
        policy_map = parse_field_policies(cfg)
        merged = merge_property_dicts(branches, policy_map)
        _drop_transform_scratch_props(merged)
        return [merged]

    working = deepcopy(dict(props))
    for step in steps:
        outs = transform_row_properties(working, step)
        if not outs:
            continue
        working = outs[0]
        if len(outs) > 1:
            for row in outs:
                _drop_transform_scratch_props(row)
            return outs
    _drop_transform_scratch_props(working)
    return [working]
