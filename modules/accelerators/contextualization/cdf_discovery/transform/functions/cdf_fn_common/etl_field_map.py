"""Apply field_map mappings to cohort row properties."""

from __future__ import annotations

from typing import Any, Dict, FrozenSet, List, Mapping, MutableMapping, Tuple

IDENTITY_PROP_KEYS: FrozenSet[str] = frozenset(
    {
        "externalId",
        "external_id",
        "space",
        "instance_space",
        "instance_id",
        "node",
        "raw_columns",
        "_variant_index",
    }
)


def parse_field_mappings(cfg: Mapping[str, Any]) -> List[Tuple[str, str]]:
    raw = cfg.get("mappings")
    if not isinstance(raw, list):
        raise ValueError("config.mappings must be a non-empty array for field_map")
    out: List[Tuple[str, str]] = []
    seen_outputs: set[str] = set()
    for item in raw:
        if not isinstance(item, dict):
            continue
        input_field = str(item.get("input_field") or "").strip()
        output_field = str(item.get("output_field") or "").strip()
        if not input_field or not output_field:
            continue
        if output_field in seen_outputs:
            raise ValueError(f"duplicate output_field in mappings: {output_field!r}")
        seen_outputs.add(output_field)
        out.append((input_field, output_field))
    if not out:
        raise ValueError("config.mappings must contain at least one input_field/output_field pair")
    return out


def apply_field_mappings(
    props: Mapping[str, Any],
    mappings: List[Tuple[str, str]],
) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key in IDENTITY_PROP_KEYS:
        if key in props:
            out[key] = props[key]
    for input_field, output_field in mappings:
        if input_field not in props:
            continue
        out[output_field] = props[input_field]
    return out


def validate_field_map_config(cfg: Mapping[str, Any]) -> List[Tuple[str, str]]:
    return parse_field_mappings(cfg)
