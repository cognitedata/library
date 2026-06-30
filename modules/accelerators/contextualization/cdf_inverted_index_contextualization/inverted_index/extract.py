"""Term extraction from DM property values."""

from __future__ import annotations

import re
from typing import Any

from inverted_index.normalize import normalize_term


def read_property_path(instance: dict[str, Any], path: str) -> Any:
    """Dot-path property read; direct relations return external_id when dict-shaped."""
    if not path:
        return None

    def _flatten_dm_properties(props: Any) -> dict[str, Any]:
        flat: dict[str, Any] = {}
        items = props.items() if hasattr(props, "items") else ()
        for _key, val in items:
            if isinstance(val, dict):
                if any(not isinstance(v, (dict, list)) for v in val.values()):
                    for pk, pv in val.items():
                        if not isinstance(pv, (dict, list)) and pk not in flat:
                            flat[pk] = pv
                else:
                    nested = _flatten_dm_properties(val)
                    for pk, pv in nested.items():
                        if pk not in flat:
                            flat[pk] = pv
        return flat

    def _walk(root: Any, parts: list[str]) -> Any:
        current = root
        for part in parts:
            if current is None:
                return None
            if isinstance(current, dict):
                if part in current:
                    current = current[part]
                    continue
                return None
            return None
        if isinstance(current, dict) and "externalId" in current:
            return current.get("externalId")
        if isinstance(current, dict) and "external_id" in current:
            return current.get("external_id")
        return current

    parts = path.split(".")
    value = _walk(instance, parts)
    if value is not None:
        return value

    props = instance.get("properties")
    if isinstance(props, dict) or hasattr(props, "items"):
        value = _walk(props, parts)
        if value is not None:
            return value
        flat = _flatten_dm_properties(props)
        if len(parts) == 1 and parts[0] in flat:
            return flat[parts[0]]
        value = _walk(flat, parts)
        if value is not None:
            return value
    return None


def _exclude_alias_terms(
    results: list[tuple[str, dict]], exclude_normalized_aliases: set[str] | None
) -> list[tuple[str, dict]]:
    if not exclude_normalized_aliases:
        return results
    return [
        (term, meta)
        for term, meta in results
        if normalize_term(term) not in exclude_normalized_aliases
    ]


def extract_terms_from_property(
    value: Any,
    property_config: dict,
    *,
    exclude_normalized_aliases: set[str] | None = None,
) -> list[tuple[str, dict]]:
    """Extract candidate terms from a property value per config."""
    if value is None:
        return []

    extract_mode = property_config.get("extract_mode", "passthrough")
    path = property_config.get("path", "")
    source_type = property_config.get("source_type", "asset_metadata")
    base_meta = {"source_property": path, "source_type": source_type}

    if isinstance(value, list):
        candidates: list[tuple[str, dict]] = []
        for idx, item in enumerate(value):
            for term, frag in extract_terms_from_property(
                item,
                property_config,
                exclude_normalized_aliases=exclude_normalized_aliases,
            ):
                meta = {**frag, "list_index": idx}
                candidates.append((term, meta))
        return candidates

    if extract_mode == "regex":
        pattern = property_config.get("extract_pattern")
        if not pattern or not str(pattern).strip():
            extract_mode = "passthrough"
        else:
            pattern = str(pattern)
            text = str(value)
            results: list[tuple[str, dict]] = []
            for match in re.finditer(pattern, text):
                term = match.group(1) if match.lastindex else match.group(0)
                if term and str(term).strip():
                    results.append(
                        (
                            str(term).strip(),
                            {
                                **base_meta,
                                "extract_mode": "regex",
                                "match_start": match.start(),
                                "match_end": match.end(),
                                "original_value": match.group(0),
                            },
                        )
                    )
            return _exclude_alias_terms(results, exclude_normalized_aliases)

    text = str(value).strip()
    if not text:
        return []
    return _exclude_alias_terms(
        [(text, {**base_meta, "extract_mode": "passthrough", "original_value": text})],
        exclude_normalized_aliases,
    )


def dedupe_extracted_terms(
    candidates: list[tuple[str, dict]],
) -> list[tuple[str, dict]]:
    """Collapse candidates sharing the same normalized term."""
    grouped: dict[str, list[tuple[str, dict]]] = {}
    order: list[str] = []
    for term, meta in candidates:
        key = normalize_term(term)
        if not key:
            continue
        if key not in grouped:
            grouped[key] = []
            order.append(key)
        grouped[key].append((term, meta))

    results: list[tuple[str, dict]] = []
    for key in order:
        group = grouped[key]
        first_term, first_meta = group[0]
        merged = dict(first_meta)
        merged["occurrence_count"] = len(group)
        spans = [
            {"start": m["match_start"], "end": m["match_end"]}
            for _, m in group
            if "match_start" in m and "match_end" in m
        ]
        if len(spans) > 1:
            merged["match_spans"] = spans
        results.append((first_term, merged))
    return results
