"""Consolidate CogniteAsset aliases into diagram-detect pattern mode entities."""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Any, Dict, Iterator, List, Mapping, MutableMapping, Optional, Set

DEFAULT_MAX_PATTERN_SAMPLES = 200
DEFAULT_RESOURCE_TYPE = "equipment"

# Debug-only explicit tag-shape samples (bypass alias consolidation).
DEBUG_EXPLICIT_PATTERN_SAMPLES: tuple[str, ...] = (
    "00-X-00",
    "00-X-000",
    "00-X-0000",
    "00-X-00000",
    "00-XX-00",
    "00-XX-000",
    "00-XX-0000",
    "00-XX-00000",
    "00-XXX-00",
    "00-XXX-000",
    "00-XXX-0000",
    "00-XXX-00000",
)


def _has_alpha_or_class(pattern: str) -> bool:
    if re.search(r"[A-Za-z]", pattern):
        return True
    if re.search(r"\[[^\]]*\|[^\]]*\]", pattern):
        return True
    return False


def _parse_alias(alias: str, resource_type_key: str) -> tuple[str, list[list[str]]]:
    tokens: list[str] = []
    current_alnum: list[str] = []
    for ch in alias:
        if ch.isalnum():
            current_alnum.append(ch)
        else:
            if current_alnum:
                tokens.append("".join(current_alnum))
                current_alnum = []
            tokens.append(ch)
    if current_alnum:
        tokens.append("".join(current_alnum))

    full_template_key_parts: list[str] = []
    all_variable_parts: list[list[str]] = []

    def is_separator(tok: str) -> bool:
        return len(tok) == 1 and not tok.isalnum()

    for i, part in enumerate(tokens):
        if not part:
            continue
        if is_separator(part):
            if part in ("-", " "):
                full_template_key_parts.append(part)
            elif part not in ("[", "]"):
                full_template_key_parts.append(f"[{part}]")
            continue
        left_ok = (i == 0) or is_separator(tokens[i - 1])
        right_ok = (i == len(tokens) - 1) or is_separator(tokens[i + 1])
        if left_ok and right_ok and part == resource_type_key:
            full_template_key_parts.append(f"[{part}]")
            continue
        segment_template = re.sub(r"\d", "0", part)
        segment_template = re.sub(r"[A-Za-z]", "A", segment_template)
        full_template_key_parts.append(segment_template)
        variable_letters = re.findall(r"[A-Za-z]+", part)
        if variable_letters:
            all_variable_parts.append(variable_letters)
    return "".join(full_template_key_parts), all_variable_parts


def _template_to_pattern_string(template_key: str, collected_vars: list[list[Set[str]]]) -> str:
    var_iter: Iterator[List[Set[str]]] = iter(collected_vars)

    def build_segment(segment_template: str) -> str:
        if "A" not in segment_template:
            return segment_template
        try:
            letter_groups_for_segment: List[Set[str]] = next(var_iter)
            letter_group_iter: Iterator[Set[str]] = iter(letter_groups_for_segment)

            def replace_a(match: re.Match[str]) -> str:
                alternatives = sorted(list(next(letter_group_iter)))
                return f"[{'|'.join(alternatives)}]"

            return re.sub(r"A+", replace_a, segment_template)
        except StopIteration:
            return segment_template

    parts = [p for p in re.split(r"(\[[^\]]+\]|[^A-Za-z0-9])", template_key) if p != ""]
    final_pattern_parts = [build_segment(p) if re.search(r"A", p) else p for p in parts]
    return "".join(final_pattern_parts)


def generate_pattern_samples_from_aliases(
    aliases: List[str],
    *,
    resource_type: str = DEFAULT_RESOURCE_TYPE,
) -> List[str]:
    """Merge aliases into deduplicated pattern strings (file_annotation algorithm)."""
    patterns: Dict[str, List[List[Set[str]]]] = {}
    for alias in aliases:
        if not alias or not str(alias).strip():
            continue
        template_key, variable_parts = _parse_alias(str(alias).strip(), resource_type)
        if template_key in patterns:
            existing = patterns[template_key]
            for i, part_group in enumerate(variable_parts):
                if i < len(existing):
                    for j, letter_group in enumerate(part_group):
                        if j < len(existing[i]):
                            existing[i][j].add(letter_group)
        else:
            patterns[template_key] = [[{lg} for lg in part_group] for part_group in variable_parts]

    final_samples: List[str] = []
    for template_key, collected_vars in patterns.items():
        pat = _template_to_pattern_string(template_key, collected_vars)
        if _has_alpha_or_class(pat):
            final_samples.append(pat)
    return sorted(set(final_samples))


def collect_aliases_from_cohort_rows(
    rows: List[Mapping[str, Any]],
    *,
    property_name: str = "aliases",
) -> List[str]:
    out: List[str] = []
    for row in rows:
        props = row.get("properties") if isinstance(row.get("properties"), dict) else {}
        val = props.get(property_name)
        if isinstance(val, list):
            for item in val:
                s = str(item or "").strip()
                if s:
                    out.append(s)
        elif val is not None:
            s = str(val).strip()
            if s:
                out.append(s)
    return out


def build_pattern_entities_from_asset_aliases(
    rows: List[Mapping[str, Any]],
    params: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    """
    Build diagram-detect ``entities`` list from CogniteAsset cohort rows.

    Returns list of dicts with ``sample`` (list of pattern strings) for pattern_mode.
    """
    prop = str(params.get("patterns_entity_property") or "aliases")
    resource_type = str(params.get("pattern_resource_type") or DEFAULT_RESOURCE_TYPE)
    max_samples = int(params.get("max_pattern_samples") or DEFAULT_MAX_PATTERN_SAMPLES)
    mode = str(params.get("pattern_normalization") or "file_annotation").strip().lower()

    aliases = collect_aliases_from_cohort_rows(rows, property_name=prop)
    if mode == "heuristic_literal":
        patterns = generate_pattern_samples_from_aliases(aliases, resource_type=resource_type)
        seeds = params.get("pattern_seed_samples")
        if isinstance(seeds, list):
            for s in seeds:
                st = str(s or "").strip()
                if st:
                    patterns.append(st)
        patterns = sorted(set(patterns))
    else:
        patterns = generate_pattern_samples_from_aliases(aliases, resource_type=resource_type)

    if len(patterns) > max_samples:
        patterns = patterns[:max_samples]

    if not patterns:
        return []

    return [{"sample": patterns, "category": resource_type}]


def build_debug_explicit_pattern_entities(
    params: Optional[Mapping[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Fixed pattern-mode entity list for diagram-detect debugging."""
    p = params or {}
    resource_type = str(p.get("pattern_resource_type") or DEFAULT_RESOURCE_TYPE)
    override = p.get("debug_explicit_pattern_samples")
    if isinstance(override, list) and override:
        samples = [str(s).strip() for s in override if str(s or "").strip()]
    else:
        samples = list(DEBUG_EXPLICIT_PATTERN_SAMPLES)
    return [{"sample": samples, "category": resource_type}]


def use_debug_explicit_pattern_samples(cfg: Mapping[str, Any]) -> bool:
    import os

    if bool(cfg.get("debug_explicit_patterns")):
        return True
    raw = (os.environ.get("KEA_DEBUG_EXPLICIT_PATTERNS") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}
