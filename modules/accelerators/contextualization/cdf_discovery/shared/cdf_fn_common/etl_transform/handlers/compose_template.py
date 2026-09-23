"""Handler: compose_template — list-aware field template composition."""

from __future__ import annotations

import re
from typing import Any, List, Mapping, Optional, Sequence, Union

from ..field_template import apply_output_template
from .base import AbstractTransformHandler, TransformResult

_PLACEHOLDER_RE = re.compile(r"\{([^{}]+)\}")

_SKIP_OPERATORS = frozenset(
    {
        "STARTS_WITH",
        "ISTARTS_WITH",
        "ENDS_WITH",
        "IENDS_WITH",
        "CONTAINS",
        "ICONTAINS",
        "EQUALS",
        "IEQUALS",
        "REGEX",
    }
)


def _scalarize_prop(value: Any) -> str:
    """Coerce a non-iterate property to a single string for template fill."""
    if value is None:
        return ""
    if isinstance(value, list):
        if not value:
            return ""
        first = value[0]
        return "" if first is None else str(first)
    return str(value)


def _iterate_values(raw: Any) -> tuple[bool, List[str]]:
    """
    Return (is_list, values).

    Lists keep element order; scalars become a one-element list for rendering,
    then the caller returns a string when ``is_list`` is False.
    """
    if isinstance(raw, list):
        return True, ["" if v is None else str(v) for v in raw]
    if raw is None:
        return False, [""]
    return False, [str(raw)]


def resolve_compose_iterate_field(
    block: Mapping[str, Any],
    fields: Sequence[Any] | None = None,
) -> str:
    """Prefer ``compose_template.iterate_field``, else first ``fields[].field_name``."""
    explicit = AbstractTransformHandler.first_nonempty(block.get("iterate_field"))
    if explicit:
        return explicit
    if isinstance(fields, list):
        for row in fields:
            if not isinstance(row, dict):
                continue
            name = AbstractTransformHandler.first_nonempty(row.get("field_name"))
            if name:
                return name
    return ""


def validate_compose_skip_if(skip_if: Any) -> None:
    """Validate optional ``compose_template.skip_if`` block."""
    if skip_if is None or skip_if == {}:
        return
    if not isinstance(skip_if, dict):
        raise ValueError("compose_template.skip_if must be an object")
    op = AbstractTransformHandler.first_nonempty(skip_if.get("operator"), "STARTS_WITH").upper()
    if op not in _SKIP_OPERATORS:
        raise ValueError(
            f"compose_template.skip_if.operator must be one of {sorted(_SKIP_OPERATORS)}; got {op!r}"
        )
    if op == "REGEX":
        pattern = AbstractTransformHandler.first_nonempty(skip_if.get("pattern"))
        if not pattern:
            raise ValueError("compose_template.skip_if.operator=REGEX requires pattern")
    else:
        prop = AbstractTransformHandler.first_nonempty(skip_if.get("property"))
        literal = skip_if.get("value")
        has_literal = literal is not None and str(literal) != ""
        if not prop and not has_literal:
            raise ValueError(
                "compose_template.skip_if requires property (row field) or value (literal)"
            )


def _skip_compare_value(skip_if: Mapping[str, Any], props: Mapping[str, Any]) -> str:
    """Resolve the RHS of a skip comparison (property first, then literal value)."""
    prop = AbstractTransformHandler.first_nonempty(skip_if.get("property"))
    if prop:
        return _scalarize_prop(props.get(prop))
    if "value" in skip_if and skip_if.get("value") is not None:
        return str(skip_if.get("value"))
    return ""


def _expand_skip_pattern(pattern: str, props: Mapping[str, Any]) -> str:
    """Allow ``{field}`` placeholders inside REGEX skip patterns."""

    def repl(match: re.Match[str]) -> str:
        return re.escape(_scalarize_prop(props.get(match.group(1))))

    return _PLACEHOLDER_RE.sub(repl, pattern)


def should_skip_compose_item(
    item: str,
    props: Mapping[str, Any],
    skip_if: Mapping[str, Any] | None,
) -> bool:
    """
    Return True when this iterate element should not be composed.

    Empty compare values never match STARTS_WITH / CONTAINS / etc. (avoids
    ``str.startswith('') == True`` accidentally skipping everything).
    """
    if not skip_if:
        return False
    op = AbstractTransformHandler.first_nonempty(skip_if.get("operator"), "STARTS_WITH").upper()
    if op not in _SKIP_OPERATORS:
        return False

    if op == "REGEX":
        raw_pat = AbstractTransformHandler.first_nonempty(skip_if.get("pattern"))
        if not raw_pat:
            return False
        pat = _expand_skip_pattern(raw_pat, props)
        flags = 0
        flag_raw = str(skip_if.get("flags") or "").upper()
        if "I" in flag_raw:
            flags |= re.IGNORECASE
        try:
            return re.search(pat, item, flags) is not None
        except re.error as ex:
            raise ValueError(f"compose_template.skip_if.pattern is invalid: {ex}") from ex

    rhs = _skip_compare_value(skip_if, props)
    if not rhs:
        return False

    if op == "STARTS_WITH":
        return item.startswith(rhs)
    if op == "ISTARTS_WITH":
        return item.casefold().startswith(rhs.casefold())
    if op == "ENDS_WITH":
        return item.endswith(rhs)
    if op == "IENDS_WITH":
        return item.casefold().endswith(rhs.casefold())
    if op == "CONTAINS":
        return rhs in item
    if op == "ICONTAINS":
        return rhs.casefold() in item.casefold()
    if op == "EQUALS":
        return item == rhs
    if op == "IEQUALS":
        return item.casefold() == rhs.casefold()
    return False


def compose_template_values(
    props: Mapping[str, Any],
    template: str,
    iterate_field: str,
    *,
    skip_if: Mapping[str, Any] | None = None,
) -> Union[str, List[str]]:
    """
    Apply ``template`` with list-aware substitution for ``iterate_field``.

    - If ``props[iterate_field]`` is a list, each element substitutes ``{iterate_field}``
      and the return type is ``list[str]`` (same length as input, minus skipped items).
    - If it is a scalar string (or missing), return a single composed ``str`` (or the
      original string when skipped).
    - Other ``{placeholders}`` are filled from ``props`` as scalars (lists → first element).
    - When ``skip_if`` matches an iterate element, that element is omitted from list
      output (so ``output_mode=append`` does not add a redundant composed variant).
    """
    tpl = str(template or "")
    if not iterate_field:
        raise ValueError("compose_template requires iterate_field (or fields[0].field_name)")
    if not tpl.strip():
        raise ValueError("compose_template requires a non-empty output_template")
    validate_compose_skip_if(skip_if)

    is_list, items = _iterate_values(props.get(iterate_field))
    placeholders = _PLACEHOLDER_RE.findall(tpl)

    rendered: List[str] = []
    for item in items:
        if should_skip_compose_item(item, props, skip_if):
            if not is_list:
                return item
            continue
        field_values: dict[str, str] = {}
        for key in placeholders:
            if key == iterate_field:
                field_values[key] = item
            else:
                field_values[key] = _scalarize_prop(props.get(key))
        field_values.setdefault(iterate_field, item)
        rendered.append(apply_output_template(tpl, field_values))

    if is_list:
        return rendered
    return rendered[0] if rendered else ""


class ComposeTemplateHandler(AbstractTransformHandler):
    handler_id = "compose_template"
    description = (
        "Compose an output string from a template with list-aware substitution. "
        "When the iterate field is a list (e.g. aliases), each element is formatted "
        "individually and the output stays a list. When it is a string (e.g. name), "
        "the template is applied once and the output stays a string. Use after a join "
        "to prefix/suffix values with enrichment fields such as pi_unit. "
        "Pair with output_mode=append to keep originals and add composed variants. "
        "Optional skip_if (STARTS_WITH / REGEX / …) omits iterate elements that already "
        "contain the prefix so pi_unit is not duplicated (e.g. 12PTE3089 → not 1212PTE3089)."
    )
    # Not a multi_value / explode handler — list outputs are list-typed field values.
    multi_value = False

    @classmethod
    def apply(
        cls,
        working: str,
        block: Mapping[str, Any],
        *,
        field_values: Optional[Mapping[str, str]] = None,
        props: Optional[Mapping[str, Any]] = None,
    ) -> TransformResult:
        """
        Prefer ``props``-based composition (list-aware). Falls back to applying the
        template once against ``field_values`` / ``working`` when props are absent.
        """
        template = cls.first_nonempty(block.get("template"), block.get("output_template"))
        iterate_field = cls.first_nonempty(block.get("iterate_field"))
        skip_raw = block.get("skip_if")
        skip_if = skip_raw if isinstance(skip_raw, dict) else None

        if props is not None and iterate_field:
            tpl = template or ("{" + iterate_field + "}")
            return compose_template_values(props, tpl, iterate_field, skip_if=skip_if)

        fv = dict(field_values or {})
        if iterate_field and iterate_field not in fv:
            fv[iterate_field] = working
        if not template:
            return working
        if not fv and working:
            return working
        return apply_output_template(template, fv)
