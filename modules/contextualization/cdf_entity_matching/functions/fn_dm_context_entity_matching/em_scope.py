"""Matching entities only against targets that share their primary and secondary scope.

With `primaryScopeProperty` set, an entity is only matched to targets with the same value
of that property - a site, say. With `secondaryScopeProperty` set as well, the targets
must also share that value - a unit within the site - unless they carry the
`ScopeWideDetect` tag, which makes them candidates in every secondary scope of their
primary one. Entities with no scope value at all are matched against every target.
Both properties empty leaves matching unscoped.
"""

from collections import defaultdict
from collections.abc import Mapping

from em_config import Parameters  # isort: skip
from em_constants import (  # isort: skip
    KEY_SCOPE_PRIMARY,
    KEY_SCOPE_SECONDARY,
    KEY_SCOPE_WIDE,
    PROP_COL_TAGS,
    QUERY_FILTER_TYPE_TARGETS,
    TAG_SCOPE_WIDE_DETECT,
)
from em_logger import CogniteFunctionLogger  # isort: skip
from em_pipeline_types import EntityMatchSource, TargetMatchRecord  # isort: skip

ScopeKey = tuple[str, str]
ScopeBatch = tuple[list[TargetMatchRecord], list[EntityMatchSource]]


def scope_properties(parameters: Parameters) -> list[str]:
    """Target properties the scope is read from, or an empty list when scoping is off."""
    names = [name for name in (parameters.primary_scope_property, parameters.secondary_scope_property) if name]
    return [*names, PROP_COL_TAGS] if names else []


def _value(properties: Mapping[str, object], name: str | None) -> str:
    value = properties.get(name) if name else None
    return "" if value is None else str(value)


def scope_of(properties: Mapping[str, object], parameters: Parameters) -> ScopeKey:
    """The (primary, secondary) scope of an instance, with a missing value read as empty."""
    return (
        _value(properties, parameters.primary_scope_property),
        _value(properties, parameters.secondary_scope_property),
    )


def is_scope_wide(properties: Mapping[str, object]) -> bool:
    tags = properties.get(PROP_COL_TAGS)
    return isinstance(tags, list) and TAG_SCOPE_WIDE_DETECT in tags


def scope_batches(
    parameters: Parameters,
    logger: CogniteFunctionLogger,
    targets: list[TargetMatchRecord],
    entities: list[EntityMatchSource],
) -> list[ScopeBatch]:
    """The entities grouped by scope, each with the targets they may be matched to.

    Returns:
        One batch of everything when scoping is off; otherwise one batch per scope that
        has targets. Entities with no primary or secondary value get every target. A
        non-empty scope without targets is logged and left out.
    """
    if not scope_properties(parameters):
        return [(targets, entities)]

    by_scope: dict[ScopeKey, list[EntityMatchSource]] = defaultdict(list)
    for entity in entities:
        by_scope[(entity.get(KEY_SCOPE_PRIMARY, ""), entity.get(KEY_SCOPE_SECONDARY, ""))].append(entity)

    batches: list[ScopeBatch] = []
    for (primary, secondary), scoped_entities in by_scope.items():
        if primary == "" and secondary == "":
            logger.warning(
                f"Entities without scope (primary={primary!r}, secondary={secondary!r}) - "
                f"{len(scoped_entities)} is tried matched against all Targets"
            )
            batches.append((targets, scoped_entities))
            continue

        scoped_targets = [
            target
            for target in targets
            if target.get(KEY_SCOPE_PRIMARY, "") == primary
            and (target.get(KEY_SCOPE_SECONDARY, "") == secondary or target.get(KEY_SCOPE_WIDE, False))
        ]
        if not scoped_targets:
            logger.warning(
                f"No {QUERY_FILTER_TYPE_TARGETS} in scope (primary={primary!r}, secondary={secondary!r}) - "
                f"{len(scoped_entities)} source record(s) not matched"
            )
            continue
        logger.info(
            f"Scope (primary={primary!r}, secondary={secondary!r}): "
            f"{len(scoped_entities)} source record(s), {len(scoped_targets)} {QUERY_FILTER_TYPE_TARGETS}"
        )
        batches.append((scoped_targets, scoped_entities))
    return batches
