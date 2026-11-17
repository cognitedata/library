"""Deciding what each function call of a run counts.

The work of a run is cut into *work items*, each counted by one function call:

- A space that holds no more than the target (`shardTargetEntities`) is counted whole, and small spaces share a call:
  `compute_shard`.
- A bigger space is split (cq.batching) by kind, and a kind that is still too big by slices of the external ids:
  `compute_batch`.

The number of calls is kept within what a workflow execution holds by making the target larger when needed.
"""

from collections.abc import Callable
from typing import Any

from cq.batching import Leaf, pack_kinds, pack_leaves
from cq.fanout import pack_spaces

# `leaves(space, key)`: the slices of one kind (or configured view) of one space, as (leaf, instances)
type LeafProvider = Callable[[str, str], list[tuple[Leaf, int]]]

GROWTH = 1.25
MAX_ROUNDS = 80


def view_key(model: str, view: str) -> str:
    return f"{model}|{view}"


def _limit(key: str, target: int, kind_targets: dict[str, int]) -> int:
    """The most instances of a kind (or configured view) one call gets: its own target when it has one."""
    return min(target, kind_targets.get(key, target))


def _batches_of_space(
    space: str,
    facility: str,
    weights: dict[str, int],
    target: int,
    leaves_of: LeafProvider,
    kind_targets: dict[str, int],
) -> list[dict[str, Any]]:
    """The work items of one big space. `weights` maps a kind, or a configured view (`view_key`), to its instances."""
    items: list[dict[str, Any]] = []
    whole: dict[str, int] = {}
    for key, count in sorted(weights.items()):
        limit = _limit(key, target, kind_targets)
        if count <= limit:
            whole[key] = count
            continue
        packs = pack_leaves(leaves_of(space, key), limit)
        for leaves, size in packs:
            items.append(_batch(space, facility, [key], leaves, size, len(packs)))
    for group in pack_kinds(whole, target):
        items.append(_batch(space, facility, group, None, sum(whole[key] for key in group), 1))
    return items


def _batch(
    space: str, facility: str, keys: list[str], leaves: list[Leaf] | None, weight: int, group_batches: int
) -> dict[str, Any]:
    return {
        "weight": weight,
        "data": {
            "action": "compute_batch",
            "space": space,
            "facility": facility,
            "kinds": [key for key in keys if "|" not in key],
            "views": [key.split("|", 1) for key in keys if "|" in key],
            "leaves": [list(leaf) for leaf in leaves] if leaves else None,
            "groupBatches": group_batches,
        },
    }


def plan_work(
    *,
    core: dict[str, str],
    facilities: dict[str, str],
    kind_counts: dict[str, dict[str, int]],
    link_counts: dict[tuple[str, str], dict[str, int]],
    target: int,
    max_tasks: int,
    leaves_of: LeafProvider,
    fits: Callable[[list[dict[str, Any]]], bool] = lambda _items: True,
    kind_targets: dict[str, int] | None = None,
    max_growth: float | None = None,
) -> list[dict[str, Any]]:
    """The work items of a run: [{"weight": instances, "data": what the call is told}], heaviest first.

    `kind_targets` gives a kind its own, smaller target, for a kind that costs more per instance than the others: a space
    that holds more of it than that is split even when it is under the target.

    `max_growth` refuses a run whose target would have to grow past that many times the one asked for: a target that has
    grown a lot makes whole kinds share one call, which defeats the split, so the run says what target fits instead.

    `fits` says whether the calls are acceptable, for example whether their data fits a task's output limit; if not, the
    target is made larger, which gives fewer, bigger calls.
    """
    kind_targets = kind_targets or {}
    weights: dict[str, dict[str, int]] = {}
    for space in facilities:
        per_space = {kind: counts.get(space, 0) for kind, counts in kind_counts.items() if space in core}
        per_space.update({view_key(*key): counts.get(space, 0) for key, counts in link_counts.items()})
        weights[space] = {key: count for key, count in per_space.items() if count > 0}
    size = {space: sum(per_space.values()) for space, per_space in weights.items()}
    # the leaves of a big kind are found once, at the smallest target; larger targets pack the same leaves bigger
    cache: dict[tuple[str, str], list[tuple[Leaf, int]]] = {}

    def leaves(space: str, key: str) -> list[tuple[Leaf, int]]:
        if (space, key) not in cache:
            cache[(space, key)] = leaves_of(space, key)
        return cache[(space, key)]

    current = float(target)
    for _ in range(MAX_ROUNDS):
        limit = int(current)
        items: list[dict[str, Any]] = []
        for space in sorted(facilities):
            if _is_big(size[space], weights[space], limit, kind_targets):
                items += _batches_of_space(space, facilities[space], weights[space], limit, leaves, kind_targets)
        small = {
            space: size[space] + 1
            for space in facilities
            if not _is_big(size[space], weights[space], limit, kind_targets)
        }
        room = max_tasks - len(items)
        if small and room >= 1:
            for spaces in pack_spaces(small, room, limit):
                items.append(_shard(spaces, core, facilities, sum(small[s] for s in spaces)))
        if (not small or room >= 1) and len(items) <= max_tasks and fits(items):
            if max_growth is not None and limit > target * max_growth:
                raise ValueError(
                    f"A target of {target} entities per call would need more calls than a run can hold. The smallest "
                    f"target that fits is about {limit}: use {limit} or more, or run fewer dimensions at a time."
                )
            return sorted(items, key=lambda item: -item["weight"])
        current *= GROWTH
    raise ValueError(
        "The run cannot be divided into few enough calls: raise shardTargetEntities or shorten the configuration."
    )


def _is_big(size: int, weights: dict[str, int], limit: int, kind_targets: dict[str, int]) -> bool:
    """Whether a space is split into batches: it holds more than the target, or more of a kind than that kind's target."""
    return size > limit or any(weights.get(key, 0) > _limit(key, limit, kind_targets) for key in kind_targets)


def _shard(spaces: list[str], core: dict[str, str], facilities: dict[str, str], weight: int) -> dict[str, Any]:
    return {
        "weight": weight,
        "data": {
            "action": "compute_shard",
            "spaces": {space: core[space] for space in spaces if space in core},
            "extraSpaces": {space: facilities[space] for space in spaces},
        },
    }
