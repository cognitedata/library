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


def _batches_of_space(
    space: str, facility: str, weights: dict[str, int], target: int, leaves_of: LeafProvider
) -> list[dict[str, Any]]:
    """The work items of one big space. `weights` maps a kind, or a configured view (`view_key`), to its instances."""
    items: list[dict[str, Any]] = []
    whole: dict[str, int] = {}
    for key, count in sorted(weights.items()):
        if count <= target:
            whole[key] = count
            continue
        packs = pack_leaves(leaves_of(space, key), target)
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
) -> list[dict[str, Any]]:
    """The work items of a run: [{"weight": instances, "data": what the call is told}], heaviest first.

    `fits` says whether the calls are acceptable, for example whether their data fits a task's output limit; if not, the
    target is made larger, which gives fewer, bigger calls.
    """
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
            if size[space] > limit:
                items += _batches_of_space(space, facilities[space], weights[space], limit, leaves)
        small = {space: size[space] + 1 for space in facilities if size[space] <= limit}
        room = max_tasks - len(items)
        if small and room >= 1:
            for spaces in pack_spaces(small, room, limit):
                items.append(_shard(spaces, core, facilities, sum(small[s] for s in spaces)))
        if (not small or room >= 1) and len(items) <= max_tasks and fits(items):
            return sorted(items, key=lambda item: -item["weight"])
        current *= GROWTH
    raise ValueError(
        "The run cannot be divided into few enough calls: raise shardTargetEntities or shorten the configuration."
    )


def _shard(spaces: list[str], core: dict[str, str], facilities: dict[str, str], weight: int) -> dict[str, Any]:
    return {
        "weight": weight,
        "data": {
            "action": "compute_shard",
            "spaces": {space: core[space] for space in spaces if space in core},
            "extraSpaces": {space: facilities[space] for space in spaces},
        },
    }
