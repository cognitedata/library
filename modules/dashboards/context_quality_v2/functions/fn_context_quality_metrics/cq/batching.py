"""Cutting one big space into batches that are counted by separate function calls.

A space with more entities than a shard should hold (`shardTargetEntities`) is split, in this order:

1. by *kind*: assets, equipment, time series and so on are counted by different batches, and small kinds share one;
2. a kind that is still too big is cut into slices by the start of the entities' external ids.

The data modeling service cannot hand out numbered pages in parallel, but it can count and list the instances whose
external id starts with a given text. So a kind is cut by repeatedly looking at the next character: a *leaf* is every
instance whose id starts with some text (or equals it, or is the rest of that text's instances that continue with a
character this module does not look at), a leaf that is still too big is cut by its next character, and the leaves are
packed into batches of about the target size. Counting only needs the service to count, so the plan does not read the
instances.
"""

import string
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from cognite.client.data_classes.data_modeling import filters

# The characters a next step looks at. An id that continues with any other character ends up in the leaf "rest".
CHARSET = string.digits + string.ascii_letters + "_-.:/ "
MAX_LEAF_DEPTH = 200
MAX_LEAVES_PER_BATCH = 12  # a batch's filter is one OR of its leaves
COUNT_WORKERS = 16

type Leaf = tuple[str, str]  # ("prefix" | "exact" | "rest", text)
type CountLeaves = Callable[[list[Leaf]], list[int]]
type SampleId = Callable[[Leaf], str | None]


def leaf_filter(leaf: Leaf, edge: bool = False) -> filters.Filter:
    """The filter for the instances of a leaf. A property of an edge is referenced as an edge's, not a node's."""
    kind, text = leaf
    ref = ["edge" if edge else "node", "externalId"]
    if kind == "prefix":
        return filters.Prefix(ref, text)
    if kind == "exact":
        return filters.Equals(ref, text)
    others = [filters.Prefix(ref, text + char) for char in CHARSET]
    return filters.And(
        filters.Prefix(ref, text), filters.Not(filters.Equals(ref, text)), filters.Not(filters.Or(*others))
    )


def batch_filter(leaves: list[Leaf], edge: bool = False) -> filters.Filter:
    if len(leaves) == 1:
        return leaf_filter(leaves[0], edge)
    return filters.Or(*(leaf_filter(leaf, edge) for leaf in leaves))


def counter(count_one: Callable[[Leaf], int]) -> CountLeaves:
    """Counts several leaves at once: the service is asked in parallel."""

    def count_many(leaves: list[Leaf]) -> list[int]:
        if len(leaves) <= 1:
            return [count_one(leaf) for leaf in leaves]
        with ThreadPoolExecutor(max_workers=COUNT_WORKERS) as pool:
            return list(pool.map(count_one, leaves))

    return count_many


def _longest_shared_prefix(start: str, count: int, count_leaves: CountLeaves, sample_id: SampleId) -> str:
    """The longest text, beginning with `start`, that every one of the `count` instances of the leaf starts with.

    Ids usually share a long beginning, and looking at it one character at a time would take as many rounds as it is
    long. The service is asked about the beginning of one sample id instead, and the longest beginning that still
    matches all of them is found by halving.
    """
    sample = sample_id(("prefix", start))
    if sample is None:
        return start
    low, high = len(start), len(sample)  # sample[:low] matches all; find the longest that still does
    while low < high:
        middle = (low + high + 1) // 2
        if count_leaves([("prefix", sample[:middle])])[0] == count:
            low = middle
        else:
            high = middle - 1
    return sample[:low]


def split_leaves(total: int, target: int, count_leaves: CountLeaves, sample_id: SampleId) -> list[tuple[Leaf, int]]:
    """Every instance of a kind, as leaves of at most `target` instances where the ids allow it. -> (leaf, count).

    The leaves cover each instance exactly once: a leaf that is too big is replaced by one leaf per next character,
    one for the text itself and one for whatever continues with a character not looked at.
    """
    work: list[tuple[Leaf, int]] = [(("prefix", ""), total)]
    done: list[tuple[Leaf, int]] = []
    while work:
        leaf, count = work.pop()
        if count == 0:
            continue
        if count <= target or leaf[0] != "prefix" or len(leaf[1]) >= MAX_LEAF_DEPTH:
            done.append((leaf, count))
            continue
        text = _longest_shared_prefix(leaf[1], count, count_leaves, sample_id)
        children: list[Leaf] = [("prefix", text + char) for char in CHARSET] + [("exact", text)]
        counts = count_leaves(children)
        rest = count - sum(counts)
        work.extend(zip(children, counts, strict=True))
        if rest > 0:
            work.append((("rest", text), rest))
    return sorted(done, key=lambda item: item[0])


def pack_leaves(leaves: list[tuple[Leaf, int]], target: int) -> list[tuple[list[Leaf], int]]:
    """Leaves, in order, into batches of about `target` instances. -> (leaves, instances) per batch.

    A leaf is never divided: one that is bigger than the target (ids that cannot be told apart) is a batch of its own.
    """
    batches: list[tuple[list[Leaf], int]] = []
    current: list[Leaf] = []
    size = 0
    for leaf, count in leaves:
        if current and (size + count > target or len(current) >= MAX_LEAVES_PER_BATCH):
            batches.append((current, size))
            current, size = [], 0
        current.append(leaf)
        size += count
    if current:
        batches.append((current, size))
    return batches


def pack_kinds(weights: dict[str, int], target: int) -> list[list[str]]:
    """Kinds that fit whole into a batch, first-fit by size, so a space's small kinds are counted together."""
    bins: list[tuple[list[str], int]] = []
    for kind, weight in sorted(weights.items(), key=lambda item: (-item[1], item[0])):
        for index, (members, size) in enumerate(bins):
            if size + weight <= target:
                bins[index] = ([*members, kind], size + weight)
                break
        else:
            bins.append(([kind], weight))
    return [sorted(members) for members, _ in bins]


def chain_tasks(tasks: list[dict[str, Any]], lanes: int) -> list[dict[str, Any]]:
    """At most `lanes` tasks run at once: task i waits for task i - lanes (0 means no limit, 1 means one after another).

    A task that fails stops the ones after it, so a run that cannot complete does not keep counting. Put the heaviest
    tasks first so the lanes finish together.
    """
    if lanes > 0:
        for index in range(lanes, len(tasks)):
            tasks[index]["dependsOn"] = [{"externalId": tasks[index - lanes]["externalId"]}]
    return tasks
