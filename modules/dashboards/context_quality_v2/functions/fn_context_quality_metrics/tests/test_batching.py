"""Cutting a big kind into slices by the start of the external ids, and packing slices into batches."""

import random
import string
from typing import Any

from cq.batching import (
    CHARSET,
    MAX_LEAVES_PER_BATCH,
    Leaf,
    batch_filter,
    chain_tasks,
    leaf_filter,
    pack_kinds,
    pack_leaves,
    split_leaves,
)


def matches(leaf: Leaf, external_id: str) -> bool:
    """What the service does for a leaf's filter."""
    kind, text = leaf
    if kind == "prefix":
        return external_id.startswith(text)
    if kind == "exact":
        return external_id == text
    return (
        external_id.startswith(text)
        and external_id != text
        and not any(external_id.startswith(text + char) for char in CHARSET)
    )


class Universe:
    """A kind of instances that is only seen through counting and sampling, as the plan sees it."""

    def __init__(self, ids: list[str]) -> None:
        self.ids = ids
        self.calls = 0

    def count_leaves(self, leaves: list[Leaf]) -> list[int]:
        self.calls += len(leaves)
        return [sum(matches(leaf, i) for i in self.ids) for leaf in leaves]

    def sample_id(self, leaf: Leaf) -> str | None:
        return next((i for i in sorted(self.ids) if matches(leaf, i)), None)

    def split(self, target: int) -> list[tuple[Leaf, int]]:
        return split_leaves(len(self.ids), target, self.count_leaves, self.sample_id)


def covered_exactly_once(universe: Universe, leaves: list[tuple[Leaf, int]]) -> bool:
    return all(sum(matches(leaf, i) for leaf, _ in leaves) == 1 for i in universe.ids)


def test_every_instance_is_in_exactly_one_leaf_and_the_counts_are_right() -> None:
    universe = Universe([f"asset-{n:05d}" for n in range(2000)])

    leaves = universe.split(target=300)

    assert covered_exactly_once(universe, leaves)
    assert sum(count for _, count in leaves) == 2000
    assert all(count == sum(matches(leaf, i) for i in universe.ids) for leaf, count in leaves)
    assert max(count for _, count in leaves) <= 300


def test_ids_that_are_a_beginning_of_other_ids_and_ids_with_other_characters_are_not_lost() -> None:
    ids = ["a", "ab", "abc", "abcd", "b", "é1", "é2", "日本", "x" * 50, "x" * 50 + "y", "x" * 49 + "~"]
    ids += [f"k{n}" for n in range(60)]
    universe = Universe(ids)

    leaves = universe.split(target=5)

    assert covered_exactly_once(universe, leaves)
    assert sum(count for _, count in leaves) == len(ids)


def test_a_long_shared_beginning_does_not_cost_a_round_per_character() -> None:
    universe = Universe([f"customer-site-north-area-prod-line-{n:04d}" for n in range(500)])

    universe.split(target=100)

    # one character at a time would be about 40 rounds of 69 questions
    assert universe.calls < 40 * len(CHARSET) / 4


def test_random_ids_are_covered_exactly_once() -> None:
    rng = random.Random(7)
    alphabet = string.ascii_letters + string.digits + "_-é~"
    ids = list({"".join(rng.choice(alphabet) for _ in range(rng.randint(1, 12))) for _ in range(1500)})
    universe = Universe(ids)

    leaves = universe.split(target=120)

    assert covered_exactly_once(universe, leaves)
    assert sum(count for _, count in leaves) == len(ids)


def test_a_kind_within_the_target_is_one_leaf_and_an_empty_kind_none() -> None:
    assert Universe(["a", "b"]).split(target=10) == [(("prefix", ""), 2)]
    assert Universe([]).split(target=10) == []


def test_ids_that_continue_with_a_character_not_looked_at_stay_together_in_one_rest_leaf() -> None:
    universe = Universe(["q" + chr(0x100 + n) for n in range(30)])

    leaves = universe.split(target=5)

    assert covered_exactly_once(universe, leaves)
    assert [count for _, count in leaves] == [
        30
    ]  # they cannot be told apart by prefix: one leaf, bigger than the target


def test_leaves_are_packed_in_order_into_batches_of_about_the_target() -> None:
    leaves: list[tuple[Leaf, int]] = [(("prefix", c), 40) for c in "abcdefghij"]

    batches = pack_leaves(leaves, target=100)

    assert [size for _, size in batches] == [80, 80, 80, 80, 80]  # two leaves each: a third would pass 100
    assert [leaf for group, _ in batches for leaf in group] == [leaf for leaf, _ in leaves]


def test_a_leaf_bigger_than_the_target_is_a_batch_of_its_own() -> None:
    batches = pack_leaves([(("prefix", "a"), 10), (("rest", "b"), 500), (("prefix", "c"), 10)], target=100)

    assert [size for _, size in batches] == [10, 500, 10]


def test_a_batch_holds_at_most_a_dozen_leaves() -> None:
    batches = pack_leaves([(("prefix", f"{n:02d}"), 1) for n in range(30)], target=10_000)

    assert [len(group) for group, _ in batches] == [MAX_LEAVES_PER_BATCH, MAX_LEAVES_PER_BATCH, 6]


def test_small_kinds_are_packed_together_and_big_ones_stay_alone() -> None:
    weights = {"assets": 90, "equipment": 60, "files": 30, "objects3d": 20, "timeseries": 100}

    packed = pack_kinds(weights, target=100)

    assert packed == [["timeseries"], ["assets"], ["equipment", "files"], ["objects3d"]]


def tasks(count: int) -> list[dict[str, Any]]:
    return [{"externalId": f"t{i}", "dependsOn": []} for i in range(count)]


def test_one_lane_makes_the_tasks_run_one_after_the_other() -> None:
    chained = chain_tasks(tasks(4), lanes=1)

    assert [t["dependsOn"] for t in chained] == [
        [],
        [{"externalId": "t0"}],
        [{"externalId": "t1"}],
        [{"externalId": "t2"}],
    ]


def test_several_lanes_make_a_task_wait_for_the_one_that_many_places_before() -> None:
    chained = chain_tasks(tasks(5), lanes=2)

    assert [t["dependsOn"] for t in chained] == [
        [],
        [],
        [{"externalId": "t0"}],
        [{"externalId": "t1"}],
        [{"externalId": "t2"}],
    ]


def test_no_lanes_means_all_at_once() -> None:
    assert all(t["dependsOn"] == [] for t in chain_tasks(tasks(5), lanes=0))


def test_a_leaf_filter_is_built_for_each_kind_of_leaf() -> None:
    for leaf in (("prefix", "ab"), ("exact", "ab"), ("rest", "ab")):
        assert leaf_filter(leaf).dump()


def test_the_filter_of_an_edge_refers_to_the_external_id_of_an_edge_and_that_of_a_node_to_a_node() -> None:
    prefix = ("prefix", "ab")
    assert leaf_filter(prefix, edge=True).dump() == {"prefix": {"property": ["edge", "externalId"], "value": "ab"}}
    assert leaf_filter(prefix).dump() == {"prefix": {"property": ["node", "externalId"], "value": "ab"}}
    assert "node" not in str(batch_filter([prefix, ("exact", "b")], edge=True).dump())
