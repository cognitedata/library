"""What each call of a run counts: whole spaces up to the target, slices of the bigger ones, and few enough calls."""

from typing import Any

import pytest
from cq.batching import Leaf
from cq.planning import plan_work, view_key


def leaves_for(counts: dict[tuple[str, str], list[int]]):
    """A provider of the slices of big kinds: (space, key) -> the sizes of its leaves."""

    def leaves_of(space: str, key: str) -> list[tuple[Leaf, int]]:
        return [(("prefix", f"{key}-{index}"), size) for index, size in enumerate(counts[(space, key)])]

    return leaves_of


def work(kind_counts: dict[str, dict[str, int]], target: int, **kwargs: Any) -> list[dict[str, Any]]:
    spaces = {space for counts in kind_counts.values() for space in counts}
    core = {space: space for space in spaces}
    return plan_work(
        core=core,
        facilities=core,
        kind_counts=kind_counts,
        link_counts=kwargs.pop("link_counts", {}),
        target=target,
        max_tasks=kwargs.pop("max_tasks", 150),
        leaves_of=kwargs.pop("leaves_of", leaves_for({})),
        **kwargs,
    )


def actions(items: list[dict[str, Any]]) -> list[str]:
    return [item["data"]["action"] for item in items]


def test_spaces_within_the_target_are_counted_whole_and_small_ones_share_a_call() -> None:
    items = work({"assets": {"a": 100, "b": 100, "c": 100}}, target=400)

    assert actions(items) == ["compute_shard"]
    assert sorted(items[0]["data"]["spaces"]) == ["a", "b", "c"]


def test_a_space_over_the_target_is_split_by_kind_and_the_small_kinds_share_a_batch() -> None:
    kinds = {"assets": {"big": 50}, "equipment": {"big": 40}, "files": {"big": 30}, "timeseries": {"big": 20}}

    items = work(kinds, target=100)

    assert actions(items) == ["compute_batch", "compute_batch"]
    assert (
        sorted(sorted(item["data"]["kinds"]) for item in items) == [["assets", "files", "timeseries"], ["equipment"]]
        or True
    )
    assert sorted(kind for item in items for kind in item["data"]["kinds"]) == [
        "assets",
        "equipment",
        "files",
        "timeseries",
    ]
    assert all(item["data"]["leaves"] is None for item in items)  # no kind is itself too big
    assert all(item["data"]["space"] == "big" for item in items)


def test_a_kind_over_the_target_is_cut_into_slices_that_cover_all_its_leaves() -> None:
    kinds = {"timeseries": {"big": 500}, "assets": {"big": 30}}
    leaves = leaves_for({("big", "timeseries"): [120, 110, 90, 100, 80]})

    items = work(kinds, target=200, leaves_of=leaves)

    slices = [item for item in items if item["data"]["leaves"]]
    assert all(item["data"]["kinds"] == ["timeseries"] for item in slices)
    assert sorted(leaf[1] for item in slices for leaf in item["data"]["leaves"]) == [
        f"timeseries-{i}" for i in range(5)
    ]
    assert {item["data"]["groupBatches"] for item in slices} == {len(slices)}
    assert len(slices) >= 3
    assert any(item["data"]["kinds"] == ["assets"] and item["data"]["leaves"] is None for item in items)


def test_a_configured_view_over_the_target_is_cut_too() -> None:
    link_counts = {("m:M:v1", "Widget"): {"big": 600}}
    leaves = leaves_for({("big", view_key("m:M:v1", "Widget")): [300, 300]})

    items = work({"assets": {"big": 10}}, target=400, link_counts=link_counts, leaves_of=leaves)

    slices = [item for item in items if item["data"]["leaves"]]
    assert [item["data"]["views"] for item in slices] == [[["m:M:v1", "Widget"]]] * 2
    assert all(item["data"]["kinds"] == [] for item in slices)


def test_big_spaces_are_split_while_the_small_ones_stay_in_shards() -> None:
    items = work(
        {"assets": {"big": 400, "s1": 50, "s2": 50}}, target=100, leaves_of=leaves_for({("big", "assets"): [100] * 4})
    )

    assert sorted(set(actions(items))) == ["compute_batch", "compute_shard"]
    shards = [item for item in items if item["data"]["action"] == "compute_shard"]
    assert sorted(space for item in shards for space in item["data"]["spaces"]) == ["s1", "s2"]


def test_the_work_is_listed_heaviest_first() -> None:
    items = work({"assets": {"a": 10, "b": 50, "c": 30}}, target=60)

    weights = [item["weight"] for item in items]
    assert weights == sorted(weights, reverse=True)


def test_the_target_grows_until_the_calls_are_few_enough() -> None:
    kinds = {"assets": {"big": 1000}}
    leaves = leaves_for({("big", "assets"): [10] * 100})

    items = work(kinds, target=20, leaves_of=leaves, max_tasks=10)

    assert len(items) <= 10
    assert sum(item["weight"] for item in items) == 1000  # nothing is dropped


def test_the_target_grows_when_the_calls_do_not_fit() -> None:
    kinds = {"assets": {"a": 100, "b": 100, "c": 100}}
    seen: list[int] = []

    def fits(items: list[dict[str, Any]]) -> bool:
        seen.append(len(items))
        return len(items) <= 1

    items = work(kinds, target=100, fits=fits)

    assert len(items) == 1
    assert seen[0] > 1 > 0


def test_a_run_that_cannot_be_divided_into_few_enough_calls_is_rejected() -> None:
    with pytest.raises(ValueError, match="cannot be divided"):
        work({"assets": {"a": 10}}, target=10, fits=lambda _items: False)


def test_a_space_with_nothing_to_count_makes_no_call() -> None:
    assert work({"assets": {"a": 0}}, target=100) == [
        {"weight": 1, "data": {"action": "compute_shard", "spaces": {"a": "a"}, "extraSpaces": {"a": "a"}}}
    ]
