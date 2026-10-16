"""Root and hierarchy semantics: roots come from real parent/child links, not from where a parent lives."""

from typing import Any

from cq.config import build_config
from cq.metrics import compute
from tiny_data import NOW, asset, data

A, B = "sp_a", "sp_b"


def run(loaded: dict[str, Any]) -> dict[str, Any]:
    return compute(loaded, build_config(None), {}, now=NOW)  # type: ignore[arg-type]


def counts(result: dict[str, Any], metric: str, unit: str | None = None) -> list[int]:
    """[eligible, passing] for a metric, optionally for one reporting unit."""
    total = [0, 0]
    for key, value in result["agg"].items():
        if key[0] == metric and (unit is None or key[2] == unit):
            total[0] += value[0]
            total[1] += value[1]
    return total


def drilled(result: dict[str, Any], metric: str) -> set[str]:
    return {item["id"] for item in result["drilldown"] if item["metric"] == metric}


def flags(loaded: dict[str, Any]) -> dict[str, tuple[bool, bool]]:
    return {a["id"]: (a["root"], a["orphan"]) for a in loaded["assets"]}


def test_a_parentless_asset_with_children_is_a_root() -> None:
    loaded = data(assets=[asset(A, "root"), asset(A, "child", parent=(A, "root"))])

    result = run(loaded)

    assert flags(loaded) == {"root": (True, False), "child": (False, False)}
    assert counts(result, "hierarchyCompletionRate") == [1, 1]  # the root is not expected to have a parent
    assert counts(result, "orphanRate") == [2, 2]


def test_a_root_inside_a_facility_space_is_not_a_hierarchy_failure() -> None:
    loaded = data(assets=[asset(A, "root"), asset(A, "x", parent=(A, "root")), asset(A, "y", parent=(A, "x"))])

    result = run(loaded)

    assert counts(result, "hierarchyCompletionRate") == [2, 2]
    assert drilled(result, "orphansByTier") == set()


def test_an_asset_whose_parent_is_outside_the_facility_spaces_is_not_a_root() -> None:
    loaded = data(assets=[asset(A, "site", parent=("sp_shared", "enterprise")), asset(A, "unit", parent=(A, "site"))])

    result = run(loaded)

    assert flags(loaded) == {"site": (False, False), "unit": (False, False)}
    assert counts(result, "hierarchyCompletionRate") == [2, 2]  # it has a parent; the parent just was not loaded
    assert drilled(result, "orphansByTier") == set()


def test_a_standalone_asset_is_an_orphan_and_misses_the_hierarchy() -> None:
    loaded = data(assets=[asset(A, "alone"), asset(A, "root"), asset(A, "child", parent=(A, "root"))])

    result = run(loaded)

    assert flags(loaded)["alone"] == (False, True)
    assert counts(result, "hierarchyCompletionRate") == [2, 1]  # alone (fails) and child (passes); root excluded
    assert counts(result, "orphanRate") == [3, 2]
    assert drilled(result, "orphansByTier") == {"alone"}


def test_a_non_root_asset_with_a_parent_passes() -> None:
    loaded = data(assets=[asset(A, "root"), asset(A, "child", parent=(A, "root"))])

    result = run(loaded)

    assert flags(loaded)["child"] == (False, False)
    assert counts(result, "hierarchyCompletionRate") == [1, 1]


def test_a_root_is_decided_by_children_in_any_facility_space() -> None:
    loaded = data(spaces=(A, B), assets=[asset(A, "root"), asset(B, "child", parent=(A, "root"))])

    result = run(loaded)

    assert flags(loaded) == {"root": (True, False), "child": (False, False)}
    assert counts(result, "hierarchyCompletionRate", A) == [0, 0]
    assert counts(result, "hierarchyCompletionRate", B) == [1, 1]
    assert drilled(result, "orphansByTier") == set()


def test_the_same_shape_gives_the_same_answer_whatever_the_space_arrangement() -> None:
    one_space = data(assets=[asset(A, "r"), asset(A, "c1", parent=(A, "r")), asset(A, "c2", parent=(A, "c1"))])
    two_spaces = data(
        spaces=(A, B), assets=[asset(A, "r"), asset(B, "c1", parent=(A, "r")), asset(A, "c2", parent=(B, "c1"))]
    )

    assert counts(run(one_space), "hierarchyCompletionRate") == counts(run(two_spaces), "hierarchyCompletionRate")
    assert counts(run(one_space), "orphanRate") == counts(run(two_spaces), "orphanRate")


def test_an_unlinked_entity_is_not_part_of_the_hierarchy() -> None:
    from tiny_data import timeseries

    loaded = data(assets=[asset(A, "root"), asset(A, "child", parent=(A, "root"))], timeseries_=[timeseries(A, "t")])

    result = run(loaded)

    assert counts(result, "hierarchyCompletionRate") == [1, 1]
    assert counts(result, "orphanRate") == [2, 2]


def test_a_parent_that_was_not_loaded_is_a_warning_not_a_hierarchy_failure() -> None:
    loaded = data(
        assets=[
            asset(A, "root"),
            asset(A, "child", parent=(A, "root")),
            asset(A, "lost1", parent=("sp_gone", "p1")),
            asset(A, "lost2", parent=("sp_gone", "p2")),
        ]
    )

    result = run(loaded)

    assert counts(result, "hierarchyCompletionRate") == [3, 3]
    assert counts(result, "orphanRate") == [4, 4]
    warnings = [w for w in result["warnings"] if w["code"] == "parents-not-loaded"]
    assert len(warnings) == 1
    assert warnings[0]["assets"] == 2
    assert "lost1" not in str(warnings)  # counts only, never entity ids


def test_a_fully_loaded_hierarchy_has_no_parents_not_loaded_warning() -> None:
    loaded = data(assets=[asset(A, "root"), asset(A, "child", parent=(A, "root"))])

    assert not [w for w in run(loaded)["warnings"] if w["code"] == "parents-not-loaded"]


def test_a_root_is_not_flagged_for_a_missing_type_but_other_assets_are() -> None:
    loaded = data(
        assets=[
            asset(A, "root"),
            asset(A, "typed", parent=(A, "root"), type_code="pump"),
            asset(A, "untyped", parent=(A, "root")),
            asset(A, "lost", parent=("sp_gone", "p")),
        ]
    )

    result = run(loaded)

    assert drilled(result, "typeCoverageGaps") == {"untyped", "lost"}


def test_the_depth_of_an_asset_counts_an_unloaded_parent_as_one_level() -> None:
    chain = [asset(A, "top", parent=("sp_gone", "p"))] + [
        asset(A, f"n{i}", parent=(A, "top" if i == 0 else f"n{i - 1}")) for i in range(8)
    ]

    result = run(data(assets=chain))

    assert drilled(result, "depthOutliers") == {
        "n6",
        "n7",
    }  # top is depth 2 with the unloaded level, n6 is 9; maxDepth is 8


def test_assets_with_no_parent_data_at_all_are_all_orphans() -> None:
    loaded = data(assets=[asset(A, "a"), asset(A, "b")])

    result = run(loaded)

    assert counts(result, "hierarchyCompletionRate") == [2, 0]
    assert counts(result, "orphanRate") == [2, 0]
