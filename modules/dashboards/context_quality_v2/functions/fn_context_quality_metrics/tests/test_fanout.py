"""Splitting a run into shards, and putting the shards' warnings back together."""

from typing import Any

from cq.fanout import MAX_TASKS_BYTES, finish_tasks, merge_warnings, pack_spaces, shard_task, tasks_fit


def test_every_space_is_in_exactly_one_shard() -> None:
    weights = {f"sp_{i}": 10 + i for i in range(20)}

    shards = pack_spaces(weights, max_shards=6, target=40)

    assert sorted(space for shard in shards for space in shard) == sorted(weights)
    assert len(shards) == 6


def test_the_number_of_shards_follows_the_target_and_never_exceeds_the_cap_or_the_spaces() -> None:
    assert len(pack_spaces({"a": 100, "b": 100, "c": 100}, max_shards=150, target=1000)) == 1
    assert len(pack_spaces({"a": 100, "b": 100, "c": 100}, max_shards=150, target=100)) == 3
    assert len(pack_spaces({"a": 100, "b": 100, "c": 100}, max_shards=2, target=10)) == 2
    assert len(pack_spaces({"a": 10**9}, max_shards=150, target=10)) == 1


def test_a_big_space_gets_a_shard_of_its_own_and_small_ones_share() -> None:
    shards = pack_spaces({"big": 900, "s1": 50, "s2": 50, "s3": 50, "s4": 50}, max_shards=2, target=500)

    assert ["big"] in shards
    assert sorted(space for shard in shards for space in shard if shard != ["big"]) == ["s1", "s2", "s3", "s4"]


def test_the_packing_is_the_same_every_time() -> None:
    weights = {"b": 5, "a": 5, "c": 5, "d": 5}

    assert pack_spaces(weights, 2, 10) == pack_spaces(dict(reversed(list(weights.items()))), 2, 10)


def test_no_spaces_make_no_shards() -> None:
    assert pack_spaces({}, 10, 10) == []


def test_a_shard_task_is_a_function_task_that_is_not_retried() -> None:
    task = shard_task(3, 7, "fn_x", {"action": "compute_shard"})

    assert task["externalId"] == "shard_003"
    assert task["type"] == "function"
    assert task["parameters"] == {"function": {"externalId": "fn_x", "data": {"action": "compute_shard"}}}
    assert task["retries"] == 0
    assert task["onFailure"] == "abortWorkflow"
    assert task["dependsOn"] == []


def test_a_batch_task_is_named_after_the_space_and_what_it_counts() -> None:
    data = {"action": "compute_batch", "space": "sp_big", "kinds": ["assets"], "views": [["m:M:v1", "Widget"]]}

    task = shard_task(1, 4, "fn_x", data)

    assert task["name"] == "Context Quality batch 2 of 4: sp_big (assets, Widget)"
    assert task["retries"] == 0


def items(count: int) -> list[dict[str, Any]]:
    return [{"weight": 10, "data": {"action": "compute_shard", "spaces": {f"s{i}": f"s{i}"}}} for i in range(count)]


def test_finished_tasks_are_numbered_in_order_and_carry_what_every_task_needs() -> None:
    tasks = finish_tasks(items(3), "fn_x", {"runId": "r1", "config": {"a": 1}}, lanes=0)

    assert [t["externalId"] for t in tasks] == ["shard_000", "shard_001", "shard_002"]
    for index, task in enumerate(tasks):
        data = task["parameters"]["function"]["data"]
        assert (data["shard"], data["shardCount"], data["runId"], data["config"]) == (index, 3, "r1", {"a": 1})
        assert task["dependsOn"] == []


def test_the_tasks_can_be_made_to_run_one_after_the_other_or_a_few_at_a_time() -> None:
    one = finish_tasks(items(3), "fn_x", {}, lanes=1)
    two = finish_tasks(items(4), "fn_x", {}, lanes=2)

    assert [t["dependsOn"] for t in one] == [[], [{"externalId": "shard_000"}], [{"externalId": "shard_001"}]]
    assert [t["dependsOn"] for t in two] == [[], [], [{"externalId": "shard_000"}], [{"externalId": "shard_001"}]]


def test_tasks_fit_while_their_data_is_within_the_limit_on_a_task_output() -> None:
    assert tasks_fit(finish_tasks(items(5), "fn_x", {}, 0))
    assert not tasks_fit(finish_tasks(items(3), "fn_x", {"pad": "x" * MAX_TASKS_BYTES}, 0))


def warning(code: str, subject: str | None = None, reason: str | None = None, **details: Any) -> dict[str, Any]:
    out: dict[str, Any] = {"code": code, "message": f"{code} message", **details}
    if subject is not None:
        out["subject"] = subject
    if reason is not None:
        out["reason"] = reason
    return out


def test_warnings_that_name_the_same_thing_are_one_warning() -> None:
    merged = merge_warnings(
        [
            [warning("mapping-empty", "tierByTypeCode"), warning("optional-source-unavailable", "cdf_idm.X")],
            [warning("mapping-empty", "tierByTypeCode"), warning("optional-source-unavailable", "cdf_idm.X")],
        ]
    )

    assert [(w["code"], w["subject"]) for w in merged] == [
        ("mapping-empty", "tierByTypeCode"),
        ("optional-source-unavailable", "cdf_idm.X"),
    ]


def test_the_counts_of_the_shards_add_up_and_the_type_codes_are_pooled_and_bounded() -> None:
    merged = merge_warnings(
        [
            [warning("unmapped-asset-types", untyped=2, unmapped=3, codes=["b", "a"])],
            [warning("unmapped-asset-types", untyped=5, unmapped=1, codes=[f"c{i}" for i in range(12)])],
            [warning("parents-not-loaded", assets=4)],
            [warning("parents-not-loaded", assets=6)],
        ]
    )

    types = next(w for w in merged if w["code"] == "unmapped-asset-types")
    assert (types["untyped"], types["unmapped"]) == (7, 4)
    assert types["codes"] == sorted({"a", "b", *(f"c{i}" for i in range(12))})[:10]
    assert next(w for w in merged if w["code"] == "parents-not-loaded")["assets"] == 10


def test_not_applicable_and_truncation_warnings_add_up_per_metric_and_keep_one_limit() -> None:
    merged = merge_warnings(
        [
            [warning("metric-not-applicable", "freshness", "all-excluded", units=1)],
            [warning("metric-not-applicable", "freshness", "all-excluded", units=2)],
            [warning("metric-not-applicable", "freshness", "no-eligible-entities", units=1)],
            [warning("drilldown-truncated", "orphansByTier", limit=500, dropped=10, units=1)],
            [warning("drilldown-truncated", "orphansByTier", limit=500, dropped=5, units=2)],
        ]
    )

    by_reason = {w["reason"]: w["units"] for w in merged if w["code"] == "metric-not-applicable"}
    assert by_reason == {"all-excluded": 3, "no-eligible-entities": 1}
    cut = next(w for w in merged if w["code"] == "drilldown-truncated")
    assert (cut["limit"], cut["dropped"], cut["units"]) == (500, 15, 3)


def test_a_shards_own_truncation_note_is_not_carried_over() -> None:
    assert merge_warnings([[warning("warnings-truncated", dropped=3)]]) == []
