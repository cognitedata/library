from typing import Any

import handler
import pytest
from cq.config import DEFAULT_CONFIG


def plan_config(monkeypatch: pytest.MonkeyPatch, data: dict[str, Any]) -> dict[str, Any]:
    """The config the plan action runs with."""
    seen: dict[str, Any] = {}

    def fake_plan(client: Any, cfg: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        seen.update(cfg)
        return {}

    monkeypatch.setitem(handler.ACTIONS, "plan", fake_plan)
    handler.handle({"action": "plan", **data}, object())  # type: ignore[arg-type]
    return seen


def test_planning_replaces_the_shard_settings_for_the_run(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = plan_config(monkeypatch, {"planning": {"shardTargetEntities": 25000, "maxShards": 100}})

    assert cfg["shardTargetEntities"] == 25000
    assert cfg["maxShards"] == 100
    assert cfg["maxConcurrentShards"] == DEFAULT_CONFIG["maxConcurrentShards"]


def test_planning_wins_over_the_deployed_config(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = plan_config(
        monkeypatch, {"config": {"shardTargetEntities": 80000}, "planning": {"shardTargetEntities": 20000}}
    )

    assert cfg["shardTargetEntities"] == 20000


@pytest.mark.parametrize("planning", [None, {}])
def test_no_planning_leaves_the_deployed_config(
    monkeypatch: pytest.MonkeyPatch, planning: dict[str, Any] | None
) -> None:
    cfg = plan_config(monkeypatch, {"config": {"shardTargetEntities": 80000}, "planning": planning})

    assert cfg["shardTargetEntities"] == 80000


def test_planning_is_ignored_by_the_other_actions(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, Any] = {}

    def fake_validate(client: Any, cfg: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        seen.update(cfg)
        return {}

    monkeypatch.setitem(handler.ACTIONS, "validate", fake_validate)
    handler.handle({"action": "validate", "planning": {"shardTargetEntities": 5}}, object())  # type: ignore[arg-type]

    assert seen["shardTargetEntities"] == DEFAULT_CONFIG["shardTargetEntities"]


@pytest.mark.parametrize(
    ("planning", "message"),
    [
        ({"stream": "x"}, "planning can only set"),
        ("25000", "planning must be an object"),
        ({"shardTargetEntities": 0}, "at least 1"),
        ({"shardTargetEntities": "25000"}, "whole number"),
        ({"maxShards": 500}, "at most"),
    ],
)
def test_a_bad_planning_fails_the_plan_before_anything_is_read(
    monkeypatch: pytest.MonkeyPatch, planning: Any, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        plan_config(monkeypatch, {"planning": planning})


def test_the_plan_hands_the_settings_it_used_to_its_tasks(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, Any] = {}

    def fake_plan(client: Any, cfg: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        seen.update(payload["config"])
        return {}

    monkeypatch.setitem(handler.ACTIONS, "plan", fake_plan)
    handler.handle(
        {"action": "plan", "config": {"stream": "s"}, "planning": {"shardTargetEntities": 25000}},
        object(),  # type: ignore[arg-type]
    )

    assert seen == {"stream": "s", "shardTargetEntities": 25000}
