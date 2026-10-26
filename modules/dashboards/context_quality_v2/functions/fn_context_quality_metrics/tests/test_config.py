"""How the run configuration is built from the defaults and the payload's overrides.

An override replaces the default it names. It is never merged into it, so an explicitly empty mapping clears a rule set.
"""

from typing import Any

import handler
import pytest
from cq.config import DEFAULT_CONFIG, build_config
from cq.metrics import compute
from parity_dataset import build_dataset
from sample_rules import SAMPLE_RULES

MAPPINGS = [
    "tierByTypeCode",
    "defaultOwners",
    "facilityNames",
    "facilitySpaces",
]
EXCLUSION_KEYS = [
    "syntheticTsPrefixes",
    "locationCentricFileCategories",
    "excludedFileExtensions",
    "decommissionedTags",
]


def test_without_overrides_the_result_is_the_defaults() -> None:
    assert build_config(None) == DEFAULT_CONFIG
    assert build_config({}) == DEFAULT_CONFIG


def test_the_result_is_a_copy_so_changing_it_never_changes_the_defaults() -> None:
    cfg = build_config(None)
    cfg["defaultOwners"]["asset-hierarchy"] = "changed"
    cfg["ownerPrefixRules"].append({"prefix": "X-", "ownerId": "leak"})
    cfg["excludeSpaces"].append("leak")
    cfg["exclusions"]["syntheticTsPrefixes"].append("LEAK-")

    fresh = build_config(None)
    assert fresh["defaultOwners"] == {}
    assert fresh["ownerPrefixRules"] == []
    assert "leak" not in fresh["excludeSpaces"]
    assert "LEAK-" not in fresh["exclusions"]["syntheticTsPrefixes"]
    assert DEFAULT_CONFIG["defaultOwners"] == {}


def test_a_non_empty_mapping_replaces_the_default_mapping() -> None:
    cfg = build_config({"tierByTypeCode": {"widget": 1}})

    assert cfg["tierByTypeCode"] == {"widget": 1}


@pytest.mark.parametrize("key", MAPPINGS)
def test_an_explicitly_empty_mapping_clears_the_default(key: str) -> None:
    cfg = build_config({key: {}})

    assert cfg[key] == {}


@pytest.mark.parametrize("key", MAPPINGS)
def test_clearing_one_mapping_leaves_every_other_default_alone(key: str) -> None:
    cfg = build_config({key: {}})

    assert {k: v for k, v in cfg.items() if k != key} == {k: v for k, v in DEFAULT_CONFIG.items() if k != key}


def test_lists_and_scalars_replace_the_defaults() -> None:
    cfg = build_config({"excludeSpaces": ["only_this"], "ownerPrefixRules": [], "batchSize": 10, "freshnessDays": 7})

    assert cfg["excludeSpaces"] == ["only_this"]
    assert cfg["ownerPrefixRules"] == []
    assert (cfg["batchSize"], cfg["freshnessDays"]) == (10, 7)
    assert cfg["maxDepth"] == DEFAULT_CONFIG["maxDepth"]


def test_an_empty_exclusions_mapping_clears_every_exclusion_list() -> None:
    cfg = build_config({"exclusions": {}})

    assert cfg["exclusions"] == {key: [] for key in EXCLUSION_KEYS}


def test_a_partial_exclusions_mapping_replaces_the_whole_mapping_and_unnamed_lists_become_empty() -> None:
    cfg = build_config({"exclusions": {"syntheticTsPrefixes": ["X-"]}})

    assert cfg["exclusions"] == {
        "syntheticTsPrefixes": ["X-"],
        "locationCentricFileCategories": [],
        "excludedFileExtensions": [],
        "decommissionedTags": [],
    }


def test_the_sample_rules_are_available_by_passing_them_explicitly() -> None:
    cfg = build_config(SAMPLE_RULES)

    assert cfg["tierByTypeCode"]["pump"] == 1
    assert cfg["exclusions"]["decommissionedTags"] == ["lifecycle:decommissioned"]
    assert cfg["retiredTag"] == "lifecycle:retired"


def test_the_overrides_are_not_changed_or_shared_with_the_result() -> None:
    overrides: dict[str, Any] = {"tierByTypeCode": {"widget": 1}, "excludeSpaces": ["a"]}

    cfg = build_config(overrides)
    cfg["tierByTypeCode"]["widget"] = 2
    cfg["excludeSpaces"].append("b")

    assert overrides == {"tierByTypeCode": {"widget": 1}, "excludeSpaces": ["a"]}


def test_a_later_layer_replaces_an_earlier_one_key_by_key() -> None:
    """Callers that layer settings (project, then task) combine them per key; a key set in the later layer wins whole."""
    project = {"tierByTypeCode": {"pump": 1, "valve": 2}, "batchSize": 100, "facilityNames": {"sp_a": "a"}}
    task = {"tierByTypeCode": {"valve": 1}, "facilityNames": {}}

    cfg = build_config({**project, **task})

    assert cfg["tierByTypeCode"] == {"valve": 1}
    assert cfg["facilityNames"] == {}
    assert cfg["batchSize"] == 100


class FakeClient:
    config = type("Config", (), {"project": "p"})()


def handle_with(config: dict[str, Any] | None) -> dict[str, Any]:
    seen: dict[str, Any] = {}

    def action(_client: Any, cfg: Any, payload: dict[str, Any]) -> dict[str, Any]:
        seen["cfg"], seen["payload"] = cfg, payload
        return {}

    original = handler.ACTIONS["validate"]
    handler.ACTIONS["validate"] = action
    try:
        handler.handle(
            {"action": "validate", **({"config": config} if config is not None else {}), "dry_run": True}, FakeClient()
        )  # type: ignore[arg-type]
    finally:
        handler.ACTIONS["validate"] = original
    return seen


def test_the_task_payload_config_is_what_the_action_runs_with() -> None:
    seen = handle_with({"tierByTypeCode": {}, "stream": "other"})

    assert seen["cfg"]["tierByTypeCode"] == {}
    assert seen["cfg"]["stream"] == "other"
    assert seen["cfg"]["freshnessDays"] == DEFAULT_CONFIG["freshnessDays"]
    assert seen["payload"]["dry_run"] is True


def test_one_tasks_overrides_never_leak_into_the_next_task() -> None:
    cleared = handle_with({"tierByTypeCode": {}, "exclusions": {}})
    plain = handle_with(None)

    assert cleared["cfg"]["tierByTypeCode"] == {}
    assert plain["cfg"] == DEFAULT_CONFIG


def test_cleared_demo_rules_do_not_come_back_in_the_metrics() -> None:
    data, latest, now = build_dataset()
    cleared = {
        "tierByTypeCode": {},
        "exclusions": {},
        "ownerPrefixRules": [],
        "defaultOwners": {},
    }

    result = compute(data, build_config({**SAMPLE_RULES, **cleared}), latest, now=now)  # type: ignore[arg-type]

    assert {a["tier"] for a in data["assets"]} == {None}  # no type code maps to a tier any more
    assert {a["disc"] for a in data["assets"]} == {None}
    # the synthetic prefixes, location-centric categories, file extensions and decommissioned tags are all cleared
    assert result["exclusions"] == {}
    # neither the PI-COMP3- rule nor the per-dimension owners remain
    assert {key[6] for key in result["agg"]} == {None}


def test_the_default_run_applies_no_demo_rules() -> None:
    data, latest, now = build_dataset()

    result = compute(data, build_config(None), latest, now=now)  # type: ignore[arg-type]

    assert {a["tier"] for a in data["assets"]} == {None}
    assert {key[1] for key in result["exclusions"]} == {  # the synthetic, file and category exclusions still default
        "synthetic-timeseries",
        "location-centric-files",
        "user:file-extension",
    }
    assert not any(a["retired"] for a in data["assets"])


def test_the_sample_rules_applies_the_sample_rules() -> None:
    data, latest, now = build_dataset()

    result = compute(data, build_config(SAMPLE_RULES), latest, now=now)  # type: ignore[arg-type]

    assert {a["tier"] for a in data["assets"]} == {None, 1, 2}
    assert "user:decommissioned" in {key[1] for key in result["exclusions"]}
    assert any(a["retired"] for a in data["assets"])
