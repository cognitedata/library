"""Lifecycle exclusions: decommissioned equipment is excluded by the configured tags, and only by those."""

from typing import Any

import pytest
from cq.config import DEFAULT_CONFIG, build_config
from cq.metrics import compute
from sample_rules import SAMPLE_RULES
from tiny_data import NOW, REF, asset, data, equipment

SP = "sp_a"
TAG = "lifecycle:decommissioned"


def run(loaded: dict[str, Any], overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    return compute(loaded, build_config(overrides), {}, now=NOW)  # type: ignore[arg-type]


def tagged() -> dict[str, Any]:
    return data(
        assets=[asset(SP, "a")],
        equipment_=[equipment(SP, "live", (SP, "a")), equipment(SP, "gone", (SP, "a"), tags=(TAG,))],
    )


def excluded(result: dict[str, Any]) -> int:
    return sum(count for key, count in result["exclusions"].items() if key[1] == "user:decommissioned")


def associated(result: dict[str, Any]) -> int:
    return sum(value[0] for key, value in result["agg"].items() if key[0] == "associationRate")


def test_the_defaults_carry_no_decommissioned_tag_and_exclude_nothing() -> None:
    assert DEFAULT_CONFIG["exclusions"].get("decommissionedTags", []) == []
    assert "decommissionedTag" not in DEFAULT_CONFIG
    result = run(tagged())

    assert excluded(result) == 0
    assert associated(result) == 2


def test_the_configured_tags_exclude_equipment() -> None:
    result = run(tagged(), {"exclusions": {"decommissionedTags": [TAG]}})

    assert excluded(result) == 1
    assert associated(result) == 1


def test_any_of_several_configured_tags_excludes() -> None:
    loaded = data(
        assets=[asset(SP, "a")], equipment_=[equipment(SP, "x", tags=("retired-x",)), equipment(SP, "y", tags=(TAG,))]
    )

    result = run(loaded, {"exclusions": {"decommissionedTags": ["retired-x", "other"]}})

    assert excluded(result) == 1


def test_an_explicitly_empty_exclusion_config_leaves_no_hidden_sample_exclusion() -> None:
    result = run(tagged(), {"exclusions": {}})

    assert result["exclusions"] == {}
    assert associated(result) == 2


def test_an_exclusion_config_that_omits_the_tags_excludes_no_equipment_by_tag() -> None:
    cfg = build_config({"exclusions": {"syntheticTsPrefixes": ["X-"]}})

    assert cfg["exclusions"]["decommissionedTags"] == []
    assert excluded(run(tagged(), {"exclusions": {"syntheticTsPrefixes": ["X-"]}})) == 0


def test_the_legacy_scalar_setting_is_rejected_rather_than_silently_ignored_or_overriding() -> None:
    with pytest.raises(ValueError, match="decommissionedTag"):
        build_config({"decommissionedTag": TAG})
    with pytest.raises(ValueError, match="decommissionedTags"):
        build_config({"decommissionedTag": TAG, "exclusions": {}})


def test_the_sample_rules_excludes_the_sample_tag() -> None:
    result = run(tagged(), SAMPLE_RULES)

    assert excluded(result) == 1


def test_the_sample_rules_is_not_changed_by_a_run() -> None:
    before = repr(SAMPLE_RULES)
    cfg = build_config(SAMPLE_RULES)
    cfg["exclusions"]["decommissionedTags"].append("more")

    assert repr(SAMPLE_RULES) == before


def test_decommissioned_equipment_is_left_out_of_the_equipment_type_classification() -> None:
    def classified(overrides: dict[str, Any]) -> list[int]:
        loaded = tagged()
        loaded["class_labels"]["equipmentType"] = {(REF, "type-pump"): "Pump"}
        for e in loaded["equipment"]:
            e["etype"] = (REF, "type-pump")
        rows = run(loaded, overrides)["classifications"]
        return [value[0] for key, value in rows.items() if key[0] == "equipmentType"]

    assert classified({"exclusions": {"decommissionedTags": [TAG]}}) == [1]
    assert classified({"exclusions": {}}) == [2]


def test_the_retired_tag_is_off_unless_configured() -> None:
    loaded = data(
        assets=[
            asset(SP, "root"),
            asset(SP, "old", parent=(SP, "root"), tags=("lifecycle:retired",)),
            asset(SP, "live", parent=(SP, "old")),
        ]
    )

    assert DEFAULT_CONFIG["retiredTag"] == ""
    assert not [i for i in run(loaded)["drilldown"] if i["metric"] == "activeChildrenUnderRetiredParents"]

    configured = run(loaded, {"retiredTag": "lifecycle:retired"})
    assert [i["id"] for i in configured["drilldown"] if i["metric"] == "activeChildrenUnderRetiredParents"] == ["live"]
    demo = run(loaded, SAMPLE_RULES)
    assert [i["id"] for i in demo["drilldown"] if i["metric"] == "activeChildrenUnderRetiredParents"] == ["live"]
