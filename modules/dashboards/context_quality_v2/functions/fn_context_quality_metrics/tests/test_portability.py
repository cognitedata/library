"""Production defaults carry no project-specific rules, the sample rules are explicit, and bad tiers fail early."""

import json
from pathlib import Path
from typing import Any

import handler
import pytest
from cq.config import DEFAULT_CONFIG, build_config
from sample_rules import SAMPLE_RULES
from tiny_data import NOW, asset, data, equipment

HERE = Path(__file__).resolve().parent.parent
PROJECT_MAPPINGS = ("tierByTypeCode", "disciplineByTypeCode", "assetTypeByEquipmentTypeCode", "defaultOwners")
PROJECT_RULES = (*PROJECT_MAPPINGS, "ownerPrefixRules")
DEMO_WORDS = (
    "sap-steward",
    "instrumentation-lead",
    "pid-review-lead",
    "model-3d-lead",
    "PI-COMP3-",
    "centrifugal_pump",
)


# ---------------------------------------------------------------- the production defaults


@pytest.mark.parametrize("key", PROJECT_RULES)
def test_the_production_defaults_have_no_project_specific_rules(key: str) -> None:
    assert not DEFAULT_CONFIG[key]  # type: ignore[literal-required]
    assert not build_config(None)[key]  # type: ignore[literal-required]


def test_the_production_defaults_name_no_demo_discipline_equipment_type_or_owner() -> None:
    text = json.dumps(DEFAULT_CONFIG)

    assert not [word for word in DEMO_WORDS if word in text]
    assert "mechanical" not in text


def test_a_default_run_assigns_no_discipline_and_no_owner() -> None:
    from cq.metrics import compute

    loaded = data(assets=[asset("sp_a", "root"), asset("sp_a", "x", parent=("sp_a", "root"), type_code="pump")])

    result = compute(loaded, build_config(None), {}, now=NOW)  # type: ignore[arg-type]

    assert {a["disc"] for a in loaded["assets"]} == {None}
    assert {key[5] for key in result["agg"]} == {None}  # discipline
    assert {key[6] for key in result["agg"]} == {None}  # owner


# ---------------------------------------------------------------- the demo configuration


def test_the_sample_rules_carries_the_sample_rules() -> None:
    cfg = build_config(SAMPLE_RULES)

    assert cfg["disciplineByTypeCode"]["pump"] == "mechanical"
    assert cfg["disciplineByTypeCode"]["sensor"] == "instrumentation"
    assert cfg["assetTypeByEquipmentTypeCode"]["centrifugal_pump"] == "pump"
    assert cfg["defaultOwners"]["asset-hierarchy"] == "sap-steward"
    assert cfg["ownerPrefixRules"] == [{"prefix": "PI-COMP3-", "ownerId": "instrumentation-lead"}]


def test_the_sample_rules_is_complete_for_every_project_specific_rule() -> None:
    assert all(SAMPLE_RULES[key] for key in PROJECT_RULES)


def test_the_sample_rules_apply_when_the_sample_rules_is_supplied() -> None:
    from cq.metrics import compute

    loaded = data(
        assets=[asset("sp_a", "root"), asset("sp_a", "x", parent=("sp_a", "root"), type_code="pump")],
        equipment_=[equipment("sp_a", "e", ("sp_a", "x"))],
    )

    result = compute(loaded, build_config(SAMPLE_RULES), {}, now=NOW)  # type: ignore[arg-type]

    assert {a["disc"] for a in loaded["assets"]} == {None, "mechanical"}
    assert {key[6] for key in result["agg"] if key[1] == "asset-hierarchy"} == {"sap-steward"}


def test_an_unrelated_task_override_does_not_change_the_sample_rules() -> None:
    cfg = build_config({**SAMPLE_RULES, "batchSize": 10})

    assert cfg["disciplineByTypeCode"] == SAMPLE_RULES["disciplineByTypeCode"]
    assert cfg["batchSize"] == 10


# ---------------------------------------------------------------- explicitly empty stays empty


@pytest.mark.parametrize("key", PROJECT_RULES)
def test_an_explicitly_empty_rule_stays_empty_over_the_sample_rules(key: str) -> None:
    empty: Any = [] if key == "ownerPrefixRules" else {}

    cfg = build_config({**SAMPLE_RULES, key: empty})

    assert cfg[key] == empty  # type: ignore[literal-required]
    assert {k: v for k, v in cfg.items() if k != key} == {
        k: v for k, v in build_config(SAMPLE_RULES).items() if k != key
    }


@pytest.mark.parametrize("key", PROJECT_RULES)
def test_a_task_override_cannot_restore_a_demo_rule_by_omission(key: str) -> None:
    empty: Any = [] if key == "ownerPrefixRules" else {}
    project = {**SAMPLE_RULES, key: empty}

    assert build_config({**project, "batchSize": 5})[key] == empty  # type: ignore[literal-required]
    assert build_config(project)[key] == empty  # type: ignore[literal-required]


# ---------------------------------------------------------------- tier mapping validation


@pytest.mark.parametrize("mapping", [{}, {"pump": 1}, {"pump": 1, "valve": 2, "widget": 3}])
def test_a_valid_tier_mapping_is_accepted(mapping: dict[str, int]) -> None:
    assert build_config({"tierByTypeCode": mapping})["tierByTypeCode"] == mapping


def test_the_sample_tier_mapping_is_valid() -> None:
    assert build_config(SAMPLE_RULES)["tierByTypeCode"] == SAMPLE_RULES["tierByTypeCode"]


@pytest.mark.parametrize("bad", ["1", "tier 1", 0, 4, -1, 1.5, 1.0, None, True, False, [1], {"t": 1}])
def test_an_invalid_tier_value_is_rejected_not_coerced(bad: Any) -> None:
    with pytest.raises(ValueError, match=r"tierByTypeCode.*pump"):
        build_config({"tierByTypeCode": {"ok": 1, "pump": bad}})


@pytest.mark.parametrize("bad", [[("pump", 1)], ["pump"], "pump", 1, None])
def test_a_tier_mapping_that_is_not_a_mapping_is_rejected(bad: Any) -> None:
    with pytest.raises(ValueError, match="tierByTypeCode"):
        build_config({"tierByTypeCode": bad})


@pytest.mark.parametrize("bad_key", ["", 1, None])
def test_a_tier_mapping_key_must_be_a_type_code(bad_key: Any) -> None:
    with pytest.raises(ValueError, match="tierByTypeCode"):
        build_config({"tierByTypeCode": {bad_key: 1}})


def test_the_error_names_the_supported_tiers() -> None:
    with pytest.raises(ValueError, match="1, 2 or 3"):
        build_config({"tierByTypeCode": {"pump": 4}})


class NoCalls:
    """A client that fails on any use, to show nothing was read or written."""

    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"the client was used: {name}")


@pytest.mark.parametrize("action", ["compute_metrics", "validate"])
def test_an_invalid_tier_mapping_fails_before_anything_is_read_or_written(action: str) -> None:
    payload = {"action": action, "config": {"tierByTypeCode": {"pump": "1"}}}

    with pytest.raises(ValueError, match="tierByTypeCode"):
        handler.handle(payload, NoCalls())  # type: ignore[arg-type]


# ---------------------------------------------------------------- the deployed package


def test_no_production_code_can_reach_the_sample_files() -> None:
    names = ("sample_rules", "SAMPLE_RULES", "tiny_data", "parity_dataset")
    sources = [HERE / "handler.py", *sorted((HERE / "cq").glob("*.py"))]

    assert [(path.name, name) for path in sources for name in names if name in path.read_text()] == []
